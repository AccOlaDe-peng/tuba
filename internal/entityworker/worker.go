// Package entityworker consumes UIM domain events, attributes every event
// role against the entity registry and publishes one attributed-event
// contribution message per role, partitioned by entity.id. Unresolved and
// ambiguous attributions are published under a dedicated unresolved key
// namespace — never dropped (E04).
package entityworker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/entity"
)

type Consumer interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

type Writer interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

// Processor attributes one UIM event (and may persist the projection). It
// returns one RoleAttribution per event role, including unresolved and
// ambiguous outcomes.
type Processor interface {
	Attribute(ctx context.Context, organizationID string, event map[string]any) ([]entity.RoleAttribution, error)
}

// AttributionProcessor is the production Processor: attribute every role of
// the UIM event and store the projection rows idempotently.
type AttributionProcessor struct {
	Attributor                *entity.Attributor
	AccountSpace, DeviceSpace string
}

func (p AttributionProcessor) Attribute(ctx context.Context, organizationID string, event map[string]any) ([]entity.RoleAttribution, error) {
	eventID, at, roles, err := entity.RolesFromUIM(event, p.AccountSpace, p.DeviceSpace)
	if err != nil {
		return nil, err
	}
	if len(roles) == 0 {
		return nil, fmt.Errorf("%w: event %s carries no attributable role", entity.ErrNoRoles, eventID)
	}
	attrs, err := p.Attributor.Attribute(ctx, organizationID, eventID, at, roles)
	if err != nil {
		return nil, err
	}
	if err := p.Attributor.Store(ctx, organizationID, at, attrs); err != nil {
		return nil, fmt.Errorf("store attributions: %w", err)
	}
	return attrs, nil
}

// EventSummary extracts the minimal detection semantics of one UIM event
// (outcome, quality status, host id, source ip) for the F08 analysis
// pipeline, which reconstructs its detection input from attributed-event
// contributions. Keys with no value are omitted; nil means the event carries
// none of them.
func EventSummary(event map[string]any) map[string]any {
	summary := map[string]any{}
	if meta, ok := event["event"].(map[string]any); ok {
		if outcome, ok := meta["outcome"].(string); ok && outcome != "" {
			summary["outcome"] = outcome
		}
	}
	if ueba, ok := event["ueba"].(map[string]any); ok {
		if quality, ok := ueba["quality"].(map[string]any); ok {
			if status, ok := quality["status"].(string); ok && status != "" {
				summary["quality"] = status
			}
		}
	}
	if host, ok := event["host"].(map[string]any); ok {
		if id, ok := host["id"].(string); ok && id != "" {
			summary["host_id"] = id
		}
	}
	if source, ok := event["source"].(map[string]any); ok {
		if ip, ok := source["ip"].(string); ok && ip != "" {
			summary["source_ip"] = ip
		}
	}
	if len(summary) == 0 {
		return nil
	}
	return summary
}

// Worker consumes one UIM domain topic and publishes attributed-event
// contributions. The input offset is committed only after every contribution
// of the event has been accepted by the output writer, so a crash redelivers
// and republishes (deduplicated downstream by attribution_id).
type Worker struct {
	Organization, Domain string
	Consumer             Consumer
	Processor            Processor
	Output               Writer
}

func (w Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		message, err := w.Consumer.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		var event map[string]any
		if err := json.Unmarshal(message.Value, &event); err != nil {
			return fmt.Errorf("decode UIM event: %w", err)
		}
		attrs, err := w.Processor.Attribute(ctx, w.Organization, event)
		if err != nil {
			return err
		}
		eventMeta, _ := event["event"].(map[string]any)
		eventID, _ := eventMeta["id"].(string)
		stamp, _ := event["@timestamp"].(string)
		eventTime, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			return fmt.Errorf("event %s @timestamp invalid: %w", eventID, err)
		}
		contributions, err := entity.Contributions(w.Organization, w.Domain, eventID, eventTime, attrs)
		if err != nil {
			return err
		}
		// Embed the detection event summary on outbound messages only (stored
		// attribution rows were already persisted without it): the F08
		// analysis pipeline rebuilds its detection input from this carrier.
		if summary := EventSummary(event); summary != nil {
			for i := range contributions {
				contributions[i].Evidence.Event = summary
			}
		}
		messages := make([]kafka.Message, 0, len(contributions))
		for _, c := range contributions {
			body, err := json.Marshal(c)
			if err != nil {
				return err
			}
			messages = append(messages, kafka.Message{
				Key:   []byte(c.PartitionKey),
				Value: body,
				Headers: []kafka.Header{
					{Key: "schema-version", Value: []byte(entity.AttributedSchemaVersion)},
					{Key: "event-id", Value: []byte(c.EventID)},
					{Key: "attribution-id", Value: []byte(c.AttributionID)},
					{Key: "role", Value: []byte(c.Role)},
					{Key: "state", Value: []byte(c.State)},
				},
			})
		}
		if err := w.Output.WriteMessages(ctx, messages...); err != nil {
			return fmt.Errorf("publish attributed contributions: %w", err)
		}
		if err := w.Consumer.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit UIM event offset: %w", err)
		}
	}
	return nil
}
