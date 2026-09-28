package normalizer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"tuba/product/internal/rawevent"
	"tuba/product/internal/uim"
)

type Consumer interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

type Writer interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

type Worker struct {
	Organization, Namespace string
	Consumer                Consumer
	DomainWriters           map[string]Writer
	QuarantineWriter        Writer
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
		var envelope rawevent.Envelope
		if err := json.Unmarshal(message.Value, &envelope); err != nil {
			if err := w.quarantine(ctx, envelope, "DIP", "RAW_ENVELOPE_INVALID", err.Error()); err != nil {
				return err
			}
		} else if err := rawevent.Validate(envelope, w.Organization, w.Namespace); err != nil {
			if err := w.quarantine(ctx, envelope, "DIP", code(err), err.Error()); err != nil {
				return err
			}
		} else {
			event, err := uim.Normalize(envelope)
			if err != nil {
				stage := "DIP"
				if strings.HasPrefix(err.Error(), "UIM_") {
					stage = "UIM"
				}
				if errors.Is(err, uim.ErrUnsupported) {
					if err := w.quarantine(ctx, envelope, stage, "EVENT_UNSUPPORTED", "no active DIP mapping for this event"); err != nil {
						return err
					}
				} else if err := w.quarantine(ctx, envelope, stage, code(err), err.Error()); err != nil {
					return err
				}
			} else {
				if err := w.publish(ctx, envelope, event); err != nil {
					return err
				}
			}
		}
		if err := w.Consumer.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit raw event offset: %w", err)
		}
	}
	return nil
}

func (w Worker) publish(ctx context.Context, source rawevent.Envelope, event map[string]any) error {
	ueba := event["ueba"].(map[string]any)
	route := ueba["route"].(map[string]any)
	domain := fmt.Sprint(route["domain"])
	writer := w.DomainWriters[domain]
	if writer == nil {
		return fmt.Errorf("no Kafka output configured for UIM domain %q", domain)
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	eventFields := event["event"].(map[string]any)
	key := source.Organization.ID + ":" + fmt.Sprint(eventFields["id"])
	message := kafka.Message{Key: []byte(key), Value: body, Headers: []kafka.Header{{Key: "schema-version", Value: []byte("1.0.0")}, {Key: "release-id", Value: []byte(source.ReleaseID)}, {Key: "raw-event-id", Value: []byte(source.RawEventID)}}}
	if err := writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish normalized event: %w", err)
	}
	return nil
}

func (w Worker) quarantine(ctx context.Context, source rawevent.Envelope, stage, code, reason string) error {
	if w.QuarantineWriter == nil {
		return errors.New("quarantine output is not configured")
	}
	identity := source.RawEventID + "|" + source.ReleaseID + "|" + stage + "|" + code
	digest := sha256.Sum256([]byte(identity))
	occurred := source.ReceivedAt
	if occurred.IsZero() {
		occurred = time.Now().UTC()
	}
	record := uim.Quarantine{ID: "qua:" + hex.EncodeToString(digest[:]), OrganizationID: source.Organization.ID, Namespace: source.Namespace, RawEventID: source.RawEventID, ReleaseID: source.ReleaseID, Stage: stage, Code: code, Reason: bounded(reason, 512), OccurredAt: occurred.UTC()}
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	key := source.Organization.ID + ":" + source.RawEventID + ":" + code
	if err := w.QuarantineWriter.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: body}); err != nil {
		return fmt.Errorf("publish quarantine event: %w", err)
	}
	return nil
}

func code(err error) string {
	name := strings.SplitN(err.Error(), ":", 2)[0]
	name = strings.TrimSpace(name)
	if len(name) > 64 {
		name = name[:64]
	}
	if name == "" {
		return "NORMALIZATION_FAILED"
	}
	return strings.ToUpper(strings.ReplaceAll(name, " ", "_"))
}

func bounded(value string, max int) string {
	if len(value) > max {
		return value[:max]
	}
	return value
}
