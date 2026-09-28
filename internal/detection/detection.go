package detection

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"tuba/product/internal/event"
)

type Document struct {
	ID     string         `json:"_id"`
	Source map[string]any `json:"_source"`
}

type priorFailure struct {
	at time.Time
	id string
}
type timedEvent struct {
	event event.Authentication
	at    time.Time
}

// FailureThenSuccess produces stable IDs for a complete, bounded event-time set.
func FailureThenSuccess(events []event.Authentication, organization string, threshold int, lookback time.Duration) ([]Document, error) {
	if threshold < 1 || lookback <= 0 {
		return nil, fmt.Errorf("threshold and lookback must be positive")
	}
	ordered := make([]timedEvent, 0, len(events))
	for _, e := range events {
		at, err := time.Parse(time.RFC3339Nano, e.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("invalid event timestamp: %w", err)
		}
		ordered = append(ordered, timedEvent{event: e, at: at.UTC()})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].at.Equal(ordered[j].at) {
			return ordered[i].event.Event.ID < ordered[j].event.Event.ID
		}
		return ordered[i].at.Before(ordered[j].at)
	})
	failures := map[string][]priorFailure{}
	seen := map[string]bool{}
	output := map[string]Document{}
	for _, item := range ordered {
		e, at := item.event, item.at
		if e.Organization.ID != organization {
			return nil, fmt.Errorf("cross-tenant event in analysis input")
		}
		if e.UEBA.Quality.Status != "qualified" && e.UEBA.Quality.Status != "partial" {
			continue
		}
		if seen[e.Event.ID] || e.User.ID == "" {
			continue
		}
		seen[e.Event.ID] = true
		prior := failures[e.User.ID]
		start := 0
		for start < len(prior) && prior[start].at.Before(at.Add(-lookback)) {
			start++
		}
		prior = prior[start:]
		switch e.Event.Outcome {
		case "failure":
			failures[e.User.ID] = append(prior, priorFailure{at, e.Event.ID})
		case "success":
			failures[e.User.ID] = prior
			if len(prior) < threshold {
				continue
			}
			window := at.Truncate(30 * time.Minute)
			key := fmt.Sprintf("%s|%s|%s|auth.failure-then-success@1.0.0", organization, e.User.ID, window.Format(time.RFC3339))
			hash := sha256.Sum256([]byte(key))
			id := "anom:" + hex.EncodeToString(hash[:])
			if _, exists := output[id]; exists {
				continue
			}
			evidence := make([]string, 0, len(prior)+1)
			for _, failure := range prior {
				evidence = append(evidence, failure.id)
			}
			evidence = append(evidence, e.Event.ID)
			sort.Strings(evidence)
			output[id] = Document{ID: id, Source: map[string]any{
				"@timestamp":   at.Format(time.RFC3339Nano),
				"organization": map[string]any{"id": organization},
				"entity":       map[string]any{"id": e.User.ID, "type": "account"},
				"anomaly":      map[string]any{"id": id, "type": "auth.failure-then-success", "severity": "high", "status": "open", "score": 1.0},
				"evidence":     map[string]any{"event_ids": evidence, "count": len(evidence)},
				"detection":    map[string]any{"rule_id": "auth.failure-then-success", "rule_version": "1.0.0"},
				"explanation": map[string]any{"reason_codes": []string{"AUTH_FAILURE_BURST_THEN_SUCCESS"},
					"summary": fmt.Sprintf("%d failed logins followed by a successful login within 30 minutes", len(prior))},
			}}
		default:
			failures[e.User.ID] = prior
		}
	}
	ids := make([]string, 0, len(output))
	for id := range output {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := make([]Document, 0, len(ids))
	for _, id := range ids {
		result = append(result, output[id])
	}
	return result, nil
}
