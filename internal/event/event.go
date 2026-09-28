package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// Authentication is the first supported standard event. Nested field names match ECS/CIM.
type Authentication struct {
	Timestamp    string `json:"@timestamp"`
	Organization struct {
		ID string `json:"id"`
	} `json:"organization"`
	Event struct {
		ID       string   `json:"id"`
		Kind     string   `json:"kind"`
		Category []string `json:"category"`
		Action   string   `json:"action"`
		Outcome  string   `json:"outcome"`
		Dataset  string   `json:"dataset"`
	} `json:"event"`
	Vendor struct {
		Dataset string `json:"dataset"`
	} `json:"vendor"`
	UEBA struct {
		Schema struct {
			Version string `json:"version"`
		} `json:"schema"`
		Quality struct {
			Status string `json:"status"`
		} `json:"quality"`
		Route struct {
			Domain string `json:"domain"`
		} `json:"route"`
	} `json:"ueba"`
	User struct {
		ID   string `json:"id,omitempty"`
		Name string `json:"name,omitempty"`
	} `json:"user,omitempty"`
}

func Parse(raw []byte, organization string) (Authentication, error) {
	var e Authentication
	if len(raw) == 0 || len(raw) > 1<<20 {
		return e, errors.New("event size must be 1 byte to 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&e); err != nil {
		return e, fmt.Errorf("invalid JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return e, errors.New("trailing data after event")
	}
	if _, err := time.Parse(time.RFC3339Nano, e.Timestamp); err != nil {
		return e, errors.New("@timestamp must be RFC3339")
	}
	if e.Organization.ID != organization {
		return e, errors.New("organization does not match service identity")
	}
	if e.Event.ID == "" || len(e.Event.ID) > 256 || e.Event.Kind != "event" || e.Event.Action == "" || e.Event.Dataset == "" || e.Vendor.Dataset == "" || e.UEBA.Schema.Version == "" {
		return e, errors.New("required CIM fields are missing")
	}
	if e.Event.Outcome != "success" && e.Event.Outcome != "failure" && e.Event.Outcome != "unknown" {
		return e, errors.New("invalid event outcome")
	}
	if e.UEBA.Quality.Status != "qualified" && e.UEBA.Quality.Status != "partial" && e.UEBA.Quality.Status != "invalid" && e.UEBA.Quality.Status != "unsupported" {
		return e, errors.New("invalid quality status")
	}
	if e.UEBA.Route.Domain != "authentication" {
		return e, errors.New("unsupported route domain")
	}
	found := false
	for _, c := range e.Event.Category {
		if c == "authentication" {
			found = true
		}
	}
	if !found {
		return e, errors.New("event category must include authentication")
	}
	return e, nil
}
