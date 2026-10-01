package control

import (
	"errors"
	"testing"
)

// validateCollectorHeartbeat bounds the component status section (COL-09).
func TestHeartbeatComponentValidation(t *testing.T) {
	valid := CollectorHeartbeat{
		Version: "1.0.0", State: "running", Sources: []CollectorSourceStatus{},
		Components: []CollectorComponentStatus{{
			Component: "filebeat", Version: "8.19.0", Phase: "idle", State: "running", Restarts: 2,
		}},
	}
	if err := validateCollectorHeartbeat(valid); err != nil {
		t.Fatalf("valid heartbeat with components was rejected: %v", err)
	}

	cases := map[string]CollectorComponentStatus{
		"empty component":  {Component: "", Version: "8.19.0", State: "running"},
		"unknown phase":    {Component: "filebeat", Version: "8.19.0", Phase: "exploding", State: "running"},
		"unknown state":    {Component: "filebeat", Version: "8.19.0", State: "sleeping"},
		"negative restart": {Component: "filebeat", Version: "8.19.0", State: "running", Restarts: -1},
	}
	for name, componentStatus := range cases {
		beat := valid
		beat.Components = []CollectorComponentStatus{componentStatus}
		if err := validateCollectorHeartbeat(beat); !errors.Is(err, ErrInvalidCollectorHeartbeat) {
			t.Fatalf("%s: err = %v, want ErrInvalidCollectorHeartbeat", name, err)
		}
	}

	duplicated := valid
	duplicated.Components = []CollectorComponentStatus{
		{Component: "filebeat", Version: "8.19.0", State: "running"},
		{Component: "filebeat", Version: "8.19.0", State: "running"},
	}
	if err := validateCollectorHeartbeat(duplicated); !errors.Is(err, ErrInvalidCollectorHeartbeat) {
		t.Fatalf("duplicate component: err = %v, want ErrInvalidCollectorHeartbeat", err)
	}
}
