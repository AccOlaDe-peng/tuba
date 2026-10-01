package component

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"tuba/product/internal/launcher"
)

// LauncherProcessManager drives a managed component that runs as a service in
// a launcher manifest, using the launcher's per-service controls (persistent
// stop marker / start). Process supervision — crash backoff, graceful stop,
// restart counters — stays entirely with the launcher.
type LauncherProcessManager struct {
	ManifestPath string
	Service      string
	Timeout      time.Duration
}

func (m *LauncherProcessManager) timeout() time.Duration {
	if m.Timeout > 0 {
		return m.Timeout
	}
	return 30 * time.Second
}

func (m *LauncherProcessManager) StopService(context.Context) error {
	return launcher.StopServices(m.ManifestPath, []string{m.Service}, m.timeout())
}

func (m *LauncherProcessManager) StartService(context.Context) error {
	return launcher.StartServices(m.ManifestPath, []string{m.Service}, m.timeout())
}

func (m *LauncherProcessManager) Alive(context.Context) (bool, error) {
	return launcher.ServiceAlive(m.ManifestPath, m.Service)
}

// HTTPHealthChecker polls a readiness endpoint (the /health/ready convention
// shared by the Go workers) and requires HTTP 200.
type HTTPHealthChecker struct {
	URL    string
	Client *http.Client
}

func (c *HTTPHealthChecker) Ready(ctx context.Context) error {
	client := c.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("readiness probe returned %s", resp.Status)
	}
	return nil
}
