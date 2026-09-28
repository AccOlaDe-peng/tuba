package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Credential struct {
	APIKey    string
	ContextID string
}

type Sender struct {
	Store    *Store
	Endpoint string
	Sources  map[string]Credential
	Client   *http.Client
}

func (s *Sender) Run(ctx context.Context) {
	if s.Client == nil {
		s.Client = &http.Client{Timeout: 15 * time.Second}
	}
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		item, err := s.Store.Next(ctx)
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
			continue
		}
		if err != nil {
			continue
		}
		cred, ok := s.Sources[item.SourceID]
		if !ok {
			_ = s.Store.Retry(ctx, item.ID, item.Attempts+1, time.Minute, errors.New("source credentials unavailable"))
			continue
		}
		receipt, retryAfter, permanent, err := s.send(ctx, item, cred)
		if err == nil {
			_ = s.Store.Ack(ctx, item.ID, receipt)
			continue
		}
		if permanent {
			_ = s.Store.Reject(ctx, item.ID, err)
			continue
		}
		delay := retryAfter
		if delay <= 0 {
			delay = time.Second * time.Duration(math.Min(300, math.Pow(2, float64(item.Attempts))))
		}
		_ = s.Store.Retry(ctx, item.ID, item.Attempts+1, delay, err)
	}
}

func (s *Sender) send(ctx context.Context, item Item, cred Credential) (string, time.Duration, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Endpoint, bytes.NewReader(item.Payload))
	if err != nil {
		return "", 0, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", cred.APIKey)
	req.Header.Set("X-Source-Position", item.Position)
	req.Header.Set("X-Source-Context", item.ContextID)
	resp, err := s.Client.Do(req)
	if err != nil {
		return "", 0, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted {
		var r struct {
			ReceiptID  string `json:"receipt_id"`
			RawEventID string `json:"raw_event_id"`
			Status     string `json:"status"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&r); err != nil {
			return "", 0, false, err
		}
		if r.Status != "accepted" || r.ReceiptID == "" || r.ReceiptID != r.RawEventID {
			return "", 0, false, errors.New("invalid ingest receipt")
		}
		return r.ReceiptID, 0, false, nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	e := fmt.Errorf("ingest returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return "", parseRetryAfter(resp.Header.Get("Retry-After")), false, e
	}
	return "", 0, true, e
}

func parseRetryAfter(v string) time.Duration {
	n, e := strconv.Atoi(v)
	if e == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	if t, e := http.ParseTime(v); e == nil && time.Until(t) > 0 {
		return time.Until(t)
	}
	return 0
}
