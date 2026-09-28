package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"tuba/product/internal/indexing"
)

type Elasticsearch struct {
	URL, APIKey, Namespace string
	Client                 *http.Client
	rawIndices             sync.Map
}

func New(baseURL, apiKey, namespace string) *Elasticsearch {
	return &Elasticsearch{URL: strings.TrimRight(baseURL, "/"), APIKey: apiKey, Namespace: namespace, Client: &http.Client{Timeout: 30 * time.Second}}
}
func (s *Elasticsearch) PutBatch(ctx context.Context, docs []indexing.Document) ([]indexing.Result, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	index := "logs-ueba.authentication-" + s.Namespace
	for _, d := range docs {
		if err := enc.Encode(map[string]any{"create": map[string]string{"_index": index, "_id": d.Event.Event.ID}}); err != nil {
			return nil, err
		}
		body.Write(d.Raw)
		body.WriteByte('\n')
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+"/_bulk", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Authorization", "ApiKey "+s.APIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("elasticsearch bulk returned %d: %s", resp.StatusCode, b)
	}
	var out struct {
		Items []map[string]struct {
			Status int `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode bulk response: %w", err)
	}
	if len(out.Items) != len(docs) {
		return nil, fmt.Errorf("bulk response item count %d, want %d", len(out.Items), len(docs))
	}
	results := make([]indexing.Result, len(docs))
	for i, item := range out.Items {
		v := item["create"]
		r := indexing.Result{Status: v.Status}
		if v.Status >= 200 && v.Status < 300 || v.Status == 409 {
			results[i] = r
			continue
		}
		if v.Error != nil {
			r.Code = v.Error.Type
			r.Message = v.Error.Reason
		}
		r.Retryable = v.Status == 429 || v.Status == 502 || v.Status == 503 || v.Status == 504 || v.Status >= 500
		results[i] = r
	}
	return results, nil
}
