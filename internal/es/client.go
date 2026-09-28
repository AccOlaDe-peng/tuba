package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	BaseURL, APIKey string
	HTTP            *http.Client
}

func New(baseURL, apiKey string) (*Client, error) {
	if !(strings.HasPrefix(baseURL, "http://") || strings.HasPrefix(baseURL, "https://")) || apiKey == "" {
		return nil, fmt.Errorf("ES_URL and ES_API_KEY are required")
	}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), APIKey: apiKey, HTTP: &http.Client{Timeout: 20 * time.Second}}, nil
}

func (c *Client) Do(ctx context.Context, method, path string, body any, result any) (int, error) {
	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		payload = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, payload)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "ApiKey "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.NewDecoder(io.LimitReader(resp.Body, 10<<20)).Decode(result); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

func EscapeID(id string) string { return url.PathEscape(id) }

type Hit struct {
	ID          string          `json:"_id"`
	Source      json.RawMessage `json:"_source"`
	SeqNo       int64           `json:"_seq_no"`
	PrimaryTerm int64           `json:"_primary_term"`
	Sort        []any           `json:"sort"`
}
type SearchResult struct {
	Hits struct {
		Total struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		Hits []Hit `json:"hits"`
	} `json:"hits"`
}
