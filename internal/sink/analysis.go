package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"tuba/product/internal/analysis"
)

func (s *Elasticsearch) PutAnalysis(ctx context.Context, result analysis.Result) error {
	index := "ueba-anomalies-" + s.Namespace
	endpoint := s.URL + "/" + index + "/_doc/" + url.PathEscape(result.ResultID) + "?op_type=index"
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(result.Document))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "ApiKey "+s.APIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("elasticsearch returned %d: %s", resp.StatusCode, body)
	}
	return nil
}
