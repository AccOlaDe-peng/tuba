package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"tuba/product/internal/rawevent"
	"tuba/product/internal/uim"
)

// PutStandard writes one validated UIM event to its stable date partition.
func (s *Elasticsearch) PutStandard(ctx context.Context, event map[string]any) error {
	results, err := s.PutStandardBatch(ctx, []map[string]any{event})
	if err != nil {
		return err
	}
	return results[0]
}

// PutStandardBatch writes UIM events with one Elasticsearch bulk request while
// preserving item-level failures for retry or quarantine by the caller.
func (s *Elasticsearch) PutStandardBatch(ctx context.Context, events []map[string]any) ([]error, error) {
	if len(events) == 0 {
		return nil, nil
	}
	results := make([]error, len(events))
	type document struct {
		position          int
		index, id, digest string
		body              []byte
	}
	documents := make([]document, 0, len(events))
	for i, event := range events {
		fields := mapValue(event["event"])
		route := mapValue(mapValue(event["ueba"])["route"])
		organization := mapValue(event["organization"])
		id, domain := stringValue(fields["id"]), stringValue(route["domain"])
		stamp, err := time.Parse(time.RFC3339Nano, stringValue(event["@timestamp"]))
		if err != nil || id == "" || stringValue(organization["id"]) == "" || !validDomain(domain) {
			results[i] = PermanentIndexError{"UIM_EVENT_INVALID", "missing event ID, tenant, timestamp, or supported route"}
			continue
		}
		day := stamp.UTC().Format("2006.01.02")
		index := fmt.Sprintf("tuba-v1-uim-%s-%s-g1-%s", domain, s.Namespace, day)
		alias := "logs-ueba." + domain + "-" + s.Namespace
		if err := s.ensureUIMIndex(ctx, index, alias); err != nil {
			results[i] = err
			continue
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			results[i] = err
			continue
		}
		digest := sha256.Sum256(encoded)
		documents = append(documents, document{position: i, index: index, id: id, digest: hex.EncodeToString(digest[:]), body: encoded})
	}
	if len(documents) == 0 {
		return results, nil
	}
	var body bytes.Buffer
	for _, doc := range documents {
		metadata := map[string]any{"create": map[string]string{"_index": doc.index, "_id": doc.id}}
		if err := json.NewEncoder(&body).Encode(metadata); err != nil {
			return nil, err
		}
		body.Write(doc.body)
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
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			return nil, fmt.Errorf("UIM bulk returned %d: %s", resp.StatusCode, message)
		}
		return nil, PermanentIndexError{"ES_BULK_REJECTED", string(message)}
	}
	var result struct {
		Items []map[string]rawBulkItem `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if len(result.Items) != len(documents) {
		return nil, fmt.Errorf("UIM bulk response item count %d, want %d", len(result.Items), len(documents))
	}
	for i, itemResult := range result.Items {
		item := itemResult["create"]
		doc := documents[i]
		switch {
		case item.Status >= 200 && item.Status < 300:
		case item.Status == http.StatusConflict:
			results[doc.position] = s.verifyStandardDuplicate(ctx, doc.index, doc.id, doc.digest)
		case item.Status == 429 || item.Status >= 500:
			results[doc.position] = fmt.Errorf("UIM document write returned %d: %s", item.Status, item.errorReason())
		default:
			results[doc.position] = PermanentIndexError{"ES_DOCUMENT_REJECTED", item.errorReason()}
		}
	}
	return results, nil
}

func (s *Elasticsearch) PutQuarantine(ctx context.Context, record uim.Quarantine) error {
	if record.ID == "" || record.RawEventID == "" || record.Namespace != s.Namespace || record.OrganizationID == "" || record.OccurredAt.IsZero() {
		return PermanentIndexError{"QUARANTINE_CONTRACT_INVALID", "missing quarantine identity, tenant, namespace, or time"}
	}
	day := record.OccurredAt.UTC().Format("2006.01.02")
	index := fmt.Sprintf("tuba-v1-quarantine-%s-g1-%s", s.Namespace, day)
	alias := "logs-ueba.quarantine-" + s.Namespace
	if _, ok := s.rawIndices.Load(index); !ok {
		body := []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"dynamic":"strict","properties":{"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"namespace":{"type":"keyword"},"raw_event_id":{"type":"keyword"},"release_id":{"type":"keyword"},"stage":{"type":"keyword"},"code":{"type":"keyword"},"reason":{"type":"keyword","ignore_above":1024},"occurred_at":{"type":"date"}}},"aliases":{"` + alias + `":{}}}`)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.URL+"/"+url.PathEscape(index), bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "ApiKey "+s.APIKey)
		resp, err := s.Client.Do(req)
		if err != nil {
			return err
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		status := resp.StatusCode
		resp.Body.Close()
		if status >= 200 && status < 300 || status == http.StatusBadRequest && strings.Contains(string(data), "resource_already_exists_exception") {
			if err := s.ensureIndexAlias(ctx, index, alias); err != nil {
				return err
			}
			s.rawIndices.Store(index, struct{}{})
		} else if status == 429 || status >= 500 {
			return fmt.Errorf("create quarantine index returned %d: %s", status, data)
		} else {
			return PermanentIndexError{"QUARANTINE_INDEX_CREATE_FAILED", string(data)}
		}
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(map[string]any{"create": map[string]string{"_index": index, "_id": record.ID}}); err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	body.Write(encoded)
	body.WriteByte('\n')
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+"/_bulk", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Authorization", "ApiKey "+s.APIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			return fmt.Errorf("quarantine bulk returned %d: %s", resp.StatusCode, message)
		}
		return PermanentIndexError{"QUARANTINE_BULK_REJECTED", string(message)}
	}
	var result struct {
		Items []map[string]rawBulkItem `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if len(result.Items) != 1 {
		return errors.New("quarantine bulk response item count mismatch")
	}
	item := result.Items[0]["create"]
	if item.Status >= 200 && item.Status < 300 || item.Status == http.StatusConflict {
		return nil
	}
	if item.Status == 429 || item.Status >= 500 {
		return fmt.Errorf("quarantine write returned %d: %s", item.Status, item.errorReason())
	}
	return PermanentIndexError{"QUARANTINE_WRITE_REJECTED", item.errorReason()}
}

func (s *Elasticsearch) ensureUIMIndex(ctx context.Context, index, alias string) error {
	if _, ok := s.rawIndices.Load(index); ok {
		return nil
	}
	body, err := uimIndexMappingBody(alias)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.URL+"/"+url.PathEscape(index), bytes.NewReader(body))
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
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := s.ensureIndexAlias(ctx, index, alias); err != nil {
			return err
		}
		s.rawIndices.Store(index, struct{}{})
		return nil
	}
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode == http.StatusBadRequest && strings.Contains(string(data), "resource_already_exists_exception") {
		if err := s.ensureIndexAlias(ctx, index, alias); err != nil {
			return err
		}
		s.rawIndices.Store(index, struct{}{})
		return nil
	}
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return fmt.Errorf("create UIM index returned %d: %s", resp.StatusCode, data)
	}
	return PermanentIndexError{"ES_INDEX_CREATE_FAILED", string(data)}
}

func (s *Elasticsearch) ensureIndexAlias(ctx context.Context, index, alias string) error {
	body, _ := json.Marshal(map[string]any{"actions": []any{map[string]any{"add": map[string]string{"index": index, "alias": alias}}}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+"/_aliases", bytes.NewReader(body))
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
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return fmt.Errorf("update UIM read alias returned %d: %s", resp.StatusCode, message)
	}
	return PermanentIndexError{"ES_ALIAS_UPDATE_FAILED", string(message)}
}

func (s *Elasticsearch) verifyStandardDuplicate(ctx context.Context, index, id, digest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL+"/"+url.PathEscape(index)+"/_doc/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "ApiKey "+s.APIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return errors.New("UIM conflict reported but existing event is missing")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("verify UIM duplicate returned %d", resp.StatusCode)
	}
	var existing struct {
		Source map[string]any `json:"_source"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&existing); err != nil {
		return err
	}
	encoded, err := json.Marshal(existing.Source)
	if err != nil {
		return err
	}
	actual := sha256.Sum256(encoded)
	if hex.EncodeToString(actual[:]) != digest {
		return PermanentIndexError{"EVENT_ID_CONFLICT", "the same event.id arrived with different normalized content"}
	}
	return nil
}

func mapValue(value any) map[string]any { result, _ := value.(map[string]any); return result }
func stringValue(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}
func validDomain(domain string) bool {
	switch domain {
	case "authentication", "session", "iam", "directory", "network", "dns", "web", "tls":
		return true
	}
	return false
}

type PermanentIndexError struct {
	Code, Message string
}

func (e PermanentIndexError) Error() string { return e.Code + ": " + e.Message }

type rawBulkItem struct {
	Status int `json:"status"`
	Error  *struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error"`
}

func (i rawBulkItem) errorReason() string {
	if i.Error == nil {
		return ""
	}
	return i.Error.Type + ": " + i.Error.Reason
}

// PutRaw writes the immutable evidence to a deterministic UTC-day index and updates a read alias.
func (s *Elasticsearch) PutRaw(ctx context.Context, envelope rawevent.Envelope) error {
	day := envelope.ReceivedAt.UTC().Format("2006.01.02")
	index := fmt.Sprintf("tuba-v1-raw-%s-g1-%s", s.Namespace, day)
	alias := "logs-ueba.raw-" + s.Namespace
	if err := s.ensureRawIndex(ctx, index, alias); err != nil {
		return err
	}

	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	if err := enc.Encode(map[string]any{"create": map[string]string{"_index": index, "_id": envelope.RawEventID}}); err != nil {
		return err
	}
	// delivery_position is receipt/transport metadata persisted in PostgreSQL.
	// Older retained Raw indices use dynamic=strict without this optional field;
	// keep the ES document backward compatible while preserving the complete
	// envelope in the Raw Kafka topic and receipt store.
	esEnvelope := envelope
	esEnvelope.DeliveryPosition = ""
	raw, err := rawevent.Marshal(esEnvelope)
	if err != nil {
		return err
	}
	body.Write(raw)
	body.WriteByte('\n')
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+"/_bulk", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Authorization", "ApiKey "+s.APIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			return fmt.Errorf("raw bulk returned %d: %s", resp.StatusCode, message)
		}
		return PermanentIndexError{"ES_BULK_REJECTED", string(message)}
	}
	var result struct {
		Items []map[string]rawBulkItem `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if len(result.Items) != 1 {
		return errors.New("raw bulk response item count mismatch")
	}
	item := result.Items[0]["create"]
	if item.Status >= 200 && item.Status < 300 {
		return nil
	}
	if item.Status == http.StatusConflict {
		return s.verifyRawDuplicate(ctx, index, envelope.RawEventID, envelope.PayloadHash)
	}
	if item.Status == 429 || item.Status >= 500 {
		return fmt.Errorf("raw document write returned %d: %s", item.Status, item.errorReason())
	}
	return PermanentIndexError{"ES_DOCUMENT_REJECTED", item.errorReason()}
}

func (s *Elasticsearch) ensureRawIndex(ctx context.Context, index, alias string) error {
	if _, ok := s.rawIndices.Load(index); ok {
		return nil
	}
	body := []byte(`{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"dynamic":"strict","properties":{"schema_version":{"type":"keyword"},"raw_event_id":{"type":"keyword"},"organization":{"properties":{"id":{"type":"keyword"}}},"namespace":{"type":"keyword"},"source_instance_id":{"type":"keyword"},"source_context_id":{"type":"keyword"},"source_position":{"type":"keyword"},"delivery_position":{"type":"keyword"},"source_epoch":{"type":"keyword"},"vendor":{"properties":{"name":{"type":"keyword"},"product":{"type":"keyword"},"dataset":{"type":"keyword"}}},"received_at":{"type":"date"},"release_id":{"type":"keyword"},"payload_hash":{"type":"keyword"},"payload":{"type":"flattened"},"encoding":{"type":"keyword"}}},"aliases":{"` + alias + `":{}}}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.URL+"/"+url.PathEscape(index), bytes.NewReader(body))
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
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := s.ensureIndexAlias(ctx, index, alias); err != nil {
			return err
		}
		s.rawIndices.Store(index, struct{}{})
		return nil
	}
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode == http.StatusBadRequest && strings.Contains(string(data), "resource_already_exists_exception") {
		if err := s.ensureIndexAlias(ctx, index, alias); err != nil {
			return err
		}
		s.rawIndices.Store(index, struct{}{})
		return nil
	}
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return fmt.Errorf("create raw index returned %d: %s", resp.StatusCode, data)
	}
	return PermanentIndexError{"ES_INDEX_CREATE_FAILED", string(data)}
}

func (s *Elasticsearch) verifyRawDuplicate(ctx context.Context, index, id, payloadHash string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL+"/"+url.PathEscape(index)+"/_doc/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "ApiKey "+s.APIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return errors.New("raw document conflict reported but document is missing")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("verify duplicate returned %d", resp.StatusCode)
	}
	var existing struct {
		Source struct {
			PayloadHash string `json:"payload_hash"`
		} `json:"_source"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&existing); err != nil {
		return err
	}
	if existing.Source.PayloadHash != payloadHash {
		return PermanentIndexError{"RAW_ID_CONFLICT", "the same raw_event_id arrived with a different payload hash"}
	}
	return nil
}
