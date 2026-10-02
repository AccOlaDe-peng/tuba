package sink

// E05 entity/attribution/relation projections.
//
// Each projection family is written in two shapes:
//
//   - state projection (read-heavy latest state): ueba-entities-<namespace>
//     and ueba-relations-<namespace>, keyed by entity.id / relation.id and
//     guarded by Elasticsearch external versioning — the monotonically
//     increasing revision carried by every projection. A write whose
//     revision is lower than the stored one is rejected by ES with a
//     version conflict and reported as StaleRevisionError: an out-of-order
//     or late write never overwrites a newer value. A replayed write with
//     the same revision and identical content is idempotently accepted.
//   - history projection (traceability): append-only dated indices
//     tuba-v1-entity-history-*, tuba-v1-relation-history-* and
//     tuba-v1-attributions-* (read aliases logs-ueba.entity-history-*,
//     logs-ueba.relation-history-*, logs-ueba.attributions-*) keyed by
//     "<id>:<revision>" / attribution_id, so every revision of an entity or
//     relation and every attribution decision stays readable with its
//     source event.id / raw_event_id references.
//
// Attribution decisions are immutable and event-keyed (E02/E04): they have
// no state projection, only the history index.

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
)

// StaleRevisionError reports that a projection write carried a revision
// older than the one already stored. The write was NOT applied: the newer
// value stays authoritative. Callers consuming an at-least-once stream
// should drop the record (commit the offset) rather than retry — retrying
// can never succeed. It mirrors the revision/retracted semantics of F06:
// order is decided by revision, not by arrival.
type StaleRevisionError struct {
	Index    string
	ID       string
	Existing int64
	Incoming int64
}

func (e StaleRevisionError) Error() string {
	return fmt.Sprintf("REVISION_STALE: %s/%s has revision %d, incoming revision %d not applied", e.Index, e.ID, e.Existing, e.Incoming)
}

// IsStaleRevision reports whether err is a StaleRevisionError.
func IsStaleRevision(err error) bool {
	var stale StaleRevisionError
	return errors.As(err, &stale)
}

// SourceRef carries the traceability references of every projection: the
// event that evidenced this fact and, when known, the raw envelope it was
// normalized from. EventID is required; RawEventID is optional (registry
// maintenance writes have no raw event).
type SourceRef struct {
	EventID    string `json:"event_id"`
	RawEventID string `json:"raw_event_id,omitempty"`
}

// EntityProjection is the entity master record: canonical identity, type,
// identity space (authority), occurrence validity, the registry revision,
// and the most recent attribution that touched this entity.
type EntityProjection struct {
	OrganizationID string
	EntityID       string
	EntityType     string
	Authority      string
	CanonicalKey   string
	Strength       string
	// Revision is the entities table revision; monotonically increasing per
	// entity. It is the ES external version of the state projection.
	Revision  int64
	Document  json.RawMessage
	ValidFrom time.Time
	ValidTo   *time.Time
	// LastAttributionID/LastAttributionRevision link the master record to
	// the newest attribution decision that resolved to this entity.
	LastAttributionID       string
	LastAttributionRevision int64
	Source                  SourceRef
	// UpdatedAt is the projection write time; it dates the history partition.
	UpdatedAt time.Time
}

// RelationProjection is one temporal relation edge with its validity
// interval and lifecycle revision.
type RelationProjection struct {
	OrganizationID string
	RelationID     string
	FromEntityID   string
	RelationType   string
	ToEntityID     string
	RuleVersion    string
	Confidence     float64
	// Revision increases on every lifecycle change of the relation
	// (assert=1, close/transfer=2, ...). It is the ES external version.
	Revision  int64
	ValidFrom time.Time
	ValidTo   *time.Time
	Evidence  json.RawMessage
	Source    SourceRef
	UpdatedAt time.Time
}

// AttributionProjection is one immutable event-role attribution decision
// (contracts/events/attributed-event/1) with its evidence and source
// references. Written to the attribution history index only.
type AttributionProjection struct {
	OrganizationID string
	AttributionID  string
	EventID        string
	RawEventID     string
	EventTime      time.Time
	Domain         string
	Role           string
	State          string
	EntityID       string
	Confidence     float64
	RuleVersion    string
	Reason         string
	PartitionKey   string
	ValidFrom      time.Time
	ValidTo        *time.Time
	Evidence       json.RawMessage
}

func entityStateIndex(namespace string) string   { return "ueba-entities-" + namespace }
func relationStateIndex(namespace string) string { return "ueba-relations-" + namespace }

// contentHash digests the canonical projection body (sans the hash field
// itself) so same-revision replays can be told apart from conflicts.
func contentHash(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// entityBody renders the state/history document. The hash is computed over
// the document without content_hash, then embedded.
func (p EntityProjection) body() ([]byte, error) {
	doc := map[string]any{
		"@timestamp":          p.UpdatedAt.UTC().Format(time.RFC3339Nano),
		"organization":        map[string]any{"id": p.OrganizationID},
		"entity":              map[string]any{"id": p.EntityID, "type": p.EntityType, "authority": p.Authority, "canonical_key": p.CanonicalKey, "identity_strength": p.Strength, "revision": p.Revision},
		"valid_from":          p.ValidFrom.UTC().Format(time.RFC3339Nano),
		"last_attribution":    map[string]any{"id": p.LastAttributionID, "revision": p.LastAttributionRevision},
		"source":              map[string]any{"event_id": p.Source.EventID, "raw_event_id": p.Source.RawEventID},
		"projection_revision": p.Revision,
	}
	if p.ValidTo != nil {
		doc["valid_to"] = p.ValidTo.UTC().Format(time.RFC3339Nano)
	}
	if len(p.Document) > 0 {
		var raw any
		if err := json.Unmarshal(p.Document, &raw); err != nil {
			return nil, PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "entity document is not valid JSON"}
		}
		doc["document"] = raw
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	doc["content_hash"] = contentHash(encoded)
	return json.Marshal(doc)
}

func (p RelationProjection) body() ([]byte, error) {
	doc := map[string]any{
		"@timestamp":   p.UpdatedAt.UTC().Format(time.RFC3339Nano),
		"organization": map[string]any{"id": p.OrganizationID},
		"relation": map[string]any{
			"id": p.RelationID, "type": p.RelationType,
			"from_entity_id": p.FromEntityID, "to_entity_id": p.ToEntityID,
			"rule_version": p.RuleVersion, "confidence": p.Confidence,
			"revision": p.Revision,
		},
		"valid_from":          p.ValidFrom.UTC().Format(time.RFC3339Nano),
		"source":              map[string]any{"event_id": p.Source.EventID, "raw_event_id": p.Source.RawEventID},
		"projection_revision": p.Revision,
	}
	if p.ValidTo != nil {
		doc["valid_to"] = p.ValidTo.UTC().Format(time.RFC3339Nano)
	}
	if len(p.Evidence) > 0 {
		var raw any
		if err := json.Unmarshal(p.Evidence, &raw); err != nil {
			return nil, PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "relation evidence is not valid JSON"}
		}
		doc["evidence"] = raw
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	doc["content_hash"] = contentHash(encoded)
	return json.Marshal(doc)
}

func (p AttributionProjection) body() ([]byte, error) {
	doc := map[string]any{
		"@timestamp":   p.EventTime.UTC().Format(time.RFC3339Nano),
		"organization": map[string]any{"id": p.OrganizationID},
		"attribution": map[string]any{
			"id": p.AttributionID, "role": p.Role, "state": p.State,
			"entity_id": p.EntityID, "confidence": p.Confidence,
			"rule_version": p.RuleVersion, "reason": p.Reason,
			"partition_key": p.PartitionKey,
		},
		"event":  map[string]any{"id": p.EventID, "time": p.EventTime.UTC().Format(time.RFC3339Nano), "domain": p.Domain},
		"source": map[string]any{"event_id": p.EventID, "raw_event_id": p.RawEventID},
	}
	if !p.ValidFrom.IsZero() {
		doc["valid_from"] = p.ValidFrom.UTC().Format(time.RFC3339Nano)
	}
	if p.ValidTo != nil {
		doc["valid_to"] = p.ValidTo.UTC().Format(time.RFC3339Nano)
	}
	if len(p.Evidence) > 0 {
		var raw any
		if err := json.Unmarshal(p.Evidence, &raw); err != nil {
			return nil, PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "attribution evidence is not valid JSON"}
		}
		doc["evidence"] = raw
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	doc["content_hash"] = contentHash(encoded)
	return json.Marshal(doc)
}

func (p EntityProjection) validate() error {
	switch {
	case p.OrganizationID == "" || p.EntityID == "" || p.EntityType == "" || p.Authority == "" || p.CanonicalKey == "":
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "missing entity identity, tenant, type, space, or canonical key"}
	case p.Revision < 1:
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "entity revision must be >= 1"}
	case p.ValidFrom.IsZero() || p.UpdatedAt.IsZero():
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "missing entity valid_from or projection update time"}
	case p.Source.EventID == "":
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "missing source event.id reference"}
	}
	return nil
}

func (p RelationProjection) validate() error {
	switch {
	case p.OrganizationID == "" || p.RelationID == "" || p.FromEntityID == "" || p.RelationType == "" || p.ToEntityID == "":
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "missing relation identity, tenant, type, or endpoints"}
	case p.Revision < 1:
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "relation revision must be >= 1"}
	case p.ValidFrom.IsZero() || p.UpdatedAt.IsZero():
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "missing relation valid_from or projection update time"}
	case p.ValidTo != nil && !p.ValidTo.After(p.ValidFrom):
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "relation valid_to must be after valid_from"}
	case p.Source.EventID == "":
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "missing source event.id reference"}
	}
	return nil
}

func (p AttributionProjection) validate() error {
	switch {
	case p.OrganizationID == "" || p.AttributionID == "" || p.Role == "" || p.State == "":
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "missing attribution identity, tenant, role, or state"}
	case p.EventID == "" || p.EventTime.IsZero():
		return PermanentIndexError{"PROJECTION_CONTRACT_INVALID", "missing attribution event.id or event time"}
	}
	return nil
}

// PutEntityProjection writes the latest entity master record to the state
// index with external versioning, then appends the same snapshot to the
// entity history index. A stale revision returns StaleRevisionError (state
// untouched); a same-revision identical replay is a no-op success.
func (s *Elasticsearch) PutEntityProjection(ctx context.Context, p EntityProjection) error {
	if err := p.validate(); err != nil {
		return err
	}
	body, err := p.body()
	if err != nil {
		return err
	}
	state := entityStateIndex(s.Namespace)
	if err := s.putVersioned(ctx, state, p.EntityID, p.Revision, body); err != nil {
		return err
	}
	historyID := fmt.Sprintf("%s:%d", p.EntityID, p.Revision)
	return s.putHistory(ctx, fmt.Sprintf("tuba-v1-entity-history-%s-g1-%s", s.Namespace, p.UpdatedAt.UTC().Format("2006.01.02")),
		"logs-ueba.entity-history-"+s.Namespace, historyID, body, entityHistoryMapping)
}

// PutRelationProjection writes the latest relation state with external
// versioning plus the append-only relation history snapshot.
func (s *Elasticsearch) PutRelationProjection(ctx context.Context, p RelationProjection) error {
	if err := p.validate(); err != nil {
		return err
	}
	body, err := p.body()
	if err != nil {
		return err
	}
	state := relationStateIndex(s.Namespace)
	if err := s.putVersioned(ctx, state, p.RelationID, p.Revision, body); err != nil {
		return err
	}
	historyID := fmt.Sprintf("%s:%d", p.RelationID, p.Revision)
	return s.putHistory(ctx, fmt.Sprintf("tuba-v1-relation-history-%s-g1-%s", s.Namespace, p.UpdatedAt.UTC().Format("2006.01.02")),
		"logs-ueba.relation-history-"+s.Namespace, historyID, body, relationHistoryMapping)
}

// PutAttributionProjection appends one immutable attribution decision to the
// attribution history index, dated by the event time. Replays of the same
// attribution_id with identical content are idempotent; the same id with
// different content is a permanent conflict.
func (s *Elasticsearch) PutAttributionProjection(ctx context.Context, p AttributionProjection) error {
	if err := p.validate(); err != nil {
		return err
	}
	body, err := p.body()
	if err != nil {
		return err
	}
	index := fmt.Sprintf("tuba-v1-attributions-%s-g1-%s", s.Namespace, p.EventTime.UTC().Format("2006.01.02"))
	return s.putHistory(ctx, index, "logs-ueba.attributions-"+s.Namespace, p.AttributionID, body, attributionHistoryMapping)
}

// putVersioned indexes one state document under an external version equal to
// the projection revision. ES rejects any write whose version is not greater
// than the stored one, which is exactly the out-of-order protection: on a
// version conflict the stored document is read back — identical content
// means an at-least-once replay (success), an older stored revision is
// impossible, otherwise the incoming write is stale and must not overwrite.
func (s *Elasticsearch) putVersioned(ctx context.Context, index, id string, revision int64, body []byte) error {
	endpoint := fmt.Sprintf("%s/%s/_doc/%s?op_type=index&version_type=external&version=%d",
		s.URL, url.PathEscape(index), url.PathEscape(id), revision)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
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
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode == http.StatusConflict && strings.Contains(string(data), "version_conflict_engine_exception") {
		return s.resolveVersionConflict(ctx, index, id, revision, body)
	}
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return fmt.Errorf("projection state write returned %d: %s", resp.StatusCode, data)
	}
	return PermanentIndexError{"PROJECTION_STATE_REJECTED", string(data)}
}

func (s *Elasticsearch) resolveVersionConflict(ctx context.Context, index, id string, incoming int64, body []byte) error {
	existingVersion, existingHash, err := s.readStoredProjection(ctx, index, id)
	if err != nil {
		return err
	}
	var incomingDoc struct {
		ContentHash string `json:"content_hash"`
	}
	if err := json.Unmarshal(body, &incomingDoc); err != nil {
		return err
	}
	if existingHash == incomingDoc.ContentHash {
		return nil
	}
	if existingVersion == incoming {
		return PermanentIndexError{"PROJECTION_REVISION_CONFLICT", "the same projection revision arrived with different content"}
	}
	return StaleRevisionError{Index: index, ID: id, Existing: existingVersion, Incoming: incoming}
}

// readStoredProjection returns the external version (== revision) and
// content hash of the stored state document.
func (s *Elasticsearch) readStoredProjection(ctx context.Context, index, id string) (int64, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL+"/"+url.PathEscape(index)+"/_doc/"+url.PathEscape(id), nil)
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Authorization", "ApiKey "+s.APIKey)
	resp, err := s.Client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return 0, "", errors.New("projection conflict reported but stored document is missing")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, "", fmt.Errorf("read stored projection returned %d", resp.StatusCode)
	}
	var existing struct {
		Version int64 `json:"_version"`
		Source  struct {
			ContentHash string `json:"content_hash"`
		} `json:"_source"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&existing); err != nil {
		return 0, "", err
	}
	return existing.Version, existing.Source.ContentHash, nil
}

// putHistory appends one immutable snapshot to a dated history index,
// creating the index with a strict bounded mapping on first use (same
// convention as the quarantine sink).
func (s *Elasticsearch) putHistory(ctx context.Context, index, alias, id string, body []byte, mapping string) error {
	if err := s.ensureHistoryIndex(ctx, index, alias, mapping); err != nil {
		return err
	}
	var bulk bytes.Buffer
	if err := json.NewEncoder(&bulk).Encode(map[string]any{"create": map[string]string{"_index": index, "_id": id}}); err != nil {
		return err
	}
	bulk.Write(body)
	bulk.WriteByte('\n')
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+"/_bulk", &bulk)
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
			return fmt.Errorf("projection history bulk returned %d: %s", resp.StatusCode, message)
		}
		return PermanentIndexError{"PROJECTION_HISTORY_REJECTED", string(message)}
	}
	var result struct {
		Items []map[string]rawBulkItem `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	if len(result.Items) != 1 {
		return errors.New("projection history bulk response item count mismatch")
	}
	item := result.Items[0]["create"]
	if item.Status >= 200 && item.Status < 300 {
		return nil
	}
	if item.Status == http.StatusConflict {
		return s.verifyHistoryDuplicate(ctx, index, id, body)
	}
	if item.Status == 429 || item.Status >= 500 {
		return fmt.Errorf("projection history write returned %d: %s", item.Status, item.errorReason())
	}
	return PermanentIndexError{"PROJECTION_HISTORY_REJECTED", item.errorReason()}
}

func (s *Elasticsearch) ensureHistoryIndex(ctx context.Context, index, alias, mapping string) error {
	if _, ok := s.rawIndices.Load(index); ok {
		return nil
	}
	body := []byte(fmt.Sprintf(`{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"dynamic":"strict","properties":%s},"aliases":{"%s":{}}}`, mapping, alias))
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
		return nil
	}
	if status == 429 || status >= 500 {
		return fmt.Errorf("create projection history index returned %d: %s", status, data)
	}
	return PermanentIndexError{"PROJECTION_INDEX_CREATE_FAILED", string(data)}
}

func (s *Elasticsearch) verifyHistoryDuplicate(ctx context.Context, index, id string, body []byte) error {
	_, existingHash, err := s.readStoredProjection(ctx, index, id)
	if err != nil {
		return err
	}
	var incomingDoc struct {
		ContentHash string `json:"content_hash"`
	}
	if err := json.Unmarshal(body, &incomingDoc); err != nil {
		return err
	}
	if existingHash != incomingDoc.ContentHash {
		return PermanentIndexError{"PROJECTION_HISTORY_CONFLICT", "the same projection history id arrived with different content"}
	}
	return nil
}

const entityHistoryMapping = `{
	"@timestamp":{"type":"date"},
	"organization":{"properties":{"id":{"type":"keyword"}}},
	"entity":{"properties":{"id":{"type":"keyword"},"type":{"type":"keyword"},"authority":{"type":"keyword"},"canonical_key":{"type":"keyword"},"identity_strength":{"type":"keyword"},"revision":{"type":"long"}}},
	"document":{"type":"object","enabled":false},
	"valid_from":{"type":"date"},"valid_to":{"type":"date"},
	"last_attribution":{"properties":{"id":{"type":"keyword"},"revision":{"type":"long"}}},
	"source":{"properties":{"event_id":{"type":"keyword"},"raw_event_id":{"type":"keyword"}}},
	"projection_revision":{"type":"long"},
	"content_hash":{"type":"keyword"}
}`

const relationHistoryMapping = `{
	"@timestamp":{"type":"date"},
	"organization":{"properties":{"id":{"type":"keyword"}}},
	"relation":{"properties":{"id":{"type":"keyword"},"type":{"type":"keyword"},"from_entity_id":{"type":"keyword"},"to_entity_id":{"type":"keyword"},"rule_version":{"type":"keyword"},"confidence":{"type":"float"},"revision":{"type":"long"}}},
	"evidence":{"type":"object","enabled":false},
	"valid_from":{"type":"date"},"valid_to":{"type":"date"},
	"source":{"properties":{"event_id":{"type":"keyword"},"raw_event_id":{"type":"keyword"}}},
	"projection_revision":{"type":"long"},
	"content_hash":{"type":"keyword"}
}`

const attributionHistoryMapping = `{
	"@timestamp":{"type":"date"},
	"organization":{"properties":{"id":{"type":"keyword"}}},
	"attribution":{"properties":{"id":{"type":"keyword"},"role":{"type":"keyword"},"state":{"type":"keyword"},"entity_id":{"type":"keyword"},"confidence":{"type":"float"},"rule_version":{"type":"keyword"},"reason":{"type":"keyword"},"partition_key":{"type":"keyword"}}},
	"event":{"properties":{"id":{"type":"keyword"},"time":{"type":"date"},"domain":{"type":"keyword"}}},
	"evidence":{"type":"object","enabled":false},
	"valid_from":{"type":"date"},"valid_to":{"type":"date"},
	"source":{"properties":{"event_id":{"type":"keyword"},"raw_event_id":{"type":"keyword"}}},
	"content_hash":{"type":"keyword"}
}`
