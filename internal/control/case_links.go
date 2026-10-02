package control

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"tuba/product/internal/auth"
)

// R03: PostgreSQL is the case system of record. These operations extend the
// existing case authority with typed entity/risk/evidence links, insert-only
// forensic snapshots, and a legal-hold flag. Every mutation goes through the
// case version guard (optimistic concurrency): a stale expected_version is
// rejected, never last-write-wins. Link association is idempotent by primary
// key: replaying the same link returns the case unchanged without bumping
// the version or writing a second audit record.

var (
	entityIDPattern     = regexp.MustCompile(`^ent:[a-f0-9]{64}$`)
	contributionPattern = regexp.MustCompile(`^rc:[a-f0-9]{64}$`)
)

type CaseLink struct {
	LinkType string    `json:"link_type"`
	RefKind  string    `json:"ref_kind"`
	TargetID string    `json:"target_id"`
	LinkedBy string    `json:"linked_by,omitempty"`
	LinkedAt time.Time `json:"linked_at"`
}

type AddCaseLinkInput struct {
	LinkType        string `json:"link_type"`
	RefKind         string `json:"ref_kind"`
	TargetID        string `json:"target_id"`
	ExpectedVersion int64  `json:"expected_version"`
}

type CaseSnapshot struct {
	ID        string          `json:"id"`
	CaseID    string          `json:"case_id"`
	Label     string          `json:"label"`
	Snapshot  json.RawMessage `json:"snapshot"`
	CreatedBy string          `json:"created_by,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

type snapshotBody struct {
	Case struct {
		ID          string     `json:"id"`
		Title       string     `json:"title"`
		Description string     `json:"description"`
		Status      string     `json:"status"`
		Severity    string     `json:"severity"`
		Verdict     string     `json:"verdict,omitempty"`
		Version     int64      `json:"version"`
		Hold        bool       `json:"hold"`
		HoldReason  string     `json:"hold_reason,omitempty"`
		ClosedAt    *time.Time `json:"closed_at,omitempty"`
	} `json:"case"`
	AnomalyIDs []string   `json:"anomaly_ids"`
	Links      []CaseLink `json:"links"`
}

type SetCaseHoldInput struct {
	Hold            bool   `json:"hold"`
	Reason          string `json:"reason"`
	ExpectedVersion int64  `json:"expected_version"`
}

// ErrVersionConflict rejects a mutation whose expected_version no longer
// matches the stored case version (optimistic concurrency): the caller must
// re-read and retry; the stored case is never overwritten silently.
var ErrVersionConflict = errors.New("version conflict")

// validateLinkShape enforces the link type/ref_kind contract and the
// reference format fail-closed, without any database lookup. Risk
// contributions must be well-formed immutable contribution ids (rc: +
// sha256, the R01 identity convention). Evidence references must carry one
// of the known ref kinds; the referenced events live in Elasticsearch, so
// resolvability here is the ref-kind and shape contract, not a PG lookup.
func validateLinkShape(linkType, refKind, targetID string) error {
	if len(targetID) < 1 || len(targetID) > 256 {
		return errors.New("invalid link target")
	}
	switch linkType {
	case "entity":
		if refKind != "entity" || !entityIDPattern.MatchString(targetID) {
			return errors.New("invalid entity link")
		}
		return nil
	case "risk_contribution":
		if refKind != "risk_contribution" || !contributionPattern.MatchString(targetID) {
			return errors.New("invalid risk contribution link")
		}
		return nil
	case "evidence":
		switch refKind {
		case "event_id", "raw_event_id", "attribution", "job_id":
			return nil
		}
		return errors.New("invalid evidence ref_kind")
	}
	return errors.New("invalid link type")
}

// validateLink additionally checks reference resolvability against PG:
// entity links must reference an entity that exists in this organization's
// entity registry (the PG projection of the E-series entity authority).
func (s *Store) validateLink(ctx context.Context, tx pgx.Tx, org string, linkType, refKind, targetID string) error {
	if err := validateLinkShape(linkType, refKind, targetID); err != nil {
		return err
	}
	if linkType != "entity" {
		return nil
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM entities WHERE organization_id=$1 AND entity_id=$2)`, org, targetID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return errors.New("entity link target not found")
	}
	return nil
}

// AddCaseLink associates an entity, risk contribution, or evidence reference
// with a case. The association is idempotent: an identical replay (same
// type/kind/target) returns the current case with an unchanged version and
// no new audit record. New associations bump the case version under the
// expected_version guard and write a case.link audit event.
func (s *Store) AddCaseLink(ctx context.Context, p auth.Principal, caseID string, in AddCaseLinkInput, requestID string) (Case, bool, error) {
	var out Case
	if in.ExpectedVersion < 1 {
		return out, false, errors.New("invalid link request")
	}
	org, user, e := s.ids(ctx, p)
	if e != nil {
		return out, false, e
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, false, e
	}
	defer tx.Rollback(ctx)
	if e = s.validateLink(ctx, tx, org, in.LinkType, in.RefKind, in.TargetID); e != nil {
		return out, false, e
	}
	var version int64
	e = tx.QueryRow(ctx, `SELECT version FROM cases WHERE organization_id=$1 AND id=$2 FOR UPDATE`, org, caseID).Scan(&version)
	if e != nil {
		return out, false, e
	}
	if version != in.ExpectedVersion {
		return out, false, ErrVersionConflict
	}
	inserted := true
	e = tx.QueryRow(ctx, `INSERT INTO case_links(case_id,link_type,ref_kind,target_id,linked_by) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING RETURNING true`, caseID, in.LinkType, in.RefKind, in.TargetID, user).Scan(&inserted)
	if e != nil {
		if errors.Is(e, pgx.ErrNoRows) {
			inserted = false
		} else {
			return out, false, e
		}
	}
	if inserted {
		e = tx.QueryRow(ctx, `UPDATE cases SET version=version+1, updated_at=now() WHERE organization_id=$1 AND id=$2 AND version=$3 RETURNING version`, org, caseID, in.ExpectedVersion).Scan(&version)
		if e != nil {
			return out, false, e
		}
		meta, _ := json.Marshal(map[string]string{"link_type": in.LinkType, "ref_kind": in.RefKind, "target_id": in.TargetID})
		if _, e = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,$2,'case.link','case',$3,$4,$5)`, org, user, caseID, requestID, meta); e != nil {
			return out, false, e
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return out, false, e
	}
	out, e = s.GetCase(ctx, p, caseID)
	return out, inserted, e
}

// ListCaseLinks returns the full typed link set of a case, ordered
// deterministically.
func (s *Store) ListCaseLinks(ctx context.Context, p auth.Principal, caseID string) ([]CaseLink, error) {
	org, _, e := s.ids(ctx, p)
	if e != nil {
		return nil, e
	}
	var exists bool
	if e = s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cases WHERE organization_id=$1 AND id=$2)`, org, caseID).Scan(&exists); e != nil {
		return nil, e
	}
	if !exists {
		return nil, errors.New("case not found")
	}
	rows, e := s.Pool.Query(ctx, `SELECT cl.link_type,cl.ref_kind,cl.target_id,coalesce(i.subject,''),cl.linked_at FROM case_links cl LEFT JOIN identities i ON i.id=cl.linked_by WHERE cl.case_id=$1 ORDER BY cl.link_type,cl.ref_kind,cl.target_id`, caseID)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []CaseLink{}
	for rows.Next() {
		var l CaseLink
		if e = rows.Scan(&l.LinkType, &l.RefKind, &l.TargetID, &l.LinkedBy, &l.LinkedAt); e != nil {
			return nil, e
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// CreateCaseSnapshot freezes the current case header plus its complete link
// set into an insert-only forensic snapshot. Snapshot creation is a case
// mutation under the expected_version guard; the snapshot row itself can
// never be updated or selectively deleted (enforced by trigger).
func (s *Store) CreateCaseSnapshot(ctx context.Context, p auth.Principal, caseID, label string, expectedVersion int64, requestID string) (CaseSnapshot, error) {
	var out CaseSnapshot
	if expectedVersion < 1 || len(label) < 1 || len(label) > 300 {
		return out, errors.New("invalid snapshot request")
	}
	org, user, e := s.ids(ctx, p)
	if e != nil {
		return out, e
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback(ctx)
	var body snapshotBody
	var version int64
	e = tx.QueryRow(ctx, `SELECT id,title,description,status,severity,coalesce(verdict,''),version,hold,hold_reason,closed_at FROM cases WHERE organization_id=$1 AND id=$2 FOR UPDATE`,
		org, caseID).Scan(&body.Case.ID, &body.Case.Title, &body.Case.Description, &body.Case.Status, &body.Case.Severity, &body.Case.Verdict, &version, &body.Case.Hold, &body.Case.HoldReason, &body.Case.ClosedAt)
	if e != nil {
		return out, e
	}
	if version != expectedVersion {
		return out, ErrVersionConflict
	}
	body.Case.Version = version
	if e = tx.QueryRow(ctx, `SELECT coalesce(array_agg(anomaly_id ORDER BY anomaly_id),'{}') FROM case_anomalies WHERE case_id=$1`, caseID).Scan(&body.AnomalyIDs); e != nil {
		return out, e
	}
	rows, e := tx.Query(ctx, `SELECT link_type,ref_kind,target_id,coalesce((SELECT subject FROM identities WHERE id=cl.linked_by),''),linked_at FROM case_links cl WHERE cl.case_id=$1 ORDER BY link_type,ref_kind,target_id`, caseID)
	if e != nil {
		return out, e
	}
	body.Links = []CaseLink{}
	for rows.Next() {
		var l CaseLink
		if e = rows.Scan(&l.LinkType, &l.RefKind, &l.TargetID, &l.LinkedBy, &l.LinkedAt); e != nil {
			rows.Close()
			return out, e
		}
		body.Links = append(body.Links, l)
	}
	if e = rows.Err(); e != nil {
		rows.Close()
		return out, e
	}
	rows.Close()
	frozen, _ := json.Marshal(body)
	e = tx.QueryRow(ctx, `INSERT INTO case_snapshots(case_id,label,snapshot,created_by) VALUES($1,$2,$3,$4) RETURNING id,created_at`, caseID, label, frozen, user).Scan(&out.ID, &out.CreatedAt)
	if e != nil {
		return out, e
	}
	if _, e = tx.Exec(ctx, `UPDATE cases SET version=version+1, updated_at=now() WHERE organization_id=$1 AND id=$2 AND version=$3`, org, caseID, expectedVersion); e != nil {
		return out, e
	}
	meta, _ := json.Marshal(map[string]string{"snapshot_id": out.ID, "label": label})
	if _, e = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,$2,'case.snapshot','case',$3,$4,$5)`, org, user, caseID, requestID, meta); e != nil {
		return out, e
	}
	if e = tx.Commit(ctx); e != nil {
		return out, e
	}
	out.CaseID = caseID
	out.Label = label
	out.Snapshot = frozen
	return out, nil
}

func (s *Store) ListCaseSnapshots(ctx context.Context, p auth.Principal, caseID string) ([]CaseSnapshot, error) {
	org, _, e := s.ids(ctx, p)
	if e != nil {
		return nil, e
	}
	var exists bool
	if e = s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cases WHERE organization_id=$1 AND id=$2)`, org, caseID).Scan(&exists); e != nil {
		return nil, e
	}
	if !exists {
		return nil, errors.New("case not found")
	}
	rows, e := s.Pool.Query(ctx, `SELECT cs.id,cs.case_id,cs.label,cs.snapshot,coalesce(i.subject,''),cs.created_at FROM case_snapshots cs LEFT JOIN identities i ON i.id=cs.created_by WHERE cs.case_id=$1 ORDER BY cs.created_at,cs.id`, caseID)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []CaseSnapshot{}
	for rows.Next() {
		var snap CaseSnapshot
		if e = rows.Scan(&snap.ID, &snap.CaseID, &snap.Label, &snap.Snapshot, &snap.CreatedBy, &snap.CreatedAt); e != nil {
			return nil, e
		}
		out = append(out, snap)
	}
	return out, rows.Err()
}

// SetCaseHold sets or clears the legal-hold flag under the expected_version
// guard. Enabling a hold requires a reason. Replaying the current hold state
// is a no-op (idempotent): no version bump, no second audit record. While a
// case is held, the retention cleaner must not sweep anything referenced as
// evidence of the case (see controlworker retention hold guard).
func (s *Store) SetCaseHold(ctx context.Context, p auth.Principal, caseID string, in SetCaseHoldInput, requestID string) (Case, bool, error) {
	var out Case
	if in.ExpectedVersion < 1 || len(in.Reason) > 2000 || (in.Hold && len(in.Reason) == 0) {
		return out, false, errors.New("invalid hold request")
	}
	org, user, e := s.ids(ctx, p)
	if e != nil {
		return out, false, e
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, false, e
	}
	defer tx.Rollback(ctx)
	var current bool
	var version int64
	e = tx.QueryRow(ctx, `SELECT hold,version FROM cases WHERE organization_id=$1 AND id=$2 FOR UPDATE`, org, caseID).Scan(&current, &version)
	if e != nil {
		return out, false, e
	}
	if version != in.ExpectedVersion {
		return out, false, ErrVersionConflict
	}
	changed := current != in.Hold
	if changed {
		e = tx.QueryRow(ctx, `UPDATE cases SET hold=$1,hold_reason=$2,hold_set_by=CASE WHEN $1 THEN $3::uuid ELSE NULL END,hold_at=CASE WHEN $1 THEN now() ELSE NULL END,version=version+1,updated_at=now() WHERE organization_id=$4 AND id=$5 AND version=$6 RETURNING version`,
			in.Hold, in.Reason, user, org, caseID, in.ExpectedVersion).Scan(&version)
		if e != nil {
			return out, false, e
		}
		action := "case.hold"
		if !in.Hold {
			action = "case.hold_release"
		}
		meta, _ := json.Marshal(map[string]any{"hold": in.Hold, "reason": in.Reason})
		if _, e = tx.Exec(ctx, `INSERT INTO audit_events(organization_id,actor_identity_id,action,resource_type,resource_id,request_id,metadata) VALUES($1,$2,$3,'case',$4,$5,$6)`, org, user, action, caseID, requestID, meta); e != nil {
			return out, false, e
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return out, false, e
	}
	out, e = s.GetCase(ctx, p, caseID)
	return out, changed, e
}
