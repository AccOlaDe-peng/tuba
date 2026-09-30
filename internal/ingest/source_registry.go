package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The three failures below are deliberately separate. "I cannot identify this
// source", "I know it and it is being held", and "I know it and it has been
// refused" call for different actions, and collapsing them cost real data: a
// revoked source used to look exactly like an unknown one, so the adapter
// retried it until the topic's retention deleted the records without recording
// that anything had been refused.
var (
	ErrSourceUnauthorized = errors.New("source credential is unknown")
	ErrSourcePaused       = errors.New("source is paused")
	ErrSourceRevoked      = errors.New("source has been revoked")
)

// classifySourceState turns the stored lifecycle state into the error the caller
// should act on. A state this build does not know is treated as paused rather
// than revoked: holding the offset is recoverable, advancing past records that
// were never accepted is not.
func classifySourceState(state string) error {
	switch state {
	case "active":
		return nil
	case "revoked":
		return ErrSourceRevoked
	default:
		return ErrSourcePaused
	}
}

type SourceResolver interface {
	ResolveSource(context.Context, string, string) (RawSource, error)
}

type TopicSourceResolver interface {
	ResolveTopic(context.Context, string) (RawSource, error)
}

type PostgresSourceResolver struct{ Pool *pgxpool.Pool }

func (r PostgresSourceResolver) ResolveSource(ctx context.Context, apiKey, contextID string) (RawSource, error) {
	if r.Pool == nil || len(apiKey) != 52 || !strings.HasPrefix(apiKey, "tuba_src_") || len(contextID) != 36 || !strings.HasPrefix(contextID, "ctx_") {
		return RawSource{}, ErrSourceUnauthorized
	}
	digest := sha256.Sum256([]byte(apiKey))
	credentialRef := "sha256:" + hex.EncodeToString(digest[:])
	var source RawSource
	var state string
	// The lifecycle state is read rather than filtered on: a `state` predicate
	// here would make a revoked source indistinguishable from an unknown one,
	// which is what let a revoked source be retried forever.
	err := r.Pool.QueryRow(ctx, `
		SELECT sc.organization_slug,sc.namespace,si.id,sc.source_epoch,
		       sc.vendor_name,sc.vendor_product,sc.vendor_dataset,
		       sc.release_id,si.rate_limit,sc.id,si.state
		FROM source_instances si
		JOIN source_credentials cr ON cr.source_instance_id=si.id
		JOIN source_contexts sc ON sc.source_instance_id=si.id
		WHERE cr.credential_ref=$1 AND (cr.valid_until IS NULL OR cr.valid_until>now())
		  AND sc.id=$2`, credentialRef, contextID).Scan(
		&source.OrganizationID, &source.Namespace, &source.SourceInstanceID, &source.SourceEpoch,
		&source.VendorName, &source.VendorProduct, &source.VendorDataset, &source.ReleaseID, &source.RateLimit, &source.SourceContextID,
		&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return RawSource{}, ErrSourceUnauthorized
	}
	if err != nil {
		return RawSource{}, err
	}
	if err := classifySourceState(state); err != nil {
		return RawSource{}, err
	}
	return source, nil
}

func (r PostgresSourceResolver) ResolveTopic(ctx context.Context, topic string) (RawSource, error) {
	contextID, ok := sourceContextFromTopic(topic)
	if r.Pool == nil || !ok {
		return RawSource{}, ErrSourceUnauthorized
	}
	var source RawSource
	var state string
	// As in ResolveSource: read the state rather than filter on it, so a topic
	// bound to a revoked source can be answered definitively instead of looking
	// like a topic nobody has heard of.
	err := r.Pool.QueryRow(ctx, `
		SELECT sc.organization_slug,sc.namespace,si.id,sc.source_epoch,
		       sc.vendor_name,sc.vendor_product,sc.vendor_dataset,
		       sc.release_id,si.rate_limit,sc.id,si.state
		FROM source_contexts sc
		JOIN source_instances si ON si.id=sc.source_instance_id
		WHERE sc.id=$1`, contextID).Scan(
		&source.OrganizationID, &source.Namespace, &source.SourceInstanceID, &source.SourceEpoch,
		&source.VendorName, &source.VendorProduct, &source.VendorDataset, &source.ReleaseID, &source.RateLimit, &source.SourceContextID,
		&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return RawSource{}, ErrSourceUnauthorized
	}
	if err != nil {
		return RawSource{}, err
	}
	if err := classifySourceState(state); err != nil {
		return RawSource{}, err
	}
	return source, nil
}

func sourceContextFromTopic(topic string) (string, bool) {
	const prefix = "tuba.source."
	const suffix = ".v1"
	if !strings.HasPrefix(topic, prefix) || !strings.HasSuffix(topic, suffix) {
		return "", false
	}
	contextID := strings.TrimSuffix(strings.TrimPrefix(topic, prefix), suffix)
	if len(contextID) != 36 || !strings.HasPrefix(contextID, "ctx_") || contextID != strings.ToLower(contextID) {
		return "", false
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(contextID, "ctx_")); err != nil {
		return "", false
	}
	return contextID, true
}
