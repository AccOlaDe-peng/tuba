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

var ErrSourceUnauthorized = errors.New("source credential is unknown or disabled")

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
	err := r.Pool.QueryRow(ctx, `
		SELECT sc.organization_slug,sc.namespace,si.id,sc.source_epoch,
		       sc.vendor_name,sc.vendor_product,sc.vendor_dataset,
		       sc.release_id,si.rate_limit,sc.id
		FROM source_instances si
		JOIN source_credentials cr ON cr.source_instance_id=si.id
		JOIN source_contexts sc ON sc.source_instance_id=si.id
		WHERE cr.credential_ref=$1 AND (cr.valid_until IS NULL OR cr.valid_until>now())
		  AND sc.id=$2 AND si.enabled=true`, credentialRef, contextID).Scan(
		&source.OrganizationID, &source.Namespace, &source.SourceInstanceID, &source.SourceEpoch,
		&source.VendorName, &source.VendorProduct, &source.VendorDataset, &source.ReleaseID, &source.RateLimit, &source.SourceContextID)
	if errors.Is(err, pgx.ErrNoRows) {
		return RawSource{}, ErrSourceUnauthorized
	}
	if err != nil {
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
	err := r.Pool.QueryRow(ctx, `
		SELECT sc.organization_slug,sc.namespace,si.id,sc.source_epoch,
		       sc.vendor_name,sc.vendor_product,sc.vendor_dataset,
		       sc.release_id,si.rate_limit,sc.id
		FROM source_contexts sc
		JOIN source_instances si ON si.id=sc.source_instance_id
		WHERE sc.id=$1 AND si.enabled=true`, contextID).Scan(
		&source.OrganizationID, &source.Namespace, &source.SourceInstanceID, &source.SourceEpoch,
		&source.VendorName, &source.VendorProduct, &source.VendorDataset, &source.ReleaseID, &source.RateLimit, &source.SourceContextID)
	if errors.Is(err, pgx.ErrNoRows) {
		return RawSource{}, ErrSourceUnauthorized
	}
	if err != nil {
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
