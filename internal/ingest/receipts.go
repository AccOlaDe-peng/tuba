package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"tuba/product/internal/rawevent"
)

var ErrRawIDConflict = errors.New("raw event ID payload conflict")

type RawReceiptStore interface {
	GetOrCreate(context.Context, string, string, string, string, string, []byte) ([]byte, bool, error)
	MarkKafkaAcked(context.Context, string, string, string, time.Time) error
}

type PostgresRawReceipts struct{ Pool *pgxpool.Pool }

type receiptMetadata struct {
	SchemaVersion    string                `json:"schema_version"`
	RawEventID       string                `json:"raw_event_id"`
	Organization     rawevent.Organization `json:"organization"`
	Namespace        string                `json:"namespace"`
	SourceInstanceID string                `json:"source_instance_id"`
	SourceContextID  string                `json:"source_context_id"`
	SourcePosition   string                `json:"source_position"`
	DeliveryPosition string                `json:"delivery_position,omitempty"`
	SourceEpoch      string                `json:"source_epoch"`
	Vendor           rawevent.Vendor       `json:"vendor"`
	ReceivedAt       time.Time             `json:"received_at"`
	ReleaseID        string                `json:"release_id"`
	PayloadHash      string                `json:"payload_hash"`
	Encoding         string                `json:"encoding"`
}

func metadataFor(e rawevent.Envelope) receiptMetadata {
	return receiptMetadata{e.SchemaVersion, e.RawEventID, e.Organization, e.Namespace, e.SourceInstanceID, e.SourceContextID, e.SourcePosition, e.DeliveryPosition, e.SourceEpoch, e.Vendor, e.ReceivedAt, e.ReleaseID, e.PayloadHash, e.Encoding}
}

func (s PostgresRawReceipts) GetOrCreate(ctx context.Context, organization, sourceID, rawID, payloadHash, contextID string, candidate []byte) ([]byte, bool, error) {
	var candidateEnvelope rawevent.Envelope
	if err := json.Unmarshal(candidate, &candidateEnvelope); err != nil {
		return nil, false, err
	}
	metadataBytes, err := json.Marshal(metadataFor(candidateEnvelope))
	if err != nil {
		return nil, false, err
	}
	var existingHash, existingContext string
	var receivedAt time.Time
	var snapshot []byte
	var acked bool
	err = s.Pool.QueryRow(ctx, `
	INSERT INTO ingest_receipts(organization_slug,source_instance_id,raw_event_id,payload_hash,received_at,source_context_id,trusted_metadata)
		VALUES($1,$2,$3,$4,$7,$5,$6::jsonb)
		ON CONFLICT(organization_slug,source_instance_id,raw_event_id)
		DO UPDATE SET raw_event_id=EXCLUDED.raw_event_id
		RETURNING payload_hash,received_at,COALESCE(source_context_id,''),trusted_metadata,kafka_acked_at IS NOT NULL`, organization, sourceID, rawID, payloadHash, contextID, metadataBytes, candidateEnvelope.ReceivedAt.UTC()).Scan(&existingHash, &receivedAt, &existingContext, &snapshot, &acked)
	if err != nil {
		return nil, false, err
	}
	if existingHash != payloadHash || (existingContext != "" && existingContext != contextID) {
		return nil, false, ErrRawIDConflict
	}
	if len(snapshot) == 0 {
		candidateEnvelope.ReceivedAt = receivedAt.UTC()
		metadata, err := json.Marshal(metadataFor(candidateEnvelope))
		if err != nil {
			return nil, false, err
		}
		if _, err = s.Pool.Exec(ctx, `UPDATE ingest_receipts SET source_context_id=$4,trusted_metadata=$5::jsonb WHERE organization_slug=$1 AND source_instance_id=$2 AND raw_event_id=$3 AND trusted_metadata IS NULL`, organization, sourceID, rawID, contextID, metadata); err != nil {
			return nil, false, err
		}
		snapshot = metadata
	}
	var meta receiptMetadata
	if err := json.Unmarshal(snapshot, &meta); err != nil {
		return nil, false, err
	}
	if meta.PayloadHash != payloadHash || meta.SourceContextID != contextID {
		return nil, false, ErrRawIDConflict
	}
	envelope := rawevent.Envelope{
		SchemaVersion: meta.SchemaVersion, RawEventID: meta.RawEventID, Organization: meta.Organization, Namespace: meta.Namespace,
		SourceInstanceID: meta.SourceInstanceID, SourceContextID: meta.SourceContextID, SourcePosition: meta.SourcePosition,
		DeliveryPosition: meta.DeliveryPosition,
		SourceEpoch:      meta.SourceEpoch, Vendor: meta.Vendor, ReceivedAt: meta.ReceivedAt, ReleaseID: meta.ReleaseID,
		PayloadHash: meta.PayloadHash, Payload: candidateEnvelope.Payload, Encoding: meta.Encoding,
	}
	encoded, err := rawevent.Marshal(envelope)
	if err != nil {
		return nil, false, err
	}
	return encoded, acked, nil
}

func (s PostgresRawReceipts) MarkKafkaAcked(ctx context.Context, organization, sourceID, rawID string, at time.Time) error {
	result, err := s.Pool.Exec(ctx, `UPDATE ingest_receipts SET kafka_acked_at=COALESCE(kafka_acked_at,$4) WHERE organization_slug=$1 AND source_instance_id=$2 AND raw_event_id=$3`, organization, sourceID, rawID, at.UTC())
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return sql.ErrNoRows
	}
	return nil
}
