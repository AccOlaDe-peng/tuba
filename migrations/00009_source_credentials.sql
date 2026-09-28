-- +goose Up
CREATE UNIQUE INDEX source_instances_credential_ref_uq ON source_instances(credential_ref);

-- +goose Down
DROP INDEX IF EXISTS source_instances_credential_ref_uq;
