-- +goose Up
-- Keep the immutable context's authorization tuple tied to its source instance.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM source_contexts sc
      JOIN source_instances si ON si.id = sc.source_instance_id
      JOIN organizations o ON o.id = si.organization_id
     WHERE sc.organization_slug IS DISTINCT FROM o.slug
        OR sc.namespace IS DISTINCT FROM si.namespace
        OR sc.vendor_name IS DISTINCT FROM si.vendor_name
        OR sc.vendor_product IS DISTINCT FROM si.vendor_product
        OR sc.vendor_dataset IS DISTINCT FROM si.vendor_dataset
        OR sc.source_epoch IS DISTINCT FROM si.source_epoch
        OR sc.release_id IS DISTINCT FROM COALESCE(si.release_id, '')
  ) THEN
    RAISE EXCEPTION 'existing source context authorization tuple does not match its source instance';
  END IF;
END;
$$;

CREATE FUNCTION enforce_source_context_scope() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  expected_slug text;
  expected_namespace text;
  expected_vendor_name text;
  expected_vendor_product text;
  expected_vendor_dataset text;
  expected_epoch text;
  expected_release text;
BEGIN
  SELECT o.slug, si.namespace, si.vendor_name, si.vendor_product, si.vendor_dataset,
         si.source_epoch, COALESCE(si.release_id, '')
    INTO expected_slug, expected_namespace, expected_vendor_name, expected_vendor_product,
         expected_vendor_dataset, expected_epoch, expected_release
    FROM source_instances si
    JOIN organizations o ON o.id = si.organization_id
   WHERE si.id = NEW.source_instance_id
   FOR KEY SHARE OF si;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'source context references an unknown source instance';
  END IF;

  IF NEW.organization_slug IS DISTINCT FROM expected_slug
     OR NEW.namespace IS DISTINCT FROM expected_namespace
     OR NEW.vendor_name IS DISTINCT FROM expected_vendor_name
     OR NEW.vendor_product IS DISTINCT FROM expected_vendor_product
     OR NEW.vendor_dataset IS DISTINCT FROM expected_vendor_dataset
     OR NEW.source_epoch IS DISTINCT FROM expected_epoch
     OR NEW.release_id IS DISTINCT FROM expected_release THEN
    RAISE EXCEPTION 'source context authorization tuple does not match its source instance';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER source_context_scope_trg
  BEFORE INSERT OR UPDATE ON source_contexts
  FOR EACH ROW EXECUTE FUNCTION enforce_source_context_scope();

-- +goose Down
DROP TRIGGER IF EXISTS source_context_scope_trg ON source_contexts;
DROP FUNCTION IF EXISTS enforce_source_context_scope();
