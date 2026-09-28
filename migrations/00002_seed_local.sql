-- +goose Up
INSERT INTO organizations(slug,name,namespace) VALUES ('tenant_a','Local tenant','tenant_a') ON CONFLICT DO NOTHING;
INSERT INTO roles(organization_id,name,permissions,system) SELECT id,'viewer',ARRAY['anomaly:read','case:read'],true FROM organizations WHERE slug='tenant_a' ON CONFLICT DO NOTHING;
INSERT INTO roles(organization_id,name,permissions,system) SELECT id,'analyst',ARRAY['anomaly:read','case:read','case:write'],true FROM organizations WHERE slug='tenant_a' ON CONFLICT DO NOTHING;
INSERT INTO roles(organization_id,name,permissions,system) SELECT id,'tenant_admin',ARRAY['anomaly:read','case:read','case:write','user:manage'],true FROM organizations WHERE slug='tenant_a' ON CONFLICT DO NOTHING;
-- +goose Down
DELETE FROM organizations WHERE slug='tenant_a';
