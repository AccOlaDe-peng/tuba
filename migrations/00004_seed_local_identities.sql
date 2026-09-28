-- +goose Up
INSERT INTO identities(issuer,subject,email,display_name) VALUES
  ('http://127.0.0.1:8180/realms/tuba','11111111-1111-1111-1111-111111111111','wang.min@example.com','Min Wang'),
  ('http://127.0.0.1:8180/realms/tuba','22222222-2222-2222-2222-222222222222','analyst.lee@example.com','Lee Analyst'),
  ('http://127.0.0.1:8180/realms/tuba','33333333-3333-3333-3333-333333333333','auditor.zhao@example.com','Zhao Auditor')
ON CONFLICT(issuer,subject) DO UPDATE SET email=excluded.email,display_name=excluded.display_name;

INSERT INTO memberships(organization_id,identity_id,role_id,revoked_at)
SELECT o.id,i.id,r.id,NULL
FROM organizations o
JOIN roles r ON r.organization_id=o.id
JOIN (
  VALUES
    ('11111111-1111-1111-1111-111111111111','tenant_admin'),
    ('22222222-2222-2222-2222-222222222222','analyst'),
    ('33333333-3333-3333-3333-333333333333','viewer')
) AS assignment(subject,role_name) ON assignment.role_name=r.name
JOIN identities i ON i.subject=assignment.subject AND i.issuer='http://127.0.0.1:8180/realms/tuba'
WHERE o.slug='tenant_a'
ON CONFLICT(organization_id,identity_id,role_id) DO UPDATE SET revoked_at=NULL;

-- +goose Down
DELETE FROM identities WHERE issuer='http://127.0.0.1:8180/realms/tuba'
  AND subject IN ('11111111-1111-1111-1111-111111111111','22222222-2222-2222-2222-222222222222','33333333-3333-3333-3333-333333333333');
