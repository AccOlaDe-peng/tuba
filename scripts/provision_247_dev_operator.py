#!/usr/bin/env python3
"""Create a single development TUBA operator in the 247 Keycloak realm.

Run on 247 as root after loading /etc/keycloak/admin.env. The generated
password is emitted once on stdout; it is never written to disk by this tool.
"""
import json
import os
import secrets
import string
import sys
import urllib.error
import urllib.parse
import urllib.request


BASE = "http://127.0.0.1:8180"
REALM = "tuba"
USERNAME = "tuba-operator-20260929"
EMAIL = "tuba-operator-20260929@localhost.invalid"
DISPLAY_NAME = "TUBA Development Operator"


def request(path, token, data=None, form=None, method=None):
    body = None
    headers = {"Authorization": f"Bearer {token}"}
    if form is not None:
        body = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif data is not None:
        body = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, response.headers, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers, exc.read()


def main() -> int:
    admin_user = os.environ.get("KC_BOOTSTRAP_ADMIN_USERNAME", "")
    admin_password = os.environ.get("KC_BOOTSTRAP_ADMIN_PASSWORD", "")
    if not admin_user or not admin_password:
        print("Keycloak bootstrap admin environment is missing", file=sys.stderr)
        return 2

    status, _, body = request(
        "/realms/master/protocol/openid-connect/token",
        "",
        form={"grant_type": "password", "client_id": "admin-cli", "username": admin_user, "password": admin_password},
    )
    if status != 200:
        print(f"Keycloak admin authentication failed (HTTP {status})", file=sys.stderr)
        return 1
    token = json.loads(body)["access_token"]

    status, _, body = request(f"/admin/realms/{REALM}/clients?clientId=tuba-web", token)
    clients = json.loads(body) if status == 200 else []
    if len(clients) != 1 or not clients[0].get("directAccessGrantsEnabled"):
        print("tuba-web client is missing or Direct Access Grants are disabled", file=sys.stderr)
        return 1
    status, _, body = request(f"/admin/realms/{REALM}/roles/tenant_admin", token)
    if status != 200:
        print("realm role tenant_admin is missing", file=sys.stderr)
        return 1
    role = json.loads(body)

    status, _, body = request(f"/admin/realms/{REALM}/users?username={USERNAME}&exact=true", token)
    if status != 200:
        print(f"could not check existing user (HTTP {status})", file=sys.stderr)
        return 1
    password = "T!" + "".join(secrets.choice(string.ascii_letters + string.digits + "-_@#") for _ in range(30))
    user = {
        "username": USERNAME,
        "enabled": True,
        "email": EMAIL,
        "emailVerified": True,
        "firstName": "TUBA",
        "lastName": "Operator",
        "attributes": {"organization_id": ["tenant_a"], "namespace": ["tenant_a"]},
    }
    if json.loads(body):
        print(f"refusing to overwrite existing user {USERNAME}", file=sys.stderr)
        return 1
    status, headers, _ = request(f"/admin/realms/{REALM}/users", token, data=user)
    if status != 201:
        print(f"user creation failed (HTTP {status})", file=sys.stderr)
        return 1
    location = headers.get("Location", "").rstrip("/")
    user_id = location.rsplit("/", 1)[-1]
    if not user_id:
        print("Keycloak created the user but did not return its id", file=sys.stderr)
        return 1

    status, _, _ = request(f"/admin/realms/{REALM}/users/{user_id}/reset-password", token,
                           data={"type": "password", "value": password, "temporary": False}, method="PUT")
    if status not in (200, 204):
        print(f"password setup failed (HTTP {status}); disable the created user in Keycloak", file=sys.stderr)
        return 1
    status, _, _ = request(f"/admin/realms/{REALM}/users/{user_id}/role-mappings/realm", token, data=[role])
    if status not in (200, 204):
        print(f"role assignment failed (HTTP {status}); disable the created user in Keycloak", file=sys.stderr)
        return 1

    print("username=" + USERNAME)
    print("password=" + password)
    print("subject=" + user_id)
    print("issuer=http://10.6.68.247:8180/realms/tuba")
    print("organization=tenant_a")
    print("role=tenant_admin")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
