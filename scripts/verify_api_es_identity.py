#!/usr/bin/env python3
"""Validate the real API OIDC, membership, and namespace-scoped ES read path."""

from __future__ import annotations

import base64
import json
import os
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "deploy" / "validation" / "compose.analysis-sink-security.yaml"
REALM_FILE = ROOT / "deploy" / "keycloak" / "local-dev" / "tuba-realm.json"
ES_URL = "http://127.0.0.1:19201"
KC_URL = "http://127.0.0.1:18182"
API_URL = "http://127.0.0.1:18788"
ISSUER = KC_URL + "/realms/tuba"


def run(command: list[str], env: dict[str, str], input_text: str | None = None) -> str:
    result = subprocess.run(command, cwd=ROOT, env=env, input=input_text, text=True,
                            capture_output=True, timeout=120, check=False)
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"command failed ({result.returncode}): {Path(command[0]).name}: {detail[-2000:]}")
    return result.stdout.strip()


def request(url: str, method: str = "GET", *, auth: str = "", body: bytes | None = None,
            content_type: str = "application/json") -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, method=method)
    if auth:
        req.add_header("Authorization", auth)
    if body is not None:
        req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=6) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(2048)


def wait_http(url: str, acceptable: set[int], timeout: int = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, _ = request(url)
            if status in acceptable:
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(1)
    raise RuntimeError(f"endpoint did not become available: {url}")


def token(username: str, password: str) -> str:
    body = urllib.parse.urlencode({
        "grant_type": "password", "client_id": "tuba-web",
        "username": username, "password": password,
    }).encode()
    status, response = request(ISSUER + "/protocol/openid-connect/token", "POST",
                              body=body, content_type="application/x-www-form-urlencoded")
    if status != 200:
        raise RuntimeError(f"isolated Keycloak password grant failed (HTTP {status})")
    return json.loads(response)["access_token"]


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def migration_up(path: Path) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    try:
        start = lines.index("-- +goose Up") + 1
        end = lines.index("-- +goose Down", start)
    except ValueError as exc:
        raise RuntimeError(f"migration has no complete Goose Up section: {path.name}") from exc
    return "\n".join(lines[start:end])


def stop_api(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
    try:
        code = process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        raise RuntimeError("tuba-api did not stop gracefully") from None
    if code != 0:
        raise RuntimeError(f"tuba-api graceful stop returned {code}")


def main() -> int:
    project = "tuba-api-check-" + uuid.uuid4().hex[:10]
    password = secrets.token_urlsafe(28)
    pg_password = secrets.token_urlsafe(28)
    kc_admin_password = secrets.token_urlsafe(28)
    env = os.environ.copy()
    env.update({"ES_VALIDATION_PASSWORD": password, "DB_VALIDATION_PASSWORD": pg_password,
                "KEYCLOAK_VALIDATION_PASSWORD": kc_admin_password})
    key_env = env.copy()
    key_env.update({"ES_ADMIN_USERNAME": "elastic", "ES_ADMIN_PASSWORD": password})
    admin_auth = "Basic " + base64.b64encode(f"elastic:{password}".encode()).decode()
    compose = ["docker", "compose", "--project-name", project, "--file", str(COMPOSE_FILE)]
    workdir = Path(tempfile.mkdtemp(prefix="tuba-api-check-"))
    keyring = workdir / "api-keyring.json"
    binary = workdir / ("tuba-api.exe" if os.name == "nt" else "tuba-api")
    log_path = workdir / "api.log"
    api_log = None
    api: subprocess.Popen[str] | None = None
    started = False
    revoked = False
    try:
        realm = json.loads(REALM_FILE.read_text(encoding="utf-8"))
        user = next(item for item in realm["users"] if item["username"] == "analyst.lee")
        username = user["username"]
        user_password = next(item["value"] for item in user["credentials"] if item["type"] == "password")
        run(["go", "build", "-o", str(binary), "./cmd/tuba-api"], env)
        started = True
        run(compose + ["--profile", "api-identity", "up", "--detach", "--wait"], env)
        status, _ = request(ES_URL + "/_cluster/health", auth=admin_auth)
        if status != 200:
            raise RuntimeError(f"secured Elasticsearch is unavailable (HTTP {status})")
        wait_http(ISSUER + "/.well-known/openid-configuration", {200})

        # Apply every checked-in migration to the disposable database, then bind
        # the Keycloak subject to the seeded tenant analyst role.
        pg_id = run(compose + ["ps", "--all", "--quiet", "postgres"], env).splitlines()[0]
        for migration in sorted((ROOT / "migrations").glob("*.sql")):
            run(["docker", "exec", "-i", pg_id, "psql", "--username", "tuba", "--dbname", "tuba",
                 "--set", "ON_ERROR_STOP=1"], env, migration_up(migration))

        access_token = token(username, user_password)
        claims = json.loads(base64.urlsafe_b64decode(access_token.split(".")[1] + "=="))
        subject = claims["sub"]
        issuer_sql, subject_sql = quote_sql(ISSUER), quote_sql(subject)
        identity_sql = (
            "INSERT INTO identities (issuer,subject,email,display_name) VALUES ("
            + issuer_sql + "," + subject_sql + "," + quote_sql(user.get("email", "")) + "," + quote_sql(username)
            + ") ON CONFLICT (issuer,subject) DO UPDATE SET disabled_at=NULL;\n"
            "INSERT INTO memberships (organization_id,identity_id,role_id) "
            "SELECT o.id,i.id,r.id FROM organizations o JOIN roles r ON r.organization_id=o.id "
            "JOIN identities i ON i.issuer=" + issuer_sql + " AND i.subject=" + subject_sql + " "
            "WHERE o.slug='tenant_a' AND r.name='analyst' ON CONFLICT DO NOTHING;"
        )
        run(["docker", "exec", "-i", pg_id, "psql", "--username", "tuba", "--dbname", "tuba",
             "--set", "ON_ERROR_STOP=1"], env, identity_sql)

        namespace, organization = "tenant_a", "tenant_a"
        run([sys.executable, str(ROOT / "scripts" / "manage_elasticsearch_api_keys.py"),
             "create", "--namespace", namespace, "--es-url", ES_URL,
             "--expiration", "1h", "--output", str(keyring)], key_env)
        api_key = json.loads(keyring.read_text(encoding="utf-8"))["services"]["api"]["encoded"]

        now = datetime.now(timezone.utc).replace(microsecond=0)
        day = now.strftime("%Y.%m.%d")
        index = f"tuba-v1-uim-network-{namespace}-g1-{day}"
        event_id = "evt:api-runtime:" + uuid.uuid4().hex
        event = {
            "@timestamp": now.isoformat().replace("+00:00", "Z"),
            "organization": {"id": organization},
            "event": {"id": event_id, "kind": "event", "category": ["network"], "dataset": "network"},
            "ueba": {"route": {"domain": "network", "generation": "g1"}},
            "source": {"ip": "192.0.2.20"},
        }
        mapping = {"mappings": {"properties": {
            "@timestamp": {"type": "date"}, "organization": {"properties": {"id": {"type": "keyword"}}},
            "event": {"properties": {"id": {"type": "keyword"}}},
            "ueba": {"properties": {"route": {"properties": {"domain": {"type": "keyword"}}}}},
        }}, "aliases": {f"logs-ueba.network-{namespace}": {}}}
        status, _ = request(ES_URL + "/" + index, "PUT", auth=admin_auth,
                            body=json.dumps(mapping, separators=(",", ":")).encode())
        if status not in (200, 201):
            raise RuntimeError(f"could not prepare isolated event index (HTTP {status})")
        status, _ = request(ES_URL + f"/{index}/_doc/{urllib.parse.quote(event_id, safe='')}?refresh=wait_for", "PUT",
                            auth=admin_auth, body=json.dumps(event, separators=(",", ":")).encode())
        if status not in (200, 201):
            raise RuntimeError(f"could not seed isolated event document (HTTP {status})")
        alias = f"logs-ueba.network-{namespace}"
        admin_status, admin_body = request(ES_URL + f"/{alias}/_search", "POST", auth=admin_auth, body=b'{"query":{"match_all":{}}}')
        admin_hits = json.loads(admin_body).get("hits", {}).get("total", {}).get("value") if admin_status == 200 else "n/a"
        status, check_body = request(ES_URL + f"/{alias}/_search", "POST", auth="ApiKey " + api_key, body=b'{"query":{"match_all":{}}}')
        if status != 200:
            raise RuntimeError(f"API ES key could not read its event alias (HTTP {status})")
        api_hits = json.loads(check_body).get("hits", {}).get("total", {}).get("value")
        if api_hits != 1:
            raise RuntimeError(f"seeded alias search counts differ or are empty (admin={admin_hits}, api={api_hits})")

        api_env = os.environ.copy()
        api_env.update({
            "OIDC_ISSUER": ISSUER, "OIDC_AUDIENCE": "tuba-api",
            "OIDC_JWKS_URL": ISSUER + "/protocol/openid-connect/certs",
            "DATABASE_URL": f"postgres://tuba:{pg_password}@127.0.0.1:15433/tuba?sslmode=disable",
            "ES_URL": ES_URL, "ES_API_KEY": api_key, "API_LISTEN": "127.0.0.1:18788",
        })
        api_log = open(log_path, "w", encoding="utf-8")
        options: dict[str, object] = {"cwd": ROOT, "env": api_env, "stdin": subprocess.DEVNULL,
                                      "stdout": api_log, "stderr": subprocess.STDOUT, "text": True}
        if os.name == "nt":
            options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            options["start_new_session"] = True
        api = subprocess.Popen([str(binary)], **options)  # type: ignore[arg-type]
        wait_http(API_URL + "/health/ready", {200})

        from_time = (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        to_time = (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        query = urllib.parse.urlencode({"domain": "network", "from": from_time, "to": to_time, "limit": "10"})
        bearer = "Bearer " + access_token
        status, body = request(API_URL + "/api/v1/events?" + query, auth=bearer)
        if status != 200:
            raise RuntimeError(f"authenticated API event query failed (HTTP {status})")
        response = json.loads(body)
        if not any(item.get("id") == event_id for item in response.get("items", [])):
            returned_ids = [item.get("id") for item in response.get("items", [])]
            raise RuntimeError(f"API response omitted seeded event; total={response.get('total')} returned_ids={returned_ids}")

        inactive_token = token("auditor.zhao", user_password)
        status, _ = request(API_URL + "/api/v1/events?" + query, auth="Bearer " + inactive_token)
        if status != 403:
            raise RuntimeError(f"identity without active membership should be denied (HTTP {status})")

        denied_status, _ = request(ES_URL + "/logs-ueba.network-outside/_search", auth="ApiKey " + api_key,
                                   body=b"{}", method="POST")
        if denied_status not in (401, 403):
            raise RuntimeError(f"API ES key unexpectedly read outside namespace (HTTP {denied_status})")

        stop_api(api)
        api = None
        api_log.close()
        api_log = None
        run([sys.executable, str(ROOT / "scripts" / "manage_elasticsearch_api_keys.py"),
             "revoke", "--es-url", ES_URL, "--keyring", str(keyring)], key_env)
        revoked = True
        print("PASS: real tuba-api authenticated a Keycloak analyst, authorized active PostgreSQL membership, returned the tenant event with its own ES key, denied inactive membership and cross-namespace ES reads, and stopped gracefully.")
        return 0
    finally:
        if api is not None:
            if api.poll() is None:
                api.kill()
                api.wait(timeout=10)
        if api_log is not None:
            api_log.close()
        if keyring.exists() and started and not revoked:
            try:
                run([sys.executable, str(ROOT / "scripts" / "manage_elasticsearch_api_keys.py"),
                     "revoke", "--es-url", ES_URL, "--keyring", str(keyring)], key_env)
            except Exception as exc:  # noqa: BLE001 - cleanup diagnostic only
                print(f"WARNING: isolated Elasticsearch API key revocation needs attention: {exc}", file=sys.stderr)
        if started:
            try:
                run(compose + ["--profile", "api-identity", "down", "--volumes", "--remove-orphans"], env)
            except Exception as exc:  # noqa: BLE001 - cleanup diagnostic only
                print(f"WARNING: isolated Compose cleanup needs attention: {exc}", file=sys.stderr)
        keyring.unlink(missing_ok=True)
        binary.unlink(missing_ok=True)
        log_path.unlink(missing_ok=True)
        workdir.rmdir()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, subprocess.SubprocessError, KeyError, ValueError, StopIteration) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
