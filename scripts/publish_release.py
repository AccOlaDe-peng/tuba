#!/usr/bin/env python3
"""Drive a TUBA semantic release through draft -> validated -> staged -> active.

Implements the publishing sequence in docs/RELEASE-PUBLISHING.md against the
deployed API, so the whole flow is auditable and re-runnable instead of a series
of hand-typed curls. Steps already completed are skipped, so a run interrupted
halfway can be resumed.

Credentials come from .env.local (gitignored) or the process environment:

    TUBA_OPERATOR_USERNAME / TUBA_OPERATOR_PASSWORD

The API is reached through 248's TLS gateway, which proxies /api/v1/ to the API
bound on loopback. That gateway currently presents a self-signed certificate
whose SAN does not cover the host address; docs/IMPLEMENTATION-TODO.md records
this as a development-stage condition (production O03 still owes a valid
certificate), so verification is disabled here and nowhere else.
"""
import argparse
import base64
import hashlib
import json
import os
import ssl
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))
# Reuse the bundle validator's canonical encoding so the idempotency key is
# derived from exactly the bytes the release registry hashes.
from validate_release_bundle import canonical_manifest_bytes  # noqa: E402
ENV_LOCAL = REPO_ROOT / ".env.local"

DEFAULT_API = "https://10.6.68.248:8443"
DEFAULT_ISSUER = "http://10.6.68.247:8180/realms/tuba"
DEFAULT_CLIENT = "tuba-web"
DEFAULT_BUNDLE = REPO_ROOT / "releases" / "windows-security-1.0.0"


def load_env_local() -> dict:
    values = {}
    if not ENV_LOCAL.exists():
        return values
    for line in ENV_LOCAL.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        values[key.strip()] = value.strip()
    return values


def setting(name: str, file_values: dict, default: str = "") -> str:
    return os.environ.get(name) or file_values.get(name) or default


def make_opener():
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return urllib.request.build_opener(urllib.request.HTTPSHandler(context=context))


def call(opener, url, token="", payload=None, extra_headers=None, method=None):
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if extra_headers:
        headers.update(extra_headers)
    body = None
    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with opener.open(req, timeout=30) as response:
            raw = response.read()
            return response.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except ValueError:
            return exc.code, {"raw": raw.decode("utf-8", "replace")[:400]}
    except urllib.error.URLError as exc:
        print(f"could not reach {url}: {exc.reason}", file=sys.stderr)
        return 0, {}


def decode_claims(access_token: str) -> dict:
    try:
        payload = access_token.split(".")[1]
        padded = payload + "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(padded))
    except (IndexError, ValueError):
        return {}


def fail(step: str, response: dict) -> int:
    print(f"{step} failed: {json.dumps(response, ensure_ascii=False)}", file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bundle", default=str(DEFAULT_BUNDLE), help="release bundle directory")
    parser.add_argument("--api", default=None, help=f"API base URL (default {DEFAULT_API})")
    parser.add_argument("--issuer", default=None, help=f"OIDC issuer (default {DEFAULT_ISSUER})")
    parser.add_argument("--skip-local-validation", action="store_true",
                        help="skip the local bundle hash check before contacting the API")
    args = parser.parse_args()

    file_values = load_env_local()
    api = args.api or setting("TUBA_API_BASE", file_values, DEFAULT_API)
    issuer = args.issuer or setting("TUBA_OIDC_ISSUER", file_values, DEFAULT_ISSUER)
    username = setting("TUBA_OPERATOR_USERNAME", file_values)
    password = setting("TUBA_OPERATOR_PASSWORD", file_values)
    if not username or not password:
        print(f"operator credential is missing; run scripts/reset_247_dev_operator_password.py "
              f"or set TUBA_OPERATOR_USERNAME/PASSWORD in {ENV_LOCAL}", file=sys.stderr)
        return 2

    bundle = Path(args.bundle)
    manifest_path = bundle / "manifest.json"
    if not manifest_path.is_file():
        print(f"manifest not found at {manifest_path}", file=sys.stderr)
        return 2
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    release_id = manifest["release_id"]

    # Verify the bundle locally first. The server rechecks every asset hash at
    # each transition anyway; failing here just avoids a round trip and leaves
    # no half-advanced record behind.
    if not args.skip_local_validation:
        validator = REPO_ROOT / "scripts" / "validate_release_bundle.py"
        result = subprocess.run([sys.executable, str(validator), str(manifest_path), str(bundle)],
                                capture_output=True, text=True)
        if result.returncode != 0:
            print(result.stdout + result.stderr, file=sys.stderr)
            print("local bundle validation failed; not contacting the API", file=sys.stderr)
            return 1
        print(f"local bundle check: OK ({result.stdout.strip().splitlines()[-1]})")

    opener = make_opener()

    form = urllib.parse.urlencode({
        "grant_type": "password", "client_id": DEFAULT_CLIENT,
        "username": username, "password": password,
    }).encode()
    req = urllib.request.Request(
        f"{issuer}/protocol/openid-connect/token", data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with opener.open(req, timeout=15) as response:
            token_response = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        print(f"password grant failed (HTTP {exc.code}): {exc.read().decode('utf-8', 'replace')[:300]}",
              file=sys.stderr)
        return 1
    except urllib.error.URLError as exc:
        print(f"could not reach issuer {issuer}: {exc.reason}", file=sys.stderr)
        return 1
    token = token_response["access_token"]
    claims = decode_claims(token)
    print(f"authenticated as sub={claims.get('sub', '?')}")

    base = f"{api}/api/v1/releases"

    status, current = call(opener, f"{base}/{release_id}", token)
    if status == 200:
        state = current.get("state", "?")
        print(f"existing release record: state={state}")
    elif status == 404:
        state = "absent"
        # Binding the idempotency key to the manifest digest keeps a retry of the
        # same content a no-op (the registry records release.create_replay) while
        # a changed bundle surfaces as a conflict instead of silently reusing a
        # draft. The key must stay within the API's 16..128 byte window.
        digest = hashlib.sha256(canonical_manifest_bytes(manifest)).hexdigest()
        key = f"{release_id}-{digest}"[:128]
        status, created = call(opener, base, token, payload=manifest,
                               extra_headers={"Idempotency-Key": key})
        if status not in (200, 201):
            return fail("register", created)
        state = created.get("state", "draft")
        print(f"registered draft: state={state}")
    else:
        return fail("read release", current)

    if state == "active":
        print("release is already active; no transitions needed")
    for step, required in (("validate", "draft"), ("stage", "validated"), ("activate", "staged")):
        if state == "active":
            break
        if state != required:
            print(f"{step}: unexpected state {state!r} (wanted {required!r}); stopping", file=sys.stderr)
            return 1
        status, result = call(opener, f"{base}/{release_id}/{step}", token, payload={})
        if status not in (200, 201):
            return fail(step, result)
        state = result.get("state", "")
        print(f"{step}: OK -> state={state}")

    status, audit = call(opener, f"{base}/{release_id}/audit", token)
    if status != 200:
        return fail("read audit", audit)
    items = audit.get("items", [])
    print(f"\naudit trail ({len(items)} events):")
    for event in items:
        print(f"  {event.get('occurred_at', '?')}  {event.get('action', '?'):<22} "
              f"actor={event.get('actor_subject') or '(bootstrap)'} "
              f"request={event.get('request_id', '?')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
