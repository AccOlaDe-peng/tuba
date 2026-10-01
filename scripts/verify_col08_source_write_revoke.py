#!/usr/bin/env python3
"""COL-08 slice 2 end-to-end driver: disable -> Kafka WRITE ACL revoked ->
enable -> restored, using a throwaway source and collector on 248.

Runs locally; API calls go to the 248 TLS gateway, Kafka/PostgreSQL checks are
executed over SSH. Never touches the four real sources (139/169/zeek x4).
Usage: python scripts/verify_col08_source_write_revoke.py
"""
import json
import os
import pathlib
import shlex
import ssl
import time
import subprocess
import sys
import urllib.parse
import urllib.request

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
ENV_LOCAL = REPO_ROOT / ".env.local"
API = os.environ.get("TUBA_API_BASE", "https://10.6.68.248:8443") + "/api/v1"
ISSUER = os.environ.get("TUBA_OIDC_ISSUER", "http://10.6.68.247:8180/realms/tuba")
SSH_TARGET = None  # filled from .env.local; empty means run Kafka/PG checks locally

PASS = 0
FAIL = 0


def check(name, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"PASS {name}")
    else:
        FAIL += 1
        print(f"FAIL {name} {detail}")


def load_env():
    values = {}
    if ENV_LOCAL.is_file():
        for line in ENV_LOCAL.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                values[k.strip()] = v.strip()
    for key in ("TUBA_SSH_USER", "TUBA_SSH_HOST", "TUBA_OPERATOR_USERNAME", "TUBA_OPERATOR_PASSWORD", "TUBA_E2E_LOCAL"):
        if os.environ.get(key):
            values[key] = os.environ[key]
    return values


def opener():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))


def call(op, path, token="", payload=None, method=None, expect=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(API + path, data=data, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with op.open(req, timeout=20) as resp:
            body = resp.read().decode()
            status = resp.status
    except urllib.error.HTTPError as exc:
        status, body = exc.code, exc.read().decode("utf-8", "replace")
    if expect is not None and status != expect:
        raise SystemExit(f"{method or ('POST' if data else 'GET')} {path} -> {status} (want {expect}): {body[:300]}")
    return status, body


def ssh(script):
    if not SSH_TARGET:
        out = subprocess.run(["bash", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             universal_newlines=True, timeout=120)
        return out.returncode, out.stdout, out.stderr
    env = dict(os.environ)
    env["SSH_ASKPASS"] = str(REPO_ROOT / ".codex-ssh-askpass.cmd").replace("/", "\\")
    env["SSH_ASKPASS_REQUIRE"] = "force"
    env["DISPLAY"] = ":0"
    out = subprocess.run(["ssh", SSH_TARGET, script], capture_output=True, text=True, env=env, timeout=120)
    return out.returncode, out.stdout, out.stderr


def kafka_acls_for(principal, topic):
    rc, out, err = ssh(
        "export JAVA_HOME=/opt/adms/adms-jdk; "
        "/opt/adms/kafka/bin/kafka-acls.sh --bootstrap-server 10.6.68.248:29292 "
        "--command-config /opt/tuba/collector-live/kafka/admin.properties --list "
        f"--topic {shlex.quote(topic)} --principal User:{shlex.quote(principal)} 2>/dev/null")
    if rc != 0:
        raise SystemExit(f"kafka-acls list failed: {err[:300]}")
    return out


def main():
    global SSH_TARGET
    env = load_env()
    if env.get("TUBA_E2E_LOCAL"):
        SSH_TARGET = ""
    else:
        SSH_TARGET = f"{env['TUBA_SSH_USER']}@{env['TUBA_SSH_HOST']}"
    op = opener()

    form = urllib.parse.urlencode({
        "grant_type": "password", "client_id": "tuba-web",
        "username": env["TUBA_OPERATOR_USERNAME"], "password": env["TUBA_OPERATOR_PASSWORD"],
    }).encode()
    with op.open(urllib.request.Request(ISSUER + "/protocol/openid-connect/token", data=form,
                                        headers={"Content-Type": "application/x-www-form-urlencoded"}), timeout=15) as resp:
        token = json.loads(resp.read())["access_token"]
    print("operator token acquired")

    # 1. throwaway collector
    _, body = call(op, "/collectors/enrollments", token, {"expires_in_minutes": 30}, expect=201)
    enrollment = json.loads(body)["enrollment_token"]
    _, body = call(op, "/collector/enroll", "", {
        "enrollment_token": enrollment, "install_id": "col08-e2e-%d" % int(time.time()), "hostname": "col08-e2e-host",
        "os": "linux", "architecture": "amd64", "version": "0.0.0-e2e"}, expect=201)
    collector = json.loads(body)["collector_id"]
    print(f"temp collector {collector}")

    # 2. throwaway source
    _, body = call(op, "/sources", token, {
        "vendor_name": "col08", "vendor_product": "e2e", "vendor_dataset": "write.revoke",
        "release_id": "windows-security-1.0.0", "rate_limit": 10}, expect=201)
    source = json.loads(body)
    source_id, ctx_id = source["id"], source["source_context_id"]
    topic = f"tuba.source.{ctx_id}.v1"
    principal = "tuba-col08-e2e"
    print(f"temp source {source_id} topic {topic}")

    # 3. provision Kafka side exactly like a real source: SCRAM user, topic, WRITE+DESCRIBE
    rc, out, err = ssh(
        "export JAVA_HOME=/opt/adms/adms-jdk; cd /opt/adms/kafka && "
        "bin/kafka-topics.sh --bootstrap-server 10.6.68.248:29292 --command-config /opt/tuba/collector-live/kafka/admin.properties "
        f"--create --if-not-exists --topic {shlex.quote(topic)} --partitions 1 --replication-factor 1 && "
        f"bin/kafka-configs.sh --bootstrap-server 10.6.68.248:29292 --command-config /opt/tuba/collector-live/kafka/admin.properties "
        f"--alter --entity-type users --entity-name {principal} --add-config 'SCRAM-SHA-512=[password=col08-e2e-secret]' && "
        f"bin/kafka-acls.sh --bootstrap-server 10.6.68.248:29292 --command-config /opt/tuba/collector-live/kafka/admin.properties "
        f"--add --allow-principal User:{principal} --operation WRITE --operation DESCRIBE --topic {topic}")
    check("kafka provision", rc == 0, err[:300])
    acl_list = kafka_acls_for(principal, topic)
    check("baseline WRITE+DESCRIBE present", "WRITE" in acl_list and "DESCRIBE" in acl_list, acl_list[:200])

    # 4. bind source write identity to the collector
    _, body = call(op, f"/collectors/{collector}/source-binding", token,
                   {"source_id": source_id, "kafka_principal": principal, "kafka_topic": topic}, method="PUT", expect=201)
    # foreign topic must be rejected
    status, _ = call(op, f"/collectors/{collector}/source-binding", token,
                     {"source_id": source_id, "kafka_principal": principal,
                      "kafka_topic": "tuba.source.ctx_00000000000000000000000000000000.v1"}, method="PUT")
    check("foreign topic binding rejected", status == 404, f"status={status}")

    # 5. disable -> WRITE revoked, DESCRIBE + SCRAM user survive
    status, _ = call(op, f"/collectors/{collector}", token, method="DELETE")
    check("disable returns 204", status == 204, f"status={status}")
    acl_list = kafka_acls_for(principal, topic)
    check("WRITE ACL revoked", "WRITE" not in acl_list, acl_list[:300])
    check("DESCRIBE ACL retained", "DESCRIBE" in acl_list, acl_list[:300])
    rc, out, _ = ssh("export JAVA_HOME=/opt/adms/adms-jdk; /opt/adms/kafka/bin/kafka-configs.sh --bootstrap-server 10.6.68.248:29292 "
                     f"--command-config /opt/tuba/collector-live/kafka/admin.properties --describe --entity-type users --entity-name {principal}")
    check("SCRAM credential retained", "SCRAM-SHA-512" in out, out[:200])

    # 6. idempotent re-disable
    status, _ = call(op, f"/collectors/{collector}", token, method="DELETE")
    check("re-disable idempotent (204)", status == 204, f"status={status}")

    # 7. enable -> WRITE restored before re-admission
    status, _ = call(op, f"/collectors/{collector}/enable", token, payload={})
    check("enable returns 204", status == 204, f"status={status}")
    acl_list = kafka_acls_for(principal, topic)
    check("WRITE ACL restored", "WRITE" in acl_list, acl_list[:300])

    # 8. audit trail
    rc, out, err = ssh("set -a; source /etc/tuba/tuba.env; set +a; export PATH=/opt/adms/postgresql/bin:$PATH; "
                       f"psql \"$DATABASE_URL\" -X -t -A -c \"SELECT action FROM audit_events WHERE resource_id='{collector}' ORDER BY occurred_at\"")
    actions = [a for a in out.split() if a]
    if not actions:
        print(f"audit query rc={rc} stderr={err[:300]}")
    for want in ("collector.enroll", "collector.source_binding.register", "collector.disable",
                 "collector.source_write.revoke", "collector.enable", "collector.source_write.restore"):
        check(f"audit {want}", want in actions, str(actions))

    # 9. cleanup: delete collector rows, source, topic, SCRAM user, ACLs
    call(op, f"/collectors/{collector}", token, method="DELETE")  # leaves collector disabled + revoked
    status, _ = call(op, f"/sources/{source_id}", token, method="DELETE")
    check("test source revoked", status == 204, f"status={status}")
    rc, _, err = ssh("set -a; source /etc/tuba/tuba.env; set +a; export PATH=/opt/adms/postgresql/bin:$PATH; "
                     f"psql \"$DATABASE_URL\" -X -q -c \"DELETE FROM collector_source_kafka_bindings WHERE collector_id='{collector}';\"")
    check("binding row cleanup", rc == 0, err[:300])
    rc, _, err = ssh("export JAVA_HOME=/opt/adms/adms-jdk; cd /opt/adms/kafka && "
                     f"bin/kafka-configs.sh --bootstrap-server 10.6.68.248:29292 --command-config /opt/tuba/collector-live/kafka/admin.properties "
                     f"--alter --entity-type users --entity-name {principal} --delete-config SCRAM-SHA-512 && "
                     f"bin/kafka-topics.sh --bootstrap-server 10.6.68.248:29292 --command-config /opt/tuba/collector-live/kafka/admin.properties "
                     f"--delete --topic {shlex.quote(topic)}")
    check("kafka cleanup", rc == 0, err[:300])
    rc, out, _ = ssh("set -a; source /etc/tuba/tuba.env; set +a; export PATH=/opt/adms/postgresql/bin:$PATH; "
                     f"psql \"$DATABASE_URL\" -X -t -A -c \"SELECT state FROM source_instances WHERE id='{source_id}'\" "
                     f"-c \"SELECT state FROM collector_agents WHERE id='{collector}'\" "
                     f"-c \"SELECT count(*) FROM collector_source_kafka_bindings WHERE collector_id='{collector}'\"")
    check("end state revoked/disabled/unbound", out.split() == ["revoked", "disabled", "0"], out)

    print(f"PASS={PASS} FAIL={FAIL}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
