#!/bin/sh
# Controlled boot entry for TUBA on 248. The Launcher is deliberately NOT
# registered with systemd; this script is invoked from the root crontab
# @reboot entry and is safe to run manually at any time: each manifest is a
# no-op when its Launcher supervisor is already running.
#   tuba-services.json    - data plane (17 services since 2026-10-02: original 11
#                           plus entity/analysis/control-worker per namespace)
#   tuba-monitoring.json  - monitoring stack (prometheus/grafana/exporters/guard)
#
# 248 部署位置：/opt/tuba/bin/tuba-boot（0700 root）。仓库副本与线上保持一致。
LAUNCHER=/opt/tuba/bin/tuba-launcher
LOG=/var/log/tuba/boot.log

mkdir -p /var/log/tuba
ts() { date "+%Y-%m-%dT%H:%M:%S%z"; }

# The TUBA validation Kafka broker (node.id=2, 10.6.68.248:29292) is not
# managed by systemd or the Launcher; without it the whole data plane stalls.
# Start it here when its port is not listening (idempotent).
KAFKA_PROP=/opt/tuba/collector-live/kafka/server.properties
if ! (exec 3<>/dev/tcp/10.6.68.248/29292) 2>/dev/null; then
  echo "$(ts) tuba-boot: starting validation kafka broker" >> "$LOG"
  if JAVA_HOME=/opt/adms/adms-jdk /opt/adms/kafka/bin/kafka-server-start.sh -daemon "$KAFKA_PROP" >> "$LOG" 2>&1; then
    echo "$(ts) tuba-boot: kafka broker start issued" >> "$LOG"
  else
    echo "$(ts) tuba-boot: kafka broker start FAILED" >> "$LOG"
  fi
else
  exec 3>&- 3<&-
  echo "$(ts) tuba-boot: kafka broker already listening, no action" >> "$LOG"
fi

rc=0
for MANIFEST in /etc/tuba/tuba-services.json /etc/tuba/tuba-monitoring.json; do
  # Positive-evidence gate: `status` exits 0 even for a stale state file
  # (it prints "stopped (stale state file)" plus stale per-service rows), so
  # require the live supervisor header line instead of trusting the exit code.
  if "$LAUNCHER" status --manifest "$MANIFEST" 2>/dev/null | grep -q "^TUBA launcher pid=[0-9]"; then
    echo "$(ts) tuba-boot: $MANIFEST already supervised, no action" >> "$LOG"
    continue
  fi
  echo "$(ts) tuba-boot: starting $MANIFEST via launcher" >> "$LOG"
  if "$LAUNCHER" start --manifest "$MANIFEST" >> "$LOG" 2>&1; then
    echo "$(ts) tuba-boot: start ok $MANIFEST" >> "$LOG"
  else
    echo "$(ts) tuba-boot: start FAILED $MANIFEST" >> "$LOG"
    rc=1
  fi
done
exit "$rc"
