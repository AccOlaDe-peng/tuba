#!/usr/bin/env bash
set -euo pipefail
: "${ES_URL:?ES_URL is required}"; : "${ES_API_KEY:?ES_API_KEY is required}"
root="$(cd "$(dirname "$0")/.." && pwd)"
command -v python3 >/dev/null || { echo "python3 is required to validate generated templates" >&2; exit 1; }
python3 "$root/scripts/generate_es_templates.py"
put(){ curl --fail --silent --show-error -X PUT -H "Authorization: ApiKey ${ES_API_KEY}" -H 'Content-Type: application/json' --data-binary "@$2" "${ES_URL%/}/$1" >/dev/null; }

# Anomaly storage remains needed by the current analysis sink, but its ILM
# delete phase is intentionally not installed until A03 approves retention.
put '_component_template/tuba-anomaly-v1' "$root/elasticsearch/component-template-anomaly-v1.json"
put '_index_template/tuba-anomaly-v1' "$root/elasticsearch/index-template-anomaly-v1.json"
indices_file="$(mktemp)"
trap 'rm -f "$indices_file"' EXIT
status="$(curl --silent --show-error --output "$indices_file" --write-out '%{http_code}' \
  -H "Authorization: ApiKey ${ES_API_KEY}" \
  "${ES_URL%/}/_cat/indices/ueba-anomalies-*?format=json")"
if [[ $status != 200 ]]; then
  echo "Could not inspect existing anomaly indices (HTTP $status)" >&2
  exit 1
fi
set +e
python3 - "$indices_file" <<'PY'
import json
import sys
try:
    rows = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception:
    sys.exit(2)
sys.exit(0 if rows else 1)
PY
indices_state=$?
set -e
if [[ $indices_state == 0 ]]; then
  put 'ueba-anomalies-*/_mapping' "$root/elasticsearch/anomaly-runtime-mapping-v1.json"
elif [[ $indices_state == 1 ]]; then
  # The installed index template already carries the full anomaly mapping.
  # There is no concrete index to update during a fresh single-node install.
  echo 'No anomaly index exists yet; the installed index template supplies its mapping on creation'
else
  echo 'Could not parse Elasticsearch anomaly index inventory' >&2
  exit 1
fi
for asset in "$root"/elasticsearch/generated-v1/component-*.json; do
  file="$(basename "$asset" .json)"
  template="tuba-${file#component-}"
  put "_component_template/$template" "$asset"
done
for asset in "$root"/elasticsearch/generated-v1/index-*.json; do
  file="$(basename "$asset" .json)"
  template="tuba-${file#index-}"
  put "_index_template/$template" "$asset"
done
# E05 entity/relation state projection templates (latest-state indices
# ueba-entities-*/ueba-relations-* with external-version revision guards).
# Projection history indices are sink-managed dated indices, like quarantine.
for projection in entity relation; do
  put "_component_template/tuba-${projection}-projection-v1" "$root/elasticsearch/component-template-${projection}-projection-v1.json"
  put "_index_template/tuba-${projection}-projection-v1" "$root/elasticsearch/index-template-${projection}-projection-v1.json"
done
echo 'TUBA generated templates, anomaly mapping and entity/relation projection templates applied; legacy authentication and ILM retention policies were left unchanged'
