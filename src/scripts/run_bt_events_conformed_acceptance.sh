#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<'EOF'
Usage: run_bt_events_conformed_acceptance.sh

Environment overrides:
  BOUNDED_RUN_TIME_MODE
  BOUNDED_RUN_STARTED_AT
  BOOTSTRAP_SERVERS
  PYTHON_BIN
  BOUNDED_RUN_CONFIG
  WAIT_AFTER_BOUNDED_RUN_SECONDS
  WAIT_AFTER_BATCH_SECONDS
  MIN_RAW_ROWS
  MIN_GOLD_ROWS
  MAX_FRESHNESS_MINUTES
  MIN_EVENTS_CONFORMED_ROWS
  EVENTS_CONFORMED_DATA_DATE
  EVENTS_CONFORMED_RUN_ID
  MIN_PROCESSED_AT_MS
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[BT-EVENTS-CONFORMED] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
BOUNDED_RUN_CONFIG="${BOUNDED_RUN_CONFIG:-docs/architecture/generator/examples/bounded_run_config.example.json}"
WAIT_AFTER_BOUNDED_RUN_SECONDS="${WAIT_AFTER_BOUNDED_RUN_SECONDS:-75}"
WAIT_AFTER_BATCH_SECONDS="${WAIT_AFTER_BATCH_SECONDS:-10}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-1}"
MIN_GOLD_ROWS="${MIN_GOLD_ROWS:-1}"
MAX_FRESHNESS_MINUTES="${MAX_FRESHNESS_MINUTES:-10}"
MIN_EVENTS_CONFORMED_ROWS="${MIN_EVENTS_CONFORMED_ROWS:-1}"
EVENTS_CONFORMED_DATA_DATE="${EVENTS_CONFORMED_DATA_DATE:-}"
EVENTS_CONFORMED_RUN_ID="${EVENTS_CONFORMED_RUN_ID:-bt_events_conformed_$(date -u +%Y%m%dT%H%M%SZ)}"
MIN_PROCESSED_AT_MS="${MIN_PROCESSED_AT_MS:-$(( $(date +%s) * 1000 ))}"

resolve_bounded_run_started_at "BT-EVENTS-CONFORMED"

if [ -z "$EVENTS_CONFORMED_DATA_DATE" ]; then
  if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
    EVENTS_CONFORMED_DATA_DATE="${BOUNDED_RUN_EFFECTIVE_STARTED_AT:0:10}"
  else
    EVENTS_CONFORMED_DATA_DATE="$("$PYTHON_BIN" - <<PY
import json
from pathlib import Path

config_path = Path("${BOUNDED_RUN_CONFIG}")
if config_path.exists():
    cfg = json.loads(config_path.read_text())
    started_at = str(cfg.get("started_at", "")).strip()
    print(started_at[:10] if started_at else "")
else:
    print("")
PY
)"
  fi
fi

cd "$REPO_ROOT"
printf '[BT-EVENTS-CONFORMED] Assuming infra + streaming are up. Run: make infra && make streaming\n'

printf '[BT-EVENTS-CONFORMED] Running bounded generator...\n'
printf '[BT-EVENTS-CONFORMED] Generator run id: %s\n' "$EVENTS_CONFORMED_RUN_ID"
if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config "$BOUNDED_RUN_CONFIG" \
    --run-id "$EVENTS_CONFORMED_RUN_ID" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --started-at "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
else
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config "$BOUNDED_RUN_CONFIG" \
    --run-id "$EVENTS_CONFORMED_RUN_ID" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS"
fi

sleep "$WAIT_AFTER_BOUNDED_RUN_SECONDS"

printf '[BT-EVENTS-CONFORMED] Verifying raw_events readiness...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_aggregator.py \
  --min-raw-rows "$MIN_RAW_ROWS" \
  --min-gold-rows "$MIN_GOLD_ROWS" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --min-processed-at-ms "$MIN_PROCESSED_AT_MS"

printf '[BT-EVENTS-CONFORMED] Running events_conformed batch transform...\n'
docker cp src/spark/bt_events_conformed.py lakehouse-spark:/home/iceberg/local/src/spark/bt_events_conformed.py
docker cp src/spark/bt_events_conformed_sql.py lakehouse-spark:/home/iceberg/local/src/spark/bt_events_conformed_sql.py
docker cp src/scripts/verify_bt_events_conformed.py lakehouse-spark:/home/iceberg/local/src/scripts/verify_bt_events_conformed.py
if [ -n "$EVENTS_CONFORMED_DATA_DATE" ]; then
  docker exec lakehouse-spark bash -lc "BT_EVENTS_CONFORMED_DATA_DATE='${EVENTS_CONFORMED_DATA_DATE}' /opt/spark/bin/spark-submit /home/iceberg/local/src/spark/bt_events_conformed.py"
else
  docker exec lakehouse-spark bash -lc "/opt/spark/bin/spark-submit /home/iceberg/local/src/spark/bt_events_conformed.py"
fi
sleep "$WAIT_AFTER_BATCH_SECONDS"

printf '[BT-EVENTS-CONFORMED] Verifying events_conformed output contract...\n'
verify_args=(
  "--table" "lakehouse.silver.events_conformed"
  "--min-row-count" "$MIN_EVENTS_CONFORMED_ROWS"
)
if [ -n "$EVENTS_CONFORMED_DATA_DATE" ]; then
  verify_args+=("--data-date" "$EVENTS_CONFORMED_DATA_DATE")
fi
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_bt_events_conformed.py "${verify_args[@]}"

printf '[BT-EVENTS-CONFORMED] Acceptance flow completed.\n'
