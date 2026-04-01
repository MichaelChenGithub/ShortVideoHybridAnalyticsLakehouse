#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<'EOF'
Usage: run_bt_user_activity_sessions_30m_acceptance.sh

Precondition: events_conformed and dim_users_scd2 tables must be populated.
Run run_bt_events_conformed_acceptance.sh and run_bt_dim_users_scd2_acceptance.sh first,
or use make integration-test which runs all scripts in order.

Environment overrides:
  BOUNDED_RUN_TIME_MODE
  BOUNDED_RUN_STARTED_AT
  BOUNDED_RUN_CONFIG
  WAIT_AFTER_BATCH_SECONDS
  MIN_ROW_COUNT
  SESSIONS_DATA_DATE
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[BT-SESSIONS-30M] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

BOUNDED_RUN_CONFIG="${BOUNDED_RUN_CONFIG:-docs/architecture/generator/examples/bounded_run_config.example.json}"
WAIT_AFTER_BATCH_SECONDS="${WAIT_AFTER_BATCH_SECONDS:-10}"
MIN_ROW_COUNT="${MIN_ROW_COUNT:-1}"
SESSIONS_DATA_DATE="${SESSIONS_DATA_DATE:-}"

resolve_bounded_run_started_at "BT-SESSIONS-30M"

if [ -z "$SESSIONS_DATA_DATE" ]; then
  if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
    SESSIONS_DATA_DATE="${BOUNDED_RUN_EFFECTIVE_STARTED_AT:0:10}"
  else
    SESSIONS_DATA_DATE="$(python3 - <<PY
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
printf '[BT-SESSIONS-30M] Assuming infrastructure is up. Run: make reset-infra\n'
printf '[BT-SESSIONS-30M] Assuming events_conformed and dim_users_scd2 are populated.\n'
printf '[BT-SESSIONS-30M] sessions_data_date=%s\n' "$SESSIONS_DATA_DATE"

printf '[BT-SESSIONS-30M] Syncing batch job scripts to Spark container...\n'
docker cp src/spark/bt_user_activity_sessions_30m.py \
  lakehouse-spark:/home/iceberg/local/src/spark/bt_user_activity_sessions_30m.py
docker cp src/spark/bt_user_activity_sessions_30m_sql.py \
  lakehouse-spark:/home/iceberg/local/src/spark/bt_user_activity_sessions_30m_sql.py
docker cp src/scripts/verify_bt_user_activity_sessions_30m.py \
  lakehouse-spark:/home/iceberg/local/src/scripts/verify_bt_user_activity_sessions_30m.py

printf '[BT-SESSIONS-30M] Running user_activity_sessions_30m batch transform...\n'
if [ -n "$SESSIONS_DATA_DATE" ]; then
  docker exec lakehouse-spark bash -lc \
    "BT_USER_ACTIVITY_SESSIONS_30M_DATA_DATE='${SESSIONS_DATA_DATE}' \
     /opt/spark/bin/spark-submit \
     /home/iceberg/local/src/spark/bt_user_activity_sessions_30m.py"
else
  docker exec lakehouse-spark bash -lc \
    "/opt/spark/bin/spark-submit \
     /home/iceberg/local/src/spark/bt_user_activity_sessions_30m.py"
fi
sleep "$WAIT_AFTER_BATCH_SECONDS"

printf '[BT-SESSIONS-30M] Verifying user_activity_sessions_30m output contract...\n'
verify_args=(
  "--table" "lakehouse.silver.user_activity_sessions_30m"
  "--min-row-count" "$MIN_ROW_COUNT"
)
if [ -n "$SESSIONS_DATA_DATE" ]; then
  verify_args+=("--data-date" "$SESSIONS_DATA_DATE")
fi
docker exec lakehouse-spark python \
  /home/iceberg/local/src/scripts/verify_bt_user_activity_sessions_30m.py \
  "${verify_args[@]}"

printf '[BT-SESSIONS-30M] Acceptance flow completed.\n'
