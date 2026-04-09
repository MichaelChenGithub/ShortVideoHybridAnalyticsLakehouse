#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<'EOF'
Usage: run_bt_dim_users_scd2_acceptance.sh

Environment overrides:
  BOUNDED_RUN_TIME_MODE
  BOUNDED_RUN_STARTED_AT
  USER_ID
  BOOTSTRAP_SERVERS
  PYTHON_BIN
  WAIT_AFTER_FIXTURE_SECONDS
  WAIT_AFTER_BATCH_SECONDS
  RAW_READY_RETRIES
  RAW_READY_SLEEP_SECONDS
  WARMUP_USER_ID
  WARMUP_WAIT_RETRIES
  WARMUP_WAIT_SLEEP_SECONDS
  POST_FIXTURE_BATCH_READY_RETRIES
  POST_FIXTURE_BATCH_READY_SLEEP_SECONDS
  USER_CDC_JOB_LOG
  BOUNDED_RUN_CONFIG
  BOUNDED_RUN_SINK
  BASE_TS_MS
  EXPECTED_LATEST_STATE
  EXPECTED_LATEST_REGION
  EXPECTED_LATEST_TS_MS
  PROBE_OLD_TS_MS
  PROBE_NEW_TS_MS
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[BT-DIM-USERS-SCD2] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

USER_ID="${USER_ID:-bt_dim_user_001}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
WAIT_AFTER_FIXTURE_SECONDS="${WAIT_AFTER_FIXTURE_SECONDS:-75}"
WAIT_AFTER_BATCH_SECONDS="${WAIT_AFTER_BATCH_SECONDS:-10}"
RAW_READY_RETRIES="${RAW_READY_RETRIES:-18}"
RAW_READY_SLEEP_SECONDS="${RAW_READY_SLEEP_SECONDS:-10}"
WARMUP_USER_ID="${WARMUP_USER_ID:-bt_dim_users_scd2_warmup_$(date +%s)}"
WARMUP_WAIT_RETRIES="${WARMUP_WAIT_RETRIES:-45}"
WARMUP_WAIT_SLEEP_SECONDS="${WARMUP_WAIT_SLEEP_SECONDS:-2}"
POST_FIXTURE_BATCH_READY_RETRIES="${POST_FIXTURE_BATCH_READY_RETRIES:-45}"
POST_FIXTURE_BATCH_READY_SLEEP_SECONDS="${POST_FIXTURE_BATCH_READY_SLEEP_SECONDS:-2}"
USER_CDC_JOB_LOG="${USER_CDC_JOB_LOG:-/tmp/streaming_user.log}"
BOUNDED_RUN_CONFIG="${BOUNDED_RUN_CONFIG:-docs/architecture/generator/examples/bounded_run_config.example.json}"
BOUNDED_RUN_SINK="${BOUNDED_RUN_SINK:-kafka}"
BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"
EXPECTED_LATEST_STATE="${EXPECTED_LATEST_STATE:-returning}"
EXPECTED_LATEST_REGION="${EXPECTED_LATEST_REGION:-LATAM}"
EXPECTED_LATEST_TS_MS="${EXPECTED_LATEST_TS_MS:-$((BASE_TS_MS + 2000))}"
PROBE_OLD_TS_MS="${PROBE_OLD_TS_MS:-$((BASE_TS_MS + 1000))}"
PROBE_NEW_TS_MS="${PROBE_NEW_TS_MS:-$((BASE_TS_MS + 2000))}"
resolve_bounded_run_started_at "BT-DIM-USERS-SCD2"

emit_warmup_user_cdc() {
  local warmup_user_id="$1"
  local warmup_ts_ms="$2"
  "$PYTHON_BIN" - <<PY
import json
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "${BOOTSTRAP_SERVERS}", "client.id": "bt-dim-users-scd2-warmup"})
payload = {
    "op": "c",
    "ts_ms": ${warmup_ts_ms},
    "schema_version": "m2_v1",
    "after": {
        "user_id": "${warmup_user_id}",
        "new_vs_returning_user": "new",
        "region": "NA",
    },
}
producer.produce("cdc.users.profiles", key="${warmup_user_id}", value=json.dumps(payload, sort_keys=True))
producer.flush()
print(json.dumps({"warmup_user_id": "${warmup_user_id}", "warmup_ts_ms": ${warmup_ts_ms}}, sort_keys=True))
PY
}

max_batch_id_from_log() {
  local log_file="$1"
  docker exec lakehouse-spark bash -lc "if [ -f '${log_file}' ]; then awk '{for (i=1; i<=NF; i++) if (\$i==\"Batch\" && (i+1)<=NF) {n=\$(i+1); gsub(/[^0-9]/, \"\", n); if ((n+0)>max) max=(n+0)}} END{if (max==\"\") print -1; else print max}' '${log_file}'; else echo -1; fi"
}

wait_for_min_batch_id() {
  local log_file="$1"
  local min_batch_id="$2"
  local retries="$3"
  local sleep_seconds="$4"
  local current_batch_id
  for ((attempt=1; attempt<=retries; attempt++)); do
    current_batch_id="$(max_batch_id_from_log "${log_file}" | tr -d '[:space:]')"
    if [[ "$current_batch_id" =~ ^-?[0-9]+$ ]] && [ "$current_batch_id" -ge "$min_batch_id" ]; then
      return 0
    fi
    sleep "$sleep_seconds"
  done
  return 1
}

cd "$REPO_ROOT"
printf '[BT-DIM-USERS-SCD2] Assuming infra + streaming are up. Run: make infra && make streaming\n'

printf '[BT-DIM-USERS-SCD2] Warming up stream readiness via single valid user CDC record...\n'
batch_before_warmup="$(max_batch_id_from_log "${USER_CDC_JOB_LOG}" | tr -d '[:space:]')"
if [[ ! "$batch_before_warmup" =~ ^-?[0-9]+$ ]]; then
  batch_before_warmup=-1
fi

printf '[BT-DIM-USERS-SCD2] Running bounded generator...\n'
if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config "$BOUNDED_RUN_CONFIG" \
    --sink "$BOUNDED_RUN_SINK" \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --started-at "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
else
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config "$BOUNDED_RUN_CONFIG" \
    --sink "$BOUNDED_RUN_SINK" \
    --bootstrap-servers "$BOOTSTRAP_SERVERS"
fi

emit_warmup_user_cdc "$WARMUP_USER_ID" "$BASE_TS_MS"
if ! wait_for_min_batch_id "$USER_CDC_JOB_LOG" "$((batch_before_warmup + 1))" "$WARMUP_WAIT_RETRIES" "$WARMUP_WAIT_SLEEP_SECONDS"; then
  echo "[BT-DIM-USERS-SCD2] ERROR: stream did not advance batch id for warmup event." >&2
  echo "[BT-DIM-USERS-SCD2] Hint: inspect ${USER_CDC_JOB_LOG} in lakehouse-spark." >&2
  exit 1
fi

printf '[BT-DIM-USERS-SCD2] Emitting deterministic user CDC fixture...\n'
batch_before_fixture="$(max_batch_id_from_log "${USER_CDC_JOB_LOG}" | tr -d '[:space:]')"
if [[ ! "$batch_before_fixture" =~ ^-?[0-9]+$ ]]; then
  batch_before_fixture=-1
fi
"$PYTHON_BIN" src/scripts/emit_cdc_users_mixed_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --user-id "$USER_ID" \
  --base-ts-ms "$BASE_TS_MS"
if ! wait_for_min_batch_id "$USER_CDC_JOB_LOG" "$((batch_before_fixture + 1))" "$POST_FIXTURE_BATCH_READY_RETRIES" "$POST_FIXTURE_BATCH_READY_SLEEP_SECONDS"; then
  echo "[BT-DIM-USERS-SCD2] ERROR: stream did not advance batch id after fixture emission." >&2
  echo "[BT-DIM-USERS-SCD2] Hint: inspect ${USER_CDC_JOB_LOG} in lakehouse-spark." >&2
  exit 1
fi
sleep "$WAIT_AFTER_FIXTURE_SECONDS"

printf '[BT-DIM-USERS-SCD2] Waiting for raw CDC landing readiness...\n'
docker cp src/scripts/verify_rt_user_cdc_raw_bronze.py lakehouse-spark:/home/iceberg/local/src/scripts/verify_rt_user_cdc_raw_bronze.py
raw_ready=0
for _ in $(seq 1 "$RAW_READY_RETRIES"); do
  if docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_user_cdc_raw_bronze.py \
    --user-id "$USER_ID" \
    --table lakehouse.bronze.raw_cdc_users \
    --min-row-count 1 \
    --expect-state "$EXPECTED_LATEST_STATE" \
    --expect-region "$EXPECTED_LATEST_REGION" \
    --expect-latest-ts-ms "$EXPECTED_LATEST_TS_MS" \
    --min-source-ts-ms "$BASE_TS_MS" >/dev/null 2>&1
  then
    raw_ready=1
    break
  fi
  sleep "$RAW_READY_SLEEP_SECONDS"
done

if [ "$raw_ready" -ne 1 ]; then
  echo "[BT-DIM-USERS-SCD2] ERROR: raw CDC rows not ready after retries." >&2
  docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_user_cdc_raw_bronze.py \
    --user-id "$USER_ID" \
    --table lakehouse.bronze.raw_cdc_users \
    --min-row-count 1 \
    --expect-state "$EXPECTED_LATEST_STATE" \
    --expect-region "$EXPECTED_LATEST_REGION" \
    --expect-latest-ts-ms "$EXPECTED_LATEST_TS_MS" \
    --min-source-ts-ms "$BASE_TS_MS"
  exit 1
fi

printf '[BT-DIM-USERS-SCD2] Running dim_users_scd2 batch transform...\n'
docker cp src/spark/bt_dim_users_scd2.py lakehouse-spark:/home/iceberg/local/src/spark/bt_dim_users_scd2.py
docker cp src/spark/bt_dim_users_scd2_sql.py lakehouse-spark:/home/iceberg/local/src/spark/bt_dim_users_scd2_sql.py
docker cp src/scripts/verify_bt_dim_users_scd2.py lakehouse-spark:/home/iceberg/local/src/scripts/verify_bt_dim_users_scd2.py
docker exec lakehouse-spark bash -lc "/opt/spark/bin/spark-submit /home/iceberg/local/src/spark/bt_dim_users_scd2.py"
sleep "$WAIT_AFTER_BATCH_SECONDS"

printf '[BT-DIM-USERS-SCD2] Verifying dim_users_scd2 output contract...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_bt_dim_users_scd2.py \
  --user-id "$USER_ID" \
  --table lakehouse.dims.dim_users_scd2 \
  --expect-latest-state "$EXPECTED_LATEST_STATE" \
  --expect-latest-region "$EXPECTED_LATEST_REGION" \
  --probe-old-ts-ms "$PROBE_OLD_TS_MS" \
  --expect-state-at-old new \
  --probe-new-ts-ms "$PROBE_NEW_TS_MS" \
  --expect-state-at-new "$EXPECTED_LATEST_STATE"

printf '[BT-DIM-USERS-SCD2] Acceptance flow completed.\n'
