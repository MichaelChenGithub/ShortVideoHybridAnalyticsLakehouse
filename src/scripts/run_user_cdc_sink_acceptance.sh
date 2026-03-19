#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_user_cdc_sink_acceptance.sh

Environment overrides:
  USER_ID
  BOOTSTRAP_SERVERS
  PYTHON_BIN
  WAIT_AFTER_JOB_START_SECONDS
  WAIT_AFTER_FIXTURE_SECONDS
  LOOKBACK_MINUTES
  MIN_INVALID_ROWS
  MIN_RAW_ROWS
  BASE_TS_MS
  EXPECTED_LATEST_STATE
  EXPECTED_LATEST_REGION
  EXPECTED_LATEST_TS_MS
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[USER-CDC-SINK] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

USER_ID="${USER_ID:-user_cdc_sink_u_001}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_FIXTURE_SECONDS="${WAIT_AFTER_FIXTURE_SECONDS:-75}"
LOOKBACK_MINUTES="${LOOKBACK_MINUTES:-30}"
MIN_INVALID_ROWS="${MIN_INVALID_ROWS:-4}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-2}"
BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"
EXPECTED_LATEST_STATE="${EXPECTED_LATEST_STATE:-returning}"
EXPECTED_LATEST_REGION="${EXPECTED_LATEST_REGION:-LATAM}"
EXPECTED_LATEST_TS_MS="${EXPECTED_LATEST_TS_MS:-$((BASE_TS_MS + 2000))}"
WARMUP_USER_ID="${WARMUP_USER_ID:-user_cdc_sink_warmup_$(date +%s)}"
WARMUP_WAIT_RETRIES="${WARMUP_WAIT_RETRIES:-45}"
WARMUP_WAIT_SLEEP_SECONDS="${WARMUP_WAIT_SLEEP_SECONDS:-2}"
POST_FIXTURE_BATCH_READY_RETRIES="${POST_FIXTURE_BATCH_READY_RETRIES:-45}"
POST_FIXTURE_BATCH_READY_SLEEP_SECONDS="${POST_FIXTURE_BATCH_READY_SLEEP_SECONDS:-2}"
USER_CDC_JOB_LOG="${USER_CDC_JOB_LOG:-/tmp/user_cdc_sink.log}"

emit_warmup_user_cdc() {
  local warmup_user_id="$1"
  local warmup_ts_ms="$2"
  "$PYTHON_BIN" - <<PY
import json
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "${BOOTSTRAP_SERVERS}", "client.id": "user-cdc-warmup-emitter"})
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

printf '[USER-CDC-SINK] Starting required services...\n'
docker compose up -d minio minio-mc iceberg-rest zookeeper kafka spark

printf '[USER-CDC-SINK] Ensuring user CDC topic exists...\n'
for _ in 1 2 3 4 5; do
  if docker exec lakehouse-kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker exec lakehouse-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic cdc.users.profiles \
  --partitions 3 \
  --replication-factor 1

printf '[USER-CDC-SINK] Starting Spark user CDC raw sink job...\n'
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_user_cdc_raw.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
docker exec lakehouse-spark bash -lc "nohup /opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_user_cdc_raw.py > '${USER_CDC_JOB_LOG}' 2>&1 &"
sleep "$WAIT_AFTER_JOB_START_SECONDS"

printf '[USER-CDC-SINK] Warming up stream readiness via single valid user CDC record...\n'
batch_before_warmup="$(max_batch_id_from_log "${USER_CDC_JOB_LOG}" | tr -d '[:space:]')"
if [[ ! "$batch_before_warmup" =~ ^-?[0-9]+$ ]]; then
  batch_before_warmup=-1
fi
emit_warmup_user_cdc "$WARMUP_USER_ID" "$BASE_TS_MS"
if ! wait_for_min_batch_id "$USER_CDC_JOB_LOG" "$((batch_before_warmup + 1))" "$WARMUP_WAIT_RETRIES" "$WARMUP_WAIT_SLEEP_SECONDS"; then
  echo "[USER-CDC-SINK] ERROR: stream did not advance batch id for warmup event." >&2
  echo "[USER-CDC-SINK] Hint: inspect /tmp/user_cdc_sink.log in lakehouse-spark." >&2
  exit 1
fi

printf '[USER-CDC-SINK] Emitting deterministic mixed user CDC fixture...\n'
batch_before_fixture="$(max_batch_id_from_log "${USER_CDC_JOB_LOG}" | tr -d '[:space:]')"
if [[ ! "$batch_before_fixture" =~ ^-?[0-9]+$ ]]; then
  batch_before_fixture=-1
fi
"$PYTHON_BIN" src/scripts/emit_cdc_users_mixed_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --user-id "$USER_ID" \
  --base-ts-ms "$BASE_TS_MS"
if ! wait_for_min_batch_id "$USER_CDC_JOB_LOG" "$((batch_before_fixture + 1))" "$POST_FIXTURE_BATCH_READY_RETRIES" "$POST_FIXTURE_BATCH_READY_SLEEP_SECONDS"; then
  echo "[USER-CDC-SINK] ERROR: stream did not advance batch id after fixture emission." >&2
  echo "[USER-CDC-SINK] Hint: inspect ${USER_CDC_JOB_LOG} in lakehouse-spark." >&2
  exit 1
fi
sleep "$WAIT_AFTER_FIXTURE_SECONDS"

printf '[USER-CDC-SINK] Verifying raw user CDC bronze landing...\n'
docker exec \
  -e VERIFY_USER_ID="$USER_ID" \
  -e VERIFY_TABLE="lakehouse.bronze.raw_cdc_users" \
  -e VERIFY_MIN_RAW_ROWS="$MIN_RAW_ROWS" \
  -e VERIFY_EXPECTED_STATE="$EXPECTED_LATEST_STATE" \
  -e VERIFY_EXPECTED_REGION="$EXPECTED_LATEST_REGION" \
  -e VERIFY_EXPECTED_TS_MS="$EXPECTED_LATEST_TS_MS" \
  -e VERIFY_BASE_TS_MS="$BASE_TS_MS" \
  lakehouse-spark python - <<'PY'
import json
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

user_id = os.environ["VERIFY_USER_ID"]
table = os.environ["VERIFY_TABLE"]
min_raw_rows = int(os.environ["VERIFY_MIN_RAW_ROWS"])
expected_state = os.environ["VERIFY_EXPECTED_STATE"]
expected_region = os.environ["VERIFY_EXPECTED_REGION"]
expected_ts_ms = int(os.environ["VERIFY_EXPECTED_TS_MS"])
base_ts_ms = int(os.environ["VERIFY_BASE_TS_MS"])

spark = SparkSession.builder.appName("verify_user_cdc_raw_bronze").getOrCreate()
rows = [
    row.asDict(recursive=True)
    for row in spark.read.format("iceberg").load(table).filter(col("user_id") == user_id).collect()
]

errors = []
if len(rows) < min_raw_rows:
    errors.append(
        f"raw_cdc_users row count below threshold for user_id={user_id}: "
        f"row_count={len(rows)}, min_row_count={min_raw_rows}"
    )

required_fields = (
    "op",
    "ts_ms",
    "schema_version",
    "user_id",
    "new_vs_returning_user",
    "region",
    "source_topic",
    "source_partition",
    "source_offset",
    "kafka_timestamp",
    "raw_value",
    "ingested_at",
)
missing_required_rows = 0
latest_row = None
latest_key = None
for row in rows:
    if row.get("user_id") != user_id:
        errors.append(f"row user_id mismatch: expected={user_id}, actual={row.get('user_id')}")

    missing_fields = [field for field in required_fields if row.get(field) is None]
    if missing_fields:
        missing_required_rows += 1

    try:
        ts_ms = int(row.get("ts_ms"))
        source_offset = int(row.get("source_offset"))
    except (TypeError, ValueError):
        errors.append(
            "raw user CDC row has non-numeric ordering fields: "
            f"ts_ms={row.get('ts_ms')}, source_offset={row.get('source_offset')}"
        )
        continue

    if ts_ms < base_ts_ms:
        continue

    key = (ts_ms, source_offset)
    if latest_key is None or key > latest_key:
        latest_key = key
        latest_row = row

if missing_required_rows > 0:
    errors.append(
        f"raw user CDC required fields contain nulls: missing_required_rows={missing_required_rows}"
    )

if latest_row is None:
    errors.append("no raw user CDC row remained after applying source-ts scope")
else:
    if latest_row.get("new_vs_returning_user") != expected_state:
        errors.append(
            "latest raw user CDC state mismatch: "
            f"expected={expected_state}, actual={latest_row.get('new_vs_returning_user')}"
        )
    if latest_row.get("region") != expected_region:
        errors.append(
            f"latest raw user CDC region mismatch: expected={expected_region}, actual={latest_row.get('region')}"
        )

    try:
        latest_ts_ms = int(latest_row.get("ts_ms"))
    except (TypeError, ValueError):
        errors.append(f"latest raw user CDC ts_ms is not numeric: {latest_row.get('ts_ms')}")
    else:
        if latest_ts_ms != expected_ts_ms:
            errors.append(
                f"latest raw user CDC ts_ms mismatch: expected={expected_ts_ms}, actual={latest_ts_ms}"
            )

    if latest_row.get("op") not in {"c", "u"}:
        errors.append(f"latest raw user CDC op must be c/u, got {latest_row.get('op')}")

if errors:
    print("FAIL: raw user CDC bronze verification failed")
    for err in errors:
        print(f" - {err}")
    raise SystemExit(1)

print("PASS: raw user CDC bronze verification succeeded")
print(
    json.dumps(
        {
            "user_id": user_id,
            "row_count": len(rows),
            "latest_op": latest_row.get("op"),
            "latest_state": latest_row.get("new_vs_returning_user"),
            "latest_region": latest_row.get("region"),
            "latest_ts_ms": latest_row.get("ts_ms"),
            "latest_source_offset": latest_row.get("source_offset"),
            "source_topic": latest_row.get("source_topic"),
            "checked_at": datetime.utcnow().isoformat() + "Z",
        },
        sort_keys=True,
        default=str,
    )
)
PY

printf '[USER-CDC-SINK] Verifying invalid user CDC quarantine records...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_invalid_cdc_quarantine.py \
  --table lakehouse.bronze.invalid_events_cdc_users \
  --lookback-minutes "$LOOKBACK_MINUTES" \
  --min-row-count "$MIN_INVALID_ROWS" \
  --expect-error-codes CDC_MISSING_OP,CDC_UNSUPPORTED_OP,CDC_MISSING_SCHEMA_VERSION,CDC_MISSING_AFTER_USER_ID,CDC_MISSING_AFTER_USER_STATE

printf '[USER-CDC-SINK] Checking checkpoint files for both sinks...\n'
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_user_cdc_raw/raw_cdc_users/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_user_cdc_raw/invalid_events_cdc_users/v1 | head -n 40"

printf '[USER-CDC-SINK] Confirming query process is alive...\n'
docker exec lakehouse-spark bash -lc "pgrep -f rt_user_cdc_raw.py"

printf '[USER-CDC-SINK] Acceptance flow completed.\n'
