#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/acceptance_common.sh"

RESET_CHECKPOINTS="${RESET_CHECKPOINTS:-0}"

usage() {
  cat <<'EOF'
Usage: run_cdc_upsert_acceptance.sh [--reset-checkpoints]

Options:
  --reset-checkpoints  Remove CDC checkpoint paths before starting the Spark job.
  -h, --help           Show this help.

Equivalent env flags:
  RESET_CHECKPOINTS=1
  ACCEPTANCE_RESET_DOCKER=1

Environment overrides:
  VIDEO_ID
  BOOTSTRAP_SERVERS
  EXPECTED_STATUS
  PYTHON_BIN
  RT_VIDEO_CDC_STARTING_OFFSETS
  WAIT_AFTER_JOB_START_SECONDS
  WAIT_AFTER_FIXTURE_SECONDS
  KAFKA_READY_RETRIES
  KAFKA_READY_SLEEP_SECONDS
  KAFKA_START_RETRIES
  CDC_JOB_READY_RETRIES
  CDC_JOB_READY_SLEEP_SECONDS
  VERIFY_RETRIES
  VERIFY_SLEEP_SECONDS
  BASE_TS_MS
  EXPECTED_SOURCE_TS_MS
  MAX_FRESHNESS_MINUTES
  MIN_RAW_ROWS
  CDC_JOB_LOG
EOF
}

while (($# > 0)); do
  case "$1" in
    --reset-checkpoints)
      RESET_CHECKPOINTS=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[CDC-UPSERT] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
done

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
DEFAULT_PYTHON_BIN="python3"
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
  DEFAULT_PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
fi
PYTHON_BIN="${PYTHON_BIN:-$DEFAULT_PYTHON_BIN}"

VIDEO_ID="${VIDEO_ID:-cdc_upsert_$(date -u +%Y%m%dT%H%M%SZ)_vid_001}"
EXPECTED_STATUS="${EXPECTED_STATUS:-copyright_strike}"
RT_VIDEO_CDC_STARTING_OFFSETS="${RT_VIDEO_CDC_STARTING_OFFSETS:-latest}"
WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_FIXTURE_SECONDS="${WAIT_AFTER_FIXTURE_SECONDS:-75}"
KAFKA_READY_RETRIES="${KAFKA_READY_RETRIES:-60}"
KAFKA_READY_SLEEP_SECONDS="${KAFKA_READY_SLEEP_SECONDS:-2}"
KAFKA_START_RETRIES="${KAFKA_START_RETRIES:-4}"
CDC_JOB_READY_RETRIES="${CDC_JOB_READY_RETRIES:-24}"
CDC_JOB_READY_SLEEP_SECONDS="${CDC_JOB_READY_SLEEP_SECONDS:-5}"
VERIFY_RETRIES="${VERIFY_RETRIES:-30}"
VERIFY_SLEEP_SECONDS="${VERIFY_SLEEP_SECONDS:-10}"
BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"
EXPECTED_SOURCE_TS_MS="${EXPECTED_SOURCE_TS_MS:-$((BASE_TS_MS + 2000))}"
MAX_FRESHNESS_MINUTES="${MAX_FRESHNESS_MINUTES:-10}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-4}"
CDC_JOB_LOG="${CDC_JOB_LOG:-/tmp/cdc_upsert_acceptance.log}"

is_kafka_container_up() {
  docker ps --format '{{.Names}} {{.Status}}' | grep -Eq '^lakehouse-kafka Up'
}

ensure_kafka_ready_and_running() {
  local attempt
  for ((attempt=1; attempt<=KAFKA_START_RETRIES; attempt++)); do
    if ! is_kafka_container_up; then
      printf '[CDC-UPSERT] Kafka container not up (attempt %s/%s); starting kafka...\n' "$attempt" "$KAFKA_START_RETRIES"
      docker compose up -d kafka
      sleep 5
    fi

    if wait_for_kafka_ready "CDC-UPSERT" "$KAFKA_READY_RETRIES" "$KAFKA_READY_SLEEP_SECONDS"; then
      return 0
    fi

    printf '[CDC-UPSERT] Kafka readiness failed on attempt %s/%s; recent kafka logs:\n' "$attempt" "$KAFKA_START_RETRIES"
    docker logs --tail 80 lakehouse-kafka || true
  done

  echo "[CDC-UPSERT] ERROR: Kafka failed to stay ready after retries." >&2
  return 1
}

wait_for_cdc_upsert_job_alive() {
  local attempt
  for ((attempt=1; attempt<=CDC_JOB_READY_RETRIES; attempt++)); do
    if docker exec lakehouse-spark bash -lc "pgrep -f rt_video_cdc_upsert.py" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$CDC_JOB_READY_SLEEP_SECONDS"
  done

  echo "[CDC-UPSERT] ERROR: CDC upsert Spark job did not stay alive after startup." >&2
  return 1
}

stop_cdc_job_if_running() {
  docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_video_cdc_upsert.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
}

tail_cdc_job_log() {
  docker exec lakehouse-spark bash -lc "tail -n 200 '$CDC_JOB_LOG'" || true
}

reset_cdc_checkpoints() {
  printf '[CDC-UPSERT] Resetting CDC checkpoint directories...\n'
  docker exec lakehouse-minio sh -lc "rm -rf \
    /data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1 \
    /data/checkpoints/jobs/spark_rt_video_cdc_upsert/raw_cdc_videos/v1 \
    /data/checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1"
}

run_dim_verifier() {
  local output=""
  local attempt

  for ((attempt=1; attempt<=VERIFY_RETRIES; attempt++)); do
    if output="$(
      docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_upsert.py \
        --video-id "$VIDEO_ID" \
        --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
        --expect-status "$EXPECTED_STATUS" \
        --expect-source-ts-ms "$EXPECTED_SOURCE_TS_MS" \
        2>&1
    )"; then
      printf '%s\n' "$output"
      return 0
    fi
    sleep "$VERIFY_SLEEP_SECONDS"
  done

  echo "[CDC-UPSERT] ERROR: dim_videos verification did not pass after retries." >&2
  printf '%s\n' "$output" >&2
  return 1
}

run_raw_verifier() {
  local output=""
  local attempt

  for ((attempt=1; attempt<=VERIFY_RETRIES; attempt++)); do
    if output="$(
      docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_raw_bronze.py \
        --video-id "$VIDEO_ID" \
        --table lakehouse.bronze.raw_cdc_videos \
        --min-row-count "$MIN_RAW_ROWS" \
        --expect-status "$EXPECTED_STATUS" \
        --expect-latest-ts-ms "$EXPECTED_SOURCE_TS_MS" \
        --min-source-ts-ms "$BASE_TS_MS" \
        2>&1
    )"; then
      printf '%s\n' "$output"
      return 0
    fi
    sleep "$VERIFY_SLEEP_SECONDS"
  done

  echo "[CDC-UPSERT] ERROR: raw CDC bronze verification did not pass after retries." >&2
  printf '%s\n' "$output" >&2
  return 1
}

assert_checkpoint_files_exist() {
  local checkpoint_path="$1"
  local label="$2"
  local count
  local listing

  listing="$(docker exec lakehouse-minio sh -lc "ls -R '$checkpoint_path' 2>/dev/null" || true)"
  count="$(printf '%s\n' "$listing" | awk 'NF && $0 !~ /:$/ {count++} END {print count + 0}')"
  if [[ ! "$count" =~ ^[0-9]+$ ]] || [ "$count" -eq 0 ]; then
    echo "[CDC-UPSERT] ERROR: no checkpoint entries found for $label at $checkpoint_path" >&2
    printf '%s\n' "$listing" >&2
    return 1
  fi

  printf '[CDC-UPSERT] PASS: %s checkpoint entries=%s (%s)\n' "$label" "$count" "$checkpoint_path"
  printf '%s\n' "$listing" | awk 'NF {print}' | head -n 40
}

cd "$REPO_ROOT"
acceptance_maybe_reset_docker "CDC-UPSERT"

printf '[CDC-UPSERT] video_id=%s\n' "$VIDEO_ID"
printf '[CDC-UPSERT] bootstrap_servers=%s\n' "$BOOTSTRAP_SERVERS"
printf '[CDC-UPSERT] expected_status=%s\n' "$EXPECTED_STATUS"
printf '[CDC-UPSERT] expected_source_ts_ms=%s\n' "$EXPECTED_SOURCE_TS_MS"
printf '[CDC-UPSERT] starting_offsets=%s\n' "$RT_VIDEO_CDC_STARTING_OFFSETS"
printf '[CDC-UPSERT] python_bin=%s\n' "$PYTHON_BIN"
printf '[CDC-UPSERT] reset_checkpoints=%s\n' "$RESET_CHECKPOINTS"

printf '[CDC-UPSERT] Starting required services...\n'
docker compose up -d minio minio-mc catalog-postgres iceberg-rest zookeeper
sleep 10
docker compose up -d kafka
ensure_kafka_ready_and_running
docker compose up -d spark

printf '[CDC-UPSERT] Ensuring CDC topic exists...\n'
docker exec lakehouse-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic cdc.content.videos \
  --partitions 3 \
  --replication-factor 1

if [ "$RESET_CHECKPOINTS" = "1" ]; then
  reset_cdc_checkpoints
fi

printf '[CDC-UPSERT] Starting Spark CDC upsert job...\n'
stop_cdc_job_if_running
docker exec lakehouse-spark bash -lc "rm -f '$CDC_JOB_LOG' && rm -rf /tmp/spark-* /tmp/blockmgr-* && mkdir -p /tmp/ivy/cdc_upsert_acceptance"
docker exec lakehouse-spark bash -lc "RT_VIDEO_CDC_STARTING_OFFSETS='$RT_VIDEO_CDC_STARTING_OFFSETS' nohup /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy='/tmp/ivy/cdc_upsert_acceptance' \
  /home/iceberg/local/src/spark/rt_video_cdc_upsert.py > '$CDC_JOB_LOG' 2>&1 &"

if ! wait_for_cdc_upsert_job_alive; then
  tail_cdc_job_log
  exit 1
fi

# Give the streaming query time to subscribe before producing latest-offset test data.
sleep "$WAIT_AFTER_JOB_START_SECONDS"

printf '[CDC-UPSERT] Emitting deterministic CDC fixture...\n'
"$PYTHON_BIN" src/scripts/emit_cdc_videos_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --video-id "$VIDEO_ID" \
  --scenario full \
  --base-ts-ms "$BASE_TS_MS"

sleep "$WAIT_AFTER_FIXTURE_SECONDS"

printf '[CDC-UPSERT] Verifying dim_videos latest row...\n'
if ! run_dim_verifier; then
  tail_cdc_job_log
  exit 1
fi

printf '[CDC-UPSERT] Verifying raw CDC bronze landing...\n'
if ! run_raw_verifier; then
  tail_cdc_job_log
  exit 1
fi

printf '[CDC-UPSERT] Checking checkpoint evidence...\n'
assert_checkpoint_files_exist "/data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1" "dim_videos"
assert_checkpoint_files_exist "/data/checkpoints/jobs/spark_rt_video_cdc_upsert/raw_cdc_videos/v1" "raw_cdc_videos"

printf '[CDC-UPSERT] Confirming query process is alive...\n'
docker exec lakehouse-spark bash -lc "pgrep -f rt_video_cdc_upsert.py"

printf '[CDC-UPSERT] Acceptance flow completed.\n'
