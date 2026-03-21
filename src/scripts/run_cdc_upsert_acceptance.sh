#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/acceptance_common.sh"

usage() {
  cat <<'EOF'
Usage: run_cdc_upsert_acceptance.sh

Environment overrides:
  ACCEPTANCE_RESET_DOCKER
  BOUNDED_RUN_TIME_MODE
  BOUNDED_RUN_STARTED_AT
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
  CDC_JOB_START_RETRIES
  CDC_JOB_START_SLEEP_SECONDS
  TRINO_READY_RETRIES
  TRINO_READY_SLEEP_SECONDS
  TRINO_VERIFY_RETRIES
  TRINO_VERIFY_SLEEP_SECONDS
  BASE_TS_MS
  EXPECTED_SOURCE_TS_MS
  MIN_RAW_ROWS
EOF
}

if (($# > 0)); then
  case "$1" in
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
fi

VIDEO_ID="${VIDEO_ID:-cdc_upsert_vid_001}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
EXPECTED_STATUS="${EXPECTED_STATUS:-copyright_strike}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
RT_VIDEO_CDC_STARTING_OFFSETS="${RT_VIDEO_CDC_STARTING_OFFSETS:-earliest}"
WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_FIXTURE_SECONDS="${WAIT_AFTER_FIXTURE_SECONDS:-75}"
KAFKA_READY_RETRIES="${KAFKA_READY_RETRIES:-60}"
KAFKA_READY_SLEEP_SECONDS="${KAFKA_READY_SLEEP_SECONDS:-2}"
KAFKA_START_RETRIES="${KAFKA_START_RETRIES:-4}"
CDC_JOB_START_RETRIES="${CDC_JOB_START_RETRIES:-24}"
CDC_JOB_START_SLEEP_SECONDS="${CDC_JOB_START_SLEEP_SECONDS:-5}"
TRINO_READY_RETRIES="${TRINO_READY_RETRIES:-45}"
TRINO_READY_SLEEP_SECONDS="${TRINO_READY_SLEEP_SECONDS:-2}"
TRINO_VERIFY_RETRIES="${TRINO_VERIFY_RETRIES:-30}"
TRINO_VERIFY_SLEEP_SECONDS="${TRINO_VERIFY_SLEEP_SECONDS:-10}"
BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"
EXPECTED_SOURCE_TS_MS="${EXPECTED_SOURCE_TS_MS:-$((BASE_TS_MS + 2000))}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-4}"

resolve_bounded_run_started_at "CDC-UPSERT"

cd "$REPO_ROOT"
acceptance_maybe_reset_docker "CDC-UPSERT"

is_kafka_container_up() {
  docker ps --format '{{.Names}} {{.Status}}' | grep -Eq '^lakehouse-kafka Up'
}

ensure_kafka_ready_and_running() {
  local attempt
  for ((attempt=1; attempt<=KAFKA_START_RETRIES; attempt++)); do
    if ! is_kafka_container_up; then
      printf '[CDC-UPSERT] Kafka container not up (attempt %s/%s); restarting kafka...\n' "$attempt" "$KAFKA_START_RETRIES"
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
  for ((attempt=1; attempt<=CDC_JOB_START_RETRIES; attempt++)); do
    if docker exec lakehouse-spark bash -lc "pgrep -f rt_video_cdc_upsert.py" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$CDC_JOB_START_SLEEP_SECONDS"
  done
  return 1
}

wait_for_trino_ready() {
  local attempt
  for ((attempt=1; attempt<=TRINO_READY_RETRIES; attempt++)); do
    if docker exec lakehouse-trino trino --output-format CSV_UNQUOTED --execute "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$TRINO_READY_SLEEP_SECONDS"
  done
  echo "[CDC-UPSERT] ERROR: Trino did not become ready after retries." >&2
  return 1
}

printf '[CDC-UPSERT] Starting base services (without kafka/spark)...\n'
docker compose up -d minio minio-mc catalog-postgres iceberg-rest zookeeper
sleep 10
printf '[CDC-UPSERT] Starting kafka...\n'
docker compose up -d kafka
ensure_kafka_ready_and_running
printf '[CDC-UPSERT] Starting spark...\n'
docker compose up -d spark
printf '[CDC-UPSERT] Starting trino...\n'
docker compose up -d trino
wait_for_trino_ready

printf '[CDC-UPSERT] Ensuring topic exists...\n'
ensure_kafka_ready_and_running
docker exec lakehouse-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic cdc.content.videos \
  --partitions 3 \
  --replication-factor 1

printf '[CDC-UPSERT] Starting Spark CDC upsert job...\n'
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_video_cdc_upsert.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
docker exec lakehouse-spark bash -lc "rm -rf /tmp/spark-* /tmp/blockmgr-* || true"
docker exec lakehouse-spark bash -lc "rm -f /root/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar /root/.ivy2/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.5.0.jar || true; rm -rf /root/.ivy2/cache/com.amazonaws/aws-java-sdk-bundle /root/.ivy2/cache/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12 || true"
docker exec lakehouse-spark bash -lc "mkdir -p /tmp/ivy/cdc_upsert && RT_VIDEO_CDC_STARTING_OFFSETS='${RT_VIDEO_CDC_STARTING_OFFSETS}' nohup /opt/spark/bin/spark-submit --conf spark.jars.ivy='/tmp/ivy/cdc_upsert' /home/iceberg/local/src/spark/rt_video_cdc_upsert.py > /tmp/cdc_upsert_cdc_upsert.log 2>&1 &"
if ! wait_for_cdc_upsert_job_alive; then
  echo "[CDC-UPSERT] ERROR: CDC upsert Spark job did not stay alive after startup." >&2
  docker exec lakehouse-spark bash -lc "tail -n 200 /tmp/cdc_upsert_cdc_upsert.log" || true
  exit 1
fi
sleep "$WAIT_AFTER_JOB_START_SECONDS"

printf '[CDC-UPSERT] Running bounded generator...\n'
if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config docs/architecture/generator/examples/bounded_run_config.example.json \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --started-at "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
else
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config docs/architecture/generator/examples/bounded_run_config.example.json \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS"
fi

printf '[CDC-UPSERT] Emitting deterministic CDC fixture...\n'
ensure_kafka_ready_and_running
"$PYTHON_BIN" src/scripts/emit_cdc_videos_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --video-id "$VIDEO_ID" \
  --scenario full \
  --base-ts-ms "$BASE_TS_MS"
sleep "$WAIT_AFTER_FIXTURE_SECONDS"

printf '[CDC-UPSERT] Verifying upsert result via Trino...\n'
upsert_ready=0
for _ in $(seq 1 "$TRINO_VERIFY_RETRIES"); do
  dim_row="$(docker exec lakehouse-trino trino --output-format CSV_UNQUOTED --execute \
    "SELECT status, source_ts_ms \
     FROM lakehouse.dims.dim_videos \
     WHERE video_id='${VIDEO_ID}' \
     ORDER BY source_ts_ms DESC \
     LIMIT 1" 2>/dev/null | tail -n 1 || true)"
  if [ -n "$dim_row" ]; then
    dim_status="${dim_row%%,*}"
    dim_source_ts="${dim_row##*,}"
    if [ "$dim_status" = "$EXPECTED_STATUS" ] && [ "$dim_source_ts" = "$EXPECTED_SOURCE_TS_MS" ]; then
      upsert_ready=1
      break
    fi
  fi
  sleep "$TRINO_VERIFY_SLEEP_SECONDS"
done

if [ "$upsert_ready" -ne 1 ]; then
  echo "[CDC-UPSERT] ERROR: dim_videos upsert output not ready after retries." >&2
  echo "[CDC-UPSERT] Last observed dim_videos row: ${dim_row:-<empty>}" >&2
  docker exec lakehouse-spark bash -lc "tail -n 200 /tmp/cdc_upsert_cdc_upsert.log" || true
  exit 1
fi
printf '[CDC-UPSERT] PASS: dim_videos latest row status=%s source_ts_ms=%s\n' "$dim_status" "$dim_source_ts"

printf '[CDC-UPSERT] Verifying raw CDC bronze landing via Trino...\n'
raw_ready=0
for _ in $(seq 1 "$TRINO_VERIFY_RETRIES"); do
  raw_row="$(docker exec lakehouse-trino trino --output-format CSV_UNQUOTED --execute \
    "SELECT CAST(COUNT(*) AS BIGINT), CAST(COALESCE(MAX(ts_ms), 0) AS BIGINT) \
     FROM lakehouse.bronze.raw_cdc_videos \
     WHERE video_id='${VIDEO_ID}' \
       AND status='${EXPECTED_STATUS}' \
       AND ts_ms >= ${BASE_TS_MS}" 2>/dev/null | tail -n 1 || true)"
  if [ -n "$raw_row" ]; then
    raw_count="${raw_row%%,*}"
    raw_latest_ts="${raw_row##*,}"
    if [ "${raw_count:-0}" -ge "$MIN_RAW_ROWS" ] && [ "${raw_latest_ts:-0}" = "$EXPECTED_SOURCE_TS_MS" ]; then
      raw_ready=1
      break
    fi
  fi
  sleep "$TRINO_VERIFY_SLEEP_SECONDS"
done

if [ "$raw_ready" -ne 1 ]; then
  echo "[CDC-UPSERT] ERROR: raw CDC bronze output not ready after retries." >&2
  echo "[CDC-UPSERT] Last observed raw row metrics: ${raw_row:-<empty>}" >&2
  docker exec lakehouse-spark bash -lc "tail -n 200 /tmp/cdc_upsert_cdc_upsert.log" || true
  exit 1
fi
printf '[CDC-UPSERT] PASS: raw_cdc_videos count=%s latest_ts_ms=%s\n' "$raw_count" "$raw_latest_ts"

printf '[CDC-UPSERT] Checking checkpoint files...\n'
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_video_cdc_upsert/raw_cdc_videos/v1 | head -n 40"

printf '[CDC-UPSERT] Confirming query process is alive...\n'
docker exec lakehouse-spark bash -lc "pgrep -f rt_video_cdc_upsert.py"

printf '[CDC-UPSERT] Acceptance flow completed.\n'
