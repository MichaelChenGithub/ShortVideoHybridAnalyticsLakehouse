#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RESET_CHECKPOINTS="${RESET_CHECKPOINTS:-0}"
PRINT_MAINTENANCE_HINT="${PRINT_MAINTENANCE_HINT:-0}"

usage() {
  cat <<'EOF'
Usage: run_mic38_observe.sh [--reset-checkpoints] [--maintenance-hint]

Options:
  --reset-checkpoints   Remove streaming checkpoint paths before restarting jobs.
  --maintenance-hint    Print suggested Iceberg maintenance SQL after startup.
  -h, --help            Show this help.

Equivalent env flags:
  RESET_CHECKPOINTS=1
  PRINT_MAINTENANCE_HINT=1

Resource-bound env flags (optional overrides):
  MIC38_SPARK_DRIVER_CORES
  MIC38_SPARK_DRIVER_MEMORY
  MIC38_SPARK_DRIVER_MEMORY_OVERHEAD
  MIC38_SPARK_EXECUTOR_INSTANCES
  MIC38_SPARK_EXECUTOR_CORES
  MIC38_SPARK_EXECUTOR_MEMORY
  MIC38_SPARK_EXECUTOR_MEMORY_OVERHEAD
  MIC38_SPARK_CORES_MAX
  MIC38_SPARK_SQL_SHUFFLE_PARTITIONS
  MIC38_SPARK_DEFAULT_PARALLELISM

Readiness tuning env flags (optional overrides):
  STREAM_BATCH_READY_RETRIES
  STREAM_BATCH_READY_SLEEP_SECONDS
EOF
}

while (($# > 0)); do
  case "$1" in
    --reset-checkpoints)
      RESET_CHECKPOINTS=1
      shift
      ;;
    --maintenance-hint)
      PRINT_MAINTENANCE_HINT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[MIC-38-OBSERVE] ERROR: unknown argument '$1'" >&2
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

SPARK_DRIVER_CORES="${MIC38_SPARK_DRIVER_CORES:-1}"
SPARK_DRIVER_MEMORY="${MIC38_SPARK_DRIVER_MEMORY:-1g}"
SPARK_DRIVER_MEMORY_OVERHEAD="${MIC38_SPARK_DRIVER_MEMORY_OVERHEAD:-512m}"
SPARK_EXECUTOR_INSTANCES="${MIC38_SPARK_EXECUTOR_INSTANCES:-1}"
SPARK_EXECUTOR_CORES="${MIC38_SPARK_EXECUTOR_CORES:-1}"
SPARK_EXECUTOR_MEMORY="${MIC38_SPARK_EXECUTOR_MEMORY:-1g}"
SPARK_EXECUTOR_MEMORY_OVERHEAD="${MIC38_SPARK_EXECUTOR_MEMORY_OVERHEAD:-512m}"
SPARK_CORES_MAX="${MIC38_SPARK_CORES_MAX:-2}"
SPARK_SQL_SHUFFLE_PARTITIONS="${MIC38_SPARK_SQL_SHUFFLE_PARTITIONS:-8}"
SPARK_DEFAULT_PARALLELISM="${MIC38_SPARK_DEFAULT_PARALLELISM:-8}"

MIC38_RUN_ID="${MIC38_RUN_ID:-mic38_observe_$(date -u +%Y%m%dT%H%M%SZ)}"
MIC38_VIDEO_ID="${MIC38_VIDEO_ID:-${MIC38_RUN_ID}_cdc_vid_001}"
CONTENT_JOB_PATTERN="[r]t_content_events_aggregator.py"
CDC_JOB_PATTERN="[r]t_video_cdc_upsert.py"
STARTED_SPARK_JOBS=0

WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_BOUNDED_RUN_SECONDS="${WAIT_AFTER_BOUNDED_RUN_SECONDS:-75}"
WAIT_AFTER_CDC_FIXTURE_SECONDS="${WAIT_AFTER_CDC_FIXTURE_SECONDS:-75}"

KAFKA_READY_RETRIES="${KAFKA_READY_RETRIES:-30}"
KAFKA_READY_SLEEP_SECONDS="${KAFKA_READY_SLEEP_SECONDS:-2}"
SPARK_JOB_READY_RETRIES="${SPARK_JOB_READY_RETRIES:-10}"
SPARK_JOB_READY_SLEEP_SECONDS="${SPARK_JOB_READY_SLEEP_SECONDS:-3}"
STREAM_BATCH_READY_RETRIES="${STREAM_BATCH_READY_RETRIES:-120}"
STREAM_BATCH_READY_SLEEP_SECONDS="${STREAM_BATCH_READY_SLEEP_SECONDS:-2}"

BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"

CONTENT_JOB_LOG="/tmp/${MIC38_RUN_ID}_content_agg.log"
CDC_JOB_LOG="/tmp/${MIC38_RUN_ID}_cdc_upsert.log"

wait_for_kafka_ready() {
  local retries="$1"
  local sleep_seconds="$2"
  for ((attempt=1; attempt<=retries; attempt++)); do
    if docker exec lakehouse-kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_seconds"
  done
  echo "[MIC-38-OBSERVE] ERROR: Kafka did not become ready after ${retries} attempts." >&2
  return 1
}

ensure_topic() {
  local topic="$1"
  local partitions="$2"
  docker exec lakehouse-kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
  docker exec lakehouse-kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --alter \
    --topic "$topic" \
    --partitions "$partitions" || true
}

stop_spark_job_if_running() {
  local pattern="$1"
  docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/${pattern}/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
}

stop_mic38_spark_jobs() {
  stop_spark_job_if_running "$CONTENT_JOB_PATTERN"
  stop_spark_job_if_running "$CDC_JOB_PATTERN"
}

cleanup_on_error() {
  local exit_code="$1"
  set +e
  if [ "$exit_code" -ne 0 ] && [ "$STARTED_SPARK_JOBS" = "1" ]; then
    printf '[MIC-38-OBSERVE] Script failed (exit=%s), stopping Spark jobs started by this run...\n' "$exit_code" >&2
    stop_mic38_spark_jobs || true
  fi
}

trap 'cleanup_on_error $?' EXIT

start_spark_job() {
  local script_path="$1"
  local log_file="$2"
  local ivy_cache="/tmp/ivy/mic38/shared"
  docker exec lakehouse-spark bash -lc "mkdir -p '${ivy_cache}' && MIC38_RUN_ID='${MIC38_RUN_ID}' nohup /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy='${ivy_cache}' \
    --conf spark.driver.cores='${SPARK_DRIVER_CORES}' \
    --conf spark.driver.memory='${SPARK_DRIVER_MEMORY}' \
    --conf spark.driver.memoryOverhead='${SPARK_DRIVER_MEMORY_OVERHEAD}' \
    --conf spark.executor.instances='${SPARK_EXECUTOR_INSTANCES}' \
    --conf spark.executor.cores='${SPARK_EXECUTOR_CORES}' \
    --conf spark.executor.memory='${SPARK_EXECUTOR_MEMORY}' \
    --conf spark.executor.memoryOverhead='${SPARK_EXECUTOR_MEMORY_OVERHEAD}' \
    --conf spark.cores.max='${SPARK_CORES_MAX}' \
    --conf spark.sql.shuffle.partitions='${SPARK_SQL_SHUFFLE_PARTITIONS}' \
    --conf spark.default.parallelism='${SPARK_DEFAULT_PARALLELISM}' \
    --conf spark.dynamicAllocation.enabled='false' \
    '${script_path}' > '${log_file}' 2>&1 &"
}

wait_for_spark_job() {
  local pattern="$1"
  local retries="$2"
  local sleep_seconds="$3"
  for ((attempt=1; attempt<=retries; attempt++)); do
    if docker exec lakehouse-spark bash -lc "pgrep -f '${pattern}'" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_seconds"
  done
  echo "[MIC-38-OBSERVE] ERROR: Spark job pattern '${pattern}' is not running after ${retries} attempts." >&2
  return 1
}

wait_for_stream_batch_log() {
  local log_file="$1"
  local label="$2"
  local retries="$3"
  local sleep_seconds="$4"
  for ((attempt=1; attempt<=retries; attempt++)); do
    if docker exec lakehouse-spark bash -lc "if [ -f '${log_file}' ] && grep -Eq 'Batch [0-9]+:' '${log_file}'; then exit 0; else exit 1; fi" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_seconds"
  done
  echo "[MIC-38-OBSERVE] ERROR: ${label} did not report a micro-batch in ${log_file} after ${retries} attempts." >&2
  return 1
}

reset_checkpoints() {
  printf '[MIC-38-OBSERVE] Resetting checkpoint directories...\n'
  docker exec lakehouse-minio sh -lc "rm -rf \
    /data/checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1 \
    /data/checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1 \
    /data/checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1 \
    /data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1 \
    /data/checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1"
}

cd "$REPO_ROOT"

printf '[MIC-38-OBSERVE] Starting manual-observe flow...\n'
printf '[MIC-38-OBSERVE] run_id=%s\n' "$MIC38_RUN_ID"
printf '[MIC-38-OBSERVE] cdc_video_id=%s\n' "$MIC38_VIDEO_ID"
printf '[MIC-38-OBSERVE] reset_checkpoints=%s\n' "$RESET_CHECKPOINTS"
printf '[MIC-38-OBSERVE] spark_driver_cores=%s\n' "$SPARK_DRIVER_CORES"
printf '[MIC-38-OBSERVE] spark_driver_memory=%s\n' "$SPARK_DRIVER_MEMORY"
printf '[MIC-38-OBSERVE] spark_driver_memory_overhead=%s\n' "$SPARK_DRIVER_MEMORY_OVERHEAD"
printf '[MIC-38-OBSERVE] spark_executor_instances=%s\n' "$SPARK_EXECUTOR_INSTANCES"
printf '[MIC-38-OBSERVE] spark_executor_cores=%s\n' "$SPARK_EXECUTOR_CORES"
printf '[MIC-38-OBSERVE] spark_executor_memory=%s\n' "$SPARK_EXECUTOR_MEMORY"
printf '[MIC-38-OBSERVE] spark_executor_memory_overhead=%s\n' "$SPARK_EXECUTOR_MEMORY_OVERHEAD"
printf '[MIC-38-OBSERVE] spark_cores_max=%s\n' "$SPARK_CORES_MAX"
printf '[MIC-38-OBSERVE] spark_sql_shuffle_partitions=%s\n' "$SPARK_SQL_SHUFFLE_PARTITIONS"
printf '[MIC-38-OBSERVE] spark_default_parallelism=%s\n' "$SPARK_DEFAULT_PARALLELISM"
printf '[MIC-38-OBSERVE] stream_batch_ready_retries=%s\n' "$STREAM_BATCH_READY_RETRIES"
printf '[MIC-38-OBSERVE] stream_batch_ready_sleep_seconds=%s\n' "$STREAM_BATCH_READY_SLEEP_SECONDS"

printf '[MIC-38-OBSERVE] Starting required services...\n'
docker compose up -d minio minio-mc iceberg-rest zookeeper kafka spark

printf '[MIC-38-OBSERVE] Ensuring required topics exist with Sprint-1 partitions...\n'
wait_for_kafka_ready "$KAFKA_READY_RETRIES" "$KAFKA_READY_SLEEP_SECONDS"
ensure_topic content_events 6
ensure_topic cdc.content.videos 3

printf '[MIC-38-OBSERVE] Restarting Spark jobs for clean observation window...\n'
stop_mic38_spark_jobs
if [ "$RESET_CHECKPOINTS" = "1" ]; then
  reset_checkpoints
fi

start_spark_job /home/iceberg/local/src/spark/rt_content_events_aggregator.py "$CONTENT_JOB_LOG"
sleep "$WAIT_AFTER_JOB_START_SECONDS"
wait_for_spark_job rt_content_events_aggregator.py "$SPARK_JOB_READY_RETRIES" "$SPARK_JOB_READY_SLEEP_SECONDS"
wait_for_stream_batch_log "$CONTENT_JOB_LOG" "content aggregator" "$STREAM_BATCH_READY_RETRIES" "$STREAM_BATCH_READY_SLEEP_SECONDS"

start_spark_job /home/iceberg/local/src/spark/rt_video_cdc_upsert.py "$CDC_JOB_LOG"
sleep "$WAIT_AFTER_JOB_START_SECONDS"
wait_for_spark_job rt_video_cdc_upsert.py "$SPARK_JOB_READY_RETRIES" "$SPARK_JOB_READY_SLEEP_SECONDS"
wait_for_stream_batch_log "$CDC_JOB_LOG" "CDC upsert" "$STREAM_BATCH_READY_RETRIES" "$STREAM_BATCH_READY_SLEEP_SECONDS"
STARTED_SPARK_JOBS=1

printf '[MIC-38-OBSERVE] Emitting bounded generator traffic (MIC-38 shared run shape)...\n'
"$PYTHON_BIN" src/generator/m1_bounded_run.py \
  --config docs/architecture/generator/examples/m1_run_config.example.json \
  --run-id "$MIC38_RUN_ID" \
  --sink kafka \
  --bootstrap-servers "$BOOTSTRAP_SERVERS"

sleep "$WAIT_AFTER_BOUNDED_RUN_SECONDS"

printf '[MIC-38-OBSERVE] Emitting mixed CDC fixture for valid+invalid CDC paths...\n'
"$PYTHON_BIN" src/scripts/emit_mic43_cdc_mixed_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --video-id "$MIC38_VIDEO_ID" \
  --base-ts-ms "$BASE_TS_MS"

sleep "$WAIT_AFTER_CDC_FIXTURE_SECONDS"

printf '[MIC-38-OBSERVE] Observation environment is ready.\n'
printf '[MIC-38-OBSERVE] Spark logs:\n'
printf '  content job: %s\n' "$CONTENT_JOB_LOG"
printf '  cdc job:     %s\n' "$CDC_JOB_LOG"
printf '[MIC-38-OBSERVE] Quick checks:\n'
printf '  docker exec lakehouse-spark bash -lc "pgrep -f rt_content_events_aggregator.py && pgrep -f rt_video_cdc_upsert.py"\n'
printf '  docker exec lakehouse-spark bash -lc "tail -n 80 %s"\n' "$CONTENT_JOB_LOG"
printf '  docker exec lakehouse-spark bash -lc "tail -n 80 %s"\n' "$CDC_JOB_LOG"
printf '[MIC-38-OBSERVE] Optional Trino entrypoint (if trino service/container exists):\n'
printf '  docker exec -it lakehouse-trino trino\n'
if [ "$PRINT_MAINTENANCE_HINT" = "1" ]; then
  printf '[MIC-38-OBSERVE] Suggested Iceberg maintenance SQL (run in Trino):\n'
  printf "  ALTER TABLE lakehouse.bronze.raw_events EXECUTE expire_snapshots(retention_threshold => '1d');\n"
  printf "  ALTER TABLE lakehouse.gold.rt_video_stats_1min EXECUTE expire_snapshots(retention_threshold => '1d');\n"
  printf "  ALTER TABLE lakehouse.bronze.invalid_events_content EXECUTE expire_snapshots(retention_threshold => '1d');\n"
  printf "  ALTER TABLE lakehouse.dims.dim_videos EXECUTE expire_snapshots(retention_threshold => '1d');\n"
  printf "  ALTER TABLE lakehouse.bronze.invalid_events_cdc_videos EXECUTE expire_snapshots(retention_threshold => '1d');\n"
  printf "  ALTER TABLE lakehouse.bronze.raw_events EXECUTE remove_orphan_files(retention_threshold => '1d');\n"
  printf "  ALTER TABLE lakehouse.gold.rt_video_stats_1min EXECUTE remove_orphan_files(retention_threshold => '1d');\n"
  printf "  ALTER TABLE lakehouse.bronze.invalid_events_content EXECUTE remove_orphan_files(retention_threshold => '1d');\n"
  printf "  ALTER TABLE lakehouse.dims.dim_videos EXECUTE remove_orphan_files(retention_threshold => '1d');\n"
  printf "  ALTER TABLE lakehouse.bronze.invalid_events_cdc_videos EXECUTE remove_orphan_files(retention_threshold => '1d');\n"
fi
printf '[MIC-38-OBSERVE] No verifier gates were executed in this flow.\n'
