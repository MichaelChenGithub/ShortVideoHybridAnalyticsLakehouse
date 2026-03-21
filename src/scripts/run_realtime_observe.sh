#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/acceptance_common.sh"

RESET_CHECKPOINTS="${RESET_CHECKPOINTS:-0}"
PRINT_MAINTENANCE_HINT="${PRINT_MAINTENANCE_HINT:-0}"

usage() {
  cat <<'EOF'
Usage: run_realtime_observe.sh [--reset-checkpoints] [--maintenance-hint]

Options:
  --reset-checkpoints   Remove streaming checkpoint paths before restarting jobs.
  --maintenance-hint    Print suggested Iceberg maintenance SQL after startup.
  -h, --help            Show this help.

Equivalent env flags:
  RESET_CHECKPOINTS=1
  PRINT_MAINTENANCE_HINT=1
  ACCEPTANCE_RESET_DOCKER=1
  BOUNDED_RUN_TIME_MODE=dynamic
  BOUNDED_RUN_STARTED_AT=2026-03-20T14:00:00Z

Resource-bound env flags (optional overrides):
  RT_SIGNOFF_SPARK_DRIVER_CORES
  RT_SIGNOFF_SPARK_DRIVER_MEMORY
  RT_SIGNOFF_SPARK_DRIVER_MEMORY_OVERHEAD
  RT_SIGNOFF_SPARK_EXECUTOR_INSTANCES
  RT_SIGNOFF_SPARK_EXECUTOR_CORES
  RT_SIGNOFF_SPARK_EXECUTOR_MEMORY
  RT_SIGNOFF_SPARK_EXECUTOR_MEMORY_OVERHEAD
  RT_SIGNOFF_SPARK_CORES_MAX
  RT_SIGNOFF_SPARK_SQL_SHUFFLE_PARTITIONS
  RT_SIGNOFF_SPARK_DEFAULT_PARALLELISM

Readiness tuning env flags (optional overrides):
  SPARK_JOB_READY_RETRIES
  SPARK_JOB_READY_SLEEP_SECONDS
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
      echo "[RT-SIGNOFF-OBSERVE] ERROR: unknown argument '$1'" >&2
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

SPARK_DRIVER_CORES="${RT_SIGNOFF_SPARK_DRIVER_CORES:-1}"
SPARK_DRIVER_MEMORY="${RT_SIGNOFF_SPARK_DRIVER_MEMORY:-1g}"
SPARK_DRIVER_MEMORY_OVERHEAD="${RT_SIGNOFF_SPARK_DRIVER_MEMORY_OVERHEAD:-512m}"
SPARK_EXECUTOR_INSTANCES="${RT_SIGNOFF_SPARK_EXECUTOR_INSTANCES:-1}"
SPARK_EXECUTOR_CORES="${RT_SIGNOFF_SPARK_EXECUTOR_CORES:-1}"
SPARK_EXECUTOR_MEMORY="${RT_SIGNOFF_SPARK_EXECUTOR_MEMORY:-1g}"
SPARK_EXECUTOR_MEMORY_OVERHEAD="${RT_SIGNOFF_SPARK_EXECUTOR_MEMORY_OVERHEAD:-512m}"
SPARK_CORES_MAX="${RT_SIGNOFF_SPARK_CORES_MAX:-2}"
SPARK_SQL_SHUFFLE_PARTITIONS="${RT_SIGNOFF_SPARK_SQL_SHUFFLE_PARTITIONS:-8}"
SPARK_DEFAULT_PARALLELISM="${RT_SIGNOFF_SPARK_DEFAULT_PARALLELISM:-8}"

RT_SIGNOFF_WATERMARK_SCENARIO="${RT_SIGNOFF_WATERMARK_SCENARIO:-baseline}"
RT_SIGNOFF_RUN_ID="${RT_SIGNOFF_RUN_ID:-realtime_observe_$(date -u +%Y%m%dT%H%M%SZ)}"
RT_SIGNOFF_VIDEO_ID="${RT_SIGNOFF_VIDEO_ID:-${RT_SIGNOFF_RUN_ID}_cdc_vid_001}"
CONTENT_JOB_PATTERN="[r]t_content_events_aggregator.py"
CDC_JOB_PATTERN="[r]t_video_cdc_upsert.py"
STARTED_SPARK_JOBS=0

WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_BOUNDED_RUN_SECONDS="${WAIT_AFTER_BOUNDED_RUN_SECONDS:-75}"
WAIT_AFTER_CDC_FIXTURE_SECONDS="${WAIT_AFTER_CDC_FIXTURE_SECONDS:-75}"

KAFKA_READY_RETRIES="${KAFKA_READY_RETRIES:-30}"
KAFKA_READY_SLEEP_SECONDS="${KAFKA_READY_SLEEP_SECONDS:-2}"
SPARK_JOB_READY_RETRIES="${SPARK_JOB_READY_RETRIES:-30}"
SPARK_JOB_READY_SLEEP_SECONDS="${SPARK_JOB_READY_SLEEP_SECONDS:-3}"

BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"

CONTENT_JOB_LOG="/tmp/${RT_SIGNOFF_RUN_ID}_content_agg.log"
CDC_JOB_LOG="/tmp/${RT_SIGNOFF_RUN_ID}_cdc_upsert.log"

case "$RT_SIGNOFF_WATERMARK_SCENARIO" in
  baseline)
    RT_CONTENT_EVENTS_WATERMARK="${RT_CONTENT_EVENTS_WATERMARK:-2 minutes}"
    ;;
  lag_prone)
    RT_CONTENT_EVENTS_WATERMARK="${RT_CONTENT_EVENTS_WATERMARK:-5 minutes}"
    ;;
  *)
    echo "[RT-SIGNOFF-OBSERVE] ERROR: RT_SIGNOFF_WATERMARK_SCENARIO must be baseline or lag_prone, got '${RT_SIGNOFF_WATERMARK_SCENARIO}'" >&2
    exit 1
    ;;
esac

resolve_bounded_run_started_at "RT-SIGNOFF-OBSERVE"

wait_for_kafka_ready() {
  local retries="$1"
  local sleep_seconds="$2"
  for ((attempt=1; attempt<=retries; attempt++)); do
    if docker exec lakehouse-kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_seconds"
  done
  echo "[RT-SIGNOFF-OBSERVE] ERROR: Kafka did not become ready after ${retries} attempts." >&2
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

stop_realtime_signoff_spark_jobs() {
  stop_spark_job_if_running "$CONTENT_JOB_PATTERN"
  stop_spark_job_if_running "$CDC_JOB_PATTERN"
}

cleanup_on_error() {
  local exit_code="$1"
  set +e
  if [ "$exit_code" -ne 0 ] && [ "$STARTED_SPARK_JOBS" = "1" ]; then
    printf '[RT-SIGNOFF-OBSERVE] Script failed (exit=%s), stopping Spark jobs started by this run...\n' "$exit_code" >&2
    stop_realtime_signoff_spark_jobs || true
  fi
}

trap 'cleanup_on_error $?' EXIT

start_spark_job() {
  local script_path="$1"
  local log_file="$2"
  local ivy_cache="/tmp/ivy/realtime_signoff/shared"
  docker exec lakehouse-spark bash -lc "mkdir -p '${ivy_cache}' && RT_SIGNOFF_RUN_ID='${RT_SIGNOFF_RUN_ID}' RT_CONTENT_EVENTS_WATERMARK='${RT_CONTENT_EVENTS_WATERMARK}' nohup /opt/spark/bin/spark-submit \
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
  echo "[RT-SIGNOFF-OBSERVE] ERROR: Spark job pattern '${pattern}' is not running after ${retries} attempts." >&2
  return 1
}

reset_checkpoints() {
  printf '[RT-SIGNOFF-OBSERVE] Resetting checkpoint directories...\n'
  docker exec lakehouse-minio sh -lc "rm -rf \
    /data/checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1 \
    /data/checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1 \
    /data/checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1 \
    /data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1 \
    /data/checkpoints/jobs/spark_rt_video_cdc_upsert/raw_cdc_videos/v1 \
    /data/checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1"
}

cd "$REPO_ROOT"
acceptance_maybe_reset_docker "RT-SIGNOFF-OBSERVE"

printf '[RT-SIGNOFF-OBSERVE] Starting manual-observe flow...\n'
printf '[RT-SIGNOFF-OBSERVE] run_id=%s\n' "$RT_SIGNOFF_RUN_ID"
printf '[RT-SIGNOFF-OBSERVE] cdc_video_id=%s\n' "$RT_SIGNOFF_VIDEO_ID"
printf '[RT-SIGNOFF-OBSERVE] watermark_scenario=%s\n' "$RT_SIGNOFF_WATERMARK_SCENARIO"
printf '[RT-SIGNOFF-OBSERVE] content_watermark=%s\n' "$RT_CONTENT_EVENTS_WATERMARK"
printf '[RT-SIGNOFF-OBSERVE] reset_checkpoints=%s\n' "$RESET_CHECKPOINTS"
printf '[RT-SIGNOFF-OBSERVE] spark_driver_cores=%s\n' "$SPARK_DRIVER_CORES"
printf '[RT-SIGNOFF-OBSERVE] spark_driver_memory=%s\n' "$SPARK_DRIVER_MEMORY"
printf '[RT-SIGNOFF-OBSERVE] spark_driver_memory_overhead=%s\n' "$SPARK_DRIVER_MEMORY_OVERHEAD"
printf '[RT-SIGNOFF-OBSERVE] spark_executor_instances=%s\n' "$SPARK_EXECUTOR_INSTANCES"
printf '[RT-SIGNOFF-OBSERVE] spark_executor_cores=%s\n' "$SPARK_EXECUTOR_CORES"
printf '[RT-SIGNOFF-OBSERVE] spark_executor_memory=%s\n' "$SPARK_EXECUTOR_MEMORY"
printf '[RT-SIGNOFF-OBSERVE] spark_executor_memory_overhead=%s\n' "$SPARK_EXECUTOR_MEMORY_OVERHEAD"
printf '[RT-SIGNOFF-OBSERVE] spark_cores_max=%s\n' "$SPARK_CORES_MAX"
printf '[RT-SIGNOFF-OBSERVE] spark_sql_shuffle_partitions=%s\n' "$SPARK_SQL_SHUFFLE_PARTITIONS"
printf '[RT-SIGNOFF-OBSERVE] spark_default_parallelism=%s\n' "$SPARK_DEFAULT_PARALLELISM"
printf '[RT-SIGNOFF-OBSERVE] spark_job_ready_retries=%s\n' "$SPARK_JOB_READY_RETRIES"
printf '[RT-SIGNOFF-OBSERVE] spark_job_ready_sleep_seconds=%s\n' "$SPARK_JOB_READY_SLEEP_SECONDS"

printf '[RT-SIGNOFF-OBSERVE] Starting required services...\n'
docker compose up -d minio minio-mc catalog-postgres iceberg-rest zookeeper kafka spark

printf '[RT-SIGNOFF-OBSERVE] Ensuring required topics exist with Sprint-1 partitions...\n'
wait_for_kafka_ready "$KAFKA_READY_RETRIES" "$KAFKA_READY_SLEEP_SECONDS"
ensure_topic content_events 6
ensure_topic cdc.content.videos 3

printf '[RT-SIGNOFF-OBSERVE] Restarting Spark jobs for clean observation window...\n'
stop_realtime_signoff_spark_jobs
if [ "$RESET_CHECKPOINTS" = "1" ]; then
  reset_checkpoints
fi

start_spark_job /home/iceberg/local/src/spark/rt_content_events_aggregator.py "$CONTENT_JOB_LOG"
sleep "$WAIT_AFTER_JOB_START_SECONDS"
wait_for_spark_job rt_content_events_aggregator.py "$SPARK_JOB_READY_RETRIES" "$SPARK_JOB_READY_SLEEP_SECONDS"

start_spark_job /home/iceberg/local/src/spark/rt_video_cdc_upsert.py "$CDC_JOB_LOG"
sleep "$WAIT_AFTER_JOB_START_SECONDS"
wait_for_spark_job rt_video_cdc_upsert.py "$SPARK_JOB_READY_RETRIES" "$SPARK_JOB_READY_SLEEP_SECONDS"
STARTED_SPARK_JOBS=1

printf '[RT-SIGNOFF-OBSERVE] Emitting bounded generator traffic (RT-SIGNOFF shared run shape)...\n'
if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config docs/architecture/generator/examples/bounded_run_config.example.json \
    --run-id "$RT_SIGNOFF_RUN_ID" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --started-at "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
else
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config docs/architecture/generator/examples/bounded_run_config.example.json \
    --run-id "$RT_SIGNOFF_RUN_ID" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS"
fi

sleep "$WAIT_AFTER_BOUNDED_RUN_SECONDS"

printf '[RT-SIGNOFF-OBSERVE] Emitting mixed CDC fixture for valid+invalid CDC paths...\n'
"$PYTHON_BIN" src/scripts/emit_cdc_mixed_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --video-id "$RT_SIGNOFF_VIDEO_ID" \
  --base-ts-ms "$BASE_TS_MS"

sleep "$WAIT_AFTER_CDC_FIXTURE_SECONDS"

printf '[RT-SIGNOFF-OBSERVE] Observation environment is ready.\n'
printf '[RT-SIGNOFF-OBSERVE] Spark logs:\n'
printf '  content job: %s\n' "$CONTENT_JOB_LOG"
printf '  cdc job:     %s\n' "$CDC_JOB_LOG"
printf '[RT-SIGNOFF-OBSERVE] Quick checks:\n'
printf '  docker exec lakehouse-spark bash -lc "pgrep -f rt_content_events_aggregator.py && pgrep -f rt_video_cdc_upsert.py"\n'
printf '  docker exec lakehouse-spark bash -lc "tail -n 80 %s"\n' "$CONTENT_JOB_LOG"
printf '  docker exec lakehouse-spark bash -lc "tail -n 80 %s"\n' "$CDC_JOB_LOG"
printf '[RT-SIGNOFF-OBSERVE] Optional Trino entrypoint (if trino service/container exists):\n'
printf '  docker exec -it lakehouse-trino trino\n'
if [ "$PRINT_MAINTENANCE_HINT" = "1" ]; then
  printf '[RT-SIGNOFF-OBSERVE] Suggested Iceberg maintenance SQL (run in Trino):\n'
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
printf '[RT-SIGNOFF-OBSERVE] No verifier gates were executed in this flow.\n'
