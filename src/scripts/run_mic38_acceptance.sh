#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RESET_CHECKPOINTS="${RESET_CHECKPOINTS:-0}"
KEEP_JOBS_RUNNING="${KEEP_JOBS_RUNNING:-0}"

usage() {
  cat <<'EOF'
Usage: run_mic38_acceptance.sh [--reset-checkpoints] [--keep-jobs-running]

Options:
  --reset-checkpoints  Remove streaming checkpoint paths before starting jobs.
  --keep-jobs-running  Do not stop started Spark jobs on script exit.
  -h, --help           Show this help.

Equivalent env flags:
  RESET_CHECKPOINTS=1
  KEEP_JOBS_RUNNING=1
EOF
}

while (($# > 0)); do
  case "$1" in
    --reset-checkpoints)
      RESET_CHECKPOINTS=1
      shift
      ;;
    --keep-jobs-running)
      KEEP_JOBS_RUNNING=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[MIC-38] ERROR: unknown argument '$1'" >&2
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

MIC38_WATERMARK_SCENARIO="${MIC38_WATERMARK_SCENARIO:-baseline}"
BASE_MIC38_RUN_ID="${MIC38_RUN_ID:-mic38_$(date -u +%Y%m%dT%H%M%SZ)}"
MIC38_RUN_ID="${BASE_MIC38_RUN_ID}_${MIC38_WATERMARK_SCENARIO}"
MIC38_VIDEO_ID="${MIC38_VIDEO_ID:-${MIC38_RUN_ID}_cdc_vid_001}"
EXPECTED_CDC_STATUS="${EXPECTED_CDC_STATUS:-copyright_strike}"

WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_BOUNDED_RUN_SECONDS="${WAIT_AFTER_BOUNDED_RUN_SECONDS:-75}"
WAIT_AFTER_CDC_FIXTURE_SECONDS="${WAIT_AFTER_CDC_FIXTURE_SECONDS:-75}"

BASE_TS_MS="${BASE_TS_MS:-}"
EXPECTED_CDC_SOURCE_TS_MS="${EXPECTED_CDC_SOURCE_TS_MS:-}"

MIN_RAW_ROWS="${MIN_RAW_ROWS:-1}"
MIN_GOLD_ROWS="${MIN_GOLD_ROWS:-1}"
MIN_CONTENT_INVALID_ROWS="${MIN_CONTENT_INVALID_ROWS:-1}"
MIN_CDC_INVALID_ROWS="${MIN_CDC_INVALID_ROWS:-4}"

MAX_CONTENT_INVALID_RATE="${MAX_CONTENT_INVALID_RATE:-0.20}"
MAX_CDC_INVALID_RATE="${MAX_CDC_INVALID_RATE:-0.20}"
MAX_FRESHNESS_MINUTES="${MAX_FRESHNESS_MINUTES:-3}"
LATENCY_THRESHOLD_MINUTES="${LATENCY_THRESHOLD_MINUTES:-3}"
LOOKBACK_MINUTES="${LOOKBACK_MINUTES:-30}"
BASELINE_MIN_WATERMARK_DROP_RATIO="${BASELINE_MIN_WATERMARK_DROP_RATIO:-0.005}"
LAG_PRONE_MAX_WATERMARK_DROP_RATIO="${LAG_PRONE_MAX_WATERMARK_DROP_RATIO:-0.005}"

KAFKA_READY_RETRIES="${KAFKA_READY_RETRIES:-30}"
KAFKA_READY_SLEEP_SECONDS="${KAFKA_READY_SLEEP_SECONDS:-2}"
SPARK_JOB_READY_RETRIES="${SPARK_JOB_READY_RETRIES:-10}"
SPARK_JOB_READY_SLEEP_SECONDS="${SPARK_JOB_READY_SLEEP_SECONDS:-3}"

RUN_CONTEXT="mic38:${MIC38_RUN_ID}"
CONTENT_JOB_LOG="/tmp/${MIC38_RUN_ID}_content_agg.log"
CDC_JOB_LOG="/tmp/${MIC38_RUN_ID}_cdc_upsert.log"
CONTENT_JOB_PATTERN="[r]t_content_events_aggregator.py"
CDC_JOB_PATTERN="[r]t_video_cdc_upsert.py"
STARTED_SPARK_JOBS=0

ARTIFACT_DIR="${MIC38_ARTIFACT_DIR:-artifacts/mic38_signoff/${MIC38_RUN_ID}}"
CONTENT_METRICS_LOG="${ARTIFACT_DIR}/content_metrics.log"
CONTENT_CONTRACT_LOG="${ARTIFACT_DIR}/content_contract.log"
CDC_UPSERT_LOG="${ARTIFACT_DIR}/cdc_upsert.log"
CDC_INVALID_LOG="${ARTIFACT_DIR}/cdc_invalid.log"
CDC_HEALTH_LOG="${ARTIFACT_DIR}/cdc_health.log"
RUNTIME_START_JSON="${ARTIFACT_DIR}/runtime_start.json"
RUNTIME_END_JSON="${ARTIFACT_DIR}/runtime_end.json"
CHECKPOINT_START_JSON="${ARTIFACT_DIR}/checkpoint_start.json"
CHECKPOINT_END_JSON="${ARTIFACT_DIR}/checkpoint_end.json"
REPORT_JSON="${ARTIFACT_DIR}/signoff_report.json"
REPORT_MD="${ARTIFACT_DIR}/signoff_summary.md"

case "$MIC38_WATERMARK_SCENARIO" in
  baseline)
    RT_CONTENT_EVENTS_WATERMARK="${RT_CONTENT_EVENTS_WATERMARK:-2 minutes}"
    MIN_WATERMARK_DROP_RATIO="${MIN_WATERMARK_DROP_RATIO:-$BASELINE_MIN_WATERMARK_DROP_RATIO}"
    MAX_WATERMARK_DROP_RATIO=""
    ;;
  lag_prone)
    RT_CONTENT_EVENTS_WATERMARK="${RT_CONTENT_EVENTS_WATERMARK:-5 minutes}"
    MIN_WATERMARK_DROP_RATIO=""
    MAX_WATERMARK_DROP_RATIO="${MAX_WATERMARK_DROP_RATIO:-$LAG_PRONE_MAX_WATERMARK_DROP_RATIO}"
    ;;
  *)
    echo "[MIC-38] ERROR: MIC38_WATERMARK_SCENARIO must be baseline or lag_prone, got '${MIC38_WATERMARK_SCENARIO}'" >&2
    exit 1
    ;;
esac

now_ms() {
  echo $(( $(date +%s) * 1000 ))
}

wait_for_kafka_ready() {
  local retries="$1"
  local sleep_seconds="$2"
  for ((attempt=1; attempt<=retries; attempt++)); do
    if docker exec lakehouse-kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_seconds"
  done
  echo "[MIC-38] ERROR: Kafka did not become ready after ${retries} attempts." >&2
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

reset_checkpoints() {
  printf '[MIC-38] Resetting checkpoint directories...\n'
  docker exec lakehouse-minio sh -lc "rm -rf \
    /data/checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1 \
    /data/checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1 \
    /data/checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1 \
    /data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1 \
    /data/checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1"
}

cleanup_on_exit() {
  local exit_code="$1"
  set +e
  if [ "$KEEP_JOBS_RUNNING" = "1" ]; then
    printf '[MIC-38] KEEP_JOBS_RUNNING=1, leaving Spark jobs running.\n'
    return
  fi
  if [ "$STARTED_SPARK_JOBS" = "1" ]; then
    printf '[MIC-38] Stopping Spark jobs started by this run...\n'
    stop_mic38_spark_jobs || true
  fi
  return "$exit_code"
}

trap 'cleanup_on_exit $?' EXIT

start_spark_job() {
  local script_path="$1"
  local log_file="$2"
  local script_name
  script_name="$(basename "${script_path}" .py)"
  local ivy_cache="/tmp/ivy/${MIC38_RUN_ID}/${script_name}"
  docker exec lakehouse-spark bash -lc "mkdir -p '${ivy_cache}' && MIC38_RUN_ID='${MIC38_RUN_ID}' RT_CONTENT_EVENTS_WATERMARK='${RT_CONTENT_EVENTS_WATERMARK}' nohup /opt/spark/bin/spark-submit --conf spark.jars.ivy='${ivy_cache}' '${script_path}' > '${log_file}' 2>&1 &"
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
  echo "[MIC-38] ERROR: Spark job pattern '${pattern}' is not running after ${retries} attempts." >&2
  return 1
}

max_batch_id_from_log() {
  local log_file="$1"
  docker exec lakehouse-spark bash -lc "if [ -f '${log_file}' ]; then awk '{for (i=1; i<=NF; i++) if (\$i==\"Batch\" && (i+1)<=NF) {n=\$(i+1); gsub(/[^0-9]/, \"\", n); if ((n+0)>max) max=(n+0)}} END{if (max==\"\") print -1; else print max}' '${log_file}'; else echo -1; fi"
}

runtime_snapshot() {
  local outfile="$1"
  local sample_at_ms
  local content_pid_count
  local cdc_pid_count
  local content_log_size
  local cdc_log_size
  local content_max_batch_id
  local cdc_max_batch_id
  local content_exception_lines
  local cdc_exception_lines

  sample_at_ms="$(now_ms)"
  content_pid_count="$(docker exec lakehouse-spark bash -lc "pgrep -f rt_content_events_aggregator.py | wc -l" | tr -d '[:space:]')"
  cdc_pid_count="$(docker exec lakehouse-spark bash -lc "pgrep -f rt_video_cdc_upsert.py | wc -l" | tr -d '[:space:]')"
  content_log_size="$(docker exec lakehouse-spark bash -lc "if [ -f '${CONTENT_JOB_LOG}' ]; then wc -c < '${CONTENT_JOB_LOG}'; else echo 0; fi" | tr -d '[:space:]')"
  cdc_log_size="$(docker exec lakehouse-spark bash -lc "if [ -f '${CDC_JOB_LOG}' ]; then wc -c < '${CDC_JOB_LOG}'; else echo 0; fi" | tr -d '[:space:]')"
  content_max_batch_id="$(max_batch_id_from_log "${CONTENT_JOB_LOG}" | tr -d '[:space:]')"
  cdc_max_batch_id="$(max_batch_id_from_log "${CDC_JOB_LOG}" | tr -d '[:space:]')"
  content_exception_lines="$(docker exec lakehouse-spark bash -lc "if [ -f '${CONTENT_JOB_LOG}' ]; then grep -Eic 'Traceback|StreamingQueryException|Exception in thread' '${CONTENT_JOB_LOG}' || true; else echo 0; fi" | tr -d '[:space:]')"
  cdc_exception_lines="$(docker exec lakehouse-spark bash -lc "if [ -f '${CDC_JOB_LOG}' ]; then grep -Eic 'Traceback|StreamingQueryException|Exception in thread' '${CDC_JOB_LOG}' || true; else echo 0; fi" | tr -d '[:space:]')"

  cat > "${outfile}" <<EOF
{"sample_at_ms":${sample_at_ms},"content_pid_count":${content_pid_count},"cdc_pid_count":${cdc_pid_count},"content_log_size":${content_log_size},"cdc_log_size":${cdc_log_size},"content_max_batch_id":${content_max_batch_id},"cdc_max_batch_id":${cdc_max_batch_id},"content_exception_lines":${content_exception_lines},"cdc_exception_lines":${cdc_exception_lines}}
EOF
}

checkpoint_file_count() {
  local path="$1"
  docker exec lakehouse-minio sh -lc "if [ -d '${path}' ]; then ls -1R '${path}' 2>/dev/null | wc -l; else echo 0; fi" | tr -d '[:space:]'
}

checkpoint_snapshot() {
  local outfile="$1"
  local sample_at_ms
  local content_raw_count
  local content_gold_count
  local content_invalid_count
  local cdc_dim_count
  local cdc_invalid_count

  sample_at_ms="$(now_ms)"
  content_raw_count="$(checkpoint_file_count /data/checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1)"
  content_gold_count="$(checkpoint_file_count /data/checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1)"
  content_invalid_count="$(checkpoint_file_count /data/checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1)"
  cdc_dim_count="$(checkpoint_file_count /data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1)"
  cdc_invalid_count="$(checkpoint_file_count /data/checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1)"

  cat > "${outfile}" <<EOF
{"sample_at_ms":${sample_at_ms},"paths":{"content_raw":{"file_count":${content_raw_count}},"content_gold":{"file_count":${content_gold_count}},"content_invalid":{"file_count":${content_invalid_count}},"cdc_dim":{"file_count":${cdc_dim_count}},"cdc_invalid":{"file_count":${cdc_invalid_count}}}}
EOF
}

printf '[MIC-38] Starting Sprint 1 sign-off orchestrator...\n'
printf '[MIC-38] run_id=%s\n' "$MIC38_RUN_ID"
printf '[MIC-38] run_context=%s\n' "$RUN_CONTEXT"
printf '[MIC-38] cdc_video_id=%s\n' "$MIC38_VIDEO_ID"
printf '[MIC-38] watermark_scenario=%s\n' "$MIC38_WATERMARK_SCENARIO"
printf '[MIC-38] content_watermark=%s\n' "$RT_CONTENT_EVENTS_WATERMARK"
printf '[MIC-38] reset_checkpoints=%s\n' "$RESET_CHECKPOINTS"
printf '[MIC-38] keep_jobs_running=%s\n' "$KEEP_JOBS_RUNNING"
printf '[MIC-38] max_freshness_minutes=%s\n' "$MAX_FRESHNESS_MINUTES"
printf '[MIC-38] latency_threshold_minutes=%s\n' "$LATENCY_THRESHOLD_MINUTES"
printf '[MIC-38] content_max_invalid_rate=%s\n' "$MAX_CONTENT_INVALID_RATE"
printf '[MIC-38] cdc_max_invalid_rate=%s\n' "$MAX_CDC_INVALID_RATE"
if [ -n "$MIN_WATERMARK_DROP_RATIO" ]; then
  printf '[MIC-38] min_watermark_drop_ratio=%s\n' "$MIN_WATERMARK_DROP_RATIO"
fi
if [ -n "$MAX_WATERMARK_DROP_RATIO" ]; then
  printf '[MIC-38] max_watermark_drop_ratio=%s\n' "$MAX_WATERMARK_DROP_RATIO"
fi

cd "$REPO_ROOT"
mkdir -p "$ARTIFACT_DIR"

printf '[MIC-38] artifact_dir=%s\n' "$ARTIFACT_DIR"
printf '[MIC-38] content_job_log=%s\n' "$CONTENT_JOB_LOG"
printf '[MIC-38] cdc_job_log=%s\n' "$CDC_JOB_LOG"

printf '[MIC-38] Starting required services...\n'
docker compose up -d minio minio-mc iceberg-rest zookeeper kafka spark

printf '[MIC-38] Ensuring required topics exist with Sprint-1 partitions...\n'
wait_for_kafka_ready "$KAFKA_READY_RETRIES" "$KAFKA_READY_SLEEP_SECONDS"
ensure_topic content_events 6
ensure_topic cdc.content.videos 3

printf '[MIC-38] Starting both Spark jobs for integrated E2E run...\n'
stop_mic38_spark_jobs
if [ "$RESET_CHECKPOINTS" = "1" ]; then
  reset_checkpoints
fi

# Start jobs sequentially to avoid Ivy/Maven cache races when resolving Spark packages.
start_spark_job /home/iceberg/local/src/spark/rt_content_events_aggregator.py "$CONTENT_JOB_LOG"
sleep "$WAIT_AFTER_JOB_START_SECONDS"
wait_for_spark_job rt_content_events_aggregator.py "$SPARK_JOB_READY_RETRIES" "$SPARK_JOB_READY_SLEEP_SECONDS"

start_spark_job /home/iceberg/local/src/spark/rt_video_cdc_upsert.py "$CDC_JOB_LOG"
sleep "$WAIT_AFTER_JOB_START_SECONDS"
wait_for_spark_job rt_video_cdc_upsert.py "$SPARK_JOB_READY_RETRIES" "$SPARK_JOB_READY_SLEEP_SECONDS"
STARTED_SPARK_JOBS=1

RUN_START_MS="${RUN_START_MS:-$(now_ms)}"
MIN_PROCESSED_AT_MS="${MIN_PROCESSED_AT_MS:-$RUN_START_MS}"
MIN_INGESTED_AT_MS="${MIN_INGESTED_AT_MS:-$RUN_START_MS}"
printf '[MIC-38] run_start_ms=%s\n' "$RUN_START_MS"
printf '[MIC-38] min_processed_at_ms=%s\n' "$MIN_PROCESSED_AT_MS"
printf '[MIC-38] min_ingested_at_ms=%s\n' "$MIN_INGESTED_AT_MS"

printf '[MIC-38] Capturing pre-run runtime/checkpoint snapshots...\n'
runtime_snapshot "$RUNTIME_START_JSON"
checkpoint_snapshot "$CHECKPOINT_START_JSON"

printf '[MIC-38] Running bounded generator once for shared integrated run...\n'
"$PYTHON_BIN" src/generator/m1_bounded_run.py \
  --config docs/architecture/generator/examples/m1_run_config.example.json \
  --run-id "$MIC38_RUN_ID" \
  --sink kafka \
  --bootstrap-servers "$BOOTSTRAP_SERVERS"

sleep "$WAIT_AFTER_BOUNDED_RUN_SECONDS"

printf '[MIC-38] Emitting deterministic mixed CDC fixture for CDC valid+invalid checks...\n'
if [ -z "$BASE_TS_MS" ]; then
  BASE_TS_MS="$(now_ms)"
fi
if [ -z "$EXPECTED_CDC_SOURCE_TS_MS" ]; then
  EXPECTED_CDC_SOURCE_TS_MS="$((BASE_TS_MS + 2000))"
fi
"$PYTHON_BIN" src/scripts/emit_mic43_cdc_mixed_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --video-id "$MIC38_VIDEO_ID" \
  --base-ts-ms "$BASE_TS_MS"

sleep "$WAIT_AFTER_CDC_FIXTURE_SECONDS"

printf '[MIC-38] Verifying content valid path (MIC-40 style gate)...\n'
declare -a WATERMARK_DROP_ARGS=()
if [ -n "$MIN_WATERMARK_DROP_RATIO" ]; then
  WATERMARK_DROP_ARGS+=(--min-watermark-drop-ratio "$MIN_WATERMARK_DROP_RATIO")
fi
if [ -n "$MAX_WATERMARK_DROP_RATIO" ]; then
  WATERMARK_DROP_ARGS+=(--max-watermark-drop-ratio "$MAX_WATERMARK_DROP_RATIO")
fi
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_aggregator.py \
  --min-raw-rows "$MIN_RAW_ROWS" \
  --min-gold-rows "$MIN_GOLD_ROWS" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --min-processed-at-ms "$MIN_PROCESSED_AT_MS" \
  "${WATERMARK_DROP_ARGS[@]}" | tee "$CONTENT_METRICS_LOG"

printf '[MIC-38] Verifying content invalid path (MIC-39 style gate)...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_contract_enforcement.py \
  --min-raw-rows "$MIN_RAW_ROWS" \
  --min-gold-rows "$MIN_GOLD_ROWS" \
  --min-invalid-rows "$MIN_CONTENT_INVALID_ROWS" \
  --max-invalid-rate "$MAX_CONTENT_INVALID_RATE" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --min-ingested-at-ms "$MIN_INGESTED_AT_MS" | tee "$CONTENT_CONTRACT_LOG"

printf '[MIC-38] Verifying CDC valid path (MIC-37 style gate)...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_upsert.py \
  --video-id "$MIC38_VIDEO_ID" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --expect-status "$EXPECTED_CDC_STATUS" \
  --expect-source-ts-ms "$EXPECTED_CDC_SOURCE_TS_MS" | tee "$CDC_UPSERT_LOG"

printf '[MIC-38] Verifying CDC invalid path (MIC-43 style gate)...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_invalid_cdc_quarantine.py \
  --table lakehouse.bronze.invalid_events_cdc_videos \
  --lookback-minutes "$LOOKBACK_MINUTES" \
  --min-row-count "$MIN_CDC_INVALID_ROWS" \
  --expect-error-codes CDC_MISSING_OP,CDC_UNSUPPORTED_OP,CDC_MISSING_SCHEMA_VERSION,CDC_MISSING_AFTER_VIDEO_ID | tee "$CDC_INVALID_LOG"

printf '[MIC-38] Verifying CDC health (freshness + invalid-rate)...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/check_rt_video_cdc_health.py \
  --dim-table lakehouse.dims.dim_videos \
  --invalid-table lakehouse.bronze.invalid_events_cdc_videos \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --lookback-minutes "$LOOKBACK_MINUTES" \
  --max-invalid-rate "$MAX_CDC_INVALID_RATE" | tee "$CDC_HEALTH_LOG"

printf '[MIC-38] Capturing post-run runtime/checkpoint snapshots...\n'
runtime_snapshot "$RUNTIME_END_JSON"
checkpoint_snapshot "$CHECKPOINT_END_JSON"

printf '[MIC-38] Running unified MIC-38 sign-off verifier...\n'
"$PYTHON_BIN" src/scripts/verify_mic38_sprint1_signoff.py \
  --run-id "$MIC38_RUN_ID" \
  --run-start-ms "$RUN_START_MS" \
  --content-metrics-log "$CONTENT_METRICS_LOG" \
  --content-contract-log "$CONTENT_CONTRACT_LOG" \
  --cdc-upsert-log "$CDC_UPSERT_LOG" \
  --cdc-invalid-log "$CDC_INVALID_LOG" \
  --cdc-health-log "$CDC_HEALTH_LOG" \
  --runtime-start-json "$RUNTIME_START_JSON" \
  --runtime-end-json "$RUNTIME_END_JSON" \
  --checkpoint-start-json "$CHECKPOINT_START_JSON" \
  --checkpoint-end-json "$CHECKPOINT_END_JSON" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --latency-threshold-minutes "$LATENCY_THRESHOLD_MINUTES" \
  --max-content-invalid-rate "$MAX_CONTENT_INVALID_RATE" \
  --max-cdc-invalid-rate "$MAX_CDC_INVALID_RATE" \
  --report-json "$REPORT_JSON" \
  --report-md "$REPORT_MD"

printf '[MIC-38] Sign-off report (JSON): %s\n' "$REPORT_JSON"
printf '[MIC-38] Sign-off summary (Markdown): %s\n' "$REPORT_MD"
printf '[MIC-38] Integrated Sprint 1 E2E verification completed.\n'
