#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
MIC38_RUN_ID="${MIC38_RUN_ID:-mic38_$(date -u +%Y%m%dT%H%M%SZ)}"
MIC38_VIDEO_ID="${MIC38_VIDEO_ID:-${MIC38_RUN_ID}_cdc_vid_001}"
EXPECTED_CDC_STATUS="${EXPECTED_CDC_STATUS:-copyright_strike}"

WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_BOUNDED_RUN_SECONDS="${WAIT_AFTER_BOUNDED_RUN_SECONDS:-75}"
WAIT_AFTER_CDC_FIXTURE_SECONDS="${WAIT_AFTER_CDC_FIXTURE_SECONDS:-75}"

BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"
EXPECTED_CDC_SOURCE_TS_MS="${EXPECTED_CDC_SOURCE_TS_MS:-$((BASE_TS_MS + 2000))}"

MIN_RAW_ROWS="${MIN_RAW_ROWS:-1}"
MIN_GOLD_ROWS="${MIN_GOLD_ROWS:-1}"
MIN_CONTENT_INVALID_ROWS="${MIN_CONTENT_INVALID_ROWS:-1}"
MIN_CDC_INVALID_ROWS="${MIN_CDC_INVALID_ROWS:-4}"

MAX_CONTENT_INVALID_RATE="${MAX_CONTENT_INVALID_RATE:-0.20}"
MAX_CDC_INVALID_RATE="${MAX_CDC_INVALID_RATE:-0.20}"
MAX_FRESHNESS_MINUTES="${MAX_FRESHNESS_MINUTES:-3}"
LOOKBACK_MINUTES="${LOOKBACK_MINUTES:-30}"

printf '[MIC-38] Starting Sprint 1 sign-off orchestrator...\n'
printf '[MIC-38] run_id=%s\n' "$MIC38_RUN_ID"
printf '[MIC-38] cdc_video_id=%s\n' "$MIC38_VIDEO_ID"
printf '[MIC-38] max_freshness_minutes=%s\n' "$MAX_FRESHNESS_MINUTES"
printf '[MIC-38] content_max_invalid_rate=%s\n' "$MAX_CONTENT_INVALID_RATE"
printf '[MIC-38] cdc_max_invalid_rate=%s\n' "$MAX_CDC_INVALID_RATE"

cd "$REPO_ROOT"

printf '[MIC-38] Starting required services...\n'
docker compose up -d minio minio-mc iceberg-rest zookeeper kafka spark

printf '[MIC-38] Ensuring required topics exist with Sprint-1 partitions...\n'
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
  --topic content_events \
  --partitions 6 \
  --replication-factor 1

docker exec lakehouse-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic cdc.content.videos \
  --partitions 3 \
  --replication-factor 1

# Keep partition contract aligned if topics already existed with smaller partition counts.
docker exec lakehouse-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --alter \
  --topic content_events \
  --partitions 6 || true

docker exec lakehouse-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --alter \
  --topic cdc.content.videos \
  --partitions 3 || true

printf '[MIC-38] Starting both Spark jobs for integrated E2E run...\n'
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_content_events_aggregator.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_video_cdc_upsert.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"

docker exec lakehouse-spark bash -lc "nohup /opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_content_events_aggregator.py > /tmp/mic38_content_agg.log 2>&1 &"
docker exec lakehouse-spark bash -lc "nohup /opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_video_cdc_upsert.py > /tmp/mic38_cdc_upsert.log 2>&1 &"
sleep "$WAIT_AFTER_JOB_START_SECONDS"

MIN_PROCESSED_AT_MS="${MIN_PROCESSED_AT_MS:-$(( $(date +%s) * 1000 ))}"
MIN_INGESTED_AT_MS="${MIN_INGESTED_AT_MS:-$MIN_PROCESSED_AT_MS}"
printf '[MIC-38] min_processed_at_ms=%s\n' "$MIN_PROCESSED_AT_MS"
printf '[MIC-38] min_ingested_at_ms=%s\n' "$MIN_INGESTED_AT_MS"

printf '[MIC-38] Running bounded generator once for shared integrated run...\n'
"$PYTHON_BIN" src/generator/m1_bounded_run.py \
  --config docs/architecture/generator/examples/m1_run_config.example.json \
  --run-id "$MIC38_RUN_ID" \
  --sink kafka \
  --bootstrap-servers "$BOOTSTRAP_SERVERS"

sleep "$WAIT_AFTER_BOUNDED_RUN_SECONDS"

printf '[MIC-38] Emitting deterministic mixed CDC fixture for CDC valid+invalid checks...\n'
"$PYTHON_BIN" src/scripts/emit_mic43_cdc_mixed_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --video-id "$MIC38_VIDEO_ID" \
  --base-ts-ms "$BASE_TS_MS"

sleep "$WAIT_AFTER_CDC_FIXTURE_SECONDS"

printf '[MIC-38] Verifying content valid path (MIC-40 style gate)...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_aggregator.py \
  --min-raw-rows "$MIN_RAW_ROWS" \
  --min-gold-rows "$MIN_GOLD_ROWS" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --min-processed-at-ms "$MIN_PROCESSED_AT_MS"

printf '[MIC-38] Verifying content invalid path (MIC-39 style gate)...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_contract_enforcement.py \
  --min-raw-rows "$MIN_RAW_ROWS" \
  --min-gold-rows "$MIN_GOLD_ROWS" \
  --min-invalid-rows "$MIN_CONTENT_INVALID_ROWS" \
  --max-invalid-rate "$MAX_CONTENT_INVALID_RATE" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --min-ingested-at-ms "$MIN_INGESTED_AT_MS"

printf '[MIC-38] Verifying CDC valid path (MIC-37 style gate)...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_upsert.py \
  --video-id "$MIC38_VIDEO_ID" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --expect-status "$EXPECTED_CDC_STATUS" \
  --expect-source-ts-ms "$EXPECTED_CDC_SOURCE_TS_MS"

printf '[MIC-38] Verifying CDC invalid path (MIC-43 style gate)...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_invalid_cdc_quarantine.py \
  --table lakehouse.bronze.invalid_events_cdc_videos \
  --lookback-minutes "$LOOKBACK_MINUTES" \
  --min-row-count "$MIN_CDC_INVALID_ROWS" \
  --expect-error-codes CDC_MISSING_OP,CDC_UNSUPPORTED_OP,CDC_MISSING_SCHEMA_VERSION,CDC_MISSING_AFTER_VIDEO_ID

printf '[MIC-38] Verifying CDC health (freshness + invalid-rate)...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/check_rt_video_cdc_health.py \
  --dim-table lakehouse.dims.dim_videos \
  --invalid-table lakehouse.bronze.invalid_events_cdc_videos \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --lookback-minutes "$LOOKBACK_MINUTES" \
  --max-invalid-rate "$MAX_CDC_INVALID_RATE"

printf '[MIC-38] Checking checkpoint evidence for all sinks...\n'
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1 | head -n 40"

printf '[MIC-38] Confirming both query processes are alive...\n'
docker exec lakehouse-spark bash -lc "pgrep -f rt_content_events_aggregator.py"
docker exec lakehouse-spark bash -lc "pgrep -f rt_video_cdc_upsert.py"

printf '[MIC-38] Integrated Sprint 1 E2E verification completed.\n'
