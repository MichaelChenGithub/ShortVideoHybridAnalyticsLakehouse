#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_cdc_contract_acceptance.sh

Environment overrides:
  VIDEO_ID
  BOOTSTRAP_SERVERS
  PYTHON_BIN
  WAIT_AFTER_JOB_START_SECONDS
  WAIT_AFTER_FIXTURE_SECONDS
  LOOKBACK_MINUTES
  MAX_FRESHNESS_MINUTES
  MIN_INVALID_ROWS
  BASE_TS_MS
  EXPECTED_STATUS
  EXPECTED_SOURCE_TS_MS
  MAX_INVALID_RATE
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
      echo "[CDC-CONTRACT] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

VIDEO_ID="${VIDEO_ID:-cdc_contract_vid_001}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_FIXTURE_SECONDS="${WAIT_AFTER_FIXTURE_SECONDS:-75}"
LOOKBACK_MINUTES="${LOOKBACK_MINUTES:-30}"
MAX_FRESHNESS_MINUTES="${MAX_FRESHNESS_MINUTES:-10}"
MIN_INVALID_ROWS="${MIN_INVALID_ROWS:-4}"
BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"
EXPECTED_STATUS="${EXPECTED_STATUS:-copyright_strike}"
EXPECTED_SOURCE_TS_MS="${EXPECTED_SOURCE_TS_MS:-$((BASE_TS_MS + 2000))}"
MAX_INVALID_RATE="${MAX_INVALID_RATE:-}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-2}"

printf '[CDC-CONTRACT] Starting required services...\n'
docker compose up -d minio minio-mc catalog-postgres iceberg-rest zookeeper kafka spark

printf '[CDC-CONTRACT] Ensuring CDC topic exists...\n'
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
  --topic cdc.content.videos \
  --partitions 3 \
  --replication-factor 1

printf '[CDC-CONTRACT] Starting Spark CDC upsert + quarantine job...\n'
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_video_cdc_upsert.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
docker exec lakehouse-spark bash -lc "nohup /opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_video_cdc_upsert.py > /tmp/cdc_contract_cdc_upsert.log 2>&1 &"
sleep "$WAIT_AFTER_JOB_START_SECONDS"

printf '[CDC-CONTRACT] Emitting deterministic mixed CDC fixture...\n'
"$PYTHON_BIN" src/scripts/emit_cdc_mixed_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --video-id "$VIDEO_ID" \
  --base-ts-ms "$BASE_TS_MS"
sleep "$WAIT_AFTER_FIXTURE_SECONDS"

printf '[CDC-CONTRACT] Verifying dim_videos upsert path still healthy...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_upsert.py \
  --video-id "$VIDEO_ID" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --expect-status "$EXPECTED_STATUS" \
  --expect-source-ts-ms "$EXPECTED_SOURCE_TS_MS"

printf '[CDC-CONTRACT] Verifying valid CDC records also land in raw bronze...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_raw_bronze.py \
  --video-id "$VIDEO_ID" \
  --table lakehouse.bronze.raw_cdc_videos \
  --min-row-count "$MIN_RAW_ROWS" \
  --expect-status "$EXPECTED_STATUS" \
  --expect-latest-ts-ms "$EXPECTED_SOURCE_TS_MS" \
  --min-source-ts-ms "$BASE_TS_MS"

printf '[CDC-CONTRACT] Verifying invalid CDC quarantine records...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_invalid_cdc_quarantine.py \
  --table lakehouse.bronze.invalid_events_cdc_videos \
  --lookback-minutes "$LOOKBACK_MINUTES" \
  --min-row-count "$MIN_INVALID_ROWS" \
  --expect-error-codes CDC_MISSING_OP,CDC_UNSUPPORTED_OP,CDC_MISSING_SCHEMA_VERSION,CDC_MISSING_AFTER_VIDEO_ID

printf '[CDC-CONTRACT] Running CDC health check (freshness + invalid-rate)...\n'
health_cmd=(
  docker exec lakehouse-spark python /home/iceberg/local/src/scripts/check_rt_video_cdc_health.py
  --dim-table lakehouse.dims.dim_videos
  --invalid-table lakehouse.bronze.invalid_events_cdc_videos
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES"
  --lookback-minutes "$LOOKBACK_MINUTES"
)
if [ -n "$MAX_INVALID_RATE" ]; then
  health_cmd+=(--max-invalid-rate "$MAX_INVALID_RATE")
fi
"${health_cmd[@]}"

printf '[CDC-CONTRACT] Checking checkpoint files for both sinks...\n'
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_video_cdc_upsert/raw_cdc_videos/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1 | head -n 40"

printf '[CDC-CONTRACT] Confirming query process is alive...\n'
docker exec lakehouse-spark bash -lc "pgrep -f rt_video_cdc_upsert.py"

printf '[CDC-CONTRACT] Acceptance flow completed.\n'
