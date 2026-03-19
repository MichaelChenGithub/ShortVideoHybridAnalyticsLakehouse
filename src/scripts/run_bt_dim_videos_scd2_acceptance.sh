#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_bt_dim_videos_scd2_acceptance.sh

Environment overrides:
  VIDEO_ID
  BOOTSTRAP_SERVERS
  PYTHON_BIN
  WAIT_AFTER_JOB_START_SECONDS
  WAIT_AFTER_FIXTURE_SECONDS
  WAIT_AFTER_BATCH_SECONDS
  RAW_READY_RETRIES
  RAW_READY_SLEEP_SECONDS
  BASE_TS_MS
  EXPECTED_SOURCE_TS_MS
  MIN_RAW_ROWS
  EXPECT_CATEGORY
  EXPECT_REGION
  EXPECT_STATUS
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[BT-DIM-VIDEOS-SCD2] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

VIDEO_ID="${VIDEO_ID:-bt_dim_vid_001}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_FIXTURE_SECONDS="${WAIT_AFTER_FIXTURE_SECONDS:-75}"
WAIT_AFTER_BATCH_SECONDS="${WAIT_AFTER_BATCH_SECONDS:-10}"
RAW_READY_RETRIES="${RAW_READY_RETRIES:-18}"
RAW_READY_SLEEP_SECONDS="${RAW_READY_SLEEP_SECONDS:-10}"
BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"
EXPECTED_SOURCE_TS_MS="${EXPECTED_SOURCE_TS_MS:-$((BASE_TS_MS + 2000))}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-4}"
EXPECT_CATEGORY="${EXPECT_CATEGORY:-Comedy}"
EXPECT_REGION="${EXPECT_REGION:-US}"
EXPECT_STATUS="${EXPECT_STATUS:-copyright_strike}"

printf '[BT-DIM-VIDEOS-SCD2] Starting required services...\n'
docker compose up -d minio minio-mc iceberg-rest zookeeper kafka spark

printf '[BT-DIM-VIDEOS-SCD2] Ensuring CDC topic exists...\n'
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

printf '[BT-DIM-VIDEOS-SCD2] Starting Spark CDC upsert job (raw bronze source)...\n'
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_video_cdc_upsert.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
docker exec lakehouse-spark bash -lc "aws_jar='/root/.ivy2/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar'; if [ -f \"\$aws_jar\" ] && ! jar tf \"\$aws_jar\" >/dev/null 2>&1; then echo '[BT-DIM-VIDEOS-SCD2] WARN: removing corrupted aws sdk bundle from ivy cache'; rm -f \"\$aws_jar\"; rm -rf /root/.ivy2/cache/com.amazonaws/aws-java-sdk-bundle; fi"
docker exec lakehouse-spark bash -lc "nohup /opt/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /home/iceberg/local/src/spark/rt_video_cdc_upsert.py > /tmp/bt_dim_videos_scd2_cdc_upsert.log 2>&1 &"
sleep "$WAIT_AFTER_JOB_START_SECONDS"

printf '[BT-DIM-VIDEOS-SCD2] Running bounded generator...\n'
"$PYTHON_BIN" src/generator/bounded_run_cli.py \
  --config docs/architecture/generator/examples/bounded_run_config.example.json \
  --sink kafka \
  --bootstrap-servers "$BOOTSTRAP_SERVERS"

printf '[BT-DIM-VIDEOS-SCD2] Emitting deterministic CDC fixture...\n'
"$PYTHON_BIN" src/scripts/emit_cdc_videos_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --video-id "$VIDEO_ID" \
  --scenario full \
  --base-ts-ms "$BASE_TS_MS"
sleep "$WAIT_AFTER_FIXTURE_SECONDS"

printf '[BT-DIM-VIDEOS-SCD2] Waiting for raw CDC landing readiness...\n'
raw_ready=0
for _ in $(seq 1 "$RAW_READY_RETRIES"); do
  if docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_raw_bronze.py \
    --video-id "$VIDEO_ID" \
    --table lakehouse.bronze.raw_cdc_videos \
    --min-row-count "$MIN_RAW_ROWS" \
    --expect-status "$EXPECT_STATUS" \
    --expect-latest-ts-ms "$EXPECTED_SOURCE_TS_MS" \
    --min-source-ts-ms "$BASE_TS_MS" >/dev/null 2>&1; then
    raw_ready=1
    break
  fi
  sleep "$RAW_READY_SLEEP_SECONDS"
done

if [ "$raw_ready" -ne 1 ]; then
  echo "[BT-DIM-VIDEOS-SCD2] ERROR: raw CDC rows not ready after retries." >&2
  docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_raw_bronze.py \
    --video-id "$VIDEO_ID" \
    --table lakehouse.bronze.raw_cdc_videos \
    --min-row-count "$MIN_RAW_ROWS" \
    --expect-status "$EXPECT_STATUS" \
    --expect-latest-ts-ms "$EXPECTED_SOURCE_TS_MS" \
    --min-source-ts-ms "$BASE_TS_MS"
  exit 1
fi

printf '[BT-DIM-VIDEOS-SCD2] Running dim_videos_scd2 batch transform...\n'
docker cp src/spark/bt_dim_videos_scd2.py lakehouse-spark:/home/iceberg/local/src/spark/bt_dim_videos_scd2.py
docker cp src/spark/bt_dim_videos_scd2_sql.py lakehouse-spark:/home/iceberg/local/src/spark/bt_dim_videos_scd2_sql.py
docker cp src/scripts/verify_bt_dim_videos_scd2.py lakehouse-spark:/home/iceberg/local/src/scripts/verify_bt_dim_videos_scd2.py
docker exec lakehouse-spark bash -lc "/opt/spark/bin/spark-submit /home/iceberg/local/src/spark/bt_dim_videos_scd2.py"
sleep "$WAIT_AFTER_BATCH_SECONDS"

printf '[BT-DIM-VIDEOS-SCD2] Verifying dim_videos_scd2 output contract...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_bt_dim_videos_scd2.py \
  --video-id "$VIDEO_ID" \
  --table lakehouse.dims.dim_videos_scd2 \
  --expect-category "$EXPECT_CATEGORY" \
  --expect-region "$EXPECT_REGION" \
  --expect-status "$EXPECT_STATUS"

printf '[BT-DIM-VIDEOS-SCD2] Acceptance flow completed.\n'
