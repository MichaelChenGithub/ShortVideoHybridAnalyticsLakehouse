#!/usr/bin/env bash
set -euo pipefail

VIDEO_ID="${VIDEO_ID:-mic37_vid_001}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
EXPECTED_STATUS="${EXPECTED_STATUS:-copyright_strike}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_FIXTURE_SECONDS="${WAIT_AFTER_FIXTURE_SECONDS:-75}"
BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"
EXPECTED_SOURCE_TS_MS="${EXPECTED_SOURCE_TS_MS:-$((BASE_TS_MS + 2000))}"

printf '[MIC-37] Starting required services...\n'
docker compose up -d minio minio-mc iceberg-rest zookeeper kafka spark

printf '[MIC-37] Ensuring topic exists...\n'
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

printf '[MIC-37] Starting Spark CDC upsert job...\n'
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_video_cdc_upsert.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
docker exec lakehouse-spark bash -lc "nohup /opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_video_cdc_upsert.py > /tmp/mic37_cdc_upsert.log 2>&1 &"
sleep "$WAIT_AFTER_JOB_START_SECONDS"

printf '[MIC-37] Running bounded generator...\n'
"$PYTHON_BIN" src/generator/m1_bounded_run.py \
  --config docs/architecture/generator/examples/m1_run_config.example.json \
  --sink kafka \
  --bootstrap-servers "$BOOTSTRAP_SERVERS"

printf '[MIC-37] Emitting deterministic CDC fixture...\n'
"$PYTHON_BIN" src/scripts/emit_cdc_videos_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --video-id "$VIDEO_ID" \
  --scenario full \
  --base-ts-ms "$BASE_TS_MS"
sleep "$WAIT_AFTER_FIXTURE_SECONDS"

printf '[MIC-37] Verifying upsert result...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_upsert.py \
  --video-id "$VIDEO_ID" \
  --max-freshness-minutes 10 \
  --expect-status "$EXPECTED_STATUS" \
  --expect-source-ts-ms "$EXPECTED_SOURCE_TS_MS"

printf '[MIC-37] Checking checkpoint files...\n'
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1 | head -n 40"

printf '[MIC-37] Confirming query process is alive...\n'
docker exec lakehouse-spark bash -lc "pgrep -f rt_video_cdc_upsert.py"

printf '[MIC-37] Acceptance flow completed.\n'
