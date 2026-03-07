#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_BOUNDED_RUN_SECONDS="${WAIT_AFTER_BOUNDED_RUN_SECONDS:-75}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-1}"
MIN_GOLD_ROWS="${MIN_GOLD_ROWS:-1}"
MAX_FRESHNESS_MINUTES="${MAX_FRESHNESS_MINUTES:-10}"
MIC40_RUN_ID="${MIC40_RUN_ID:-mic40_$(date -u +%Y%m%dT%H%M%SZ)}"

printf '[MIC-40] Starting required services...\n'
docker compose up -d minio minio-mc iceberg-rest zookeeper kafka spark

printf '[MIC-40] Ensuring topic exists...\n'
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

# Existing topics may already exist with too few partitions for M1 preflight.
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

printf '[MIC-40] Starting Spark content aggregator job...\n'
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_content_events_aggregator.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
docker exec lakehouse-spark bash -lc "nohup /opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_content_events_aggregator.py > /tmp/mic40_content_agg.log 2>&1 &"
sleep "$WAIT_AFTER_JOB_START_SECONDS"

printf '[MIC-40] Running bounded generator...\n'
MIN_PROCESSED_AT_MS="${MIN_PROCESSED_AT_MS:-$(( $(date +%s) * 1000 ))}"
printf '[MIC-40] Generator run id: %s\n' "$MIC40_RUN_ID"
"$PYTHON_BIN" src/generator/m1_bounded_run.py \
  --config docs/architecture/generator/examples/m1_run_config.example.json \
  --run-id "$MIC40_RUN_ID" \
  --sink kafka \
  --bootstrap-servers "$BOOTSTRAP_SERVERS"

sleep "$WAIT_AFTER_BOUNDED_RUN_SECONDS"

printf '[MIC-40] Verifying Bronze/Gold outputs...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_aggregator.py \
  --min-raw-rows "$MIN_RAW_ROWS" \
  --min-gold-rows "$MIN_GOLD_ROWS" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --min-processed-at-ms "$MIN_PROCESSED_AT_MS"

printf '[MIC-40] Checking checkpoint files...\n'
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1 | head -n 40"

printf '[MIC-40] Confirming query process is alive...\n'
docker exec lakehouse-spark bash -lc "pgrep -f rt_content_events_aggregator.py"

printf '[MIC-40] Acceptance flow completed.\n'
