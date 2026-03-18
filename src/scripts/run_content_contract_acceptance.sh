#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_content_contract_acceptance.sh

Environment overrides:
  BOOTSTRAP_SERVERS
  PYTHON_BIN
  WAIT_AFTER_JOB_START_SECONDS
  WAIT_AFTER_BOUNDED_RUN_SECONDS
  MIN_RAW_ROWS
  MIN_GOLD_ROWS
  MIN_INVALID_ROWS
  MAX_INVALID_RATE
  MAX_FRESHNESS_MINUTES
  CONTENT_CONTRACT_RUN_ID
  MIN_INGESTED_AT_MS
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[CONTENT-CONTRACT] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_BOUNDED_RUN_SECONDS="${WAIT_AFTER_BOUNDED_RUN_SECONDS:-75}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-1}"
MIN_GOLD_ROWS="${MIN_GOLD_ROWS:-1}"
MIN_INVALID_ROWS="${MIN_INVALID_ROWS:-1}"
MAX_INVALID_RATE="${MAX_INVALID_RATE:-0.20}"
MAX_FRESHNESS_MINUTES="${MAX_FRESHNESS_MINUTES:-10}"
CONTENT_CONTRACT_RUN_ID="${CONTENT_CONTRACT_RUN_ID:-content_contract_$(date -u +%Y%m%dT%H%M%SZ)}"

printf '[CONTENT-CONTRACT] Starting required services...\n'
docker compose up -d minio minio-mc catalog-postgres iceberg-rest zookeeper kafka spark

printf '[CONTENT-CONTRACT] Ensuring required topics exist...\n'
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

# Existing topics may already exist with too few partitions for local preflight.
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

printf '[CONTENT-CONTRACT] Starting Spark content aggregator job...\n'
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_content_events_aggregator.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
docker exec lakehouse-spark bash -lc "nohup /opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_content_events_aggregator.py > /tmp/content_contract_content_agg.log 2>&1 &"
sleep "$WAIT_AFTER_JOB_START_SECONDS"

MIN_INGESTED_AT_MS="${MIN_INGESTED_AT_MS:-$(( $(date +%s) * 1000 ))}"
printf '[CONTENT-CONTRACT] Generator run id: %s\n' "$CONTENT_CONTRACT_RUN_ID"
printf '[CONTENT-CONTRACT] min_ingested_at_ms: %s\n' "$MIN_INGESTED_AT_MS"

printf '[CONTENT-CONTRACT] Running bounded generator...\n'
"$PYTHON_BIN" src/generator/bounded_run_cli.py \
  --config docs/architecture/generator/examples/bounded_run_config.example.json \
  --run-id "$CONTENT_CONTRACT_RUN_ID" \
  --sink kafka \
  --bootstrap-servers "$BOOTSTRAP_SERVERS"

sleep "$WAIT_AFTER_BOUNDED_RUN_SECONDS"

printf '[CONTENT-CONTRACT] Verifying contract enforcement and quarantine health...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_contract_enforcement.py \
  --min-raw-rows "$MIN_RAW_ROWS" \
  --min-gold-rows "$MIN_GOLD_ROWS" \
  --min-invalid-rows "$MIN_INVALID_ROWS" \
  --max-invalid-rate "$MAX_INVALID_RATE" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --min-ingested-at-ms "$MIN_INGESTED_AT_MS"

printf '[CONTENT-CONTRACT] Checking checkpoint files...\n'
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1 | head -n 40"

printf '[CONTENT-CONTRACT] Confirming query process is alive...\n'
docker exec lakehouse-spark bash -lc "pgrep -f rt_content_events_aggregator.py"

printf '[CONTENT-CONTRACT] Acceptance flow completed.\n'
