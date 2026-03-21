#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/acceptance_common.sh"

usage() {
  cat <<'EOF'
Usage: run_content_aggregator_acceptance.sh

Environment overrides:
  ACCEPTANCE_RESET_DOCKER
  BOUNDED_RUN_TIME_MODE
  BOUNDED_RUN_STARTED_AT
  BOOTSTRAP_SERVERS
  PYTHON_BIN
  WAIT_AFTER_JOB_START_SECONDS
  WAIT_AFTER_BOUNDED_RUN_SECONDS
  MIN_RAW_ROWS
  MIN_GOLD_ROWS
  MAX_FRESHNESS_MINUTES
  CONTENT_AGGREGATOR_RUN_ID
  MIN_PROCESSED_AT_MS
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[CONTENT-AGGREGATOR] ERROR: unknown argument '$1'" >&2
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
MAX_FRESHNESS_MINUTES="${MAX_FRESHNESS_MINUTES:-10}"
CONTENT_AGGREGATOR_RUN_ID="${CONTENT_AGGREGATOR_RUN_ID:-content_aggregator_$(date -u +%Y%m%dT%H%M%SZ)}"

resolve_bounded_run_started_at "CONTENT-AGGREGATOR"

cd "$REPO_ROOT"
acceptance_maybe_reset_docker "CONTENT-AGGREGATOR"

printf '[CONTENT-AGGREGATOR] Starting required services...\n'
docker compose up -d minio minio-mc catalog-postgres iceberg-rest zookeeper kafka spark

printf '[CONTENT-AGGREGATOR] Ensuring topic exists...\n'
wait_for_kafka_ready "CONTENT-AGGREGATOR" 60 2

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

printf '[CONTENT-AGGREGATOR] Starting Spark content aggregator job...\n'
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_content_events_aggregator.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
docker exec lakehouse-spark bash -lc "nohup /opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_content_events_aggregator.py > /tmp/content_aggregator_content_agg.log 2>&1 &"
sleep "$WAIT_AFTER_JOB_START_SECONDS"

printf '[CONTENT-AGGREGATOR] Running bounded generator...\n'
MIN_PROCESSED_AT_MS="${MIN_PROCESSED_AT_MS:-$(( $(date +%s) * 1000 ))}"
printf '[CONTENT-AGGREGATOR] Generator run id: %s\n' "$CONTENT_AGGREGATOR_RUN_ID"
if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config docs/architecture/generator/examples/bounded_run_config.example.json \
    --run-id "$CONTENT_AGGREGATOR_RUN_ID" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --started-at "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
else
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config docs/architecture/generator/examples/bounded_run_config.example.json \
    --run-id "$CONTENT_AGGREGATOR_RUN_ID" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS"
fi

sleep "$WAIT_AFTER_BOUNDED_RUN_SECONDS"

printf '[CONTENT-AGGREGATOR] Verifying Bronze/Gold outputs...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_aggregator.py \
  --min-raw-rows "$MIN_RAW_ROWS" \
  --min-gold-rows "$MIN_GOLD_ROWS" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --min-processed-at-ms "$MIN_PROCESSED_AT_MS"

printf '[CONTENT-AGGREGATOR] Checking checkpoint files...\n'
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1 | head -n 40"

printf '[CONTENT-AGGREGATOR] Confirming query process is alive...\n'
docker exec lakehouse-spark bash -lc "pgrep -f rt_content_events_aggregator.py"

printf '[CONTENT-AGGREGATOR] Acceptance flow completed.\n'
