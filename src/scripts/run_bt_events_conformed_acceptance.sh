#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: run_bt_events_conformed_acceptance.sh

Environment overrides:
  BOOTSTRAP_SERVERS
  PYTHON_BIN
  WAIT_AFTER_JOB_START_SECONDS
  WAIT_AFTER_BOUNDED_RUN_SECONDS
  WAIT_AFTER_BATCH_SECONDS
  MIN_RAW_ROWS
  MIN_GOLD_ROWS
  MAX_FRESHNESS_MINUTES
  MIN_EVENTS_CONFORMED_ROWS
  EVENTS_CONFORMED_DATA_DATE
  EVENTS_CONFORMED_RUN_ID
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
      echo "[BT-EVENTS-CONFORMED] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_BOUNDED_RUN_SECONDS="${WAIT_AFTER_BOUNDED_RUN_SECONDS:-75}"
WAIT_AFTER_BATCH_SECONDS="${WAIT_AFTER_BATCH_SECONDS:-10}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-1}"
MIN_GOLD_ROWS="${MIN_GOLD_ROWS:-1}"
MAX_FRESHNESS_MINUTES="${MAX_FRESHNESS_MINUTES:-10}"
MIN_EVENTS_CONFORMED_ROWS="${MIN_EVENTS_CONFORMED_ROWS:-1}"
EVENTS_CONFORMED_DATA_DATE="${EVENTS_CONFORMED_DATA_DATE:-}"
EVENTS_CONFORMED_RUN_ID="${EVENTS_CONFORMED_RUN_ID:-bt_events_conformed_$(date -u +%Y%m%dT%H%M%SZ)}"
MIN_PROCESSED_AT_MS="${MIN_PROCESSED_AT_MS:-$(( $(date +%s) * 1000 ))}"

printf '[BT-EVENTS-CONFORMED] Starting required services...\n'
docker compose up -d minio minio-mc catalog-postgres iceberg-rest zookeeper kafka spark

printf '[BT-EVENTS-CONFORMED] Ensuring required topics exist...\n'
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

printf '[BT-EVENTS-CONFORMED] Starting Spark content aggregator job (raw_events producer)...\n'
docker exec lakehouse-spark bash -lc "pids=\$(ps -eo pid,args | awk '/[r]t_content_events_aggregator.py/ {print \$1}'); if [ -n \"\$pids\" ]; then kill \$pids || true; fi"
docker exec lakehouse-spark bash -lc "nohup /opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_content_events_aggregator.py > /tmp/bt_events_conformed_content_agg.log 2>&1 &"
sleep "$WAIT_AFTER_JOB_START_SECONDS"

printf '[BT-EVENTS-CONFORMED] Running bounded generator...\n'
printf '[BT-EVENTS-CONFORMED] Generator run id: %s\n' "$EVENTS_CONFORMED_RUN_ID"
"$PYTHON_BIN" src/generator/bounded_run_cli.py \
  --config docs/architecture/generator/examples/bounded_run_config.example.json \
  --run-id "$EVENTS_CONFORMED_RUN_ID" \
  --sink kafka \
  --bootstrap-servers "$BOOTSTRAP_SERVERS"

sleep "$WAIT_AFTER_BOUNDED_RUN_SECONDS"

printf '[BT-EVENTS-CONFORMED] Verifying raw_events readiness...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_aggregator.py \
  --min-raw-rows "$MIN_RAW_ROWS" \
  --min-gold-rows "$MIN_GOLD_ROWS" \
  --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
  --min-processed-at-ms "$MIN_PROCESSED_AT_MS"

printf '[BT-EVENTS-CONFORMED] Running events_conformed batch transform...\n'
docker cp src/spark/bt_events_conformed.py lakehouse-spark:/home/iceberg/local/src/spark/bt_events_conformed.py
docker cp src/spark/bt_events_conformed_sql.py lakehouse-spark:/home/iceberg/local/src/spark/bt_events_conformed_sql.py
docker cp src/scripts/verify_bt_events_conformed.py lakehouse-spark:/home/iceberg/local/src/scripts/verify_bt_events_conformed.py
docker exec lakehouse-spark bash -lc "/opt/spark/bin/spark-submit /home/iceberg/local/src/spark/bt_events_conformed.py"
sleep "$WAIT_AFTER_BATCH_SECONDS"

printf '[BT-EVENTS-CONFORMED] Verifying events_conformed output contract...\n'
verify_args=(
  "--table" "lakehouse.silver.events_conformed"
  "--min-row-count" "$MIN_EVENTS_CONFORMED_ROWS"
)
if [ -n "$EVENTS_CONFORMED_DATA_DATE" ]; then
  verify_args+=("--data-date" "$EVENTS_CONFORMED_DATA_DATE")
fi
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_bt_events_conformed.py "${verify_args[@]}"

printf '[BT-EVENTS-CONFORMED] Acceptance flow completed.\n'
