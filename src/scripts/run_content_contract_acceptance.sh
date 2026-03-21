#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/acceptance_common.sh"

usage() {
  cat <<'EOF'
Usage: run_content_contract_acceptance.sh

Environment overrides:
  ACCEPTANCE_RESET_DOCKER
  BOUNDED_RUN_TIME_MODE
  BOUNDED_RUN_STARTED_AT
  BOUNDED_RUN_CONFIG
  BOOTSTRAP_SERVERS
  PYTHON_BIN
  WAIT_AFTER_JOB_START_SECONDS
  WAIT_AFTER_BOUNDED_RUN_SECONDS
  CONTRACT_READY_RETRIES
  CONTRACT_READY_SLEEP_SECONDS
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
BOUNDED_RUN_CONFIG="${BOUNDED_RUN_CONFIG:-docs/architecture/generator/examples/bounded_run_config.example.json}"
WAIT_AFTER_JOB_START_SECONDS="${WAIT_AFTER_JOB_START_SECONDS:-30}"
WAIT_AFTER_BOUNDED_RUN_SECONDS="${WAIT_AFTER_BOUNDED_RUN_SECONDS:-75}"
CONTRACT_READY_RETRIES="${CONTRACT_READY_RETRIES:-18}"
CONTRACT_READY_SLEEP_SECONDS="${CONTRACT_READY_SLEEP_SECONDS:-10}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-1}"
MIN_GOLD_ROWS="${MIN_GOLD_ROWS:-1}"
MIN_INVALID_ROWS="${MIN_INVALID_ROWS:-1}"
MAX_INVALID_RATE="${MAX_INVALID_RATE:-0.20}"
MAX_FRESHNESS_MINUTES="${MAX_FRESHNESS_MINUTES:-10}"
CONTENT_CONTRACT_RUN_ID="${CONTENT_CONTRACT_RUN_ID:-content_contract_$(date -u +%Y%m%dT%H%M%SZ)}"

resolve_bounded_run_started_at "CONTENT-CONTRACT"

cd "$REPO_ROOT"
acceptance_maybe_reset_docker "CONTENT-CONTRACT"

printf '[CONTENT-CONTRACT] Starting required services...\n'
docker compose up -d minio minio-mc catalog-postgres iceberg-rest zookeeper kafka spark

printf '[CONTENT-CONTRACT] Ensuring required topics exist...\n'
wait_for_kafka_ready "CONTENT-CONTRACT" 60 2

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

MIN_INGESTED_AT_MS="${MIN_INGESTED_AT_MS:-}"
if [ -z "${MIN_INGESTED_AT_MS:-}" ]; then
  MIN_INGESTED_AT_MS="$("$PYTHON_BIN" - <<PY
import json
from datetime import datetime, timezone
from pathlib import Path

started_at = "${BOUNDED_RUN_EFFECTIVE_STARTED_AT}".strip()
if not started_at:
    cfg_path = Path("${BOUNDED_RUN_CONFIG}")
    if cfg_path.exists():
        cfg = json.loads(cfg_path.read_text())
        started_at = str(cfg.get("started_at", "")).strip()
if not started_at:
    print(int(datetime.now(timezone.utc).timestamp() * 1000))
else:
    if started_at.endswith("Z"):
        started_at = started_at[:-1] + "+00:00"
    dt = datetime.fromisoformat(started_at).astimezone(timezone.utc)
    print(int(dt.timestamp() * 1000))
PY
)"
fi
printf '[CONTENT-CONTRACT] Generator run id: %s\n' "$CONTENT_CONTRACT_RUN_ID"
printf '[CONTENT-CONTRACT] min_ingested_at_ms: %s\n' "$MIN_INGESTED_AT_MS"

printf '[CONTENT-CONTRACT] Running bounded generator...\n'
if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config "$BOUNDED_RUN_CONFIG" \
    --run-id "$CONTENT_CONTRACT_RUN_ID" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --started-at "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
else
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config "$BOUNDED_RUN_CONFIG" \
    --run-id "$CONTENT_CONTRACT_RUN_ID" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS"
fi

sleep "$WAIT_AFTER_BOUNDED_RUN_SECONDS"

printf '[CONTENT-CONTRACT] Verifying contract enforcement and quarantine health...\n'
contract_ready=0
for _ in $(seq 1 "$CONTRACT_READY_RETRIES"); do
  if docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_contract_enforcement.py \
    --min-raw-rows "$MIN_RAW_ROWS" \
    --min-gold-rows "$MIN_GOLD_ROWS" \
    --min-invalid-rows "$MIN_INVALID_ROWS" \
    --max-invalid-rate "$MAX_INVALID_RATE" \
    --max-freshness-minutes "$MAX_FRESHNESS_MINUTES" \
    --min-ingested-at-ms "$MIN_INGESTED_AT_MS" >/dev/null 2>&1
  then
    contract_ready=1
    break
  fi
  sleep "$CONTRACT_READY_SLEEP_SECONDS"
done

if [ "$contract_ready" -ne 1 ]; then
  echo "[CONTENT-CONTRACT] ERROR: content contract outputs not ready after retries." >&2
fi

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
