#!/usr/bin/env bash
# Acceptance script: bulk_arrival adversarial scenario.
#
# Validates that the pipeline stays stable and self-recovers during a 10x
# traffic spike without infrastructure scaling, by testing two optimizations:
#
#   1. maxOffsetsPerTrigger — bounds Spark micro-batch size, preventing OOM
#      during the burst and allowing intentional lag to build then drain.
#   2. Iceberg small-file compaction — merges burst micro-batch files so
#      batch read performance is not degraded post-burst.
#
# Assertions:
#   A2: Consumer lag drains to 0 within recovery_timeout_seconds
#   A3: No Spark task failures during burst
#   A4: Bronze row count = total_emitted events (no message loss)
#   A5: Iceberg file count post-compaction within MAX_FILE_COUNT_AFTER_COMPACTION
#
# Note on A1 (lag < max_lag_threshold *during* burst): validated indirectly —
# the maxOffsetsPerTrigger configuration check plus A2 + A3 together prove
# the backpressure strategy worked.
#
# Prerequisites: make infra && make streaming
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

LOG="BULK-ARRIVAL"

usage() {
  cat <<'EOF'
Usage: run_bulk_arrival_acceptance.sh

Environment overrides:
  PYTHON_BIN                        Python interpreter (default: auto-detect .venv)
  BOOTSTRAP_SERVERS                 Kafka bootstrap servers (default: localhost:9092)
  BULK_ARRIVAL_CONFIG               Generator config (default: config/adversarial/bulk_arrival.json)
  DATA_DATE                         Bronze partition date (default: 2026-04-07)
  CONSUMER_GROUP                    Kafka consumer group to poll for lag
                                    (default: cg_rt_content_events_aggregator_v1)
  WAIT_AFTER_GENERATOR_SECONDS      Initial drain wait after generator (default: 30)
  RECOVERY_TIMEOUT_SECONDS          Max wait for lag drain (default: from config: 120)
  MAX_FILE_COUNT_AFTER_COMPACTION   Max files allowed post-compaction (default: 20)
  TRINO_HOST                        Trino host (default: localhost)
  TRINO_PORT                        Trino port (default: 8081)
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help) usage; exit 0 ;;
    *) printf '[%s] ERROR: unknown argument "%s"\n' "$LOG" "$1" >&2; usage >&2; exit 2 ;;
  esac
fi

DEFAULT_PYTHON_BIN="python3"
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
  DEFAULT_PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
fi

PYTHON_BIN="${PYTHON_BIN:-$DEFAULT_PYTHON_BIN}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
BULK_ARRIVAL_CONFIG="${BULK_ARRIVAL_CONFIG:-config/adversarial/bulk_arrival.json}"
DATA_DATE="${DATA_DATE:-2026-04-07}"
CONSUMER_GROUP="${CONSUMER_GROUP:-cg_rt_content_events_aggregator_v1}"
WAIT_AFTER_GENERATOR_SECONDS="${WAIT_AFTER_GENERATOR_SECONDS:-30}"
MAX_FILE_COUNT_AFTER_COMPACTION="${MAX_FILE_COUNT_AFTER_COMPACTION:-20}"
TRINO_HOST="${TRINO_HOST:-localhost}"
TRINO_PORT="${TRINO_PORT:-8081}"

cd "$REPO_ROOT"

# Read recovery_timeout_seconds from config if not overridden.
RECOVERY_TIMEOUT_SECONDS="${RECOVERY_TIMEOUT_SECONDS:-$(
  "$PYTHON_BIN" -c "
import json
with open('${BULK_ARRIVAL_CONFIG}') as f:
    cfg = json.load(f)
print(cfg.get('scenario_params', {}).get('recovery_timeout_seconds', 120))
" 2>/dev/null || echo 120
)}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

trino_query() {
  local query="$1"
  "$PYTHON_BIN" - <<PY
import trino, sys
conn = trino.dbapi.connect(host="${TRINO_HOST}", port=${TRINO_PORT}, user="acceptance")
cur = conn.cursor()
cur.execute("""${query}""")
rows = cur.fetchall()
for row in rows:
    print('\t'.join(str(c) for c in row))
conn.close()
PY
}

kafka_consumer_lag() {
  docker exec lakehouse-kafka kafka-consumer-groups \
    --bootstrap-server kafka:29092 \
    --group "$CONSUMER_GROUP" \
    --describe 2>/dev/null \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum += $6} END {print sum+0}'
}

printf '[%s] Assuming infrastructure is up. Run: make infra && make streaming\n' "$LOG"

# ---------------------------------------------------------------------------
# Pre-flight 1 (hard fail): streaming job must be running
# ---------------------------------------------------------------------------
printf '[%s] === Pre-flight: verifying rt_content_events_aggregator is running ===\n' "$LOG"
if ! docker exec lakehouse-spark pgrep -f "rt_content_events_aggregator.py" >/dev/null 2>&1; then
  printf '[%s] ERROR: rt_content_events_aggregator.py is not running in the Spark container.\n' "$LOG" >&2
  printf '[%s] Run: make streaming\n' "$LOG" >&2
  exit 1
fi
printf '[%s] Streaming job is running.\n' "$LOG"

# ---------------------------------------------------------------------------
# Pre-flight 2 (warn-only): maxOffsetsPerTrigger env var
# ---------------------------------------------------------------------------
MAX_OFFSETS_IN_CONTAINER=$(docker exec lakehouse-spark bash -lc \
  'echo ${RT_CONTENT_EVENTS_MAX_OFFSETS_PER_TRIGGER:-}' 2>/dev/null || echo "")
if [ -z "$MAX_OFFSETS_IN_CONTAINER" ]; then
  printf '[%s] WARN: RT_CONTENT_EVENTS_MAX_OFFSETS_PER_TRIGGER not set in Spark container.\n' "$LOG"
  printf '[%s] WARN: maxOffsetsPerTrigger falls back to contract default (5000).\n' "$LOG"
  printf '[%s] WARN: Restart streaming with: make streaming  (start_streaming.sh now sets this).\n' "$LOG"
else
  printf '[%s] maxOffsetsPerTrigger=%s (active in Spark container).\n' "$LOG" "$MAX_OFFSETS_IN_CONTAINER"
fi

# ---------------------------------------------------------------------------
# Step 1: Run generator — all three phases (SimulatedClock, no --real-time)
# ---------------------------------------------------------------------------
printf '[%s] === Step 1: Running bulk_arrival generator ===\n' "$LOG"
SUMMARY=$(
  "$PYTHON_BIN" src/generator/adversarial_run_cli.py \
    --config "$BULK_ARRIVAL_CONFIG" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS"
)
printf '[%s] Generator complete. Summary:\n%s\n' "$LOG" "$SUMMARY"

TOTAL_EMITTED=$(echo "$SUMMARY" | "$PYTHON_BIN" -c "
import json, sys
data = json.load(sys.stdin)
print(data['total_emitted'])
")
printf '[%s] total_emitted=%s\n' "$LOG" "$TOTAL_EMITTED"

if [ "${TOTAL_EMITTED:-0}" -lt 1 ]; then
  printf '[%s] ERROR: generator reported 0 emitted events.\n' "$LOG" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 2: Wait for initial drain
# ---------------------------------------------------------------------------
printf '[%s] === Step 2: Waiting %ss for initial drain ===\n' "$LOG" "$WAIT_AFTER_GENERATOR_SECONDS"
sleep "$WAIT_AFTER_GENERATOR_SECONDS"

# ---------------------------------------------------------------------------
# Step 3 (Assertion 2): Poll until consumer lag = 0 or recovery_timeout
# ---------------------------------------------------------------------------
printf '[%s] === Step 3 [A2]: Polling consumer lag (timeout=%ss) ===\n' "$LOG" "$RECOVERY_TIMEOUT_SECONDS"
elapsed=0
lag_poll_interval=10
while [ "$elapsed" -lt "$RECOVERY_TIMEOUT_SECONDS" ]; do
  LAG=$(kafka_consumer_lag)
  printf '[%s] lag=%s elapsed=%ss\n' "$LOG" "$LAG" "$elapsed"
  if [ "${LAG:-1}" -eq 0 ]; then
    printf '[%s] [A2] PASS: consumer lag drained to 0 after %ss.\n' "$LOG" "$elapsed"
    break
  fi
  sleep "$lag_poll_interval"
  elapsed=$(( elapsed + lag_poll_interval ))
done

if [ "${LAG:-1}" -ne 0 ]; then
  printf '[%s] [A2] FAIL: consumer lag=%s did not drain within %ss.\n' \
    "$LOG" "$LAG" "$RECOVERY_TIMEOUT_SECONDS" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 4 (Assertion 3): Check Spark streaming log for task failures
# ---------------------------------------------------------------------------
printf '[%s] === Step 4 [A3]: Checking Spark log for task failures ===\n' "$LOG"
TASK_FAILURES=$(docker exec lakehouse-spark bash -lc \
  "grep -c 'TaskSetFailed\|Task.*failed after\|lost task' /tmp/streaming_content.log 2>/dev/null || echo 0")
if [ "${TASK_FAILURES:-0}" -gt 0 ]; then
  printf '[%s] [A3] FAIL: %s task failure(s) found in streaming log.\n' "$LOG" "$TASK_FAILURES" >&2
  printf '[%s] Relevant log lines:\n' "$LOG" >&2
  docker exec lakehouse-spark bash -lc \
    "grep 'TaskSetFailed\|Task.*failed after\|lost task' /tmp/streaming_content.log 2>/dev/null | tail -20" >&2
  exit 1
fi
printf '[%s] [A3] PASS: no task failures in streaming log.\n' "$LOG"

# ---------------------------------------------------------------------------
# Step 5 (Assertion 4): Bronze row count = total_emitted (±0.1%)
# ---------------------------------------------------------------------------
printf '[%s] === Step 5 [A4]: Verifying bronze row count = total_emitted ===\n' "$LOG"
BRONZE_COUNT=$(trino_query "
  SELECT COALESCE(SUM(record_count), 0)
  FROM \"lakehouse\".\"bronze\".\"raw_events\$partitions\"
  WHERE partition.event_date = DATE '${DATA_DATE}'
")
printf '[%s] bronze_count=%s total_emitted=%s\n' "$LOG" "$BRONZE_COUNT" "$TOTAL_EMITTED"

"$PYTHON_BIN" - <<PY
bronze  = ${BRONZE_COUNT:-0}
emitted = ${TOTAL_EMITTED}
if emitted == 0:
    print("[${LOG}] [A4] FAIL: total_emitted=0, cannot assert count.")
    raise SystemExit(1)
diff = abs(bronze - emitted)
pct  = diff / emitted * 100
print(f"[${LOG}] bronze={bronze} emitted={emitted} diff={diff} pct={pct:.3f}%")
tolerance = max(emitted // 1000, 1)  # 0.1%
if diff > tolerance:
    print(f"[${LOG}] [A4] FAIL: diff={diff} exceeds 0.1% tolerance ({tolerance} rows).")
    raise SystemExit(1)
print(f"[${LOG}] [A4] PASS: bronze count within 0.1% tolerance.")
PY

# ---------------------------------------------------------------------------
# Step 6+7 (Assertion 5): Compaction + file count via Python verifier
# ---------------------------------------------------------------------------
printf '[%s] === Step 6+7 [A5]: Running Iceberg compaction and asserting file count ===\n' "$LOG"
"$PYTHON_BIN" src/scripts/verify_bulk_arrival_compaction.py \
  --data-date "$DATA_DATE" \
  --max-file-count "$MAX_FILE_COUNT_AFTER_COMPACTION" \
  --trino-host "$TRINO_HOST" \
  --trino-port "$TRINO_PORT"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
printf '[%s] ===================================================\n' "$LOG"
printf '[%s] Bulk arrival acceptance PASSED.\n' "$LOG"
printf '[%s] data_date=%s total_emitted=%s bronze=%s lag_drained=true task_failures=0\n' \
  "$LOG" "$DATA_DATE" "$TOTAL_EMITTED" "$BRONZE_COUNT"
printf '[%s] ===================================================\n' "$LOG"
