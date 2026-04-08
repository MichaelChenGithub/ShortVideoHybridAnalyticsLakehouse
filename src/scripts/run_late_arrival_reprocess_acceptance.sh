#!/usr/bin/env bash
# Acceptance script: late arrival reprocess scenario.
#
# Validates that the batch pipeline detects and reprocesses D-1 events that
# arrive in bronze after the D-1 batch job has already completed.
#
# Two-phase flow:
#   Phase A  →  batch job (publishes D-1, writes manifest)  →  Phase B
#   →  sensor triggered  →  backfill runs  →  final counts verified
#
# Prerequisites: make reset-infra (infrastructure must be up)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

LOG="LATE-ARRIVAL-REPROCESS"

usage() {
  cat <<'EOF'
Usage: run_late_arrival_reprocess_acceptance.sh

Environment overrides:
  PYTHON_BIN                    Python interpreter (default: python3)
  BOOTSTRAP_SERVERS             Kafka bootstrap servers (default: localhost:9092)
  PHASE_A_CONFIG                Phase A generator config (default: config/adversarial/late_arrival_phase_a.json)
  PHASE_B_CONFIG                Phase B generator config (default: config/adversarial/late_arrival_phase_b.json)
  DATA_DATE                     D-1 date to process (default: 2026-04-07, from Phase A config started_at)
  WAIT_AFTER_PHASE_A_SECONDS    Drain wait after Phase A (default: 90)
  WAIT_AFTER_BATCH_SECONDS      Poll interval for batch DAG completion (default: 30)
  BATCH_DAG_TIMEOUT_SECONDS     Max wait for batch DAG (default: 600)
  WAIT_AFTER_PHASE_B_SECONDS    Drain wait after Phase B (default: 90)
  BACKFILL_DAG_TIMEOUT_SECONDS  Max wait for backfill DAG (default: 600)
  LATE_ARRIVAL_THRESHOLD        Minimum delta to trigger backfill (default: 1000)
  TRINO_HOST                    Trino host reachable from host machine (default: localhost)
  TRINO_PORT                    Trino port on host (default: 8081)
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help) usage; exit 0 ;;
    *) echo "[$LOG] ERROR: unknown argument '$1'" >&2; usage >&2; exit 2 ;;
  esac
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PHASE_A_CONFIG="${PHASE_A_CONFIG:-config/adversarial/late_arrival_phase_a.json}"
PHASE_B_CONFIG="${PHASE_B_CONFIG:-config/adversarial/late_arrival_phase_b.json}"
DATA_DATE="${DATA_DATE:-2026-04-07}"
WAIT_AFTER_PHASE_A_SECONDS="${WAIT_AFTER_PHASE_A_SECONDS:-90}"
WAIT_AFTER_BATCH_SECONDS="${WAIT_AFTER_BATCH_SECONDS:-30}"
BATCH_DAG_TIMEOUT_SECONDS="${BATCH_DAG_TIMEOUT_SECONDS:-600}"
WAIT_AFTER_PHASE_B_SECONDS="${WAIT_AFTER_PHASE_B_SECONDS:-90}"
BACKFILL_DAG_TIMEOUT_SECONDS="${BACKFILL_DAG_TIMEOUT_SECONDS:-600}"
LATE_ARRIVAL_THRESHOLD="${LATE_ARRIVAL_THRESHOLD:-1000}"
TRINO_HOST="${TRINO_HOST:-localhost}"
TRINO_PORT="${TRINO_PORT:-8081}"

cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# Helper: run a Trino query from the host via the trino Python client.
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

# ---------------------------------------------------------------------------
# Helper: poll Airflow DAG until success or timeout.
# Returns 0 on success, 1 on timeout or failure.
# ---------------------------------------------------------------------------
wait_for_dag() {
  local dag_id="$1"
  local timeout="$2"
  local elapsed=0
  printf '[%s] Waiting for DAG %s to complete (timeout=%ss)...\n' "$LOG" "$dag_id" "$timeout"
  while [ "$elapsed" -lt "$timeout" ]; do
    local state
    state=$(docker exec lakehouse-airflow airflow dags list-runs \
      --dag-id "$dag_id" \
      --output plain 2>/dev/null \
      | awk 'NR>1 {print $4}' \
      | head -1)
    if [ "$state" = "success" ]; then
      printf '[%s] DAG %s completed successfully.\n' "$LOG" "$dag_id"
      return 0
    fi
    if [ "$state" = "failed" ]; then
      printf '[%s] ERROR: DAG %s failed.\n' "$LOG" "$dag_id" >&2
      return 1
    fi
    sleep "$WAIT_AFTER_BATCH_SECONDS"
    elapsed=$(( elapsed + WAIT_AFTER_BATCH_SECONDS ))
  done
  printf '[%s] ERROR: timed out waiting for DAG %s after %ss.\n' "$LOG" "$dag_id" "$timeout" >&2
  return 1
}

printf '[%s] Assuming infrastructure is up. Run: make reset-infra\n' "$LOG"

# ---------------------------------------------------------------------------
# Step 1: Phase A — normal volume, D-1 started_at
# ---------------------------------------------------------------------------
printf '[%s] === Step 1: Running Phase A generator ===\n' "$LOG"
"$PYTHON_BIN" src/generator/adversarial_run_cli.py \
  --config "$PHASE_A_CONFIG" \
  --sink kafka \
  --bootstrap-servers "$BOOTSTRAP_SERVERS"
printf '[%s] Phase A generator complete.\n' "$LOG"

# ---------------------------------------------------------------------------
# Step 2: Drain Kafka — wait for Phase A events to land in bronze
# ---------------------------------------------------------------------------
printf '[%s] === Step 2: Waiting for Phase A events to drain into bronze (%ss) ===\n' "$LOG" "$WAIT_AFTER_PHASE_A_SECONDS"
sleep "$WAIT_AFTER_PHASE_A_SECONDS"

PHASE_A_BRONZE_COUNT=$(trino_query "
  SELECT COALESCE(SUM(record_count), 0)
  FROM \"lakehouse\".\"bronze\".\"raw_events\$partitions\"
  WHERE partition.event_date = DATE '${DATA_DATE}'
")
printf '[%s] Phase A bronze count for %s: %s\n' "$LOG" "$DATA_DATE" "$PHASE_A_BRONZE_COUNT"

if [ "${PHASE_A_BRONZE_COUNT:-0}" -lt 1 ]; then
  printf '[%s] ERROR: Phase A events did not land in bronze partition %s.\n' "$LOG" "$DATA_DATE" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 3: Run D-1 batch job via Airflow
# ---------------------------------------------------------------------------
printf '[%s] === Step 3: Triggering batch_publish_daily for data_date=%s ===\n' "$LOG" "$DATA_DATE"
docker exec lakehouse-airflow airflow dags trigger batch_publish_daily \
  --conf "{\"data_date\": \"${DATA_DATE}\"}"

wait_for_dag "batch_publish_daily" "$BATCH_DAG_TIMEOUT_SECONDS"

# ---------------------------------------------------------------------------
# Step 4: Verify manifest entry written
# ---------------------------------------------------------------------------
printf '[%s] === Step 4: Verifying bronze partition manifest entry ===\n' "$LOG"
MANIFEST_COUNT=$(trino_query "
  SELECT bronze_row_count
  FROM lakehouse.qa.bronze_partition_manifest
  WHERE event_date = DATE '${DATA_DATE}'
  ORDER BY completed_at DESC
  LIMIT 1
")
printf '[%s] Manifest row count for %s: %s\n' "$LOG" "$DATA_DATE" "$MANIFEST_COUNT"

if [ "${MANIFEST_COUNT:-0}" -lt 1 ]; then
  printf '[%s] ERROR: manifest entry not written for %s.\n' "$LOG" "$DATA_DATE" >&2
  exit 1
fi

if [ "$MANIFEST_COUNT" != "$PHASE_A_BRONZE_COUNT" ]; then
  printf '[%s] ERROR: manifest count (%s) != Phase A bronze count (%s).\n' \
    "$LOG" "$MANIFEST_COUNT" "$PHASE_A_BRONZE_COUNT" >&2
  exit 1
fi
printf '[%s] Manifest entry verified: row_count=%s matches bronze at batch completion.\n' "$LOG" "$MANIFEST_COUNT"

# ---------------------------------------------------------------------------
# Step 5: Phase B — smaller burst, same D-1 started_at, no CDC bootstrap
# ---------------------------------------------------------------------------
printf '[%s] === Step 5: Running Phase B generator (late client flush) ===\n' "$LOG"
"$PYTHON_BIN" src/generator/adversarial_run_cli.py \
  --config "$PHASE_B_CONFIG" \
  --sink kafka \
  --bootstrap-servers "$BOOTSTRAP_SERVERS"
printf '[%s] Phase B generator complete.\n' "$LOG"

# ---------------------------------------------------------------------------
# Step 6: Drain Kafka — wait for Phase B events to land in bronze
# ---------------------------------------------------------------------------
printf '[%s] === Step 6: Waiting for Phase B events to drain into bronze (%ss) ===\n' "$LOG" "$WAIT_AFTER_PHASE_B_SECONDS"
sleep "$WAIT_AFTER_PHASE_B_SECONDS"

PHASE_B_BRONZE_COUNT=$(trino_query "
  SELECT COALESCE(SUM(record_count), 0)
  FROM \"lakehouse\".\"bronze\".\"raw_events\$partitions\"
  WHERE partition.event_date = DATE '${DATA_DATE}'
")
PHASE_B_DELTA=$(( PHASE_B_BRONZE_COUNT - MANIFEST_COUNT ))
printf '[%s] Phase B bronze count: %s (delta from manifest: %s)\n' "$LOG" "$PHASE_B_BRONZE_COUNT" "$PHASE_B_DELTA"

if [ "$PHASE_B_DELTA" -lt 1 ]; then
  printf '[%s] ERROR: Phase B events did not arrive in bronze partition %s.\n' "$LOG" "$DATA_DATE" >&2
  exit 1
fi

if [ "$PHASE_B_DELTA" -lt "$LATE_ARRIVAL_THRESHOLD" ]; then
  printf '[%s] WARN: delta (%s) is below LATE_ARRIVAL_THRESHOLD (%s) — sensor may not trigger.\n' \
    "$LOG" "$PHASE_B_DELTA" "$LATE_ARRIVAL_THRESHOLD"
fi

# ---------------------------------------------------------------------------
# Step 7: Trigger detection sensor manually
# ---------------------------------------------------------------------------
printf '[%s] === Step 7: Triggering late_arrival_sensor manually ===\n' "$LOG"
docker exec lakehouse-airflow airflow dags trigger late_arrival_sensor

wait_for_dag "late_arrival_sensor" "$BATCH_DAG_TIMEOUT_SECONDS"

# ---------------------------------------------------------------------------
# Step 8: Verify trigger log written and backfill DAG was triggered
# ---------------------------------------------------------------------------
printf '[%s] === Step 8: Verifying trigger log and backfill DAG triggered for %s ===\n' "$LOG" "$DATA_DATE"

TRIGGER_LOG_ROW=$(trino_query "
  SELECT triggered_at
  FROM lakehouse.qa.late_arrival_trigger_log
  WHERE event_date = DATE '${DATA_DATE}'
  ORDER BY triggered_at DESC
  LIMIT 1
")
if [ -z "$TRIGGER_LOG_ROW" ]; then
  printf '[%s] ERROR: no trigger log entry found for event_date=%s.\n' "$LOG" "$DATA_DATE" >&2
  exit 1
fi
printf '[%s] Trigger log entry found: triggered_at=%s\n' "$LOG" "$TRIGGER_LOG_ROW"

BACKFILL_RUN=$(docker exec lakehouse-airflow airflow dags list-runs \
  --dag-id batch_backfill \
  --output plain 2>/dev/null \
  | awk -v date="$DATA_DATE" '$0 ~ date {print; exit}')

if [ -z "$BACKFILL_RUN" ]; then
  printf '[%s] ERROR: no batch_backfill run found for event_date=%s.\n' "$LOG" "$DATA_DATE" >&2
  exit 1
fi
printf '[%s] Backfill DAG run found: %s\n' "$LOG" "$BACKFILL_RUN"

# ---------------------------------------------------------------------------
# Step 9: Wait for backfill to complete
# ---------------------------------------------------------------------------
printf '[%s] === Step 9: Waiting for backfill DAG to complete ===\n' "$LOG"
wait_for_dag "batch_backfill" "$BACKFILL_DAG_TIMEOUT_SECONDS"

# ---------------------------------------------------------------------------
# Step 10: Verify final D-1 metrics = Phase A + Phase B combined (within ±0.1%)
# ---------------------------------------------------------------------------
printf '[%s] === Step 10: Verifying final D-1 metric counts ===\n' "$LOG"

FINAL_EVENTS_CONFORMED=$(trino_query "
  SELECT COUNT(*)
  FROM lakehouse.silver.events_conformed
  WHERE data_date = DATE '${DATA_DATE}'
")
printf '[%s] Final events_conformed count for %s: %s\n' "$LOG" "$DATA_DATE" "$FINAL_EVENTS_CONFORMED"

EXPECTED_TOTAL="$PHASE_B_BRONZE_COUNT"
TOLERANCE=$(( EXPECTED_TOTAL / 1000 ))  # 0.1%

"$PYTHON_BIN" - <<PY
final = ${FINAL_EVENTS_CONFORMED:-0}
expected = ${EXPECTED_TOTAL}
tolerance = max(${TOLERANCE}, 1)

diff = abs(final - expected)
pct = (diff / expected * 100) if expected > 0 else 0

print(f"[${LOG}] final={final} expected={expected} diff={diff} pct={pct:.3f}%")

if diff > tolerance:
    print(f"[${LOG}] ERROR: final count {final} deviates from expected {expected} by {diff} rows ({pct:.3f}%), exceeds 0.1% tolerance.")
    raise SystemExit(1)
else:
    print(f"[${LOG}] Final count within tolerance. PASS.")
PY

printf '[%s] ===================================================\n' "$LOG"
printf '[%s] Late arrival reprocess acceptance PASSED.\n' "$LOG"
printf '[%s] data_date=%s phase_a=%s phase_b_total=%s final=%s\n' \
  "$LOG" "$DATA_DATE" "$PHASE_A_BRONZE_COUNT" "$PHASE_B_BRONZE_COUNT" "$FINAL_EVENTS_CONFORMED"
printf '[%s] ===================================================\n' "$LOG"
