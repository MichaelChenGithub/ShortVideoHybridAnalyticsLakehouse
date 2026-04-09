#!/usr/bin/env bash
# seed_bronze.sh — Run the bounded generator via Kafka, wait for data to drain
# into Iceberg, then verify that parquet files exist in the MinIO warehouse bucket.
#
# Precondition: `make infra` and `make streaming` (or `make up`) have already
# brought up all services and started the realtime Spark streaming jobs.
#
# Usage:
#   bash src/scripts/seed_bronze.sh
#
# Environment overrides:
#   BOOTSTRAP_SERVERS       Kafka bootstrap address (default: localhost:9092)
#   BOUNDED_RUN_CONFIG      path to generator config JSON
#   BOUNDED_RUN_TIME_MODE   deterministic (default) or dynamic
#   BOUNDED_RUN_STARTED_AT  explicit ISO-8601 override for generator started_at
#   WAIT_SECONDS            seconds to let streaming jobs drain after generator (default: 60)
#   PYTHON_BIN              Python interpreter (default: .venv/bin/python or python3)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"
LOG_PREFIX="SEED-BRONZE"

BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
BOUNDED_RUN_CONFIG="${BOUNDED_RUN_CONFIG:-docs/architecture/generator/examples/bounded_run_config.example.json}"
WAIT_SECONDS="${WAIT_SECONDS:-60}"

DEFAULT_PYTHON_BIN="python3"
if [ -x "$REPO_ROOT/.venv/bin/python" ]; then
  DEFAULT_PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
fi
PYTHON_BIN="${PYTHON_BIN:-$DEFAULT_PYTHON_BIN}"

log() { printf '[%s] %s\n' "$LOG_PREFIX" "$*"; }
err() { printf '[%s] ERROR: %s\n' "$LOG_PREFIX" "$*" >&2; }

resolve_bounded_run_started_at "SEED-BRONZE"

cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# 1. Fast probe — fail immediately if streaming jobs are not running
# ---------------------------------------------------------------------------
log "Probing streaming jobs..."

if ! docker exec lakehouse-spark pgrep -f "rt_content_events_aggregator.py" >/dev/null 2>&1; then
  err "Streaming jobs are not running. Run: make streaming"
  exit 1
fi

log "Streaming jobs are up."

# ---------------------------------------------------------------------------
# 2. Run the bounded generator (Kafka sink)
# ---------------------------------------------------------------------------
log "Running bounded generator (sink=kafka, config=$BOUNDED_RUN_CONFIG)..."
if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config "$BOUNDED_RUN_CONFIG" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --started-at "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
else
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config "$BOUNDED_RUN_CONFIG" \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS"
fi

# ---------------------------------------------------------------------------
# 3. Wait for streaming jobs to drain Kafka into Iceberg
# ---------------------------------------------------------------------------
log "Waiting ${WAIT_SECONDS}s for streaming jobs to drain into Iceberg..."
sleep "$WAIT_SECONDS"

# ---------------------------------------------------------------------------
# 4. Poll streaming job logs for confirmed write markers
# ---------------------------------------------------------------------------
log "Polling streaming logs for bronze write markers..."

poll_log_pattern() {
  local log_file="$1" pattern="$2" label="$3"
  local retries=24 sleep_s=5 attempt
  for ((attempt=1; attempt<=retries; attempt++)); do
    if docker exec lakehouse-spark bash -lc \
        "grep -E '${pattern}' '${log_file}'" >/dev/null 2>&1; then
      log "$label: write marker found in logs"
      return 0
    fi
    sleep "$sleep_s"
  done
  err "$label: no write marker found in $log_file after $((retries * sleep_s))s"
  return 1
}

poll_log_pattern \
  "/tmp/streaming_content.log" \
  "writing [0-9]+ rows to lakehouse\\.bronze\\.raw_events\\." \
  "raw_events"

poll_log_pattern \
  "/tmp/streaming_user.log" \
  "writing [0-9]+ raw user CDC rows to lakehouse\\.bronze\\.raw_cdc_users\\." \
  "raw_cdc_users"

poll_log_pattern \
  "/tmp/streaming_video.log" \
  "writing [0-9]+ raw CDC rows to lakehouse\\.bronze\\.raw_cdc_videos\\." \
  "raw_cdc_videos"

# ---------------------------------------------------------------------------
# 5. Verify parquet files exist in MinIO warehouse bucket
# ---------------------------------------------------------------------------
log "Verifying parquet files in MinIO warehouse bucket..."

"$PYTHON_BIN" - <<'PY'
import sys
try:
    import s3fs
except ImportError:
    print("[SEED-BRONZE] ERROR: s3fs not installed. Run: pip install s3fs", file=sys.stderr)
    sys.exit(1)

fs = s3fs.S3FileSystem(
    key="admin",
    secret="password",
    endpoint_url="http://localhost:9000",
    use_ssl=False,
)

tables = {
    "raw_events":    "warehouse/bronze/raw_events",
    "raw_cdc_users": "warehouse/bronze/raw_cdc_users",
    "raw_cdc_videos":"warehouse/bronze/raw_cdc_videos",
}

failed = []
for name, prefix in tables.items():
    try:
        files = [f for f in fs.find(prefix) if f.endswith(".parquet")]
    except Exception as exc:
        print(f"[SEED-BRONZE] FAIL {name}: s3fs error: {exc}", file=sys.stderr)
        failed.append(name)
        continue
    if not files:
        print(f"[SEED-BRONZE] FAIL {name}: no parquet files found under minio/{prefix}", file=sys.stderr)
        failed.append(name)
    else:
        print(f"[SEED-BRONZE] PASS {name}: {len(files)} parquet file(s) in minio")

if failed:
    sys.exit(1)
PY

# ---------------------------------------------------------------------------
# 6. Summary
# ---------------------------------------------------------------------------
log "Bronze tables seeded and verified in MinIO."
printf '\n'
printf 'Streaming job logs (inside Spark container):\n'
printf '  content:  docker exec lakehouse-spark bash -lc "tail -n 40 /tmp/streaming_content.log"\n'
printf '  video:    docker exec lakehouse-spark bash -lc "tail -n 40 /tmp/streaming_video.log"\n'
printf '  user:     docker exec lakehouse-spark bash -lc "tail -n 40 /tmp/streaming_user.log"\n'
printf '\n'
printf 'Generator artifacts: %s/artifacts/generator_runs/\n' "$REPO_ROOT"
