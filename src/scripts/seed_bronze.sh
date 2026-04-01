#!/usr/bin/env bash
# seed_bronze.sh — Start the three realtime Spark streaming jobs, run the
# bounded generator via Kafka, wait for data to drain into Iceberg, then
# verify that parquet files exist in the MinIO warehouse bucket.
#
# Precondition: reset_infra.sh (or equivalent) has already brought up the
# core pipeline services (minio, kafka, iceberg-rest, spark).
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

SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# Local checkpoint root inside the Spark container — always fresh per seed run.
CP_ROOT="/tmp/seed_bronze_cp"

log() { printf '[%s] %s\n' "$LOG_PREFIX" "$*"; }
err() { printf '[%s] ERROR: %s\n' "$LOG_PREFIX" "$*" >&2; }

poll_until() {
  local label="$1" retries="$2" sleep_s="$3"; shift 3
  local attempt
  for ((attempt=1; attempt<=retries; attempt++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep "$sleep_s"
  done
  err "$label not ready after $retries attempts"
  return 1
}

resolve_bounded_run_started_at "SEED-BRONZE"

cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# 1. Fast infra probe — fail immediately with a clear message if not ready
# ---------------------------------------------------------------------------
log "Probing core infra..."

if ! curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
  err "MinIO is not reachable at localhost:9000. Run reset_infra.sh first."
  exit 1
fi

if ! docker exec lakehouse-kafka kafka-topics \
      --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
  err "Kafka is not reachable. Run reset_infra.sh first."
  exit 1
fi

if ! docker exec lakehouse-spark \
      /opt/spark/bin/spark-submit --version >/dev/null 2>&1; then
  err "Spark container is not ready. Run reset_infra.sh first."
  exit 1
fi

log "Infra probes passed."

# ---------------------------------------------------------------------------
# 2. Create Kafka topics (idempotent)
# ---------------------------------------------------------------------------
log "Ensuring Kafka topics exist..."
docker exec lakehouse-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic content_events \
  --partitions 6 \
  --replication-factor 1

docker exec lakehouse-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic "cdc.content.videos" \
  --partitions 3 \
  --replication-factor 1

docker exec lakehouse-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic "cdc.users.profiles" \
  --partitions 3 \
  --replication-factor 1

# ---------------------------------------------------------------------------
# 3. Stop any streaming jobs left from a previous seed run
# ---------------------------------------------------------------------------
log "Stopping any leftover streaming jobs..."
for pattern in "[r]t_content_events_aggregator.py" "[r]t_video_cdc_upsert.py" "[r]t_user_cdc_raw.py"; do
  docker exec lakehouse-spark bash -lc \
    "pids=\$(pgrep -f '${pattern}' || true); [ -n \"\$pids\" ] && kill \$pids || true" \
    2>/dev/null || true
done
sleep 2

# ---------------------------------------------------------------------------
# 4. Reset checkpoint directory inside Spark container
# ---------------------------------------------------------------------------
log "Resetting checkpoint directory $CP_ROOT in Spark container..."
docker exec lakehouse-spark bash -lc "rm -rf '${CP_ROOT}' && mkdir -p '${CP_ROOT}'"

# ---------------------------------------------------------------------------
# 5. Start streaming Spark jobs (background, local checkpoints)
# ---------------------------------------------------------------------------
log "Starting rt_content_events_aggregator..."
docker exec lakehouse-spark bash -lc "nohup env \
  RT_CONTENT_EVENTS_STARTING_OFFSETS=earliest \
  RT_CONTENT_EVENTS_CHECKPOINT_RAW=${CP_ROOT}/content/raw \
  RT_CONTENT_EVENTS_CHECKPOINT_GOLD=${CP_ROOT}/content/gold \
  RT_CONTENT_EVENTS_CHECKPOINT_INVALID=${CP_ROOT}/content/invalid \
  /opt/spark/bin/spark-submit \
    --packages '${SPARK_PACKAGES}' \
    --conf spark.driver.memory=512m \
    --conf spark.executor.memory=512m \
    /home/iceberg/local/src/spark/rt_content_events_aggregator.py \
  > /tmp/seed_content.log 2>&1 &"

log "Starting rt_video_cdc_upsert..."
docker exec lakehouse-spark bash -lc "nohup env \
  RT_VIDEO_CDC_STARTING_OFFSETS=earliest \
  RT_VIDEO_CDC_CHECKPOINT_DIM_VIDEOS=${CP_ROOT}/video/dim \
  RT_VIDEO_CDC_CHECKPOINT_RAW=${CP_ROOT}/video/raw \
  RT_VIDEO_CDC_CHECKPOINT_INVALID_CDC_VIDEOS=${CP_ROOT}/video/invalid \
  /opt/spark/bin/spark-submit \
    --packages '${SPARK_PACKAGES}' \
    --conf spark.driver.memory=512m \
    --conf spark.executor.memory=512m \
    /home/iceberg/local/src/spark/rt_video_cdc_upsert.py \
  > /tmp/seed_video.log 2>&1 &"

log "Starting rt_user_cdc_raw..."
docker exec lakehouse-spark bash -lc "nohup env \
  RT_USER_CDC_STARTING_OFFSETS=earliest \
  RT_USER_CDC_CHECKPOINT_RAW=${CP_ROOT}/user/raw \
  RT_USER_CDC_CHECKPOINT_INVALID=${CP_ROOT}/user/invalid \
  /opt/spark/bin/spark-submit \
    --packages '${SPARK_PACKAGES}' \
    --conf spark.driver.memory=512m \
    --conf spark.executor.memory=512m \
    /home/iceberg/local/src/spark/rt_user_cdc_raw.py \
  > /tmp/seed_user.log 2>&1 &"

# ---------------------------------------------------------------------------
# 6. Wait for all three jobs to appear in the process list
# ---------------------------------------------------------------------------
log "Waiting for streaming jobs to start..."
poll_until "rt_content_events_aggregator" 30 3 \
  docker exec lakehouse-spark pgrep -f "rt_content_events_aggregator.py"

poll_until "rt_video_cdc_upsert" 30 3 \
  docker exec lakehouse-spark pgrep -f "rt_video_cdc_upsert.py"

poll_until "rt_user_cdc_raw" 30 3 \
  docker exec lakehouse-spark pgrep -f "rt_user_cdc_raw.py"

log "All three streaming jobs are running."

# ---------------------------------------------------------------------------
# 7. Run the bounded generator (Kafka sink)
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
# 8. Wait for streaming jobs to drain Kafka into Iceberg
# ---------------------------------------------------------------------------
log "Waiting ${WAIT_SECONDS}s for streaming jobs to drain into Iceberg..."
sleep "$WAIT_SECONDS"

# ---------------------------------------------------------------------------
# 9. Poll streaming job logs for confirmed write markers
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
  "/tmp/seed_content.log" \
  "writing [0-9]+ rows to lakehouse\\.bronze\\.raw_events\\." \
  "raw_events"

poll_log_pattern \
  "/tmp/seed_user.log" \
  "writing [0-9]+ raw user CDC rows to lakehouse\\.bronze\\.raw_cdc_users\\." \
  "raw_cdc_users"

poll_log_pattern \
  "/tmp/seed_video.log" \
  "writing [0-9]+ raw CDC rows to lakehouse\\.bronze\\.raw_cdc_videos\\." \
  "raw_cdc_videos"

# ---------------------------------------------------------------------------
# 10. Verify parquet files exist in MinIO warehouse bucket
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
# 11. Summary
# ---------------------------------------------------------------------------
log "Bronze tables seeded and verified in MinIO."
printf '\n'
printf 'Streaming job logs (inside Spark container):\n'
printf '  content:  docker exec lakehouse-spark bash -lc "tail -n 40 /tmp/seed_content.log"\n'
printf '  video:    docker exec lakehouse-spark bash -lc "tail -n 40 /tmp/seed_video.log"\n'
printf '  user:     docker exec lakehouse-spark bash -lc "tail -n 40 /tmp/seed_user.log"\n'
printf '\n'
printf 'Generator artifacts: %s/artifacts/generator_runs/\n' "$REPO_ROOT"
