#!/usr/bin/env bash
# start_streaming.sh — Create Kafka topics (idempotent) and start the three
# realtime Spark structured streaming jobs.
#
# Idempotent: safe to re-run; kills any leftover streaming jobs before restarting.
#
# Precondition: reset_infra.sh (or `make infra`) has brought up all services.
#
# Usage:
#   bash src/scripts/start_streaming.sh
#
# Environment overrides:
#   COMPOSE_FILE   path to docker-compose.yml (default: docker-compose.yml in repo root)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$REPO_ROOT/docker-compose.yml}"
LOG_PREFIX="START-STREAMING"

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

SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# 1. Infra probe
# ---------------------------------------------------------------------------
log "Probing core infra..."

if ! curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
  err "MinIO not reachable. Run: make infra"
  exit 1
fi
if ! docker exec lakehouse-kafka kafka-topics \
      --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
  err "Kafka not reachable. Run: make infra"
  exit 1
fi
if ! docker exec lakehouse-spark \
      /opt/spark/bin/spark-submit --version >/dev/null 2>&1; then
  err "Spark not ready. Run: make infra"
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

log "Kafka topics ready."

# ---------------------------------------------------------------------------
# 3. Stop any leftover streaming jobs
# ---------------------------------------------------------------------------
log "Stopping any leftover streaming jobs..."
for pattern in "[r]t_content_events_aggregator.py" "[r]t_video_cdc_upsert.py" "[r]t_user_cdc_raw.py"; do
  docker exec lakehouse-spark bash -lc \
    "pids=\$(pgrep -f '${pattern}' || true); [ -n \"\$pids\" ] && kill \$pids || true" \
    2>/dev/null || true
done
sleep 2

# ---------------------------------------------------------------------------
# 4. Start streaming Spark jobs (background)
# ---------------------------------------------------------------------------
# Checkpoint locations default to s3a://checkpoints/jobs/... (MinIO) as defined
# in each job's contract file. No override needed here — the acceptance scripts
# snapshot those MinIO paths to verify checkpoint growth.
log "Starting rt_content_events_aggregator..."
docker exec lakehouse-spark bash -lc "nohup env \
  RT_CONTENT_EVENTS_STARTING_OFFSETS=earliest \
  RT_CONTENT_EVENTS_MAX_OFFSETS_PER_TRIGGER=5000 \
  /opt/spark/bin/spark-submit \
    --packages '${SPARK_PACKAGES}' \
    --conf spark.driver.memory=512m \
    --conf spark.executor.memory=512m \
    /home/iceberg/local/src/spark/rt_content_events_aggregator.py \
  > /tmp/streaming_content.log 2>&1 &"

log "Starting rt_video_cdc_upsert..."
docker exec lakehouse-spark bash -lc "nohup env \
  RT_VIDEO_CDC_STARTING_OFFSETS=earliest \
  /opt/spark/bin/spark-submit \
    --packages '${SPARK_PACKAGES}' \
    --conf spark.driver.memory=512m \
    --conf spark.executor.memory=512m \
    /home/iceberg/local/src/spark/rt_video_cdc_upsert.py \
  > /tmp/streaming_video.log 2>&1 &"

log "Starting rt_user_cdc_raw..."
docker exec lakehouse-spark bash -lc "nohup env \
  RT_USER_CDC_STARTING_OFFSETS=earliest \
  /opt/spark/bin/spark-submit \
    --packages '${SPARK_PACKAGES}' \
    --conf spark.driver.memory=512m \
    --conf spark.executor.memory=512m \
    /home/iceberg/local/src/spark/rt_user_cdc_raw.py \
  > /tmp/streaming_user.log 2>&1 &"

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
printf '\n'
printf 'Streaming job logs (inside Spark container):\n'
printf '  content:  docker exec lakehouse-spark bash -lc "tail -n 40 /tmp/streaming_content.log"\n'
printf '  video:    docker exec lakehouse-spark bash -lc "tail -n 40 /tmp/streaming_video.log"\n'
printf '  user:     docker exec lakehouse-spark bash -lc "tail -n 40 /tmp/streaming_user.log"\n'
printf '\n'
printf 'Next: run  make seed-bronze  to emit events and verify bronze tables.\n'
