#!/usr/bin/env bash
# reset_infra.sh — Tear down the entire stack, clean all cached state, and bring
# up the core data pipeline services in dependency order.
#
# Services started: minio, catalog-postgres, zookeeper, kafka, iceberg-rest, spark
# Services NOT started: airflow, trino, grafana, metabase (start manually when needed)
#
# Usage:
#   bash src/scripts/reset_infra.sh
#
# Environment overrides:
#   COMPOSE_FILE      path to docker-compose.yml (default: docker-compose.yml in repo root)
#   WIPE_IVY_CACHE    set to 1 to delete ivy_cache (forces jar re-download; slow)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$REPO_ROOT/docker-compose.yml}"
LOG_PREFIX="RESET-INFRA"

log() { printf '[%s] %s\n' "$LOG_PREFIX" "$*"; }
err() { printf '[%s] ERROR: %s\n' "$LOG_PREFIX" "$*" >&2; }

poll_until() {
  local label="$1" retries="$2" sleep_s="$3"; shift 3
  local attempt
  for ((attempt=1; attempt<=retries; attempt++)); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep "$sleep_s"
  done
  err "$label did not become ready after $retries attempts"
  return 1
}

cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# 1. Teardown
# ---------------------------------------------------------------------------
log "Stopping all containers and removing named volumes..."
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans

# ---------------------------------------------------------------------------
# 2. Clean host-mounted state
#    ivy_cache: Spark/Maven dependency cache — preserved by default (re-download is slow).
#               Set WIPE_IVY_CACHE=1 to force a clean download (use when cache is corrupted).
#    spark-events: Spark history logs — always wiped (can grow unbounded).
#    checkpoint: any leftover local streaming checkpoints — always wiped.
# ---------------------------------------------------------------------------
log "Cleaning host-mounted state..."
if [ "${WIPE_IVY_CACHE:-0}" = "1" ]; then
  log "WIPE_IVY_CACHE=1: removing ivy_cache (jar re-download will happen)..."
  rm -rf "$REPO_ROOT/ivy_cache"
fi
rm -rf "$REPO_ROOT/spark-events"
rm -rf "$REPO_ROOT/checkpoint"

mkdir -p "$REPO_ROOT/ivy_cache"
mkdir -p "$REPO_ROOT/spark-events"

# ---------------------------------------------------------------------------
# 3. Start independent base services
# ---------------------------------------------------------------------------
log "Starting minio, catalog-postgres, zookeeper..."
docker compose -f "$COMPOSE_FILE" up -d minio catalog-postgres zookeeper

log "Waiting for MinIO..."
poll_until "MinIO" 30 2 \
  curl -sf http://localhost:9000/minio/health/live

log "Waiting for catalog-postgres..."
poll_until "catalog-postgres" 30 2 \
  docker exec lakehouse-catalog-postgres pg_isready -U iceberg -d iceberg

# ---------------------------------------------------------------------------
# 4. Start services that depend on minio + postgres
# ---------------------------------------------------------------------------
log "Starting minio-mc, iceberg-rest, kafka..."
docker compose -f "$COMPOSE_FILE" up -d minio-mc iceberg-rest kafka

log "Waiting for Iceberg REST catalog..."
poll_until "iceberg-rest" 30 2 \
  curl -sf http://localhost:8181/v1/config

log "Waiting for Kafka..."
poll_until "Kafka" 30 2 \
  docker exec lakehouse-kafka kafka-topics \
    --bootstrap-server kafka:29092 --list

# ---------------------------------------------------------------------------
# 5. Start Spark
# ---------------------------------------------------------------------------
log "Starting Spark..."
docker compose -f "$COMPOSE_FILE" up -d spark

log "Waiting for Spark (first start downloads ivy deps — may take 2-3 min)..."
poll_until "Spark" 60 5 \
  docker exec lakehouse-spark /opt/spark/bin/spark-submit --version

# ---------------------------------------------------------------------------
# 6. Bootstrap Iceberg namespaces and tables
# ---------------------------------------------------------------------------
log "Bootstrapping Iceberg namespaces and tables..."
docker exec lakehouse-spark \
  /opt/spark/bin/spark-submit \
  /home/iceberg/local/src/scripts/bootstrap_lakehouse_tables.py

# ---------------------------------------------------------------------------
# 7. Done
# ---------------------------------------------------------------------------
log "Core pipeline stack is ready."
printf '\n'
printf '  MinIO console:   http://localhost:9001  (admin / password)\n'
printf '  Iceberg REST:    http://localhost:8181\n'
printf '  Kafka:           localhost:9092\n'
printf '  Spark UI:        http://localhost:9090\n'
printf '\n'
printf 'Next: run  bash src/scripts/seed_bronze.sh  to seed bronze tables.\n'
