#!/usr/bin/env bash

resolve_bounded_run_started_at() {
  local log_prefix="${1:-ACCEPTANCE}"
  BOUNDED_RUN_TIME_MODE="${BOUNDED_RUN_TIME_MODE:-deterministic}"
  BOUNDED_RUN_EFFECTIVE_STARTED_AT=""

  case "$BOUNDED_RUN_TIME_MODE" in
    deterministic|dynamic)
      ;;
    *)
      echo "[$log_prefix] ERROR: BOUNDED_RUN_TIME_MODE must be deterministic or dynamic, got '${BOUNDED_RUN_TIME_MODE}'" >&2
      return 1
      ;;
  esac

  if [ -n "${BOUNDED_RUN_STARTED_AT:-}" ]; then
    if ! python3 - <<'PY' "$BOUNDED_RUN_STARTED_AT"
from datetime import datetime
import sys

text = sys.argv[1].strip()
if text.endswith("Z"):
    text = text[:-1] + "+00:00"
datetime.fromisoformat(text)
PY
    then
      echo "[$log_prefix] ERROR: BOUNDED_RUN_STARTED_AT must be valid ISO-8601 timestamp" >&2
      return 1
    fi
    BOUNDED_RUN_EFFECTIVE_STARTED_AT="$BOUNDED_RUN_STARTED_AT"
  elif [ "$BOUNDED_RUN_TIME_MODE" = "dynamic" ]; then
    BOUNDED_RUN_EFFECTIVE_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  fi

  if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
    printf '[%s] bounded_run_time_mode=%s started_at=%s\n' "$log_prefix" "$BOUNDED_RUN_TIME_MODE" "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
  else
    printf '[%s] bounded_run_time_mode=%s started_at=config_default\n' "$log_prefix" "$BOUNDED_RUN_TIME_MODE"
  fi
}

acceptance_should_reset_docker() {
  local reset="${ACCEPTANCE_RESET_DOCKER:-0}"
  case "$reset" in
    0)
      return 1
      ;;
    1)
      return 0
      ;;
    *)
      echo "[ACCEPTANCE] ERROR: ACCEPTANCE_RESET_DOCKER must be 0 or 1, got '${reset}'" >&2
      return 2
      ;;
  esac
}

acceptance_maybe_reset_docker() {
  local log_prefix="${1:-ACCEPTANCE}"
  if acceptance_should_reset_docker; then
    printf '[%s] ACCEPTANCE_RESET_DOCKER=1, running docker compose down -v for isolated acceptance state...\n' "$log_prefix"
    docker compose down -v || true
  fi
}

wait_for_kafka_ready() {
  local log_prefix="${1:-ACCEPTANCE}"
  local retries="${2:-30}"
  local sleep_seconds="${3:-2}"
  local attempt
  for ((attempt=1; attempt<=retries; attempt++)); do
    if docker exec lakehouse-kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep "$sleep_seconds"
  done
  echo "[$log_prefix] ERROR: Kafka did not become ready after ${retries} attempts." >&2
  return 1
}
