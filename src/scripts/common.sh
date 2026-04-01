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
