#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BASE_RT_SIGNOFF_RUN_ID="${RT_SIGNOFF_RUN_ID:-realtime_signoff_dual_$(date -u +%Y%m%dT%H%M%SZ)}"
RESET_CHECKPOINTS="${RESET_CHECKPOINTS:-0}"

usage() {
  cat <<'EOF'
Usage: run_realtime_signoff_dual_acceptance.sh [--reset-checkpoints]

Options:
  --reset-checkpoints  Remove streaming checkpoint paths before each scenario run.
  -h, --help           Show this help.

Equivalent env flags:
  RESET_CHECKPOINTS=1

Resource-bound env flags are forwarded to each scenario run:
  RT_SIGNOFF_SPARK_DRIVER_CORES
  RT_SIGNOFF_SPARK_DRIVER_MEMORY
  RT_SIGNOFF_SPARK_DRIVER_MEMORY_OVERHEAD
  RT_SIGNOFF_SPARK_EXECUTOR_INSTANCES
  RT_SIGNOFF_SPARK_EXECUTOR_CORES
  RT_SIGNOFF_SPARK_EXECUTOR_MEMORY
  RT_SIGNOFF_SPARK_EXECUTOR_MEMORY_OVERHEAD
  RT_SIGNOFF_SPARK_CORES_MAX
  RT_SIGNOFF_SPARK_SQL_SHUFFLE_PARTITIONS
  RT_SIGNOFF_SPARK_DEFAULT_PARALLELISM
EOF
}

while (($# > 0)); do
  case "$1" in
    --reset-checkpoints)
      RESET_CHECKPOINTS=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[RT-SIGNOFF-DUAL] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
done

printf '[RT-SIGNOFF-DUAL] Base run id: %s\n' "$BASE_RT_SIGNOFF_RUN_ID"
printf '[RT-SIGNOFF-DUAL] reset_checkpoints=%s\n' "$RESET_CHECKPOINTS"

ACCEPTANCE_ARGS=()
if [ "$RESET_CHECKPOINTS" = "1" ]; then
  ACCEPTANCE_ARGS+=(--reset-checkpoints)
fi

printf '[RT-SIGNOFF-DUAL] Running baseline scenario (2 minutes watermark)...\n'
RT_SIGNOFF_RUN_ID="$BASE_RT_SIGNOFF_RUN_ID" \
RT_SIGNOFF_WATERMARK_SCENARIO="baseline" \
bash "$SCRIPT_DIR/run_realtime_signoff_acceptance.sh" "${ACCEPTANCE_ARGS[@]}"

printf '[RT-SIGNOFF-DUAL] Running lag-prone scenario (5 minutes watermark)...\n'
RT_SIGNOFF_RUN_ID="$BASE_RT_SIGNOFF_RUN_ID" \
RT_SIGNOFF_WATERMARK_SCENARIO="lag_prone" \
bash "$SCRIPT_DIR/run_realtime_signoff_acceptance.sh" "${ACCEPTANCE_ARGS[@]}"

printf '[RT-SIGNOFF-DUAL] Completed both scenarios.\n'
