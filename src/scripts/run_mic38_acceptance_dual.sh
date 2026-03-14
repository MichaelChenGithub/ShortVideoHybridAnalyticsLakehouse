#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BASE_MIC38_RUN_ID="${MIC38_RUN_ID:-mic38_dual_$(date -u +%Y%m%dT%H%M%SZ)}"
RESET_CHECKPOINTS="${RESET_CHECKPOINTS:-0}"

usage() {
  cat <<'EOF'
Usage: run_mic38_acceptance_dual.sh [--reset-checkpoints]

Options:
  --reset-checkpoints  Remove streaming checkpoint paths before each scenario run.
  -h, --help           Show this help.

Equivalent env flags:
  RESET_CHECKPOINTS=1
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
      echo "[MIC-38-DUAL] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
done

printf '[MIC-38-DUAL] Base run id: %s\n' "$BASE_MIC38_RUN_ID"
printf '[MIC-38-DUAL] reset_checkpoints=%s\n' "$RESET_CHECKPOINTS"

ACCEPTANCE_ARGS=()
if [ "$RESET_CHECKPOINTS" = "1" ]; then
  ACCEPTANCE_ARGS+=(--reset-checkpoints)
fi

printf '[MIC-38-DUAL] Running baseline scenario (2 minutes watermark)...\n'
MIC38_RUN_ID="$BASE_MIC38_RUN_ID" \
MIC38_WATERMARK_SCENARIO="baseline" \
bash "$SCRIPT_DIR/run_mic38_acceptance.sh" "${ACCEPTANCE_ARGS[@]}"

printf '[MIC-38-DUAL] Running lag-prone scenario (5 minutes watermark)...\n'
MIC38_RUN_ID="$BASE_MIC38_RUN_ID" \
MIC38_WATERMARK_SCENARIO="lag_prone" \
bash "$SCRIPT_DIR/run_mic38_acceptance.sh" "${ACCEPTANCE_ARGS[@]}"

printf '[MIC-38-DUAL] Completed both scenarios.\n'
