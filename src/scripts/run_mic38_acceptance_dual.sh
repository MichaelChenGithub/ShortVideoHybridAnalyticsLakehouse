#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BASE_MIC38_RUN_ID="${MIC38_RUN_ID:-mic38_dual_$(date -u +%Y%m%dT%H%M%SZ)}"

printf '[MIC-38-DUAL] Base run id: %s\n' "$BASE_MIC38_RUN_ID"
printf '[MIC-38-DUAL] Running baseline scenario (2 minutes watermark)...\n'
MIC38_RUN_ID="$BASE_MIC38_RUN_ID" \
MIC38_WATERMARK_SCENARIO="baseline" \
bash "$SCRIPT_DIR/run_mic38_acceptance.sh"

printf '[MIC-38-DUAL] Running lag-prone scenario (5 minutes watermark)...\n'
MIC38_RUN_ID="$BASE_MIC38_RUN_ID" \
MIC38_WATERMARK_SCENARIO="lag_prone" \
bash "$SCRIPT_DIR/run_mic38_acceptance.sh"

printf '[MIC-38-DUAL] Completed both scenarios.\n'
