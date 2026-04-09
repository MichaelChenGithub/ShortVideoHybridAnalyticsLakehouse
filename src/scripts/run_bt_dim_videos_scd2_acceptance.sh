#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

usage() {
  cat <<'EOF'
Usage: run_bt_dim_videos_scd2_acceptance.sh

Environment overrides:
  BOUNDED_RUN_TIME_MODE
  BOUNDED_RUN_STARTED_AT
  VIDEO_ID
  BOOTSTRAP_SERVERS
  PYTHON_BIN
  WAIT_AFTER_FIXTURE_SECONDS
  WAIT_AFTER_BATCH_SECONDS
  RAW_READY_RETRIES
  RAW_READY_SLEEP_SECONDS
  BASE_TS_MS
  EXPECTED_SOURCE_TS_MS
  MIN_RAW_ROWS
  EXPECT_CATEGORY
  EXPECT_REGION
  EXPECT_STATUS
EOF
}

if (($# > 0)); then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[BT-DIM-VIDEOS-SCD2] ERROR: unknown argument '$1'" >&2
      usage >&2
      exit 2
      ;;
  esac
fi

VIDEO_ID="${VIDEO_ID:-bt_dim_vid_001}"
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
WAIT_AFTER_FIXTURE_SECONDS="${WAIT_AFTER_FIXTURE_SECONDS:-75}"
WAIT_AFTER_BATCH_SECONDS="${WAIT_AFTER_BATCH_SECONDS:-10}"
RAW_READY_RETRIES="${RAW_READY_RETRIES:-18}"
RAW_READY_SLEEP_SECONDS="${RAW_READY_SLEEP_SECONDS:-10}"
BASE_TS_MS="${BASE_TS_MS:-$(( $(date +%s) * 1000 ))}"
EXPECTED_SOURCE_TS_MS="${EXPECTED_SOURCE_TS_MS:-$((BASE_TS_MS + 2000))}"
MIN_RAW_ROWS="${MIN_RAW_ROWS:-4}"
EXPECT_CATEGORY="${EXPECT_CATEGORY:-Comedy}"
EXPECT_REGION="${EXPECT_REGION:-US}"
EXPECT_STATUS="${EXPECT_STATUS:-copyright_strike}"

resolve_bounded_run_started_at "BT-DIM-VIDEOS-SCD2"

cd "$REPO_ROOT"
printf '[BT-DIM-VIDEOS-SCD2] Assuming infra + streaming are up. Run: make infra && make streaming\n'

printf '[BT-DIM-VIDEOS-SCD2] Running bounded generator...\n'
if [ -n "$BOUNDED_RUN_EFFECTIVE_STARTED_AT" ]; then
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config docs/architecture/generator/examples/bounded_run_config.example.json \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --started-at "$BOUNDED_RUN_EFFECTIVE_STARTED_AT"
else
  "$PYTHON_BIN" src/generator/bounded_run_cli.py \
    --config docs/architecture/generator/examples/bounded_run_config.example.json \
    --sink kafka \
    --bootstrap-servers "$BOOTSTRAP_SERVERS"
fi

printf '[BT-DIM-VIDEOS-SCD2] Emitting deterministic CDC fixture...\n'
"$PYTHON_BIN" src/scripts/emit_cdc_videos_fixture.py \
  --bootstrap-servers "$BOOTSTRAP_SERVERS" \
  --video-id "$VIDEO_ID" \
  --scenario full \
  --base-ts-ms "$BASE_TS_MS"
sleep "$WAIT_AFTER_FIXTURE_SECONDS"

printf '[BT-DIM-VIDEOS-SCD2] Waiting for raw CDC landing readiness...\n'
raw_ready=0
for _ in $(seq 1 "$RAW_READY_RETRIES"); do
  if docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_raw_bronze.py \
    --video-id "$VIDEO_ID" \
    --table lakehouse.bronze.raw_cdc_videos \
    --min-row-count "$MIN_RAW_ROWS" \
    --expect-status "$EXPECT_STATUS" \
    --expect-latest-ts-ms "$EXPECTED_SOURCE_TS_MS" \
    --min-source-ts-ms "$BASE_TS_MS" >/dev/null 2>&1; then
    raw_ready=1
    break
  fi
  sleep "$RAW_READY_SLEEP_SECONDS"
done

if [ "$raw_ready" -ne 1 ]; then
  echo "[BT-DIM-VIDEOS-SCD2] ERROR: raw CDC rows not ready after retries." >&2
  docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_raw_bronze.py \
    --video-id "$VIDEO_ID" \
    --table lakehouse.bronze.raw_cdc_videos \
    --min-row-count "$MIN_RAW_ROWS" \
    --expect-status "$EXPECT_STATUS" \
    --expect-latest-ts-ms "$EXPECTED_SOURCE_TS_MS" \
    --min-source-ts-ms "$BASE_TS_MS"
  exit 1
fi

printf '[BT-DIM-VIDEOS-SCD2] Running dim_videos_scd2 batch transform...\n'
docker cp src/spark/bt_dim_videos_scd2.py lakehouse-spark:/home/iceberg/local/src/spark/bt_dim_videos_scd2.py
docker cp src/spark/bt_dim_videos_scd2_sql.py lakehouse-spark:/home/iceberg/local/src/spark/bt_dim_videos_scd2_sql.py
docker cp src/scripts/verify_bt_dim_videos_scd2.py lakehouse-spark:/home/iceberg/local/src/scripts/verify_bt_dim_videos_scd2.py
docker exec lakehouse-spark bash -lc "/opt/spark/bin/spark-submit /home/iceberg/local/src/spark/bt_dim_videos_scd2.py"
sleep "$WAIT_AFTER_BATCH_SECONDS"

printf '[BT-DIM-VIDEOS-SCD2] Verifying dim_videos_scd2 output contract...\n'
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_bt_dim_videos_scd2.py \
  --video-id "$VIDEO_ID" \
  --table lakehouse.dims.dim_videos_scd2 \
  --expect-category "$EXPECT_CATEGORY" \
  --expect-region "$EXPECT_REGION" \
  --expect-status "$EXPECT_STATUS"

printf '[BT-DIM-VIDEOS-SCD2] Acceptance flow completed.\n'
