# MIC-37 CDC Upsert Acceptance Reference

## Scope and Boundaries

In scope:
1. Stable `rt_video_cdc_upsert` execution for `cdc.content.videos`.
2. `lakehouse.dims.dim_videos` insert/update health verification.
3. Checkpoint evidence for `s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1`.

Out of scope:
1. CDC quarantine/contract enforcement (`MIC-43`).
2. `content_events` pipeline validation (`MIC-39`/`MIC-40`).

Runtime defaults (MIC-37):
1. `startingOffsets=latest`
2. trigger interval `1 minute`
3. checkpoint `s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1`

## One-Command Entrypoint

```bash
bash src/scripts/run_mic37_acceptance.sh
```

## Required Environment Variables and Defaults

Required env vars: none.

Optional variables (script defaults):
1. `VIDEO_ID=mic37_vid_001`
2. `BOOTSTRAP_SERVERS=localhost:9092`
3. `EXPECTED_STATUS=copyright_strike`
4. `PYTHON_BIN=python3`
5. `WAIT_AFTER_JOB_START_SECONDS=30`
6. `WAIT_AFTER_FIXTURE_SECONDS=75`
7. `BASE_TS_MS=$(( $(date +%s) * 1000 ))`
8. `EXPECTED_SOURCE_TS_MS=$((BASE_TS_MS + 2000))`

## Artifacts and Output Interpretation

Primary verification signal is command output from:
1. `verify_rt_video_cdc_upsert.py` (PASS when expected final row state is present and freshness gate passes).
2. MinIO checkpoint listing command (PASS when checkpoint files exist under expected prefix).
3. Query liveness check via `pgrep -f rt_video_cdc_upsert.py`.

Expected deterministic `full` scenario final row for `mic37_vid_001`:
1. `status=copyright_strike`
2. `source_ts_ms=$EXPECTED_SOURCE_TS_MS`
