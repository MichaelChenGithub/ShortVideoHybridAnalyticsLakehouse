# MIC-43 CDC Contract Enforcement Acceptance Reference

## Scope and Boundaries

In scope:
1. CDC contract validation for `cdc.content.videos`.
2. Invalid CDC quarantine routing to `lakehouse.bronze.invalid_events_cdc_videos`.
3. CDC health checks for freshness and invalid-rate visibility.

Out of scope:
1. Base CDC upsert bring-up scope (`MIC-37`).
2. `content_events` contract enforcement (`MIC-39`).

## One-Command Entrypoint

```bash
bash src/scripts/run_mic43_acceptance.sh
```

Optional invalid-rate gate override:

```bash
MAX_INVALID_RATE=1.0 bash src/scripts/run_mic43_acceptance.sh
```

## Required Environment Variables and Defaults

Required env vars: none.

Optional:
1. `MAX_INVALID_RATE`: if set, fails when lookback invalid rate exceeds threshold.

Verifier defaults in manual flow:
1. `max_freshness_minutes=10`
2. `lookback_minutes=30`
3. `min_row_count=4` for quarantine fixture verification

## Artifacts and Output Interpretation

Pass criteria:
1. `verify_rt_video_cdc_upsert.py` passes for valid path.
2. `verify_invalid_cdc_quarantine.py` confirms quarantine rows and expected error codes.
3. `check_rt_video_cdc_health.py` freshness check passes and invalid-rate is reported (and gated only if `--max-invalid-rate` set).
4. Checkpoint listings exist for both valid and invalid CDC sinks.

Expected quarantine error codes for mixed fixture:
1. `CDC_MISSING_OP`
2. `CDC_UNSUPPORTED_OP`
3. `CDC_MISSING_SCHEMA_VERSION`
4. `CDC_MISSING_AFTER_VIDEO_ID`
