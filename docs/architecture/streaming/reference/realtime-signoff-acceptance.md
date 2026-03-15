# RT-SIGNOFF Sprint 1 Sign-off Acceptance Runbook

## 1. Purpose and Scope

This document is the operational runbook for RT-SIGNOFF Sprint 1 sign-off in Realtime Sign-off scope.

In scope:
1. Integrated acceptance run for content + CDC paths.
2. PASS/FAIL sign-off using verifier outputs and SLA-aligned gates.
3. Static watermark profile selection (`baseline` or `lag_prone`).

Out of scope:
1. Runtime behavior/code changes.
2. Dynamic runtime auto-tuning.
3. Non-Sprint-1 SLA targets.

## 2. Canonical Entrypoints

Primary acceptance:
```bash
bash src/scripts/run_realtime_signoff_acceptance.sh
```

Optional checkpoint reset for acceptance flow:
```bash
bash src/scripts/run_realtime_signoff_acceptance.sh --reset-checkpoints
```

Dual-scenario acceptance (baseline then lag-prone):
```bash
bash src/scripts/run_realtime_signoff_dual_acceptance.sh
```

Optional checkpoint reset for dual-scenario flow:
```bash
bash src/scripts/run_realtime_signoff_dual_acceptance.sh --reset-checkpoints
```

Manual observation (no verifier gates):
```bash
bash src/scripts/run_realtime_observe.sh
```

Optional checkpoint reset for observation flow:
```bash
bash src/scripts/run_realtime_observe.sh --reset-checkpoints
```

## 3. Runtime Profiles and Defaults

SLA-aligned defaults:
1. `MAX_FRESHNESS_MINUTES=3`
2. `LATENCY_THRESHOLD_MINUTES=3`
3. `MAX_CONTENT_INVALID_RATE=0.20`
4. `MAX_CDC_INVALID_RATE=0.20`

Watermark scenario defaults:
1. `RT_SIGNOFF_WATERMARK_SCENARIO=baseline`
2. `BASELINE_MIN_WATERMARK_DROP_RATIO=0.0`
3. `LAG_PRONE_MAX_WATERMARK_DROP_RATIO=0.005`

Watermark policy by scenario:
1. `baseline` -> `RT_CONTENT_EVENTS_WATERMARK=2 minutes`
2. `lag_prone` -> `RT_CONTENT_EVENTS_WATERMARK=5 minutes`

Baseline trigger guidance:
1. Keep `trigger_gold=1 minute` for sign-off.
2. If trigger is changed for temporary testing, restore to `1 minute` before sign-off runs.

Resource-bound Spark defaults (tunable via env):
1. `RT_SIGNOFF_SPARK_DRIVER_CORES=1`
2. `RT_SIGNOFF_SPARK_DRIVER_MEMORY=1g`
3. `RT_SIGNOFF_SPARK_DRIVER_MEMORY_OVERHEAD=512m`
4. `RT_SIGNOFF_SPARK_EXECUTOR_INSTANCES=1`
5. `RT_SIGNOFF_SPARK_EXECUTOR_CORES=1`
6. `RT_SIGNOFF_SPARK_EXECUTOR_MEMORY=1g`
7. `RT_SIGNOFF_SPARK_EXECUTOR_MEMORY_OVERHEAD=512m`
8. `RT_SIGNOFF_SPARK_CORES_MAX=2`
9. `RT_SIGNOFF_SPARK_SQL_SHUFFLE_PARTITIONS=8`
10. `RT_SIGNOFF_SPARK_DEFAULT_PARALLELISM=8`

Readiness and settle waits (tunable via env):
1. `STREAM_BATCH_READY_RETRIES=120`
2. `STREAM_BATCH_READY_SLEEP_SECONDS=2`
3. `POST_RUN_BATCH_READY_RETRIES=60`
4. `POST_RUN_BATCH_READY_SLEEP_SECONDS=5`

## 4. Static Tuning Workflow (No Dynamic Tuning)

Use this flow when selecting between static watermark profiles.

Step 1: Pick scenario
1. Default environment: `baseline`.
2. Lag-prone environment: `lag_prone`.

Step 2: Execute acceptance
1. Single scenario:
```bash
RT_SIGNOFF_WATERMARK_SCENARIO=baseline bash src/scripts/run_realtime_signoff_acceptance.sh
```
2. Dual scenario:
```bash
RT_SIGNOFF_RUN_ID=mic38_signoff_20260310 bash src/scripts/run_realtime_signoff_dual_acceptance.sh
```

Step 3: Review outputs
1. Check `content_metrics.log` for content verifier result.
2. Check `signoff_report.json` for final RT-SIGNOFF gate summary.
3. Unified freshness/latency gates are computed with a single run reference time from `runtime_end.sample_at_ms` to avoid verifier-order timestamp drift.

Step 4: Decide and record
1. If gates pass, keep selected static scenario for that environment.
2. If gates fail, switch scenario or rollback and rerun.

## 5. Artifacts

Default output path:
1. `artifacts/realtime_signoff/<RT_SIGNOFF_RUN_ID>/`

Expected files:
1. `content_metrics.log`
2. `content_contract.log`
3. `cdc_upsert.log`
4. `cdc_invalid.log`
5. `cdc_health.log`
6. `runtime_start.json`
7. `runtime_end.json`
8. `checkpoint_start.json`
9. `checkpoint_end.json`
10. `signoff_report.json`
11. `signoff_summary.md`

## 6. Output Interpretation

1. `signoff_report.json` is the machine-decidable sign-off result.
2. `signoff_summary.md` is reviewer-oriented summary text.
3. Script exits non-zero on any failed gate.
4. Single-scenario run appends scenario suffix (`_baseline` or `_lag_prone`) to run id.
5. Dual run emits two report folders under the same base run id.

Manual observation mode:
1. Runs integrated dataflow but does not execute verifier gates.
2. Use Spark logs and query tools (for example Trino) for manual inspection.

Acceptance cleanup behavior:
1. `run_realtime_signoff_acceptance.sh` stops started streaming jobs before running verifier Spark sessions.
2. To keep jobs running for debug only, set `KEEP_JOBS_RUNNING=1` or pass `--keep-jobs-running`.

## 7. Late-Drop Validation Boundary

1. Timestamp-backshift traffic validates out-of-order behavior, not guaranteed too-late drop behavior.
2. Strict late-drop assertion for `watermark_drop_ratio` requires arrival-timing control profile (`delivery_profile=watermark_pushback`) once implemented.
3. Until that profile is enabled, `watermark_drop_ratio=0.0` may still be a valid result for some runs.

## 8. Rollback and Reset

Rollback to baseline scenario:
```bash
RT_SIGNOFF_WATERMARK_SCENARIO=baseline bash src/scripts/run_realtime_signoff_acceptance.sh
```

If stale test state is suspected:
```bash
docker compose down -v
```

Then rerun acceptance and confirm outputs from fresh artifacts.

If stale invalid-rate appears due to old quarantine rows:
```bash
bash src/scripts/run_realtime_signoff_acceptance.sh --reset-checkpoints
```
The acceptance flow also enforces run-scoped freshness windows via `min_ingested_at_ms` and `now-ms` passed into verifier commands.
