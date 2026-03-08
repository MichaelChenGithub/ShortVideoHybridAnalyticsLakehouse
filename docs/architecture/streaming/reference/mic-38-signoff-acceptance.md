# MIC-38 Sprint 1 Sign-off Acceptance Reference

## Scope and Boundaries

In scope:
1. MIC-38 Sprint 1 sign-off only (MIC-33 Sprint 1 scope framing).
2. Unified verifier pass/fail against freshness, latency proxy, and invalid-rate gates.
3. Reviewer-readable sign-off artifacts under `artifacts/mic38_signoff/`.

Out of scope:
1. Runtime behavior changes.
2. Non-Sprint-1 SLA targets.

## One-Command Entrypoint

```bash
bash src/scripts/run_mic38_acceptance.sh
```

Manual observation entrypoint (same integrated MIC-38 scope, no verifier gates):

```bash
bash src/scripts/run_mic38_observe.sh
```

Optional checkpoint reset:

```bash
bash src/scripts/run_mic38_observe.sh --reset-checkpoints
```

## Required Environment Variables and Defaults

Defaults are SLA-aligned with `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`:
1. `MAX_FRESHNESS_MINUTES=3`
2. `LATENCY_THRESHOLD_MINUTES=3`
3. `MAX_CONTENT_INVALID_RATE=0.20`
4. `MAX_CDC_INVALID_RATE=0.20`

Manual observation script defaults (`run_mic38_observe.sh`):
1. `MIC38_RUN_ID=mic38_observe_<utc timestamp>`
2. `MIC38_VIDEO_ID=${MIC38_RUN_ID}_cdc_vid_001`
3. `BOOTSTRAP_SERVERS=localhost:9092`
4. `WAIT_AFTER_JOB_START_SECONDS=30`
5. `WAIT_AFTER_BOUNDED_RUN_SECONDS=75`
6. `WAIT_AFTER_CDC_FIXTURE_SECONDS=75`
7. `RESET_CHECKPOINTS=0` (set `1` or pass `--reset-checkpoints` to clear only checkpoint paths)
8. `PRINT_MAINTENANCE_HINT=0` (set `1` or pass `--maintenance-hint` to print Trino maintenance SQL)

Override example:

```bash
MAX_FRESHNESS_MINUTES=3 \
LATENCY_THRESHOLD_MINUTES=3 \
MAX_CONTENT_INVALID_RATE=0.20 \
MAX_CDC_INVALID_RATE=0.20 \
bash src/scripts/run_mic38_acceptance.sh
```

## Artifacts and Output Interpretation

Default output path:
1. `artifacts/mic38_signoff/<MIC38_RUN_ID>/`

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

Interpretation:
1. `signoff_report.json` is machine-decidable and contains per-gate `PASS`/`FAIL`, metrics, blockers, and carry-over items.
2. `signoff_summary.md` is reviewer-focused summary output.
3. Script exits non-zero on any failed gate.

Manual observation mode interpretation:
1. `run_mic38_observe.sh` sets up services, topics, both Spark jobs, bounded generator traffic, and mixed CDC fixture.
2. No verifier scripts are executed, so there is no PASS/FAIL sign-off report.
3. Use Spark logs and downstream query tools (for example Trino) for manual inspection.
