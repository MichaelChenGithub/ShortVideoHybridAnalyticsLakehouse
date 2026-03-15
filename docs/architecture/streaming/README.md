# Streaming Contracts

This folder contains Spark Structured Streaming execution contracts and acceptance runbook entrypoints.

## Current Specs

1. `spark-realtime-jobs-contract-m1.md`

## Acceptance Runbook Index (Sprint 1)

Use these as the canonical entrypoints. Detailed command ownership is split into per-MIC references under `reference/`.

| MIC | Purpose | One-command entrypoint | Details |
| --- | --- | --- | --- |
| MIC-38 | Sprint 1 sign-off (MIC-33 scope) | `bash src/scripts/run_mic38_acceptance.sh` | [`reference/mic-38-signoff-acceptance.md`](reference/mic-38-signoff-acceptance.md) |
| MIC-38 (Observe) | Sprint 1 integrated dataflow for manual observation/Trino queries (no verifier gates) | `bash src/scripts/run_mic38_observe.sh` | [`reference/mic-38-signoff-acceptance.md`](reference/mic-38-signoff-acceptance.md) |
| MIC-40 | Content aggregator bring-up | `bash src/scripts/run_mic40_acceptance.sh` | [`reference/mic-40-content-aggregator-acceptance.md`](reference/mic-40-content-aggregator-acceptance.md) |
| MIC-39 | Content contract enforcement | `bash src/scripts/run_mic39_acceptance.sh` | [`reference/mic-39-content-contract-acceptance.md`](reference/mic-39-content-contract-acceptance.md) |
| MIC-43 | CDC contract enforcement + quarantine | `bash src/scripts/run_mic43_acceptance.sh` | [`reference/mic-43-cdc-contract-acceptance.md`](reference/mic-43-cdc-contract-acceptance.md) |
| MIC-37 | CDC upsert bring-up and deterministic verification | `bash src/scripts/run_mic37_acceptance.sh` | [`reference/mic-37-cdc-upsert-acceptance.md`](reference/mic-37-cdc-upsert-acceptance.md) |

## Shared SLA and Scope Anchors

1. Keep MIC-38 and MIC-33 Sprint 1 scope framing unchanged.
2. SLA thresholds are anchored by `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`.
3. Contract semantics remain in streaming contract specs; run scripts are operational source of truth.

## Manual Observation Mode (MIC-38 Scope)

Use this when you want integrated Sprint 1 data flow running for manual observation or Trino queries without PASS/FAIL verifier gates:

```bash
bash src/scripts/run_mic38_observe.sh
```

Optional (clear checkpoints before job restart):

```bash
bash src/scripts/run_mic38_observe.sh --reset-checkpoints
```

Acceptance and dual-scenario flows also support checkpoint reset:

```bash
bash src/scripts/run_mic38_acceptance.sh --reset-checkpoints
bash src/scripts/run_mic38_acceptance_dual.sh --reset-checkpoints
```

## Maintenance Rules

1. Add or update acceptance commands in script files under `src/scripts/` first.
2. In `reference/`, document command usage, env vars/defaults, and output interpretation per MIC.
3. Keep this `README.md` index-style only (entrypoints, links, and scope anchors); do not add large multi-step shell blocks.
4. If a MIC has no wrapper script, keep only minimal manual-run guidance in its reference doc and avoid duplicating script internals.
5. When verifier gates change, update the corresponding MIC reference doc in the same PR.
