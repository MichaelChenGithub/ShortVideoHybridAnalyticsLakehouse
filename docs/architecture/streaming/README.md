# Streaming Contracts

This folder contains Spark Structured Streaming execution contracts and acceptance runbook entrypoints.

## Current Specs

1. `spark-realtime-jobs-contract.md`

## Acceptance Runbook Index (Sprint 1)

Use these as the maintained automated entrypoints. Domain-specific references under `reference/` retain scope notes and manual verifier commands, but they do not imply dedicated wrapper scripts.

| Flow | Purpose | One-command entrypoint | Details |
| --- | --- | --- | --- |
| RT-SIGNOFF | Sprint 1 sign-off | `bash src/scripts/run_realtime_signoff_acceptance.sh` | [`reference/realtime-signoff-acceptance.md`](reference/realtime-signoff-acceptance.md) |
| FULL-ACCEPTANCE | Repo-wide streaming + batch acceptance sweep | `make integration-test` | [`../batch-analytics/reference/batch-acceptance-runbook.md`](../batch-analytics/reference/batch-acceptance-runbook.md) |

## Shared SLA and Scope Anchors

1. Keep RT-SIGNOFF and Realtime Sign-off Sprint 1 scope framing unchanged.
2. SLA thresholds are anchored by `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`.
3. Contract semantics remain in streaming contract specs; run scripts are operational source of truth.

## Manual Observation Mode (RT-SIGNOFF Scope)

Use this when you want the integrated local data flow running for manual observation or Trino queries without RT-SIGNOFF verifier gates:

```bash
make up
```

Checkpoint reset for the maintained sign-off flow:

```bash
bash src/scripts/run_realtime_signoff_acceptance.sh --reset-checkpoints
```

Common acceptance env controls:
```bash
BOUNDED_RUN_TIME_MODE=dynamic
BOUNDED_RUN_STARTED_AT=2026-03-20T14:00:00Z
ACCEPTANCE_RESET_DOCKER=1
```

## Maintenance Rules

1. Repo-root Makefile entrypoints are authoritative for maintained acceptance flows.
2. Add or update acceptance commands in script files under `src/scripts/` first.
3. In `reference/`, document command usage, env vars/defaults, and output interpretation per flow.
4. Keep this `README.md` index-style only (entrypoints, links, and scope anchors); do not add large multi-step shell blocks.
5. If a flow has no dedicated wrapper script, keep only manual verifier guidance in its reference doc and avoid implying an automated entrypoint that does not exist.
6. When verifier gates change, update the corresponding flow reference doc in the same PR.
