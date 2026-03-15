# Streaming Contracts

This folder contains Spark Structured Streaming execution contracts and acceptance runbook entrypoints.

## Current Specs

1. `spark-realtime-jobs-contract.md`

## Acceptance Runbook Index (Sprint 1)

Use these as canonical entrypoints. Detailed command ownership is split into domain-specific references under `reference/`.

| Flow | Purpose | One-command entrypoint | Details |
| --- | --- | --- | --- |
| RT-SIGNOFF | Sprint 1 sign-off | `bash src/scripts/run_realtime_signoff_acceptance.sh` | [`reference/realtime-signoff-acceptance.md`](reference/realtime-signoff-acceptance.md) |
| RT-SIGNOFF (Observe) | Sprint 1 integrated dataflow for manual observation/Trino queries (no verifier gates) | `bash src/scripts/run_realtime_observe.sh` | [`reference/realtime-signoff-acceptance.md`](reference/realtime-signoff-acceptance.md) |
| CONTENT-AGGREGATOR | Content aggregator bring-up | `bash src/scripts/run_content_aggregator_acceptance.sh` | [`reference/content-aggregator-acceptance.md`](reference/content-aggregator-acceptance.md) |
| CONTENT-CONTRACT | Content contract enforcement | `bash src/scripts/run_content_contract_acceptance.sh` | [`reference/content-contract-acceptance.md`](reference/content-contract-acceptance.md) |
| CDC-CONTRACT | CDC contract enforcement + quarantine | `bash src/scripts/run_cdc_contract_acceptance.sh` | [`reference/cdc-contract-and-quarantine-acceptance.md`](reference/cdc-contract-and-quarantine-acceptance.md) |
| CDC-UPSERT | CDC upsert bring-up and deterministic verification | `bash src/scripts/run_cdc_upsert_acceptance.sh` | [`reference/cdc-upsert-acceptance.md`](reference/cdc-upsert-acceptance.md) |

## Shared SLA and Scope Anchors

1. Keep RT-SIGNOFF and Realtime Sign-off Sprint 1 scope framing unchanged.
2. SLA thresholds are anchored by `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`.
3. Contract semantics remain in streaming contract specs; run scripts are operational source of truth.

## Manual Observation Mode (RT-SIGNOFF Scope)

Use this when you want integrated Sprint 1 data flow running for manual observation or Trino queries without PASS/FAIL verifier gates:

```bash
bash src/scripts/run_realtime_observe.sh
```

Optional (clear checkpoints before job restart):

```bash
bash src/scripts/run_realtime_observe.sh --reset-checkpoints
```

Acceptance and dual-scenario flows also support checkpoint reset:

```bash
bash src/scripts/run_realtime_signoff_acceptance.sh --reset-checkpoints
bash src/scripts/run_realtime_signoff_dual_acceptance.sh --reset-checkpoints
```

## Maintenance Rules

1. Add or update acceptance commands in script files under `src/scripts/` first.
2. In `reference/`, document command usage, env vars/defaults, and output interpretation per flow.
3. Keep this `README.md` index-style only (entrypoints, links, and scope anchors); do not add large multi-step shell blocks.
4. If a flow has no wrapper script, keep only minimal manual-run guidance in its reference doc and avoid duplicating script internals.
5. When verifier gates change, update the corresponding flow reference doc in the same PR.
