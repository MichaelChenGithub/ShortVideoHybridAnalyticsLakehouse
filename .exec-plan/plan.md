# MIC-38 Sprint 1 Sign-off Execution Plan

## Goal
Deliver MIC-38 as a one-command, machine-decidable Sprint 1 sign-off flow for MIC-33 scope only.

## Scope Lock
In scope: bounded generator + Kafka + Spark jobs + Sprint-1 key tables + runtime health gates + sign-off evidence.
Out of scope: full M1 close, full rt_action_queue contract gate, degraded-mode control-plane implementation.

## Contract Source of Truth
- MIC-38 issue scope/acceptance (updated)
- MIC-33 Sprint-1 DoD
- /docs as gold contracts (especially streaming + SLA)

## Step-by-Step Plan

1. Add orchestrator script `src/scripts/run_mic38_acceptance.sh`.
Done when: one entrypoint can execute full Sprint-1 sign-off flow.

2. In orchestrator, bootstrap runtime prerequisites.
Do: start required services, enforce topic readiness, stop stale Spark processes, start both Spark jobs.
Done when: both jobs are running under a single controlled run context.

3. Add run boundary for strict scoping.
Do: define `RUN_ID` + `RUN_START_MS`; scope checks to current run only.
Done when: verification is not polluted by historical data.

4. Drive deterministic Sprint-1 traffic.
Do: run bounded generator once; emit CDC mixed fixture for CDC invalid-path evidence.
Done when: current run has content + CDC + invalid candidates.

5. Capture streaming health telemetry.
Do: export job health/progress snapshots for gate checks (`isActive`, exceptions, progress movement).
Done when: health gates can be evaluated without manual log reading.

6. Implement unified verifier `src/scripts/verify_mic38_sprint1_signoff.py`.
Gates:
- Sprint-1 key tables have run-scoped rows.
- `gold.rt_video_stats_1min` contract checks: unique `video_id+window_start`, non-negative metrics.
- invalid sink required-field checks for content/CDC tables.
- invalid-rate checks for content and CDC.
- query health: active, no exception, progress moving.
- checkpoint stagnation: offsets/commits grow over sample window.
- SLA from /docs:
  - freshness breach threshold `> 3m` => fail
  - latency target `P95 < 3m` (proxy metric allowed by MIC-38 wording) => fail on `>= 3m`
Done when: verifier returns machine-decidable PASS/FAIL + non-zero exit on failure.

7. Emit sign-off artifacts.
Do: produce JSON machine report + markdown sprint summary.
Done when: output includes per-gate metrics, failure reasons, blockers, carry-over items.

8. Wire hard fail behavior into orchestrator.
Do: abort immediately on gate failure; propagate non-zero status.
Done when: CI/manual run can trust exit code as final verdict.

9. Document MIC-38 runbook in `docs/architecture/streaming/README.md`.
Do: add one-command usage, thresholds, artifact locations, interpretation.
Done when: reviewer can run and interpret MIC-38 from docs only.

10. Validate via tests and one E2E rehearsal.
Do: add verifier unit tests + shell syntax checks + regression checks on existing verifiers.
Done when: tests pass and one full MIC-38 run produces expected artifact schema.

## Acceptance Mapping
- One-command / one-doc flow: Steps 1, 9
- Machine-decidable pass/fail: Steps 6, 8
- Strong gates: Steps 5, 6
- Sign-off evidence: Step 7
- Sprint close-out summary: Step 7
