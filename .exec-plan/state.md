# MIC-38 Implementation State

## Tracking Rules
- Execute strictly in step order (1 -> 10).
- Only one step may be `in_progress` at a time.
- Allowed statuses: `pending`, `in_progress`, `blocked`, `done`.
- Do not mark a step `done` without evidence path/command in this file.

## Plan Reference
- plan_file: `.exec-plan/plan.md`
- issue: `MIC-38`
- scope_anchor: `MIC-33 Sprint-1 DoD`
- contracts: `/docs` as gold contracts
- created_at: `2026-03-08`

## Step Tracker

| step | title | status | started_at | completed_at | evidence | notes |
|---|---|---|---|---|---|---|
| 1 | Add orchestrator script `run_mic38_acceptance.sh` | done | 2026-03-08T05:14:08Z | 2026-03-08T05:20:49Z | `src/scripts/run_mic38_acceptance.sh`, `bash -n src/scripts/run_mic38_acceptance.sh` | Upgraded to integrated single-run script covering content valid/invalid and CDC valid/invalid checks together. |
| 2 | Bootstrap runtime prerequisites in orchestrator | pending |  |  |  |  |
| 3 | Add `RUN_ID` + `RUN_START_MS` scoping boundary | pending |  |  |  |  |
| 4 | Execute deterministic bounded run + CDC mixed fixture | pending |  |  |  |  |
| 5 | Capture streaming health telemetry for strong gates | pending |  |  |  |  |
| 6 | Implement unified verifier `verify_mic38_sprint1_signoff.py` | pending |  |  |  |  |
| 7 | Emit JSON + Markdown sign-off artifacts | pending |  |  |  |  |
| 8 | Wire hard-fail exit behavior in orchestrator | pending |  |  |  |  |
| 9 | Update streaming README MIC-38 runbook | pending |  |  |  |  |
| 10 | Add tests + run one end-to-end rehearsal | pending |  |  |  |  |

## Locked Thresholds and Gates
- freshness breach: `> 3m` => fail
- latency target: `P95 < 3m` (proxy allowed by MIC-38) => fail on `>= 3m`
- fail conditions:
  - inactive query / query exception
  - no progress / offset stagnation
  - checkpoint stagnation
  - freshness breach
  - latency breach
  - invalid-rate breach (when threshold configured)

## Execution Log
| time | step | action | result |
|---|---|---|---|
| 2026-03-08T05:14:26Z | 1 | Added `run_mic38_acceptance.sh`, set executable bit, ran shell syntax check | PASS |
| 2026-03-08T05:20:49Z | 1 | Reworked `run_mic38_acceptance.sh` into integrated E2E flow with all four Sprint-1 check paths | PASS |

## Blockers
- none
