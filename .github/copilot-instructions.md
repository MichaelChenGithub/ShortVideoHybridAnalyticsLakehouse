# Agent Execution Rules

These rules apply only when performing implementation tasks
(e.g., fixing issues, implementing features, modifying code).
They do not apply to general discussion or architecture questions.

## Workflow Overview
1. Conflict Check (required)
2. Execution Preparation (assume `plan.md` is already stored at repo root by the user)
3. Controlled Execution (one step at a time)

## General
- Always check issue scope against repo docs/contracts before coding.
- Report any conflict between issue scope and design docs before implementation.
- Do not silently expand scope.
- For non-trivial tasks, use repo-root `plan.md` and `state.md`.

## Phase 0: Conflict Check
- After reading scope and aligning with repo docs/contracts, report status before any execution work.
- The status report must include: understood scope, alignment result, conflicts/risks, and open questions.
- If conflicts exist, stop and wait for user decision.
- If no conflicts but repo-root `plan.md` is missing, stop and wait for user to provide it.

## Phase 1: Execution Preparation
- Do not generate plan content in this phase; `plan.md` is user-provided.
- Read repo-root `plan.md`, then create/update repo-root `state.md`.
- Decompose `plan.md` into atomic execution steps.
- For every planned step, include:
  - `Change`: what will be changed
  - `Files`: exact files expected
  - `Verify`: exact command(s) to run
  - `Pass Signal`: observable success criteria
- Default limit: <= 5 files changed per step.
- `state.md` must track: current step, completed work, pending decisions, files changed, verification evidence, next action.
- Exactly one step may be marked `in_progress` at any time.

## Phase 2: Controlled Execution
- Start execution only after explicit user approval.
- Implement only one step at a time.
- Each step must be independently reviewable.
- For each step, include concrete verification command(s) and observed result(s) in `state.md`.
- Stop after completing the current step, update `state.md`, and wait for explicit instruction before the next step.

## Scope Change Control
- If execution reveals out-of-scope work, stop immediately.
- Report scope delta, impact, and recommendation.
- Wait for explicit user approval before continuing.

## Review
- Summarize what changed, why, and what remains.
- Do not proceed to next step until explicitly instructed.
