# Agent Execution Rules

These rules apply only when performing implementation tasks
(e.g., fixing issues, implementing features, modifying code).
They do not apply to general discussion or architecture questions.

## General
- Always check issue scope against repo docs/contracts before coding.
- Report any conflict between issue scope and design docs before implementation.
- Do not silently expand scope.

## Pre-Planning
- After reading scope and aligning with repo docs/contracts, report status before any planning work.
- The status report must include: understood scope, alignment result, conflicts/risks, and open questions.
- Do not create or modify `plan.md`/`state.md` until the user explicitly says to start planning (for example: "start plan").
- If explicit start-planning instruction is not provided, stop and wait.

## Planning
- Create plan.md and state.md at the **repo root** before implementation for non-trivial tasks. Never place them in subdirectories.
- Plan must define objective, scope, out-of-scope, constraints, steps, acceptance criteria.
- State must track current step, completed work, pending decisions, files changed, next action.
- After writing `plan.md` and `state.md`, stop and request review/approval.
- Do not start implementation until the user explicitly approves (for example: "approved", "go implement").
- If the user requests plan changes, update `plan.md`/`state.md` and wait for approval again.

## Execution
- Start execution only after explicit approval of the plan.
- Implement only one step at a time.
- Each step must be independently reviewable.
- Default limit: <= 5 files changed per step.
- Stop after completing the current step and update state.md.

## Review
- Summarize what changed, why, and what remains.
- Do not proceed to next step until explicitly instructed.
