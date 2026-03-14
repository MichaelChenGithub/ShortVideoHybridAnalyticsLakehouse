# M3 Action Queue Scope and Reference

## 1. Purpose

This document isolates action-queue scope from Milestone 1 contracts.

M1 delivers recommendation preview and health metrics through serving views and dashboards.

M3 will deliver operational action-queue execution semantics.

## 2. M3 Scope

1. `lakehouse.gold.rt_action_queue` as the operational current-state queue.
2. Queue consumer lifecycle (`PENDING`, `ACKED`, `DONE`, `EXPIRED`, `HOLD`).
3. Queue-serving views:
   - `lakehouse.serving.v_rt_action_queue_current`
   - `lakehouse.serving.v_rt_action_queue_active`
4. Automated degraded-mode response that changes execution behavior.
5. Automated rollout gating based on reconciliation states (`WARN` / `CRIT`).

## 3. Queue Contract Summary

1. Current-state upsert/update model (not append-only event history).
2. `decision_type` domain: `BOOST`, `REVIEW`, `RESCUE` (`NO_ACTION` is not persisted).
3. Cooldown: max one action per `video_id` per 60 minutes.
4. Dedupe key: `video_id + window_start`.
5. Expiration:
   - `BOOST`: `decided_at + 15m`
   - `REVIEW`: `decided_at + 30m`
   - `RESCUE`: `decided_at + 30m`
6. Active queue filter: `state in ('PENDING', 'ACKED') and expires_at > now()`.

## 4. M3 Operational Automation

1. Freshness breach response for execution mode switching (for example freeze or restricted action modes).
2. Queue-state transition guards (compare-and-set, retry/backoff, hold routing).
3. Automated rollout block policy tied to reconciliation status.

## 5. Detailed Specs

1. `docs/architecture/realtime-decisioning/action-queue-contract.md`
2. `docs/architecture/realtime-decisioning/realtime-action-queue-decision-behavior-spec.md`

