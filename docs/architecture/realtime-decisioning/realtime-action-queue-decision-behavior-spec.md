# Design Doc: Realtime Metrics-to-Action Decisioning and Current-State Queue

Date: `2026-03-08`  

## 1. Purpose

This document defines how realtime metrics produce business actions and how the action queue is maintained as an up-to-date current-state table.

Primary objective:

1. make realtime decision outputs operationally actionable
2. keep queue semantics simple and low-latency for execution

## 2. Scope

In scope now:

1. metrics-to-action decision logic
2. current-state action queue contract
3. minimal operational states and transitions
4. dedupe, cooldown, expiration, and SLA behavior

Future scope (not required now):

1. append-only event log for full replay/audit
2. advanced state machine expansion
3. automated low-risk action execution

## 3. Source Contracts

1. [`metric-contract.md`](metric-contract.md)
2. [`action-queue-contract.md`](action-queue-contract.md)
3. [`reconciliation-and-slo.md`](reconciliation-and-slo.md)
4. [`acceptance-criteria.md`](acceptance-criteria.md)

## 4. Current-State Queue Model

Canonical queue table:

1. `lakehouse.gold.rt_action_queue`

Model:

1. one row represents the latest state of one actionable item
2. queue rows are updated in place (upsert/update), not append-only
3. consumer reads only active rows, not historical events

Active queue filter:

1. `state in ('PENDING', 'ACKED')`
2. `expires_at > now`

## 5. Minimal State Model

States:

1. `PENDING`: action generated and waiting for owner handling
2. `ACKED`: owner has acknowledged and started handling
3. `DONE`: action execution completed
4. `EXPIRED`: action is stale and no longer executable
5. `HOLD`: manual hold due to escalation or execution issue

Allowed transitions:

1. `PENDING -> ACKED`
2. `ACKED -> DONE`
3. `PENDING|ACKED -> EXPIRED`
4. `PENDING|ACKED -> HOLD`

Terminal states:

1. `DONE`
2. `EXPIRED`
3. `HOLD`

## 6. Decision Semantics and Priority

Decision mapping:

1. `BOOST`: candidate and quality gate pass
2. `REVIEW`: candidate and quality gate fail
3. `RESCUE`: non-candidate, gate pass, new upload, under-exposed
4. `NO_ACTION`: otherwise

Policy selection precedence:

1. `BOOST > REVIEW > RESCUE > NO_ACTION`

Execution urgency precedence (only when same dedupe key collides):

1. `RESCUE > REVIEW > BOOST`
2. tie-breaker: latest `created_at`

## 7. Write and Update Behavior

Producer behavior:

1. evaluate metrics and generate actionable decisions
2. upsert current-state row with `state = PENDING`
3. when collision exists, keep urgency winner and suppress non-winner

Consumer behavior:

1. acknowledge updates `PENDING -> ACKED`
2. successful execution updates `ACKED -> DONE`
3. expiration scheduler updates open rows to `EXPIRED`
4. unresolved escalation updates open rows to `HOLD`

Update guard:

1. use compare-and-set condition with expected prior state to avoid race updates

## 8. Dedupe and Cooldown

Dedupe key:

1. `video_id + window_start`

Cooldown:

1. producer-side rule: at most one emitted action per `video_id` in 60 minutes
2. queue guard rule: active queue also enforces at most one active action per `video_id` in 60 minutes

Queue guard is mandatory because producer-only control is not enough under backfill/retry/race scenarios.

## 9. Expiration and SLA Budget

TTL:

1. `BOOST`: `decided_at + 15m`
2. `REVIEW`: `decided_at + 30m`
3. `RESCUE`: `decided_at + 30m`

SLA:

1. `BOOST` ack <= 15m, execute <= 45m
2. `REVIEW` ack <= 15m, execute <= 60m
3. `RESCUE` ack <= 5m, execute <= 30m

TTL-SLA check:

1. `ttl_seconds >= ack_budget + execution_start_budget + safety_buffer`
2. if `expiration_rate` rises, adjust TTL or SLA before rollout

## 10. Performance and Reliability

Performance requirements:

1. active queue queries must hit indexed current-state rows only
2. avoid full-table scan style latest-state reconstruction

Recommended indexing:

1. `(state, expires_at)`
2. `(video_id, window_start)`
3. unique key on `action_id`

Reliability guardrails:

1. bounded retry with backoff for transient write failures
2. retry exhaustion routes row to `HOLD`
3. no duplicate external execution for same `action_id`

## 11. Observability

Required metrics:

1. action_inflow_rate by decision type
2. ack_latency and exec_latency (p50/p95/p99)
3. expiration_rate
4. hold_rate
5. cooldown_guard_violation_rate
6. queue_consumer_lag_seconds p95

## 12. Future Extension

When stronger auditability is required, extend with:

1. `rt_action_event_log` append-only history table
2. richer state machine (`EXECUTING`, `FAILED`, `SKIPPED_DUPLICATE`)
3. replay tooling and policy simulation
