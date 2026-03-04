# Contract: Realtime Action Queue (M1)

## 1. Purpose

`lakehouse.gold.rt_action_queue` is the execution interface for realtime decisions.

It is consumed by:

1. Ops workflow and execution UI
2. decision audit and replay analysis

It is not the only source for KPI trend panels; trend metrics should still use realtime fact tables/views.

## 2. Write Policy

1. Append-only
2. Actionable decisions only (`BOOST`, `REVIEW`, `RESCUE`)
3. `NO_ACTION` is not persisted

Cooldown:

1. at most one emitted action per `video_id` within 60 minutes

## 3. Required Schema

1. `action_id` (STRING, unique)
2. `video_id` (STRING)
3. `decision_type` (STRING: `BOOST`/`REVIEW`/`RESCUE`)
4. `priority` (INT)
5. `decided_at` (TIMESTAMP)
6. `window_start` (TIMESTAMP)
7. `window_end` (TIMESTAMP)
8. `expires_at` (TIMESTAMP)
9. `rule_version` (STRING)
10. `velocity_30m` (DOUBLE)
11. `completion_rate_30m` (DOUBLE)
12. `skip_rate_30m` (DOUBLE)
13. `impressions_30m` (BIGINT)
14. `reason_codes` (ARRAY<'STRING'>)
15. `created_at` (TIMESTAMP)

## 4. Expiration Policy

1. `BOOST`: `decided_at + 15m`
2. `REVIEW`: `decided_at + 30m`
3. `RESCUE`: `decided_at + 30m`

## 5. Standard Reason Codes

1. `HIGH_VELOCITY_P90`
2. `GATE_PASS`
3. `LOW_COMPLETION`
4. `HIGH_SKIP`
5. `NEW_UPLOAD_LT_60M`
6. `UNDER_EXPOSED_P40`

## 6. Validations

1. `action_id`, `video_id`, `decision_type`, `decided_at`, `expires_at`, `rule_version` are non-null.
2. `decision_type` only in `BOOST`, `REVIEW`, `RESCUE`.
3. `expires_at > decided_at`.
4. same input state must produce deterministic output rows.
