# Contract: Realtime Action Queue (Current-State, M1)

## 1. Purpose

`lakehouse.gold.rt_action_queue` is the operational queue for realtime business actions.

This table is designed as a current-state queue:

1. up-to-date and directly consumable by ops/automation
2. optimized for low-latency action handling

## 2. Data Model Policy

1. current-state table (upsert/update in place)
2. actionable decisions only (`BOOST`, `REVIEW`, `RESCUE`)
3. `NO_ACTION` is not persisted
4. one row represents the latest state of one action item

## 3. Required Schema

1. `action_id` (STRING, unique)
2. `video_id` (STRING)
3. `decision_type` (STRING: `BOOST`/`REVIEW`/`RESCUE`)
4. `priority` (INT)
5. `state` (STRING: `PENDING`/`ACKED`/`DONE`/`EXPIRED`/`HOLD`)
6. `decided_at` (TIMESTAMP)
7. `window_start` (TIMESTAMP)
8. `window_end` (TIMESTAMP)
9. `expires_at` (TIMESTAMP)
10. `rule_version` (STRING)
11. `velocity_30m` (DOUBLE)
12. `completion_rate_30m` (DOUBLE)
13. `skip_rate_30m` (DOUBLE)
14. `impressions_30m` (BIGINT)
15. `reason_codes` (ARRAY<'STRING'>)
16. `created_at` (TIMESTAMP)
17. `updated_at` (TIMESTAMP)
18. `state_updated_at` (TIMESTAMP)

## 4. Cooldown and Dedupe Policy

Cooldown:

1. producer-side: at most one emitted action per `video_id` within 60 minutes
2. queue-side guard: active queue also enforces at most one active action per `video_id` within 60 minutes

Dedupe key:

1. `video_id + window_start`

Conflict handling:

1. execution urgency precedence: `RESCUE > REVIEW > BOOST`
2. tie-breaker: latest `created_at`

## 5. Expiration Policy

1. `BOOST`: `decided_at + 15m`
2. `REVIEW`: `decided_at + 30m`
3. `RESCUE`: `decided_at + 30m`

## 6. Validations

1. `action_id`, `video_id`, `decision_type`, `state`, `decided_at`, `expires_at`, `rule_version` are non-null.
2. `decision_type` only in `BOOST`, `REVIEW`, `RESCUE`.
3. `state` only in `PENDING`, `ACKED`, `DONE`, `EXPIRED`, `HOLD`.
4. `expires_at > decided_at`.
5. active rows must satisfy `state in ('PENDING', 'ACKED') and expires_at > now`.
6. active queue guard enforces one active action per `video_id` in 60-minute horizon.
7. state updates must be deterministic and race-safe (compare-and-set on expected prior state).
