# Design Doc: Realtime Metrics & Decision Contract (M1)

## 1. Purpose

This document defines the production-style contract for realtime decision metrics in the short-video lakehouse.
Scope is intentionally limited to M1 decisions:

1. `BOOST` / `REVIEW` using `Viral Velocity + Quality Gate`
2. `RESCUE` using `Cold-start Rescue` logic

This contract is designed for Product/Analytics DE delivery quality: deterministic outputs, versioned rules, measurable SLA, and auditable decisions.

---

## 2. Decision Scope (M1)

### 2.1 In Scope

1. Realtime candidate scoring per video
2. Quality guardrails for risky false positives
3. Under-exposed high-quality cold-start rescue
4. Action queue output for Ops execution

### 2.2 Out of Scope (M1)

1. Category- or region-specific policy tuning beyond under-exposure baseline
2. Realtime dynamic threshold updates
3. Action backfill/rewrite from late events

---

## 3. Core Data Grain

Realtime fact grain:

1. `video_id`
2. `window_start` (1-minute bucket, event time)

Primary key contract:

1. `video_id + window_start` must be unique in the realtime fact source.

---

## 4. Metric Definitions (Rolling 30m)

All decision metrics use a rolling 30-minute window.

### 4.1 Viral Velocity

```text
velocity_30m = (likes_30m + 5 * shares_30m) / max(impressions_30m, 100)
```

Rationale:

1. `share` is weighted higher than `like` as a stronger intent signal.
2. denominator floor `100` prevents small-sample score inflation.

### 4.2 Quality Gate Metrics

```text
completion_rate_30m = play_finish_30m / max(play_start_30m, 1)
skip_rate_30m       = skips_30m / max(play_start_30m, 1)
```

Gate thresholds:

1. `completion_rate_30m >= 0.55`
2. `skip_rate_30m <= 0.35`
3. minimum sample condition: `play_start_30m >= 30`

### 4.3 Candidate and Under-exposed Definitions

Candidate:

1. `velocity_30m >= p90`
2. `impressions_30m >= 100`

Under-exposed:

1. within `category + region` cohort, `impressions_30m <= p40`
2. fallback to global p40 when cohort sample size is insufficient

---

## 5. Decision Policy

Priority order:

1. `BOOST`
2. `REVIEW`
3. `RESCUE`
4. `NO_ACTION`

Policy:

1. `BOOST`: `candidate` and `gate_pass`
2. `REVIEW`: `candidate` and `not gate_pass`
3. `RESCUE`: `not candidate` and `gate_pass` and `upload_age <= 60m` and `under_exposed`
4. `NO_ACTION`: otherwise

Cooldown:

1. each `video_id` can emit at most 1 action per 60 minutes.

---

## 6. Action Queue Contract

Target table:

1. `lakehouse.gold.rt_action_queue`

Purpose:

1. execution interface for Ops/business workflows
2. decision audit trail

Required columns:

1. `action_id` (unique)
2. `video_id`
3. `decision_type` (`BOOST`, `REVIEW`, `RESCUE`)
4. `priority`
5. `decided_at`
6. `window_start`
7. `window_end`
8. `expires_at`
9. `rule_version`
10. `velocity_30m`
11. `completion_rate_30m`
12. `skip_rate_30m`
13. `impressions_30m`
14. `reason_codes` (array of standardized codes)
15. `created_at`

Expiration policy:

1. `BOOST`: `decided_at + 15m`
2. `REVIEW`: `decided_at + 30m`
3. `RESCUE`: `decided_at + 30m`

Standard reason codes:

1. `HIGH_VELOCITY_P90`
2. `GATE_PASS`
3. `LOW_COMPLETION`
4. `HIGH_SKIP`
5. `NEW_UPLOAD_LT_60M`
6. `UNDER_EXPOSED_P40`

---

## 7. Rule Version Governance

`rule_version` stores policy configuration identity (weights, thresholds, quantile baselines, cooldown, priority).

M1 default:

1. `rt_rules_v1`

Governance:

1. rule updates are calibrated daily from T+1 analysis
2. no intraday realtime threshold drifting
3. every action row must carry the exact `rule_version`

---

## 8. Late Data Policy

Policy:

1. realtime action queue is append-only and immutable
2. late events do not rewrite already-emitted actions
3. late events are tracked in audit and reflected in downstream reconciliation/T+1

Rationale:

1. decision output must remain auditable and deterministic

---

## 9. Degraded Mode Policy

Trigger A:

1. freshness `P95 > 3m` for 5 consecutive minutes
2. behavior: emit `REVIEW` only; pause `BOOST` and `RESCUE`

Trigger B:

1. freshness `> 10m` or realtime source outage
2. behavior: pause all actions

Recovery:

1. require 15 consecutive healthy minutes before exiting degraded mode

---

## 10. RT vs T+1 Reconciliation (Global Only for M1)

Schedule:

1. run daily after T+1 data readiness

Metrics:

1. counts: `impressions`, `play_start`, `play_finish`, `likes`, `shares`, `skips`
2. rates: `completion_rate`, `skip_rate`

Error formulas:

1. count global relative error: `abs(sum_rt - sum_batch) / max(sum_batch, 1)`
2. count minute-level error p95: `p95(abs(rt_1m - batch_1m) / max(batch_1m, 100))`
3. rate minute-level absolute error p95: `p95(abs(rt_rate_1m - batch_rate_1m))`

Threshold:

1. default pass target: count `p95 <= 0.08`, rate `p95 <= 0.03`

Failure handling:

1. `WARN` or `CRIT` freezes new `rule_version` rollout
2. `CRIT` enters conservative mode until stability is restored

---

## 11. Acceptance Criteria (M1)

### 11.1 Functional

1. action queue is produced every minute
2. decisions follow exact priority and policy rules
3. outputs are deterministic for identical input

### 11.2 Contract and Data Quality

1. realtime fact enforces unique `video_id + window_start`
2. required action queue columns are non-null
3. `decision_type` only in `BOOST`, `REVIEW`, `RESCUE`
4. `expires_at > decided_at`
5. cooldown rule is enforced

### 11.3 SLA and Reliability

1. event-to-action latency `P95 < 3m`
2. freshness breaches trigger degraded mode logic

### 11.4 Accuracy and Reconciliation

1. global RT vs T+1 reconciliation computed daily
2. count and rate error thresholds are monitored and enforced
3. rule rollout is blocked under reconciliation failure states

