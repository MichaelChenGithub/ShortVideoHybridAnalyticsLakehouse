# Batch Metrics Contract

Status: Draft

## 1. Purpose

Define authoritative batch metric semantics for M2 analytics outputs:

1. retention (`D1`, `D7`)
2. engagement (daily KPI + lightweight funnel)
3. sessionization (30-minute inactivity-gap behavior)

This document is the formula/semantic source of truth for batch outputs.

## 2. Scope

In scope (M2):

1. `lakehouse.gold.batch_retention_daily`
2. `lakehouse.gold.batch_engagement_daily`
3. `lakehouse.gold.batch_sessionization_daily`

Out of scope (deferred):

1. T+1 reconciliation implementation details
2. per-table metric version columns
3. advanced cohort/fallback policy optimization

## 3. Contract Precedence

1. business KPI intent: `docs/product/business-decision-prd-kpi-tree.md`
2. table grain/field contracts: `docs/architecture/data-model/data-model-contract.md`
3. quality gates: `docs/architecture/quality/dbt-semantic-quality-contract.md`
4. publish schedule/orchestration: `docs/architecture/batch-analytics/batch-jobs-and-orchestration-contract.md`

If formula text conflicts with downstream SQL examples, this contract wins.

## 4. Shared Semantics

1. batch publish target: `D-1` outputs ready by `08:00` (`America/New_York`).
2. minimum segmentation coverage: `date x category x region`, with required `new_vs_returning_user`.
3. when user-state attribution is unavailable, use explicit `new_vs_returning_user = 'unknown'`.
4. denominator protection uses `max(denominator, 1)` to avoid divide-by-zero.

## 5. Retention Metrics

Target output:

1. `lakehouse.gold.batch_retention_daily`

Grain:

1. `cohort_date + day_n + category + region + new_vs_returning_user`

Domain:

1. `day_n in {1, 7}` for M2

Definitions:

1. `cohort_users`: number of unique users in the `cohort_date` cohort.
2. `retained_users`: number of those cohort users active on `cohort_date + day_n`.
3. `retention_rate = retained_users / max(cohort_users, 1)`.

## 6. Engagement Metrics

Target output:

1. `lakehouse.gold.batch_engagement_daily`

Grain:

1. `data_date + category + region + new_vs_returning_user`

Core counts:

1. `impressions`
2. `play_start`
3. `play_finish`
4. `likes`
5. `shares`
6. `skips`

Rate definitions:

1. `play_start_rate = play_start / max(impressions, 1)`
2. `completion_rate = play_finish / max(play_start, 1)`
3. `interaction_rate = (likes + shares) / max(play_finish, 1)`
4. `skip_rate = skips / max(play_start, 1)`

## 7. Sessionization Metrics

Target output:

1. `lakehouse.gold.batch_sessionization_daily`

Upstream session logic:

1. session split uses 30-minute inactivity gap (`lakehouse.silver.user_activity_sessions_30m`).

Grain:

1. `data_date + category + region + new_vs_returning_user`

Definitions:

1. `sessions = count(session_id)`
2. `sessions_per_user = sessions / max(count(distinct user_id), 1)`
3. `avg_session_duration_sec = avg(session_duration_sec)`
4. `events_per_session = sum(event_count) / max(sessions, 1)`
5. `watch_time_per_session_ms = sum(watch_time_sum_ms) / max(sessions, 1)`

## 8. Future Plan (Deferred)

1. explicit batch metric versioning contract
2. expanded retention horizons beyond `D1`/`D7`
3. additional engagement/session metrics that are not required for M2 KPI coverage
