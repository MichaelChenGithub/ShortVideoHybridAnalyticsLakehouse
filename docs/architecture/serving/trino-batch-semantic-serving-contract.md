# Design Doc: Trino Batch Semantic Serving Contract

Date: `2026-03-15`
Status: `Final`

## 1. Purpose

Define stable consumer-facing semantic interfaces for current batch analytics outputs.

## 2. Scope

In scope:

1. batch semantic views for retention, engagement, and sessionization
2. serving contracts (grain, keys, required fields, freshness/publish expectations)
3. BI consumption contract for daily `D-1` analytics use

Out of scope (deferred):

1. T+1 reconciliation implementation details
2. advanced semantic automation workflows
3. additional batch marts not required by current KPI scope

## 3. Contract Precedence

Authoritative upstream contracts:

1. `docs/product/business-decision-prd-kpi-tree.md`
2. `docs/architecture/data-model/data-model-contract.md`
3. `docs/architecture/batch-analytics/batch-metrics-contract.md`
4. `docs/architecture/batch-analytics/batch-jobs-and-orchestration-contract.md`
5. `docs/architecture/quality/dbt-semantic-quality-contract.md`

## 4. Semantic Schema and Naming Convention

Serving schema:

1. `lakehouse.serving`

Naming convention:

1. view prefix must be `v_`
2. batch domain segment must be `bt`
3. grain hint should be explicit in view names

## 5. Semantic View Inventory

### 5.1 `lakehouse.serving.v_bt_retention_daily`

Purpose:

1. semantic serving interface for `D1`/`D7` retention analytics outputs

Primary source:

1. `lakehouse.gold.batch_retention_daily`

Grain:

1. `cohort_date + day_n + category + region + new_vs_returning_user`

Key required fields:

1. `cohort_date`
2. `day_n`
3. `category`
4. `region`
5. `new_vs_returning_user`
6. `cohort_users`
7. `retained_users`
8. `retention_rate`
9. `data_date`
10. `published_at`

Semantic notes:

1. `day_n` is limited to `{1, 7}` in current scope.
2. cohort is activity-date based and supports governed `new`, `returning`, and `unknown` segments.
3. when `cohort_users = 0`, retention output should be treated as not computable (`NULL`/no row), not forced to `0`.

### 5.2 `lakehouse.serving.v_bt_engagement_daily`

Purpose:

1. semantic serving interface for daily engagement KPI + lightweight funnel outputs

Primary source:

1. `lakehouse.gold.batch_engagement_daily`

Grain:

1. `data_date + category + region + new_vs_returning_user`

Key required fields:

1. `data_date`
2. `category`
3. `region`
4. `new_vs_returning_user`
5. `impressions`
6. `play_start`
7. `play_finish`
8. `likes`
9. `shares`
10. `skips`
11. `play_start_rate`
12. `completion_rate`
13. `interaction_rate`
14. `skip_rate`
15. `published_at`

Semantic notes:

1. engagement rate formulas are governed by `docs/architecture/batch-analytics/batch-metrics-contract.md`.
2. `category` is sourced from video attribution; `region` and `new_vs_returning_user` are sourced from user attribution.
3. `region` in this view represents audience region rather than video region.

### 5.3 `lakehouse.serving.v_bt_sessionization_daily`

Purpose:

1. semantic serving interface for daily sessionization outputs

Primary source:

1. `lakehouse.gold.batch_sessionization_daily`

Grain:

1. `data_date + category + region + new_vs_returning_user`

Key required fields:

1. `data_date`
2. `category`
3. `region`
4. `new_vs_returning_user`
5. `sessions`
6. `sessions_per_user`
7. `avg_session_duration_sec`
8. `events_per_session`
9. `watch_time_per_session_ms`
10. `published_at`

Semantic notes:

1. `sessions_per_user` is defined as `sessions / distinct_active_users` within the same grain.
2. sessionization metric formulas are governed by `docs/architecture/batch-analytics/batch-metrics-contract.md`.

## 6. View-to-Metric Mapping

Source-to-view mapping:

1. `lakehouse.gold.batch_retention_daily` -> `lakehouse.serving.v_bt_retention_daily`
2. `lakehouse.gold.batch_engagement_daily` -> `lakehouse.serving.v_bt_engagement_daily`
3. `lakehouse.gold.batch_sessionization_daily` -> `lakehouse.serving.v_bt_sessionization_daily`

Formula governance:

1. batch metric formulas and denominator protections are governed by:
   - `docs/architecture/batch-analytics/batch-metrics-contract.md`
2. this serving contract does not redefine metric formulas independently.

## 7. Serving Contracts

### 7.1 Contract: `v_bt_retention_daily`

1. key uniqueness: `cohort_date + day_n + category + region + new_vs_returning_user` must be unique.
2. `day_n` must be in `{1, 7}` for current scope.
3. required fields from section 5.1 must be present.
4. `new_vs_returning_user` must use governed values (`new`, `returning`, `unknown`).
5. publish expectation: rows must reflect `D-1` batch publish cycle.

### 7.2 Contract: `v_bt_engagement_daily`

1. key uniqueness: `data_date + category + region + new_vs_returning_user` must be unique.
2. required fields from section 5.2 must be present.
3. engagement formula semantics must follow batch metrics contract.
4. publish expectation: rows must reflect `D-1` batch publish cycle.

### 7.3 Contract: `v_bt_sessionization_daily`

1. key uniqueness: `data_date + category + region + new_vs_returning_user` must be unique.
2. required fields from section 5.3 must be present.
3. sessionization formula semantics must follow batch metrics contract.
4. publish expectation: rows must reflect `D-1` batch publish cycle.

## 8. BI Consumption Contract

1. BI consumers should read `lakehouse.serving.v_bt_*` views, not raw batch gold tables.
2. BI queries must not redefine batch formulas; use semantic fields as provided.
3. default dashboard context should use `data_date = current_date - 1` and display `published_at` for recency visibility.

## 9. Acceptance Mapping

1. Batch semantic view inventory is explicit and queryable:
   - covered by section 5
2. Source-to-view mapping and formula authority are explicit:
   - covered by section 6
3. Uniqueness, required fields, and publish expectations are contract-defined:
   - covered by section 7
4. BI consumption boundaries are explicit:
   - covered by section 8
5. Metabase dashboard and acceptance query pack reference:
   - `src/metabase/batch-metrics-sql-pack.sql`
   - `docs/architecture/serving/reference/metabase-batch-dashboard-acceptance-runbook.md`

## 10. Future Plan (Deferred)

1. canonical deferred-scope reference: `docs/milestone/future-plan.md`
