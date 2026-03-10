# Design Doc: Trino Semantic Layer and Serving Contract (M1 Sprint 2 Prerequisite)

Date: `2026-03-08`  
Status: `Draft`

## 1. Purpose

Define stable consumer-facing semantic interfaces between:

1. source fact table `lakehouse.gold.rt_video_stats_1min`
2. decision output table `lakehouse.gold.rt_action_queue`
3. Sprint 2 BI/dashboard SQL consumers

This document standardizes:

1. semantic view inventory and naming conventions
2. view-to-metric and view-to-decision mappings
3. serving contracts (grain, keys, required fields, freshness expectations)
4. read-time join and performance guardrails for demo-safe Trino workloads
5. explicit boundary with M2 dbt/data-quality expansion

## 2. Scope

In scope (M1 Sprint 2 prerequisite):

1. Trino semantic views for BI/dashboard queries
2. contract definitions for metric-serving and action-serving views
3. traceability requirements for `rule_version`
4. join compatibility and query safety constraints for realtime dashboards

Out of scope (deferred to M2):

1. replacing Gold tables as source of truth (not planned in M1/M2)
2. full production hardening and advanced performance optimization
3. full dbt model rollout and automated semantic test suite

## 3. Contract Precedence

Authoritative upstream contracts:

1. `docs/architecture/data-model/m1-data-model-v1.md`
2. `docs/architecture/realtime-decisioning/metric-contract.md`
3. `docs/architecture/realtime-decisioning/action-queue-contract.md`
4. `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`

Conflict resolution for this design:

1. realtime decisioning contract docs are authoritative for queue semantics
2. semantic views must not redefine metric formulas independently
3. if documentation text conflicts with the above contracts, this serving doc follows the contract files listed above

## 4. Semantic Schema and Naming Convention

Serving schema:

1. `lakehouse.serving`

Naming convention:

1. view prefix must be `v_`
2. realtime domain segment must be `rt`
3. subject and window must be explicit in name (`video_metrics_30m`, `action_queue_current`)
4. grain hint should be encoded when not obvious (`_1m` for per-minute grain)
5. breaking semantic changes require a new versioned view name suffix (`_v2`, `_v3`)
6. additive columns are allowed without version bump

## 5. Semantic View Inventory (M1 Sprint 2)

### 5.1 `lakehouse.serving.v_rt_video_metrics_30m_1m`

Purpose:

1. BI-friendly rolling 30-minute metric view derived from 1-minute fact

Primary source:

1. `lakehouse.gold.rt_video_stats_1min`

Grain:

1. `video_id + metric_minute`

Key required fields:

1. `video_id`
2. `metric_minute`
3. `impressions_30m`
4. `play_start_30m`
5. `play_finish_30m`
6. `likes_30m`
7. `shares_30m`
8. `skips_30m`
9. `velocity_30m`
10. `completion_rate_30m`
11. `skip_rate_30m`
12. `processed_at_max`

### 5.2 `lakehouse.serving.v_rt_video_decision_context_30m_1m`

Purpose:

1. traceable decision-context view for table drill-down and root-cause analysis
2. enrich rolling metrics with metadata and threshold context used by decisioning

Primary sources:

1. `lakehouse.serving.v_rt_video_metrics_30m_1m`
2. `lakehouse.dims.dim_videos`

Grain:

1. `video_id + metric_minute`

Key required fields:

1. `video_id`
2. `metric_minute`
3. `category`
4. `region`
5. `status`
6. `upload_time`
7. `upload_age_minutes`
8. `velocity_30m`
9. `completion_rate_30m`
10. `skip_rate_30m`
11. `rule_version`
12. `candidate_flag`
13. `quality_gate_pass`
14. `under_exposed_flag`
15. `decision_type_preview`
16. `p90_velocity_threshold`
17. `p40_impressions_threshold`

### 5.3 `lakehouse.serving.v_rt_action_queue_current`

Purpose:

1. stable consumer projection of current-state action queue

Primary source:

1. `lakehouse.gold.rt_action_queue`

Grain:

1. `action_id`

Key required fields:

1. `action_id`
2. `video_id`
3. `decision_type`
4. `priority`
5. `state`
6. `decided_at`
7. `window_start`
8. `window_end`
9. `expires_at`
10. `rule_version`
11. `reason_codes`
12. `velocity_30m`
13. `completion_rate_30m`
14. `skip_rate_30m`
15. `impressions_30m`
16. `created_at`
17. `updated_at`
18. `state_updated_at`

### 5.4 `lakehouse.serving.v_rt_action_queue_active`

Purpose:

1. direct BI/ops consumption view for active action workload

Primary source:

1. `lakehouse.serving.v_rt_action_queue_current`

Grain:

1. `action_id`

Active filter contract:

1. `state in ('PENDING', 'ACKED')`
2. `expires_at > now()`

## 6. View-to-Metric and View-to-Decision Mapping

`rt_video_stats_1min` to semantic metrics:

1. `metric_minute = window_start`
2. `impressions_30m = sum(impressions)` over event-time range `[metric_minute - 29m, metric_minute]`
3. `play_start_30m = sum(play_start)` over the same 30-minute range
4. `play_finish_30m = sum(play_finish)` over the same 30-minute range
5. `likes_30m = sum(likes)` over the same 30-minute range
6. `shares_30m = sum(shares)` over the same 30-minute range
7. `skips_30m = sum(skips)` over the same 30-minute range
8. `velocity_30m = (likes_30m + 5 * shares_30m) / max(impressions_30m, 100)`
9. `completion_rate_30m = play_finish_30m / max(play_start_30m, 1)`
10. `skip_rate_30m = skips_30m / max(play_start_30m, 1)`

`rt_action_queue` to semantic decision fields:

1. `decision_type` is consumed directly as final decision output
2. `rule_version` is consumed directly for rule traceability
3. `reason_codes` is consumed directly for decision explainability
4. `velocity_30m`, `completion_rate_30m`, `skip_rate_30m`, `impressions_30m` are consumed as point-in-time decision evidence
5. queue state tracking fields (`state`, `state_updated_at`, `updated_at`) are consumed for operational dashboarding

`decision_type_preview` derivation contract:

1. semantic meaning must follow `docs/architecture/realtime-decisioning/metric-contract.md` section 4 (`Decision Mapping`)
2. `decision_type_preview` is for BI traceability and does not replace producer-written `decision_type` in `rt_action_queue`
3. preview output domain: `BOOST`, `REVIEW`, `RESCUE`, `NO_ACTION`
4. implementation must use the same threshold inputs (`p90`, `p40`) and gate definitions declared in the metric contract

Quantile baseline standard (`p90` / `p40`):

1. baseline source: `lakehouse.dims.rt_rule_quantile_baselines`
2. candidate threshold: global `p90` on `velocity_30m`
3. M1 under-exposure threshold: global `p40` on `impressions_30m`
4. baseline refresh cadence: daily after T+1 completion

`rule_version` propagation strategy:

1. M1 lock: use fixed `rule_version = rt_rules_v1` for semantic views and baseline joins
2. baseline rows for the locked version are immutable after publish
3. `v_rt_video_decision_context_30m_1m` must join baselines by locked `rule_version`; M1 uses global baseline rows only
4. `v_rt_action_queue_current` consumes producer-written `rule_version` as final execution evidence
5. BI panels comparing metrics and actions must filter or group by `rule_version` to avoid cross-version mixing
6. future extension (post-batch/backfill): switch to effective-date version selection using `metric_minute` against baseline validity range

## 7. Serving Contracts

### 7.1 Contract: `v_rt_video_metrics_30m_1m`

1. key uniqueness: `video_id + metric_minute` must be unique
2. required fields from section 5.1 must be non-null except rate fields when denominator is zero by policy
3. formula compatibility must match `metric-contract.md` exactly
4. freshness expectation:
   - healthy target: latest `metric_minute` lag `<= 3 minutes` at p95
   - severe breach threshold: lag `> 10 minutes`

### 7.2 Contract: `v_rt_video_decision_context_30m_1m`

1. join key: `video_id`
2. join type: `LEFT JOIN` from metrics view to `dim_videos`
3. grain safety: output row count per `video_id + metric_minute` must not exceed input metrics row count
4. missing-dimension fallback: null dimension context must not produce unsafe boost interpretation
5. `rule_version` is mandatory and must identify the baseline used for threshold interpretation
6. threshold fields (`p90_velocity_threshold`, `p40_impressions_threshold`) must come from the same `rule_version`
7. M1 `under_exposed_flag` must be computed using global `p40`
8. `decision_type_preview` must follow `metric-contract.md` decision mapping and must not introduce independent rules

### 7.3 Contract: `v_rt_action_queue_current`

1. key uniqueness: `action_id` must be unique
2. required fields must match `action-queue-contract.md`
3. queue semantics are current-state upsert/update, not append-only action history
4. `decision_type` allowed values: `BOOST`, `REVIEW`, `RESCUE`
5. `state` allowed values: `PENDING`, `ACKED`, `DONE`, `EXPIRED`, `HOLD`
6. freshness expectation:
   - healthy target: latest `decided_at` lag `<= 3 minutes` at p95
   - severe breach threshold: lag `> 10 minutes`

### 7.4 Contract: `v_rt_action_queue_active`

1. filter semantics must match active queue contract
2. consumer queries on active workload should use this view by default
3. expired or terminal actions are excluded from active operational panels
4. freshness expectation inherits `v_rt_action_queue_current` and is measured on latest `decided_at`

## 8. Read-Time Join and Query Guardrails (Demo-Safe)

Allowed join path:

1. `v_rt_video_metrics_30m_1m.video_id = dim_videos.video_id`

Guardrails:

1. joins must preserve `video_id + metric_minute` grain
2. no fact-to-fact read-time joins in dashboard queries
3. realtime dashboard queries must include bounded time filter on `metric_minute`
4. ranking queries must use `ORDER BY ... LIMIT ...`
5. avoid `SELECT *` in BI production queries; explicitly select required fields
6. dashboard default refresh query window should be `<= 4 hours` unless explicitly marked as backfill/analysis mode
7. semantic views are logical interfaces; no requirement to materialize data in serving schema for M1

Compatibility checks:

1. run a grain-safety check query before publishing dashboard SQL
2. run freshness lag check for `metric_minute` and `decided_at`
3. run null-rate checks on required serving fields

Retention policy note for `rt_video_stats_1min`:

1. 30 minutes is a metric window, not a table retention window
2. do not purge rows only because `window_start` is older than 30 minutes
3. retention should follow operational/reconciliation needs (for example 7 to 30 days) plus Iceberg maintenance routines

## 9. BI Consumption Contract

Default BI consumption pattern:

1. global distribution and trend panels use `v_rt_video_metrics_30m_1m`
2. traceable decision table panels use `v_rt_video_decision_context_30m_1m`
3. action execution panels use `v_rt_action_queue_active`
4. drill-down uses `v_rt_action_queue_current` for current-state action details per `action_id`

Expected BI simplification outcome:

1. BI SQL should not reimplement rolling-window formulas
2. BI SQL should not reimplement decision field derivation
3. BI SQL should consume stable, named semantic fields and contracts

## 10. Boundary with M2 dbt and Data-Quality Expansion

M1 Sprint 2 boundary:

1. Trino semantic contract is defined and queryable
2. contract checks may be script-based and manual where needed
3. no change to Gold tables as canonical sources

M2 planned expansion:

1. move semantic definitions into dbt models with test coverage
2. add automated contract tests (grain, nullability, formula consistency, freshness)
3. add semantic lineage and versioned release workflow

## 11. Acceptance Mapping for Sprint 2 Prerequisite Scope

1. view-to-decision mapping explicit:
   - covered by sections 5, 6, and 9
2. serving definitions consistent with realtime contracts:
   - covered by sections 3 and 7
3. Sprint 2 BI SQL can proceed without semantic ambiguity:
   - covered by sections 4, 5, 8, and 9
4. M2 dbt boundary documented:
   - covered by section 10

## 12. Locked Decisions (M1 Sprint 2)

1. Gold tables remain source of truth; serving layer is a read-only semantic interface.
2. `p90/p40` thresholds are governed by published quantile baselines; M1 uses global-only thresholds.
3. `rule_version` is mandatory on decision-context and action-serving paths for traceability.
4. M1 locks `rule_version` to `rt_rules_v1`.
5. split-view strategy is retained:
   - `v_rt_video_metrics_30m_1m` for global/trend analysis
   - `v_rt_video_decision_context_30m_1m` for traceable metadata-rich drill-down

## 13. Future Plan

1. add cohort `p40` baselines by `category + region` for under-exposure thresholding.
2. add cohort/global fallback selection in semantic SQL:
   - use cohort `p40` when `sample_size >= 200`
   - fallback to global `p40` when cohort `sample_size < 200`
3. add fallback marker semantics for BI traceability in decision-context serving fields.
4. extend baseline selection to effective-date multi-version mode using `metric_minute` against baseline validity range.
