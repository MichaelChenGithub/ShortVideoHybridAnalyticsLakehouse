# MIC-57 Metabase Dashboard and Acceptance Runbook for Realtime Metrics and Action Recommendation Preview (M1-S2)

## 1. Purpose and Scope

This runbook defines how to use the MIC-57 SQL pack for:

1. operator-facing Metabase dashboard panels
2. BI/QA acceptance checks
3. Sprint 2 sign-off evidence collection

In scope:

1. serving-view-only BI consumption
2. recommendation preview traceability using decision-context fields
3. contract-mapped acceptance evidence

Out of scope:

1. `v_rt_action_queue_current` and `v_rt_action_queue_active` (M3 reference only)
2. production queue-consumer automation (deferred to M3)
3. M2 dbt semantic testing pipeline

## 2. Canonical SQL Pack

Source of truth:

1. `src/metabase/realtime-metrics-sql-pack.sql`

Serving views used by this pack:

1. `lakehouse.serving.v_rt_video_metrics_30m_1m`
2. `lakehouse.serving.v_rt_video_decision_context_30m_1m`

## 3. Panel-to-Query Mapping (Section A)

1. `A1` platform health trend:
   - line chart
   - window: last 4 hours
   - fields: `avg_velocity_30m`, `avg_completion_rate_30m`, `avg_skip_rate_30m`
2. `A1b` platform coverage trend:
   - line chart (separate panel)
   - window: last 4 hours
   - field: `active_videos`
3. `A3` recommendation traceability table:
   - latest row per `video_id`
   - window: last 30 minutes
   - includes `NO_ACTION` for full auditability
4. `A4` actionable recommendation table:
   - latest row per `video_id`
   - window: last 30 minutes
   - only `BOOST`, `REVIEW`, `RESCUE`

## 4. Acceptance Query Mapping (Section B)

1. `B1` metrics grain uniqueness
2. `B2` decision-context grain uniqueness
3. `B3` decision-context row amplification guard (`row_delta` expected `= 0`)
4. `B4` freshness lag checks
5. `B5` strict required-field null-rate checks
6. `B6` dimension fallback null-rate observability
7. `B7` recommendation domain/rule-version/traceability integrity checks
8. `B8` latest recommendation distribution snapshot

## 5. Contract Clause Traceability

Primary contract references:

1. `docs/architecture/serving/trino-semantic-layer-serving-contract-m1-s2.md`
2. `docs/architecture/realtime-decisioning/realtime-action-queue-decision-behavior-spec.md` (M3 reference for future execution semantics)

Clause mapping:

1. read-time guardrails and bounded BI query windows:
   - serving contract section 8
   - evidence: A1/A1b/A3/A4 bounded windows and explicit field projection
2. BI consumption interface and no formula/decision remapping in BI:
   - serving contract section 9
   - evidence: Section A queries consume semantic fields from serving views only
3. Sprint 2 acceptance traceability:
   - serving contract section 11
   - evidence: Section B checks B1-B8
4. observability fit for recommendation workflow:
   - decision behavior spec section 11
   - evidence: B4 freshness, B5/B6 null-rate observability, B8 decision distribution snapshot

## 6. Execution Checklist

1. Open `src/metabase/realtime-metrics-sql-pack.sql`.
2. Create/refresh Metabase questions for Section A queries.
3. Run Section B queries and capture result tables.
4. Confirm expected acceptance outcomes:
   - B1 duplicate groups = 0
   - B2 duplicate groups = 0
   - B3 row_delta = 0
   - B7 invalid/traceability-gap counters = 0
5. Record freshness and null-rate values from B4-B6 for reviewer context.

## 7. Sprint 2 Sign-off Evidence Package

Default artifact folder:

1. `artifacts/mic57_signoff/<RUN_ID>/`

Required artifacts:

1. screenshot: A1 panel
2. screenshot: A1b panel
3. screenshot: A3 table panel
4. screenshot: A4 table panel
5. query output: B1
6. query output: B2
7. query output: B3
8. query output: B4
9. query output: B5
10. query output: B6
11. query output: B7
12. query output: B8
13. summary note linking outputs to section-5 contract mappings

## 8. Notes

1. `Section B` queries are QA/sign-off checks, not operator business panels.
2. Recommendation preview in MIC-57 is traceability-oriented (`decision_type_preview`), not production action execution state.
