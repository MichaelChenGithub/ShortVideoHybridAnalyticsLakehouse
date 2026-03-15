# Realtime Decisioning (Realtime Scope, M3 Future Plan)

This module defines realtime decisioning contracts for operational preview.

Scope anchors:

1. `docs/milestone/m2_scope.md`
2. `docs/milestone/m3_scope.md`

Scope boundary:

1. Batch metrics, semantic expansion, and dbt quality workflows are defined outside this module.
2. This module only defines realtime decision logic and its serving-facing contracts.

Upstream business spec:

1. `docs/product/business-decision-prd-kpi-tree.md`

Upstream model spec:

1. `docs/architecture/data-model/m1-data-model-v1.md`

Upstream streaming execution spec:

1. `docs/architecture/streaming/spark-realtime-jobs-contract-m1.md`

## 1. Business Decisions in Scope

1. `BOOST`: high momentum and quality-passing videos.
2. `REVIEW`: high momentum but quality-failing videos.
3. `RESCUE`: high-quality new videos with under-exposure.

Serving surface:

1. Trino semantic views and BI dashboards for recommendation preview and health metrics.

## 2. Policy Priority

1. `BOOST`
2. `REVIEW`
3. `RESCUE`
4. `NO_ACTION`

## 3. Rule Baseline

1. `rule_version = rt_rules_v1`
2. rolling window = 30 minutes
3. core grain = `video_id + window_start` (1-minute event-time bucket)
4. baseline registry table = `lakehouse.dims.rt_rule_quantile_baselines`
5. published validity window = `effective_from = 2026-01-01`, `effective_to = 2099-12-31`
6. baseline publish semantics = insert-only (`rule_version + effective_from`)
7. threshold scope = global `p90` (`velocity_30m`) + global `p40` (`impressions_30m`)

## 4. Future Plan (Deferred to M3)

1. Queue execution semantics are outside M1 + M2 delivery scope.
2. T+1 reconciliation implementation is outside M1 + M2 delivery scope.
3. Automated degraded-mode switching and automated rollout blocking are outside M1 + M2 delivery scope.
4. Canonical deferred-scope reference:
   - `docs/milestone/m3_scope.md`

## 5. Spec Files

1. `metric-contract.md`
2. `reconciliation-and-slo.md`
3. `acceptance-criteria.md`
4. `m3-action-queue-reference.md`
