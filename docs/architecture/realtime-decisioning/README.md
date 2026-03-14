# Realtime Decisioning (M1 Scope + M3 Queue References)

This module defines the realtime decision system for short-video operations.

Upstream business spec:

1. `docs/product/business-decision-prd-kpi-tree.md`

Upstream model spec:

1. `docs/architecture/data-model/m1-data-model-v1.md`

Upstream streaming execution spec:

1. `docs/architecture/streaming/spark-realtime-jobs-contract-m1.md`

## Business Decisions in Scope

1. `BOOST`: high momentum and quality-passing videos.
2. `REVIEW`: high momentum but quality-failing videos.
3. `RESCUE`: high-quality new videos with under-exposure.

M1 serving surface:

1. Trino semantic views and Metabase operations dashboard (health metrics + recommendation preview).

## Policy Priority

1. `BOOST`
2. `REVIEW`
3. `RESCUE`
4. `NO_ACTION`

## Rule Baseline (M1 Active)

1. `rule_version = rt_rules_v1`
2. Rolling window = 30 minutes
3. Core grain = `video_id + window_start` (1-minute event-time bucket)
4. Baseline registry table = `lakehouse.dims.rt_rule_quantile_baselines`
5. M1 published validity window = `effective_from = 2026-01-01`, `effective_to = 2099-12-31`
6. Baseline publish semantics = insert-only (immutable by `rule_version + effective_from`)
7. M1 threshold scope = global `p90` (`velocity_30m`) + global `p40` (`impressions_30m`)
8. Cohort (`category + region`) baseline and fallback semantics are deferred to future plan

## M3 Queue Reference

1. Queue execution semantics are outside M1 delivery scope and deferred to M3:
   - `m3-action-queue-reference.md`

## Spec Files

1. `metric-contract.md`
2. `reconciliation-and-slo.md`
3. `acceptance-criteria.md`
4. `m3-action-queue-reference.md`
