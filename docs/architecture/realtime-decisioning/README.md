# Realtime Decisioning (M1)

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

## Policy Priority

1. `BOOST`
2. `REVIEW`
3. `RESCUE`
4. `NO_ACTION`

## Rule Baseline

1. `rule_version = rt_rules_v1`
2. Rolling window = 30 minutes
3. Core grain = `video_id + window_start` (1-minute event-time bucket)
4. Action queue model = current-state table (upsert/update), not append-only history
5. Baseline registry table = `lakehouse.dims.rt_rule_quantile_baselines`
6. M1 published validity window = `effective_from = 2026-01-01`, `effective_to = 2099-12-31`
7. Baseline publish semantics = insert-only (immutable by `rule_version + effective_from`)
8. M1 threshold scope = global `p90` (`velocity_30m`) + global `p40` (`impressions_30m`)
9. Cohort (`category + region`) baseline and fallback semantics are deferred to future plan

## Spec Files

1. `metric-contract.md`
2. `action-queue-contract.md`
3. `reconciliation-and-slo.md`
4. `acceptance-criteria.md`
5. `realtime-action-queue-decision-behavior-spec.md`
