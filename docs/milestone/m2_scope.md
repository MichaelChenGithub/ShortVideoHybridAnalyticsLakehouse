# Milestone 2 Scope (Current Through 2026-03-28)

## 1. Purpose

Define the single active scope narrative through M2 end date (`2026-03-28`).

This is the authoritative current-scope document for planning and documentation alignment.

## 2. In Scope (M2)

1. Batch metrics expansion:
   - retention metrics (`D1`, `D7` cohort outputs)
   - engagement metrics (daily KPI + lightweight funnel)
   - sessionization metrics (30-minute inactivity-gap session outputs)
   - minimum analysis cuts: `date x category x region` (plus `new_vs_returning_user` where available)
2. Semantic + dbt quality expansion:
   - semantic serving layer refinement for analytics usage
   - dbt model organization and quality checks
   - daily publish quality target for core semantic products: `>= 99%`
3. Cloud deployment + scale benchmark:
   - cloud baseline stack fixed to AWS:
     - `MSK + Spark + S3 + Glue + Trino/Athena + dbt Core`
   - benchmark-oriented evidence for data volume, throughput, and freshness behavior
   - scale targets:
     - sustained ingest `>= 5,000 events/sec`
     - peak ingest `>= 10,000 events/sec`
     - equivalent daily processed volume `>= 432M rows/day`

## 3. Out of Scope (M2, Deferred)

1. T+1 reconciliation implementation (deferred to M3).
2. Operational action queue execution and consumer automation (deferred to M3).
3. Automated degraded-mode switching and automated rollout blocking workflow (deferred to M3).
4. Broader optimization-only initiatives not required for feature delivery narrative.

## 4. M2 Definition of Done

1. Realtime preview and health metrics remain contract-valid with freshness `P95 <= 3 minutes`.
2. Batch analytics outcomes are published daily by `08:00` (`America/New_York`) for `D-1` data.
3. Batch metric outputs include `Retention (D1/D7)`, `Engagement (daily KPI + funnel)`, and `Sessionization (30-minute gap)`.
4. Core semantic products meet daily publish quality target `>= 99%`.
5. Cloud benchmark artifacts demonstrate ingest/volume targets (`>= 5,000 events/sec` sustained, `>= 10,000 events/sec` peak, `>= 432M rows/day`).
6. Cross-document wording is aligned to this through-M2 scope without conflicting milestone claims.
7. Deferred/conflicting items are centralized under `docs/milestone/m3_scope.md`.

## 5. Related Documents

1. `docs/milestone/m1_scope.md`
2. `docs/milestone/m3_scope.md`
3. `docs/product/business-decision-prd-kpi-tree.md`
4. `docs/architecture/batch/batch-metrics-contract-m2.md`
5. `docs/architecture/quality/dbt-semantic-quality-contract-m2.md`
6. `docs/architecture/cloud/aws-deployment-and-scale-benchmark-m2.md`
7. `README.md`
8. `docs/README.md`
