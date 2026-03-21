# Current Scope (Through 2026-03-28)

## 1. Purpose

Define the single active scope narrative through the current target end date (`2026-03-28`).

This is the authoritative current-scope document for planning and documentation alignment.

## 2. In Scope

1. Batch metrics expansion:
   - retention metrics (`D1`, `D7` cohort outputs)
   - engagement metrics (daily KPI + lightweight funnel)
   - sessionization metrics (30-minute inactivity-gap session outputs)
   - minimum analysis cuts: `date x category x region x new_vs_returning_user`
2. Batch orchestration operationalization on AWS:
   - `MWAA` as the batch scheduler/orchestrator
   - one daily Airflow DAG for `D-1` publish readiness
   - automated platform-team-owned execution flow
   - retries, email alerts, runbook evidence, and bounded operator-approved backfill (`<= 30 days`)
3. Semantic + dbt quality expansion:
   - semantic serving layer refinement for analytics usage
   - dbt model organization and quality checks
   - daily publish quality target for core semantic products: `>= 99%`
4. Cloud deployment + scale benchmark:
   - cloud baseline stack fixed to AWS:
     - `MSK + Spark + S3 + Glue + Trino/Athena + dbt Core + MWAA`
   - benchmark-oriented evidence for data volume, throughput, and freshness behavior
   - scale targets:
     - sustained ingest `>= 5,000 events/sec`
     - peak ingest `>= 10,000 events/sec`
     - equivalent daily processed volume `>= 432M rows/day`

## 3. Out of Scope
1. T+1 reconciliation implementation (deferred to future plan).
2. Operational action queue execution and consumer automation (deferred to future plan).
3. Automated degraded-mode switching and automated rollout blocking workflow (deferred to future plan).
4. Broader optimization-only initiatives not required for feature delivery narrative.

## 4. Definition of Done

1. Realtime preview and health metrics remain contract-valid with freshness `P95 <= 3 minutes`.
2. Batch analytics outcomes are published daily by `08:00` (`America/New_York`) for `D-1` data.
3. Batch metric outputs include `Retention (D1/D7)`, `Engagement (daily KPI + funnel)`, and `Sessionization (30-minute gap)`.
4. `MWAA` batch orchestration is production-operable with retries, email alerts, runbook evidence, and bounded operator-approved backfill.
5. Core semantic products meet daily publish quality target `>= 99%`.
6. Cloud benchmark artifacts demonstrate ingest/volume targets (`>= 5,000 events/sec` sustained, `>= 10,000 events/sec` peak, `>= 432M rows/day`).
7. Cross-document wording is aligned to this current scope without conflicting milestone claims.
8. Deferred/conflicting items are centralized under `docs/milestone/future-plan.md`.

## 5. Related Documents

1. `docs/milestone/delivered-scope.md`
2. `docs/milestone/future-plan.md`
3. `docs/product/business-decision-prd-kpi-tree.md`
4. `docs/architecture/realtime-decisioning/metric-contract.md`
5. `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`
6. `docs/architecture/realtime-decisioning/acceptance-domain-realtime.md`
7. `docs/milestone/realtime-platform-acceptance.md`
8. `docs/architecture/streaming/spark-realtime-jobs-contract.md`
9. `docs/architecture/serving/trino-realtime-semantic-serving-contract.md`
10. `docs/architecture/data-model/data-model-contract.md`
11. `docs/architecture/batch-analytics/batch-metrics-contract.md`
12. `docs/architecture/batch-analytics/batch-jobs-and-orchestration-contract.md`
13. `docs/architecture/batch-analytics/airflow-batch-orchestration-spec.md`
14. `docs/architecture/serving/trino-batch-semantic-serving-contract.md`
15. `docs/architecture/quality/dbt-semantic-quality-contract.md`
16. `docs/architecture/batch-analytics/reference/batch-acceptance-runbook.md`
17. `docs/milestone/batch-platform-acceptance.md`
18. `docs/architecture/cloud/aws-deployment-and-scale-benchmark.md`
19. `README.md`
20. `docs/README.md`
