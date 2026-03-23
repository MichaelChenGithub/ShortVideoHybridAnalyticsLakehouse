# Batch Architecture

Status: Final

## 1. Purpose

This module defines production-facing batch analytics contracts for the platform expansion scope.

## 2. Contract Reading Order

1. metric semantics: `batch-metrics-contract.md`
2. orchestration and publish readiness: `batch-jobs-and-orchestration-contract.md`
3. Airflow execution design on AWS: `airflow-batch-orchestration-spec.md`
4. batch semantic serving interface: `../serving/trino-batch-semantic-serving-contract.md`
5. dbt semantic quality and publish gates: `../quality/dbt-semantic-quality-contract.md`
6. domain acceptance criteria: `acceptance-domain-batch.md`
7. execution runbook and evidence capture: `reference/batch-acceptance-runbook.md`
8. local compose runtime for Airflow DAG loading: `local-airflow-dev-runtime.md`

## 3. Contract Boundaries

1. Batch domains covered: retention (`D1`,`D7`), engagement KPI + lightweight funnel, and sessionization.
2. Publish target: `D-1` outputs ready by `08:00` (`America/New_York`).
3. Airflow-specific orchestration behavior is defined separately from the scheduler-agnostic publish contract.
4. Batch outputs are consumed via serving semantic views, not raw model re-derivation in BI.
5. Fully automated remediation remains deferred to future plan.

## 4. Canonical Outputs

1. `lakehouse.gold.batch_retention_daily`
2. `lakehouse.gold.batch_engagement_daily`
3. `lakehouse.gold.batch_sessionization_daily`
4. `lakehouse.gold.batch_publish_manifest`

## 5. Future Plan Pointer

1. canonical deferred-scope reference: `../../milestone/future-plan.md`
