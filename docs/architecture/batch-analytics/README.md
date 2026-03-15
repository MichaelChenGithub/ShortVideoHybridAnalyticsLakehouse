# Batch Architecture

Status: Draft

## 1. Purpose

This module defines production-facing batch analytics contracts for the platform expansion scope.

## 2. Contract Reading Order

1. metric semantics: `batch-metrics-contract.md`
2. orchestration and publish readiness: `batch-jobs-and-orchestration-contract.md`
3. batch semantic serving interface: `../serving/trino-batch-semantic-serving-contract.md`
4. dbt semantic quality and publish gates: `../quality/dbt-semantic-quality-contract.md`
5. domain acceptance criteria: `acceptance-domain-batch.md`
6. execution runbook and evidence capture: `reference/batch-acceptance-runbook.md`

## 3. Contract Boundaries

1. Batch domains covered: retention (`D1`,`D7`), engagement KPI + lightweight funnel, and sessionization.
2. Publish target: `D-1` outputs ready by `08:00` (`America/New_York`).
3. Batch outputs are consumed via serving semantic views, not raw model re-derivation in BI.
4. Alerts/notifications and fully automated remediation are deferred to future plan.

## 4. Canonical Outputs

1. `lakehouse.gold.batch_retention_daily`
2. `lakehouse.gold.batch_engagement_daily`
3. `lakehouse.gold.batch_sessionization_daily`
4. `lakehouse.gold.batch_publish_manifest`

## 5. Future Plan Pointer

1. canonical deferred-scope reference: `../../milestone/future-plan.md`
