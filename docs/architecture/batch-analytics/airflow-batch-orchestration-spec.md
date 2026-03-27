# Airflow Batch Orchestration Spec

Status: Draft

## 1. Purpose

Define the Airflow-specific orchestration design for current batch analytics delivery on AWS.

This document operationalizes the scheduler-agnostic contract in `batch-jobs-and-orchestration-contract.md` for a production-operable `MWAA` deployment.

## 2. Scope

In scope:

1. `MWAA` as the orchestrator for current batch analytics delivery
2. one daily Airflow DAG for `D-1` batch publish readiness
3. automated platform-team-owned execution flow
4. retries, timeout, concurrency, and SLA handling policies
5. email-only alerting for failure and SLA miss conditions
6. bounded operator-approved rerun and backfill workflow
7. evidence, auditability, and runbook expectations for production operation

Out of scope:

1. self-managed Airflow on `EKS` or `ECS`
2. multi-DAG domain decomposition
3. autonomous remediation or self-healing workflows
4. non-email notification channels such as paging integrations
5. arbitrary unbounded historical rebuilds

## 3. Contract Precedence

This document inherits and does not override the following documents:

1. `batch-jobs-and-orchestration-contract.md` for publish-readiness, `D-1`, and branch-promotion semantics
2. `../quality/dbt-semantic-quality-contract.md` for quality-gate requirements
3. `../serving/trino-batch-semantic-serving-contract.md` for serving handoff semantics

If this document conflicts with those contracts, the upstream contract remains authoritative.

## 4. DAG Topology

Current scope uses one Airflow DAG for the daily batch publish path.

The canonical task sequence is:

1. resolve `data_date` as `D-1` in `America/New_York`
2. create one run-scoped Iceberg branch from `main`
3. build `lakehouse.silver.events_conformed`
4. build `lakehouse.silver.user_activity_sessions_30m`
5. build `lakehouse.gold.batch_retention_daily`
6. build `lakehouse.gold.batch_engagement_daily`
7. build `lakehouse.gold.batch_sessionization_daily`
8. run dbt/data quality gates on the run branch
9. promote the run branch through `merge_coordinator`
10. emit publish-ready signal for semantic serving and BI use
11. clean up the run branch and close the Airflow run

Airflow implementation should use `TaskGroup`s to keep operator boundaries readable:

1. conformed-events
2. sessionization
3. batch-gold-metrics
4. quality-gates
5. publish-and-evidence

## 5. Schedule and Data-Date Mapping

1. the DAG runs daily on `MWAA`
2. the business timezone is fixed to `America/New_York`
3. the publish target remains `D-1` for the business date in `America/New_York`
4. the Airflow run must compute and pass one canonical `data_date` across all tasks
5. `catchup` must be disabled for the scheduled daily DAG
6. the `08:00` (`America/New_York`) readiness deadline remains fixed by the scheduler-agnostic contract

## 6. Runtime Policy

1. all contract-required tasks are hard-blocking for publish-ready emission
2. transient task failures may retry within the same run window
3. each task must define explicit timeout settings appropriate to its workload
4. the DAG must restrict concurrent active runs to avoid overlapping publish for the same `data_date`
5. the DAG must enforce idempotent behavior for rerun of the same `data_date`
6. publish-ready emission is forbidden if any upstream task or quality gate fails
7. all silver-to-gold writes for one DAG run must target one shared run branch rather than `main`
8. branch promotion is coordinated by Airflow task dependencies; it is not described as strict multi-table storage atomicity beyond the underlying Iceberg procedure guarantees

## 7. Retry and Failure Handling

1. retry policy is intended for transient infrastructure or dependency failures, not semantic contract violations
2. semantic or quality-gate failures must leave the run in failed state and must not publish
3. an `MWAA` run that completes after `08:00` (`America/New_York`) may still publish if contract gates pass, but the run artifacts/logs must record the late completion
4. failure handling must preserve enough traceability to map Airflow task failure to `dag_run_id` and run branch name
5. deferred scope does not include automatic remediation beyond configured retries

## 8. Alerts and Notifications

Current scope supports email notifications only.

Email notifications must trigger for:

1. DAG failure
2. task failure after retries are exhausted
3. SLA miss where publish completes after `08:00` (`America/New_York`)
4. branch creation, promotion, or cleanup failure

Paging, chat integrations, and workflow-driven escalation remain future extensions.

## 9. Rerun and Backfill Workflow

1. scheduled daily runs are automatic
2. reruns and backfills are operator-triggered by the platform team
3. backfill requires explicit `start_date`, `end_date`, and operator-entered reason
4. current-scope backfill window is bounded to at most `30` calendar days per request
5. each rerun or backfill execution must use a distinct run branch name derived from the Airflow run ID
6. rerun and backfill must remain idempotent at partition grain and must not create duplicate published slices
7. backfill success does not change the standing `08:00` daily SLA for scheduled runs

## 10. AWS Deployment Boundary

1. Airflow deployment target is `Amazon MWAA`
2. Airflow is part of the current AWS baseline stack alongside `MSK + Spark + S3 + Glue + Trino/Athena + dbt Core`
3. DAG code, runtime configuration, and environment references must be deployable through the AWS delivery path used by the platform team
4. secrets and connection material must be managed through AWS-compatible secure configuration mechanisms rather than hardcoded DAG values
5. this document defines orchestration behavior, not low-level infrastructure-as-code layout

## 11. Evidence and Observability

Per Airflow run, the platform must preserve traceable evidence for:

1. `dag_run_id`
2. run branch name
3. `data_date`
4. task-level success/failure status
5. `merge_coordinator` promotion status and on-time/late determination
6. links or references to execution logs and collected artifacts

Airflow evidence must be mappable to the acceptance artifact structure in `reference/batch-acceptance-runbook.md`.

## 12. Validation and Acceptance Mapping

Airflow implementation is accepted only when it demonstrates:

1. correct `D-1` date resolution in `America/New_York`
2. branch creation and branch-targeted execution for one shared run workspace
3. correct execution order matching the orchestration contract
4. blocked publish on any failed quality gate or missing required output
5. successful `merge_coordinator` promotion only after all upstream tasks succeed
6. email notification behavior for failure and SLA miss paths
7. operator-triggered rerun/backfill behavior within the `30`-day bound
8. evidence package sufficient for batch acceptance sign-off

## 13. Future Extension

Future plan candidates:

1. multi-DAG decomposition if batch domains require independent scaling or ownership
2. paging and chat-based operational alerts
3. broader historical rebuild policies after stronger capacity controls exist
4. automated remediation and policy-driven recovery actions
