# Batch Jobs and Orchestration Contract

Status: Final

## 1. Purpose

Define orchestration contracts for current batch analytics delivery:

1. job dependency order
2. daily schedule and publish readiness target
3. quality-gated publish behavior
4. rerun and failure-handling boundaries

## 2. Scope

In scope:

1. batch DAG order for retention/engagement/sessionization outputs
2. publish readiness workflow for `D-1` outputs
3. handoff conditions to semantic serving/BI consumption

Out of scope (deferred):

1. automated notification routing
2. full reconciliation automation
3. autonomous remediation workflows

## 3. Canonical Job Dependency Order

1. build `lakehouse.silver.events_conformed`
2. build `lakehouse.silver.user_activity_sessions_30m`
3. build `lakehouse.gold.batch_retention_daily`
4. build `lakehouse.gold.batch_engagement_daily`
5. build `lakehouse.gold.batch_sessionization_daily`
6. run dbt/data quality gates
7. emit publish-ready signal for semantic serving/BI use

## 4. Schedule and Data-Date Contract

1. batch cadence is daily.
2. target publish slice is `D-1` (`America/New_York` business date).
3. publish-readiness deadline is `08:00` (`America/New_York`) for `D-1` outputs.
4. all orchestrated jobs and quality gates must complete before publish-ready emission.
5. scheduler implementation detail (Airflow/Cron/other) is implementation-specific, but SLA contract is fixed by this document.

## 5. Publish-Ready Gate Contract

`publish-ready` is valid only if all conditions hold for the same `data_date`:

1. all upstream jobs in Section 3 succeed.
2. dbt/semantic quality gates pass.
3. required gold outputs exist and are non-empty:
   - `lakehouse.gold.batch_retention_daily`
   - `lakehouse.gold.batch_engagement_daily`
   - `lakehouse.gold.batch_sessionization_daily`

If any condition fails, publish-ready signal must not be emitted.

## 6. Retry, Rerun, and Backfill Boundaries

1. retries are allowed inside the same daily run window to recover transient failures.
2. rerun for the same `data_date` must be idempotent at table-partition grain (no duplicate published slice).
3. rerun/backfill is operator-triggered (manual in current scope).
4. a rerun completed after `08:00` (`America/New_York`) can still publish but is marked late by manifest evidence.
5. automated remediation workflows are deferred to future plan.

## 7. Runtime Evidence Requirements

Per publish date, orchestration must leave traceable evidence:

1. `data_date`
2. `published_at` timestamp
3. publish status (`success`/`failed`)
4. on-time status versus `08:00` (`America/New_York`) deadline
5. run identifier for traceability to job logs/artifacts

## 8. Future Plan (Deferred)

1. automated notification routing for publish failures/SLA misses
2. autonomous remediation/recovery workflows
3. deeper reconciliation orchestration coupled with policy-gated release flow
