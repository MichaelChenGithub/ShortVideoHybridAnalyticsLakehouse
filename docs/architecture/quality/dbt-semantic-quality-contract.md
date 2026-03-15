# dbt Semantic Quality Contract

Status: Draft

## 1. Purpose

Define dbt-governed semantic data quality gates for batch outputs before publish-ready state.

## 2. Scope

In scope:

1. dbt model/test coverage for retention, engagement, and sessionization batch outputs
2. publish gate conditions tied to dbt test outcomes
3. failure-handling boundaries for manual operations in current scope

Out of scope:

1. automated notification routing
2. autonomous remediation and rollback workflows
3. policy-driven auto-block rollout orchestration

## 3. Model and Test Coverage Baseline

1. retention model coverage:
   - grain uniqueness (`cohort_date + day_n + category + region + new_vs_returning_user`)
   - required field non-null checks
   - `day_n` domain check (`{1,7}`)
2. engagement model coverage:
   - grain uniqueness (`data_date + category + region + new_vs_returning_user`)
   - required field non-null checks for core counts and rate outputs
   - denominator-safe rate checks
3. sessionization model coverage:
   - grain uniqueness (`data_date + category + region + new_vs_returning_user`)
   - required field non-null checks
   - metric sanity checks (`sessions >= 0`, duration non-negative)
4. segmentation governance:
   - `new_vs_returning_user` in (`new`, `returning`, `unknown`)

## 4. Publish Gate Contract

`publish-ready` for a `data_date` requires all of the following:

1. batch jobs complete successfully for required outputs
2. dbt semantic quality tests pass for all required models
3. required output tables are non-empty for `data_date`
4. publish manifest records successful publish for same `data_date`

If any gate fails, publish-ready signal must not be emitted.

## 5. Failure Handling (Manual in Current Scope)

1. quality gate failures require operator review and rerun/backfill workflow
2. rerun/backfill remains manual and must preserve idempotent partition behavior
3. late publish after `08:00` ET is allowed with explicit late status evidence

## 6. Evidence Requirements

Per publish date, retain:

1. dbt run summary
2. failing/passing test artifacts
3. publish manifest record (`data_date`, `published_at`, status, on-time flag)
4. traceable run identifier linked to orchestration logs

## 7. Future Plan Pointer

1. canonical deferred-scope reference: `../../milestone/future-plan.md`
