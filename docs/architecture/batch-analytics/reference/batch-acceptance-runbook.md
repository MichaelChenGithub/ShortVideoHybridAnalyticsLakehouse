# Batch Acceptance Runbook

Status: Final

## 1. Purpose

Provide execution steps and evidence requirements for batch analytics acceptance sign-off.

## 2. Preconditions

1. required batch contracts are up to date:
   - `../batch-metrics-contract.md`
   - `../batch-jobs-and-orchestration-contract.md`
   - `../../serving/trino-batch-semantic-serving-contract.md`
   - `../../quality/dbt-semantic-quality-contract.md`
2. batch pipelines and serving layer are queryable
3. verification environment can run SQL/dbt checks and collect artifacts

## 3. Execution Steps

1. execute batch orchestration for `data_date = D-1`
2. verify one run-scoped branch is created and used across the batch run
3. run dbt semantic quality checks for batch models on that branch
4. verify required batch gold outputs exist and are non-empty
5. verify serving views are queryable with required fields after branch promotion
6. verify `merge_coordinator` succeeds for the same `data_date`
7. record publish readiness status versus `08:00` ET target

## 4. Artifact Schema

Store acceptance evidence under:

1. `artifacts/batch_acceptance/<RUN_ID>/`

Required files (minimum):

1. `run_metadata.json`
2. `batch_table_counts.csv`
3. `dbt_test_summary.json`
4. `serving_contract_checks.csv`
5. `branch_promotion_summary.json`
6. `acceptance_summary.md`

## 5. Pass/Fail Gates

Pass conditions:

1. all required quality checks pass
2. required tables/views satisfy contract checks
3. branch promotion succeeds for `data_date`
4. evidence package is complete and traceable

Fail conditions:

1. any quality gate failure
2. missing/empty required output table or serving view contract violation
3. missing or failed branch promotion for `data_date`
4. incomplete or non-traceable artifact package

## 6. Sign-off Template

Acceptance summary must include:

1. run id and publish date
2. pass/fail outcome per gate
3. on-time/late determination
4. unresolved issues (if any)
5. operator/reviewer sign-off metadata

## 7. Future Plan Pointer

1. canonical deferred-scope reference: `../../../milestone/future-plan.md`
