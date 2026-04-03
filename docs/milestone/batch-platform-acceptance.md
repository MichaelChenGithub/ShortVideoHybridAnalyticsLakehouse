# Acceptance Criteria: Batch Analytics, Semantic Serving, and Quality

Status: Final

## 1. Functional Coverage

1. Batch outputs provide retention, engagement, and sessionization metric coverage for analytics use.
2. Batch semantic serving views are queryable for daily analytics consumption:
   - `lakehouse.serving.v_bt_retention_daily`
   - `lakehouse.serving.v_bt_engagement_daily`
   - `lakehouse.serving.v_bt_sessionization_daily`
3. Batch outputs are consumable by BI without re-implementing metric formulas in dashboards.

## 2. Contract and Data Quality

1. Batch table grains are unique and contract-valid for all three output domains.
2. Required fields for batch semantic views are present and non-ambiguous.
3. Semantic/dbt quality checks are defined for core model constraints and business-critical fields.
4. `new_vs_returning_user` segmentation is present in batch outputs (`new`, `returning`, `unknown`).
5. `day_n` in retention outputs is restricted to `{1, 7}`.

## 3. Reliability and Publish Readiness

1. Batch publish readiness target remains daily by `08:00` (`America/New_York`) for `D-1` outputs.
2. Required batch output tables must exist and be non-empty for the publish date:
   - `lakehouse.gold.batch_retention_daily`
   - `lakehouse.gold.batch_engagement_daily`
   - `lakehouse.gold.batch_sessionization_daily`
3. Batch publish evidence includes `data_date`, `published_at`, and on-time status.

## 4. Cloud and Scale Evidence

1. Cloud baseline architecture is documented as `MSK + Spark + S3 + Glue + Trino/Athena + dbt Core`.
2. Scale benchmark runs produce artifacts that report throughput, data volume, and publish/freshness behavior.
3. Benchmark evidence is retained as release/readiness proof for portfolio and technical review.

## 5. Verification Coverage

1. Acceptance checks validate grain uniqueness, required-field coverage, and publish readiness.
2. Batch semantic quality evidence is captured alongside publish artifacts.
3. Verification includes batch serving output checks for retention/engagement/sessionization domains.
4. Verification artifacts are traceable by `data_date`.

## 6. Future Plan (Deferred)

1. canonical deferred-scope reference: `docs/milestone/future-plan.md`
