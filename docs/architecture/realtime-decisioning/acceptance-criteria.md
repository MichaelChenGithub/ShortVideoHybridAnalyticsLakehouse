# Acceptance Criteria (M1 + M2): Decision Preview, Batch Analytics, and Semantic Quality

## 1. Functional Coverage

1. Realtime serving views refresh on 1-minute cadence for recommendation preview workflows.
2. Decision mapping follows policy precedence: `BOOST > REVIEW > RESCUE > NO_ACTION`.
3. Preview artifacts expose deterministic latest recommendation per `video_id` in bounded windows.
4. Batch outputs provide retention, engagement, and sessionization metric coverage for analytics use.

## 2. Contract and Data Quality

1. Realtime metrics grain `video_id + window_start` is unique.
2. Decision-context grain `video_id + window_start` is unique.
3. Required serving fields are non-null for active preview rows.
4. `decision_type_preview` only in `BOOST`, `REVIEW`, `RESCUE`, `NO_ACTION`.
5. `rule_version` and threshold-traceability fields are present and queryable.
6. Semantic/dbt quality checks are defined for core model constraints and business-critical fields.

## 3. Reliability and Freshness

1. Event-to-preview freshness latency target remains `P95 < 3m`.
2. Freshness guardrails are monitored and surfaced for operations.
3. Batch publish readiness target remains daily by `08:00` (`America/New_York`) for D-1 outputs.
4. Bounded BI query windows and read-time protections are applied for dashboard and QA usage.

## 4. Cloud and Scale Evidence

1. Cloud baseline architecture is documented as `MSK + Spark + S3 + Glue + Trino/Athena + dbt Core`.
2. Scale benchmark runs produce artifacts that report throughput, data volume, and freshness behavior.
3. Benchmark evidence is retained as release/readiness proof for portfolio and technical review.

## 5. Verification Coverage

1. Acceptance checks validate grain uniqueness, null-rate controls, freshness lag, and decision-domain integrity.
2. Recommendation distribution snapshots are captured for sign-off context.
3. Deterministic replay checks confirm stable recommendation outcomes for fixed inputs.
4. Batch/semantic quality evidence is captured alongside realtime sign-off artifacts.

## 6. Future Plan (Deferred to M3)

1. T+1 reconciliation implementation and operationalization.
2. `rt_action_queue` execution and queue-consumer validation.
3. Automated degraded-mode switching and automated `WARN/CRIT` rollout blocking.
4. Canonical deferred-scope reference:
   - `docs/milestone/m3_scope.md`
