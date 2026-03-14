# Acceptance Criteria (M1): Realtime Decisioning and Recommendation Preview

## 1. Functional

1. Serving views refresh on 1-minute cadence for recommendation preview workflows.
2. Decision mapping matches metric contract definitions.
3. Policy selection precedence is enforced: `BOOST > REVIEW > RESCUE > NO_ACTION`.
4. Preview artifacts expose deterministic latest recommendation per `video_id` in bounded windows.

## 2. Contract and Data Quality

1. Realtime metrics grain `video_id + window_start` is unique.
2. Decision-context grain `video_id + window_start` is unique.
3. Required serving fields are non-null for active preview rows.
4. `decision_type_preview` only in `BOOST`, `REVIEW`, `RESCUE`, `NO_ACTION`.
5. `rule_version` and threshold-traceability fields are present and queryable.
6. Row amplification guard between metrics and decision-context views stays within contract bounds (`row_delta = 0`).

## 3. Serving Reliability and Freshness

1. Event-to-preview freshness latency `P95 < 3m`.
2. Freshness guardrails are monitored and surfaced for operations.
3. Freshness-breach detection is validated via serving lag checks and verifier outputs.
4. Bounded BI query windows and read-time protections are applied for dashboard and QA usage.

## 4. Reconciliation and Accuracy

1. Global RT vs T+1 reconciliation runs daily.
2. Count and rate errors are computed with agreed formulas.
3. Reconciliation thresholds are enforced.
4. Reconciliation status is recorded for manual release gating.

## 5. Verification Coverage

1. Serving acceptance checks validate grain uniqueness, null-rate controls, freshness lag, and decision-domain integrity.
2. Recommendation distribution snapshots are captured for sign-off context.
3. Deterministic replay checks confirm stable decision preview outcomes for fixed inputs.
4. `rt_action_queue` execution and consumer validations are deferred to M3.
5. Automated degraded-mode switching and automated `WARN/CRIT` rollout blocking are deferred to M3.
