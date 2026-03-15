# Domain Acceptance Criteria: Realtime Decisioning

Status: Draft

## 1. Functional Coverage

1. Realtime serving views refresh on 1-minute cadence for recommendation preview workflows.
2. Decision mapping follows policy precedence: `BOOST > REVIEW > RESCUE > NO_ACTION`.
3. Preview artifacts expose deterministic latest recommendation per `video_id` in bounded windows.

## 2. Contract and Data Quality

1. Realtime metrics grain `video_id + window_start` is unique.
2. Decision-context grain `video_id + window_start` is unique.
3. Required serving fields are non-null for active preview rows.
4. `decision_type_preview` domain is limited to `BOOST`, `REVIEW`, `RESCUE`, `NO_ACTION`.
5. `rule_version` and threshold-traceability fields are present and queryable.

## 3. Reliability and Freshness

1. Event-to-preview freshness latency target remains `P95 < 3m`.
2. Freshness guardrails are monitored and surfaced for operations.
3. Bounded BI query windows and read-time protections are applied for dashboard and QA usage.

## 4. Verification Coverage

1. Acceptance checks validate grain uniqueness, null-rate controls, freshness lag, and decision-domain integrity.
2. Recommendation distribution snapshots are captured for sign-off context.
3. Deterministic replay checks confirm stable recommendation outcomes for fixed inputs.

## 5. Future Plan (Deferred)

1. `rt_action_queue` execution and queue-consumer validation.
2. Automated degraded-mode switching and automated `WARN/CRIT` rollout blocking.
3. Canonical deferred-scope reference:
   - `docs/milestone/future-plan.md`
