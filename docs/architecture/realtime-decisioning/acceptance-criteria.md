# Acceptance Criteria (M1): Realtime Decisioning and Current-State Queue

## 1. Functional

1. Action queue is generated every minute.
2. Decision mapping matches metric contract definitions.
3. Policy selection precedence is enforced: `BOOST > REVIEW > RESCUE > NO_ACTION`.
4. Conflict urgency precedence is enforced for same dedupe key: `RESCUE > REVIEW > BOOST`.

## 2. Contract and Data Quality

1. Realtime fact key `video_id + window_start` is unique.
2. Required queue fields are non-null, including `state`.
3. `decision_type` only in `BOOST`, `REVIEW`, `RESCUE`.
4. `state` only in `PENDING`, `ACKED`, `DONE`, `EXPIRED`, `HOLD`.
5. `expires_at > decided_at`.
6. Current-state queue uses upsert/update semantics (not append-only history table).

## 3. Queue Behavior and Reliability

1. Active queue filter is correct: `state in ('PENDING', 'ACKED') and expires_at > now`.
2. Cooldown rule (`1 action / video / 60m`) is enforced at producer and queue guard layers.
3. Compare-and-set semantics protect state transitions from race conditions.
4. Retry/backoff policy handles transient write failures.
5. Retry exhaustion routes actions to `HOLD`.

## 4. SLA and Freshness

1. Event-to-action latency `P95 < 3m`.
2. SLA targets are tracked for ack and execute timing.
3. TTL-SLA budget check is enforced before rollout.
4. Expiration behavior is correct: expired open actions transition to `EXPIRED`.

## 5. Reconciliation and Accuracy

1. Global RT vs T+1 reconciliation runs daily.
2. Count and rate errors are computed with agreed formulas.
3. Reconciliation thresholds are enforced.
4. Rule rollout is blocked under `WARN/CRIT` states.

## 6. Verification Coverage

1. Conflict race tests exist for same dedupe key multi-action collisions.
2. Cooldown guard tests verify queue-layer enforcement under producer imperfection.
3. State transition tests cover all allowed transitions of minimal state model.
4. Expiration tests validate open-to-expired transition correctness.
5. Throughput/performance tests validate active queue query latency on current-state table.
