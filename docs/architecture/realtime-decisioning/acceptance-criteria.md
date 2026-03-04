# Acceptance Criteria (M1): Realtime Decisioning

## 1. Functional

1. Action queue is generated every minute.
2. Decision priority is enforced: `BOOST > REVIEW > RESCUE > NO_ACTION`.
3. Policy logic matches metric contract definitions.

## 2. Contract and Data Quality

1. Realtime fact key `video_id + window_start` is unique.
2. Required action queue fields are non-null.
3. `decision_type` only in `BOOST`, `REVIEW`, `RESCUE`.
4. `expires_at > decided_at`.
5. Cooldown rule (`1 action / video / 60m`) is enforced.
6. `reason_codes` are present and valid for each action type.

## 3. SLA and Reliability

1. Event-to-action latency `P95 < 3m`.
2. Degraded mode triggers behave exactly as specified.
3. Recovery from degraded mode requires 15 healthy consecutive minutes.

## 4. Reconciliation and Accuracy

1. Global RT vs T+1 reconciliation runs daily.
2. Count and rate errors are computed with agreed formulas.
3. Reconciliation thresholds are enforced.
4. Rule rollout is blocked under `WARN/CRIT` states.

## 5. Governance and Auditability

1. Every action row includes `rule_version`.
2. Same input snapshot produces deterministic action output.
3. Historical actions are immutable (no retroactive rewrite).

