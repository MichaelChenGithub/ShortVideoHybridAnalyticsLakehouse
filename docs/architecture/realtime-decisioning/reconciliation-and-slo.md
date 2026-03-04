# Reconciliation and SLO (M1)

## 1. Runtime SLA

1. Event-to-action latency target: `P95 < 3 minutes`
2. Freshness breach threshold: `> 3 minutes`

## 2. Degraded Mode

Trigger A:

1. freshness `P95 > 3m` for 5 consecutive minutes
2. behavior: pause `BOOST` and `RESCUE`, keep `REVIEW` only

Trigger B:

1. freshness `> 10m` or realtime ingestion outage
2. behavior: pause all actions

Recovery:

1. require 15 consecutive healthy minutes before returning to normal mode

## 3. Late Data Policy

1. action queue is immutable append-only
2. late events do not rewrite emitted actions
3. late-event impact is captured via reconciliation and audit logs

## 4. RT vs T+1 Reconciliation Scope

M1 scope:

1. global only (no segment-level reconciliation yet)
2. run once daily after T+1 batch completion

Metrics:

1. counts: `impressions`, `play_start`, `play_finish`, `likes`, `shares`, `skips`
2. rates: `completion_rate`, `skip_rate`

## 5. Error Formulas

Count global relative error:

```text
abs(sum_rt - sum_batch) / max(sum_batch, 1)
```

Count minute-level p95:

```text
p95(abs(rt_1m - batch_1m) / max(batch_1m, 100))
```

Rate minute-level p95 absolute error:

```text
p95(abs(rt_rate_1m - batch_rate_1m))
```

## 6. Alert Thresholds

1. PASS target:
   - count p95 <= 0.08
   - rate p95 absolute diff <= 0.03
2. WARN:
   - threshold near breach or one-metric soft breach
3. CRIT:
   - any hard breach above target threshold

## 7. Rule Rollout Guard

1. `WARN` freezes new `rule_version` rollout.
2. `CRIT` freezes rollout and enters conservative policy mode.

