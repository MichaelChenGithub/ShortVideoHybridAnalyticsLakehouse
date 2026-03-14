# Reconciliation and SLO (M1)

## 1. Runtime SLA

1. Event-to-serving freshness latency target: `P95 < 3 minutes`
2. Freshness breach threshold: `> 3 minutes`

## 2. Freshness Response Policy (M1)

Trigger A:

1. freshness `P95 > 3m` for 5 consecutive minutes
2. behavior: mark serving status as degraded and require manual review before operational use

Trigger B:

1. freshness `> 10m` or realtime ingestion outage
2. behavior: mark serving status as stale and block sign-off until healthy again

Recovery:

1. require 15 consecutive healthy minutes before returning to normal mode

## 3. Late Data Policy

1. late events may update realtime facts and serving preview while within configured watermark policy
2. events beyond watermark are tracked through drop counters and observability logs
3. late-event impact is captured via reconciliation metrics and operational counters

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

1. `WARN` requires manual review before promoting new `rule_version`.
2. `CRIT` blocks new `rule_version` promotion until reconciliation returns to PASS.
3. Automated rollout blocking workflows are deferred to M3:
   - `docs/architecture/realtime-decisioning/m3-action-queue-reference.md`
