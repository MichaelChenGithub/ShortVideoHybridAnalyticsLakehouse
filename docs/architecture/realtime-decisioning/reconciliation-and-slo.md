# Reliability and SLO

## 1. Runtime SLO

### 1.1 Realtime SLO

1. Event-to-serving freshness latency target: `P95 < 3 minutes`.
2. Freshness breach threshold: `> 3 minutes`.

### 1.2 Batch SLO

1. Batch publish readiness target: daily by `08:00` (`America/New_York`) for `D-1` outputs.

## 2. Realtime Freshness Response Policy

Trigger A:

1. freshness `P95 > 3m` for 5 consecutive minutes
2. behavior: mark serving status as degraded and require manual review before operational use

Trigger B:

1. freshness `> 10m` or realtime ingestion outage
2. behavior: mark serving status as stale and block sign-off until healthy again

Recovery:

1. require sustained healthy windows before returning to normal release posture

## 3. Batch Publish Reliability Policy

1. scheduled batch runs must complete for `D-1` publish windows.
2. `retention`, `engagement`, and `sessionization` outputs must be ready by daily `08:00` (`America/New_York`).
3. batch freshness check: publishable outputs must represent `data_date = current_date - 1`.
4. batch completeness check: required output tables must exist and be non-empty for publish date:
   - `lakehouse.gold.batch_retention_daily`
   - `lakehouse.gold.batch_engagement_daily`
   - `lakehouse.gold.batch_sessionization_daily`
5. semantic/dbt quality checks must pass before publishing batch-derived outputs.
6. publish manifest check: `lakehouse.gold.batch_publish_manifest` must contain a successful publish record for the same `data_date`.
7. publish failures require manual operator review and rerun workflow before downstream use.

## 4. Realtime Baseline Reference (Historical)

1. Watermark handling exists in M1 streaming logic, but explicit late-data monitoring and watermark-drop observability instrumentation are not part of M1+M2 delivery.
2. Manual release-guard behavior (`WARN/CRIT` style operator review) is inherited as historical baseline, not an M2 delivery item.
3. Reference scope anchor:
   - `docs/milestone/delivered-scope.md`

## 5. Future Plan (Deferred)

1. T+1 reconciliation implementation and operationalization.
2. Reconciliation formulas, thresholds, and automated policy gating.
3. Automated degraded-mode switching tied to reconciliation states.
4. Automated notification mechanisms for freshness/batch/quality breaches.
5. Automated release-guard workflows and rollout blocking.
6. Explicit late-data monitoring and watermark-drop observability implementation.
7. Canonical deferred-scope reference:
   - `docs/milestone/future-plan.md`
