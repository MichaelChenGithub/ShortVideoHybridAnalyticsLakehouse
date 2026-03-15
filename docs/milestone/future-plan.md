# Future Plan
## 1. Purpose

Canonical future-plan document for all deferred items from delivered and current scope.

When other docs mention deferred or conflicting scope, they should point here.

## 2. Deferred from Delivered/Current Scope

1. T+1 reconciliation implementation and operationalization.
2. Operational action queue execution model:
   - `rt_action_queue` current-state lifecycle
   - queue-serving views and consumer automation
3. Automated degraded-mode action switching.
4. Automated rollout blocking tied to reconciliation states (`WARN` / `CRIT`).
5. Automated notification mechanisms for freshness, batch readiness, and quality breaches.
6. Policy-driven release-guard automation replacing manual review workflows.
7. Explicit late-data monitoring and watermark-drop observability instrumentation.

## 3. Future Expansion Candidates

1. Segment/cohort-level reconciliation and policy controls.
2. Expanded reliability automation and operational guardrails.
3. Additional optimization and platform-hardening initiatives after M2 feature delivery.

## 4. Input References

1. `docs/architecture/realtime-decisioning/action-queue-future-plan.md`
2. `docs/architecture/realtime-decisioning/action-queue-contract.md`
3. `docs/architecture/realtime-decisioning/realtime-action-queue-decision-behavior-spec.md`
4. `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`
5. `docs/milestone/current-scope.md`
