# Business Decision PRD & KPI Tree (M1 + M2 Scope)

## 1. Document Purpose

Define the business objective, KPI tree, and success criteria for the M1 + M2 platform scope.

This PRD is the upstream business contract for realtime decisioning, batch analytics expansion, semantic serving, and data-quality governance.

## 2. Problem Statement

Short-video operations and analytics teams need a unified platform that supports both:

1. low-latency decision preview for operational actions
2. analytics-grade batch metrics for longer-horizon performance analysis

Without a governed realtime + batch platform, operational response is delayed, analytics are inconsistent, and outcomes are hard to audit.

## 3. Business Objective (M1 + M2)

Deliver a coherent decision + analytics platform that provides:

1. realtime recommendation preview for `BOOST`, `REVIEW`, and `RESCUE`
2. batch metrics for `retention`, `engagement`, and `sessionization`
3. semantic + dbt quality controls for trusted downstream consumption
4. cloud deployment and scale benchmark evidence for production-style readiness

## 4. Users and Decision/Analysis Cadence

1. `Content Ops`: consume `BOOST` candidates every minute.
2. `Trust & Safety Ops`: consume `REVIEW` candidates every minute.
3. `Creator Ops`: consume `RESCUE` candidates every 5 minutes.
4. `Analytics and BI`: consume retention/engagement/sessionization outputs on daily batch cadence.
5. `Product and Strategy`: use semantic KPI outputs for planning and performance review.

## 5. In Scope and Out of Scope

### 5.1 In Scope (M1 + M2)

1. Realtime decision preview (`BOOST`, `REVIEW`, `RESCUE`) and health metrics.
2. Batch metric expansion for retention, engagement, and sessionization.
3. Semantic serving contracts and dbt quality/test coverage.
4. Cloud baseline deployment and scale benchmark evidence.
5. Rule version traceability and freshness-response observability.

### 5.2 Out of Scope (Deferred to M3)

1. T+1 reconciliation implementation and operationalization.
2. Operational `rt_action_queue` execution and queue-consumer automation.
3. Automated degraded-mode switching and automated rollout blocking workflow.
4. Full autonomous policy optimization loop in production.

## 6. KPI Tree (M1 + M2)

### 6.1 North Star

`Decision-ready operations with analytics-grade trust`

### 6.2 Driver KPIs

1. `Decision Latency (P95)`  
Definition: event-to-preview latency in serving views.  
Target: `< 3 minutes`.

2. `Boost Precision (simulation-backed)`  
Definition: share of `BOOST` previews that later meet success outcome criteria in simulation windows.  
Target: `>= 0.75`.

3. `Rescue Success Rate (simulation-backed)`  
Definition: share of `RESCUE` previews that later show recovery outcomes in simulation windows.  
Target: `>= 0.70`.

4. `Batch Metric Coverage`  
Definition: retention/engagement/sessionization outputs are published in governed semantic surfaces.

5. `Semantic Quality Coverage`  
Definition: dbt quality checks cover core model constraints and business-critical fields.

6. `Cloud Scale Evidence`  
Definition: benchmark artifacts report supported throughput/volume and freshness behavior.

### 6.3 Guardrail KPIs

1. `Realtime Freshness Breach`  
Definition: periods where realtime freshness exceeds policy thresholds.

2. `Batch Completeness and Freshness`  
Definition: scheduled batch outputs satisfy completeness and freshness checks.

3. `Data Quality Gate Health`  
Definition: semantic/dbt quality checks pass for publishable outputs.

4. `False Suppression Rate (simulation-backed)`  
Definition: share of recommendations that suppress content later evaluated as high quality.  
Guardrail target: `<= 0.10`.

## 7. Outcomes (Realtime + Batch)

### 7.1 Operational Decision Outcomes (Realtime)

1. `BOOST`: high momentum and quality-passing candidates
2. `REVIEW`: high momentum but quality-failing candidates
3. `RESCUE`: high-quality new uploads with under-exposure

Priority order:
1. `BOOST > REVIEW > RESCUE > NO_ACTION`

### 7.2 Analytics Outcomes (Batch)

1. `RETENTION_METRICS`: D1/D7 retention outputs for cohort/trend analysis
2. `ENGAGEMENT_METRICS`: watch/completion/skip/interaction aggregates for performance diagnosis
3. `SESSIONIZATION_METRICS`: session-level behavior outputs for usage pattern analysis

Publish contract:
1. Batch outcomes are publishable data products (not queue actions).
2. Outputs must satisfy batch freshness/completeness and semantic/dbt quality gates before release.

## 8. Success Criteria (M1 + M2)

M1 + M2 scope is considered complete when:

1. realtime recommendation preview and health metrics are queryable and contract-valid
2. batch retention/engagement/sessionization metrics are implemented and documented
3. semantic + dbt quality workflows are defined and testable
4. cloud benchmark artifacts are available for delivery evidence and portfolio storytelling
5. platform outputs remain deterministic, auditable, and version-traceable

## 9. Risks and Trade-offs

1. Simulation evidence does not prove production causal lift.  
Mitigation: report outcome KPIs as simulation-backed and avoid overclaiming causality.

2. Batch expansion can increase semantic complexity and drift risk.  
Mitigation: enforce semantic contracts and dbt quality coverage before publish.

3. Scale benchmarks can expose infra bottlenecks before feature completeness.  
Mitigation: preserve benchmark artifacts and tune incrementally by measured bottlenecks.

4. Deferred M3 items may be requested early by stakeholders.  
Mitigation: keep deferred scope centralized and explicit in future-plan references.

## 10. Linked Contracts and Scope Anchors

1. `docs/milestone/m1_scope.md`
2. `docs/milestone/m2_scope.md`
3. `docs/milestone/m3_scope.md`
4. `docs/architecture/realtime-decisioning/metric-contract.md`
5. `docs/architecture/realtime-decisioning/acceptance-criteria.md`
6. `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`
