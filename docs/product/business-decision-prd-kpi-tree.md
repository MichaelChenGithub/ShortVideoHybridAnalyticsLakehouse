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
Definition: governed semantic outputs include `Retention` (`D1`, `D7` cohort metrics), `Engagement` (daily KPI plus lightweight funnel), and `Sessionization` (30-minute inactivity-gap session metrics).
Coverage baseline: at minimum, batch outputs are analyzable by `date x category x region` (and optionally `new_vs_returning_user` where available).

5. `Semantic Quality Coverage`  
Definition: core semantic data products pass defined quality gates before daily publish for stable cross-team interpretation and reuse.
Target: daily publish availability for core semantic products `>= 99%`.
Implementation details: quality gate implementation is defined in `docs/architecture/quality/dbt-semantic-quality-contract-m2.md`.

6. `Cloud Scale Evidence`  
Definition: benchmark artifacts report supported throughput/volume and freshness behavior for business reporting continuity.
Target:
1. sustained ingest rate `>= 5,000 events/sec`
2. peak ingest rate `>= 10,000 events/sec`
3. equivalent daily processed volume `>= 432M rows/day`

### 6.3 Guardrail KPIs

1. `Realtime Freshness Breach`  
Definition: periods where realtime freshness exceeds policy thresholds.
Guardrail target: `P95 <= 3 minutes`

2. `Batch Completeness and Freshness`  
Definition: scheduled batch outputs satisfy completeness and freshness checks.
Guardrail target: `D-1` outputs are ready by `08:00` (`America/New_York`).

3. `Data Quality Gate Health`  
Definition: semantic/dbt quality checks pass for publishable outputs.
Guardrail target: daily publish availability for core semantic products `>= 99%`.

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

1. `RETENTION_METRICS`: `D1` and `D7` retention, shaped as `cohort_date x day_n x segment` (for example `region`, `category`, `new_vs_returning_user`) to identify fast-decay cohorts and higher-return-visit content patterns.
2. `ENGAGEMENT_METRICS`: daily KPI plus lightweight funnel (`impression -> play_start -> play_finish -> interaction`) with core counts and rates (`play_start_rate`, `completion_rate`, `interaction_rate`, `skip_rate`) to diagnose stage-level drop-off and segment-level quality mismatch.
3. `SESSIONIZATION_METRICS`: session outputs using 30-minute inactivity split (`sessions`, `sessions_per_user`, `avg_session_duration`, `events_per_session`, `watch_time_per_session`) to evaluate visit frequency, session depth, and stickiness changes.

Publish contract:
1. Batch outcomes are publishable data products (not queue actions).
2. Outputs must satisfy batch freshness/completeness and semantic/dbt quality gates before release.

## 8. Success Criteria (M1 + M2)

M1 + M2 scope is considered complete when:

1. realtime recommendation preview and health metrics are queryable and contract-valid, with realtime freshness `P95 <= 3 minutes`.
2. batch analytics outcomes are published daily by `08:00` (`America/New_York`) for `D-1` data.
3. batch metric coverage includes:
   - `Retention`: `D1` and `D7` cohort outputs
   - `Engagement`: daily KPI plus lightweight funnel outputs
   - `Sessionization`: 30-minute inactivity-gap session outputs
4. batch outputs are analyzable at minimum by `date x category x region` (and `new_vs_returning_user` where available).
5. core semantic data products meet daily publish availability target `>= 99%`.
6. cloud benchmark artifacts demonstrate ingest/volume targets (`>= 5,000 events/sec`, `>= 10,000 events/sec` peak, `>= 432M rows/day`) and are retained as delivery evidence.
7. platform outputs remain deterministic, auditable, and version-traceable.

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
7. `docs/architecture/batch-analytics/batch-metrics-contract-m2.md`
8. `docs/architecture/quality/dbt-semantic-quality-contract-m2.md`
9. `docs/architecture/cloud/aws-deployment-and-scale-benchmark-m2.md`
