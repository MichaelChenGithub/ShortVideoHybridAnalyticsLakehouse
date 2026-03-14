# Business Decision PRD & KPI Tree (M1)

## 1. Document Purpose

This PRD defines the top-level business objective for Milestone 1 and the KPI tree used to evaluate whether the realtime decisioning system is effective.

This document is the upstream source for technical contracts in:

1. `docs/architecture/realtime-decisioning/*`

---

## 2. Problem Statement

Short-video operations need decision-ready signals quickly after upload to:

1. amplify high-potential, high-quality videos
2. flag high-momentum but risky/low-quality videos for review
3. rescue high-quality new videos that are under-exposed

Without a consistent decision product, actions are delayed, inconsistent, and hard to audit.

---

## 3. Business Objective (M1)

Deliver a realtime decisioning vertical slice that produces actionable `BOOST`, `REVIEW`, and `RESCUE` recommendations within operational latency and quality constraints.

Delivery path:

1. `content_events -> Kafka -> Spark RT -> Iceberg RT fact -> Trino views -> Metabase operations dashboard (health metrics + recommendation preview)`

---

## 4. Users and Decision Cadence

1. `Content Ops`: consume `BOOST` candidates every minute.
2. `Trust & Safety Ops`: consume `REVIEW` actions every minute.
3. `Creator Ops`: consume `RESCUE` actions every 5 minutes.
4. `Ops Leads`: monitor platform health metrics (`avg_velocity_30m`, `avg_completion_rate_30m`, `avg_skip_rate_30m`, `active_videos`) every minute.

Decision serving SLA:

1. event to serving freshness for dashboard metrics and recommendation preview `P95 < 3 minutes`

---

## 5. In Scope and Out of Scope

### 5.1 In Scope (M1)

1. Realtime decisioning for `BOOST`, `REVIEW`, `RESCUE`
2. Trino semantic serving contract, recommendation preview traceability, and platform health/coverage dashboard metrics
3. Global RT vs T+1 reconciliation baseline
4. Rule version traceability and freshness-breach observability

### 5.2 Out of Scope (M1)

1. Full batch decision system expansion
2. Segment-level reconciliation beyond global baseline
3. Automated policy optimization loop in production
4. Operational `rt_action_queue` execution and queue-consumer automation (deferred to M3)

---

## 6. KPI Tree (M1)

### 6.1 North Star

`Decision-ready quality at low latency`

Interpretation:

1. recommendations are timely enough for operations
2. recommendations are reliable enough to drive action safely

### 6.2 Driver KPIs

1. `Decision Latency (P95)`  
Definition: time from event ingestion to recommendation-preview availability in serving views.  
Target: `< 3 minutes`.

2. `Boost Precision (simulation-backed)`  
Definition: share of `BOOST` actions that later meet success outcome criteria in simulation evaluation windows.  
Target: `>= 0.75` (M1 baseline).

3. `Rescue Success Rate (simulation-backed)`  
Definition: share of `RESCUE` actions that later demonstrate recovery outcome in simulation evaluation windows.  
Target: `>= 0.70` (M1 baseline).

### 6.3 Guardrail KPIs

1. `Realtime Freshness Breach`  
Definition: periods where freshness exceeds operational threshold.  
Guardrail: detect breaches and gate releases/operations with documented playbook response.

2. `RT vs T+1 Reconciliation Error`  
Definition: global mismatch between realtime and T+1 metrics.  
Guardrail: count p95 `<= 0.08`, rate p95 absolute diff `<= 0.03`.

3. `False Suppression Rate (simulation-backed)`  
Definition: share of actions that incorrectly suppress/review videos that later evaluate as high quality.  
Guardrail: keep below agreed policy limit (M1 baseline target `<= 0.10`).

---

## 7. Decision Outcomes

Actions emitted by M1:

1. `BOOST`: high momentum and quality-passing candidates
2. `REVIEW`: high momentum but quality-failing candidates
3. `RESCUE`: high-quality and under-exposed new uploads

Priority order:

1. `BOOST > REVIEW > RESCUE > NO_ACTION`

---

## 8. Success Criteria for Milestone 1

M1 is successful when:

1. semantic serving views are refreshed every minute and recommendation preview is operationally usable
2. platform health and coverage metrics are queryable for operations (`avg_velocity_30m`, `avg_completion_rate_30m`, `avg_skip_rate_30m`, `active_videos`)
3. decision latency meets SLA (`P95 < 3m`)
4. KPI and guardrail metrics are measurable and tracked
5. rule changes are versioned and auditable
6. realtime contracts and acceptance criteria are met

---

## 9. Risks and Trade-offs

1. Simulation data does not prove real production business lift.  
Mitigation: report metrics as `simulation-backed`, avoid causal overclaims.

2. Low-latency decisions can drift from T+1 truth due to late data.  
Mitigation: reconciliation guardrail and manual rollout gating on failure states.

3. Aggressive boosting can increase false positives.  
Mitigation: quality gate and false suppression guardrail.

---

## 10. Linked Technical Contracts

1. `docs/architecture/realtime-decisioning/metric-contract.md`
2. `docs/architecture/realtime-decisioning/action-queue-contract.md` (M3 reference)
3. `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`
4. `docs/architecture/realtime-decisioning/acceptance-criteria.md`
