# Real-time Transactional Data Lakehouse

![Python](https://img.shields.io/static/v1?label=Python&message=3.10&color=3776AB&logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming-orange)
![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-Lakehouse-green)
![Kafka](https://img.shields.io/badge/Kafka-Event%20Streaming-black)
![Trino](https://img.shields.io/badge/Trino-Serving%20Layer-blueviolet)

Contract-driven realtime decisioning for short-video operations.  
This README is intentionally aligned to `docs/` contracts (source of truth for scope, KPI, and governance).

## Business Problem

Short-video operations need decision-ready signals quickly after upload to:

1. Amplify high-potential, high-quality videos.
2. Flag high-momentum but risky/low-quality videos for review.
3. Rescue high-quality new uploads that are under-exposed.

Without a governed decision product, actions are delayed, inconsistent, and hard to audit.

## Project Goal (Milestone 1)

Deliver a realtime decisioning vertical slice that emits actionable `BOOST`, `REVIEW`, and `RESCUE` recommendations within latency and quality constraints.

Delivery path:

`content_events -> Kafka -> Spark RT -> Iceberg RT fact -> Trino views -> rt_action_queue`

## Decision Consumers and Cadence

1. `Content Ops`: consume `BOOST` candidates every minute.
2. `Trust & Safety Ops`: consume `REVIEW` actions every minute.
3. `Creator Ops`: consume `RESCUE` actions every 5 minutes.

## Expected Business Impact

1. Reduce time-to-distribution for high-potential content via faster `BOOST` decisions.
2. Reduce unsafe amplification by routing risky high-momentum content to `REVIEW` earlier.
3. Improve creator-side fairness by rescuing high-quality but under-exposed new uploads.
4. Reduce decision variance across ops teams with one governed realtime action interface.

## Business Impact Model (KPI Tree)

North star:

`Decision-ready quality at low latency`

Driver KPIs (M1 targets):

1. `Decision Latency (P95) < 3 minutes`
2. `Boost Precision (simulation-backed) >= 0.75`
3. `Rescue Success Rate (simulation-backed) >= 0.70`

Guardrails:

1. Realtime freshness breaches trigger degraded mode.
2. RT vs T+1 reconciliation error stays within thresholds:
   - count p95 `<= 0.08`
   - rate p95 absolute diff `<= 0.03`
3. False Suppression Rate (simulation-backed) target `<= 0.10`.

## High-Level Architecture

![Data Flow](design_doc/dataflow_diagram.png)

```text
content_events + cdc.content.videos
        -> Kafka contracts
        -> Spark Structured Streaming
        -> Iceberg tables (bronze / dims / gold)
        -> Trino semantic serving
        -> lakehouse.gold.rt_action_queue
```

## Decision Contract and Governance

1. Action queue is append-only and stores actionable outcomes only (`BOOST`, `REVIEW`, `RESCUE`).
2. Priority order is fixed: `BOOST > REVIEW > RESCUE > NO_ACTION`.
3. Cooldown is enforced: max 1 action per `video_id` per 60 minutes.
4. Expiration is explicit:
   - `BOOST`: `+15m`
   - `REVIEW`: `+30m`
   - `RESCUE`: `+30m`
5. Every action includes `rule_version` and `reason_codes` for auditability.

## Reliability Controls

1. SLA target: event-to-action latency `P95 < 3 minutes`.
2. Degraded mode:
   - freshness `P95 > 3m` for 5 minutes: keep `REVIEW` only
   - freshness `> 10m` or ingestion outage: pause all actions
3. Recovery requires 15 consecutive healthy minutes.
4. Late data does not rewrite historical actions; impact is handled via reconciliation and audit logs.
5. Rule rollout is gated by reconciliation health (`WARN`/`CRIT` freeze behavior).

## Scope Boundaries (M1)

In scope:

1. Realtime decisioning for `BOOST`, `REVIEW`, `RESCUE`
2. Action queue contract and expiration handling
3. Global RT vs T+1 reconciliation baseline
4. Rule versioning and degraded mode behavior

Out of scope:

1. Full batch decision system expansion
2. Segment-level reconciliation beyond global baseline
3. Automated policy optimization loop in production

## Milestone Acceptance Definition

M1 is considered successful when:

1. Action queue is generated every minute and operationally usable.
2. Decision logic matches metric and policy contracts.
3. Data contract constraints hold (key uniqueness, non-null required fields, valid decision types, cooldown, valid reason codes).
4. SLA and degraded-mode behavior meet contract.
5. Actions are deterministic and immutable, with auditable `rule_version`.

## Documentation Map (Source of Truth)

1. [Docs Overview](docs/README.md)
2. [Business Decision PRD & KPI Tree (M1)](docs/product/business-decision-prd-kpi-tree.md)
3. [Realtime Decisioning Contracts](docs/architecture/realtime-decisioning/README.md)
4. [Metric Contract](docs/architecture/realtime-decisioning/metric-contract.md)
5. [Action Queue Contract](docs/architecture/realtime-decisioning/action-queue-contract.md)
6. [Reconciliation and SLO](docs/architecture/realtime-decisioning/reconciliation-and-slo.md)
7. [Acceptance Criteria (M1)](docs/architecture/realtime-decisioning/acceptance-criteria.md)
8. [Streaming Execution Contract](docs/architecture/streaming/spark-realtime-jobs-contract-m1.md)
9. [Kafka Contract](docs/architecture/messaging/kafka-topic-schema-retention-contract-m1.md)
10. [Data Model Contract](docs/architecture/data-model/m1-data-model-v1.md)
11. [Generator Contract and Scenario Matrix](docs/architecture/generator/mock-event-generator-contract-scenario-matrix-m1.md)

Note:

1. Simulation-backed KPIs are reported as simulation evidence, not causal proof of production lift.
