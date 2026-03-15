# Real-time Transactional Data Lakehouse

![Python](https://img.shields.io/static/v1?label=Python&message=3.10&color=3776AB&logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-Structured%20Streaming-orange)
![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-Lakehouse-green)
![Kafka](https://img.shields.io/badge/Kafka-Event%20Streaming-black)
![Trino](https://img.shields.io/badge/Trino-Serving%20Layer-blueviolet)

Contract-driven data platform for short-video operations across realtime and batch analytics layers.  
This README is aligned to `docs/` contracts and milestone scope docs (source of truth for scope, KPI, and governance).

## Business Problem

Short-video operations need decision-ready signals quickly after upload to:

1. Amplify high-potential, high-quality videos.
2. Flag high-momentum but risky/low-quality videos for review.
3. Rescue high-quality new uploads that are under-exposed.

At the same time, analytics stakeholders need trustworthy batch metrics to evaluate retention, engagement, and session behavior over longer horizons.

Without a governed realtime + batch platform, actions are delayed, analytics are fragmented, and decisions are hard to audit end-to-end.

## Project Goal (M1 + M2)

Deliver a portfolio-ready analytics platform narrative that includes:

1. Realtime decision preview path (`BOOST`, `REVIEW`, `RESCUE`) with auditable contracts.
2. Batch analytics expansion (`retention`, `engagement`, `sessionization`).
3. Semantic + dbt quality layer suitable for analytics consumers.
4. Cloud deployment and scale benchmark evidence.

Realtime delivery path baseline:

`generator -> Kafka -> Spark RT -> Iceberg Gold -> Trino semantic views -> BI dashboard`

## Decision Consumers and Cadence

1. `Content Ops`: consume `BOOST` candidates every minute.
2. `Trust & Safety Ops`: consume `REVIEW` actions every minute.
3. `Creator Ops`: consume `RESCUE` actions every 5 minutes.
4. `Analytics and BI`: consume retention/engagement/sessionization metrics on daily batch cadence.
5. `Product and Strategy`: review semantic KPI outputs for planning, prioritization, and performance analysis.

## Expected Business Impact

1. Reduce time-to-distribution for high-potential content via faster `BOOST` decisions.
2. Reduce unsafe amplification by routing risky high-momentum content to `REVIEW` earlier.
3. Improve creator-side fairness by rescuing high-quality but under-exposed new uploads.
4. Reduce decision variance across ops teams with one governed realtime decision-preview interface.
5. Improve cross-team analytics consistency with standardized batch metric definitions and semantic fields.
6. Speed up analysis cycles by providing trusted, testable data products for operational and business reporting.

## Platform Capabilities (M1 + M2)

1. Realtime decision preview path:
   - executable and auditable flow from generator/Kafka/Spark to Gold/Trino/BI
   - deterministic recommendation preview for `BOOST`, `REVIEW`, `RESCUE`
2. Batch analytics expansion:
   - retention, engagement, and sessionization metric coverage
   - analytics-ready metric serving for business and ops analysis
3. Semantic + dbt quality layer:
   - stable semantic fields for BI consumers
   - dbt-based quality controls for model integrity and trust
4. Cloud deployment and scale validation:
   - AWS baseline stack (`MSK + Spark + S3 + Glue + Trino/Athena + dbt Core`)
   - benchmark evidence for data volume, throughput, and freshness behavior

## Business Impact Model (KPI Tree)

North star:

`Decision-ready operations with analytics-grade trust`

Driver KPIs:

1. `Decision Latency (P95) < 3 minutes`
2. `Boost Precision (simulation-backed) >= 0.75`
3. `Rescue Success Rate (simulation-backed) >= 0.70`
4. `Batch Metric Coverage`: retention/engagement/sessionization are available in governed semantic outputs.
5. `Semantic Quality Coverage`: dbt tests cover core model constraints and business-critical fields.
6. `Cloud Scale Evidence`: benchmark artifacts report supported throughput/volume profile.

Guardrails:

1. Realtime freshness breaches trigger degraded-status handling and manual review.
2. Batch outputs must pass completeness and freshness checks for scheduled deliveries.
3. Data quality gates are enforced through semantic/dbt test coverage.
4. Cloud benchmark artifacts report throughput, volume, and freshness behavior under scale tests.
5. False Suppression Rate (simulation-backed) target `<= 0.10`.

## High-Level Architecture

![Data Flow](docs/dataflow_diagram.png)

```text
content_events + cdc.content.videos
        -> Kafka contracts
        -> Spark Structured Streaming
        -> Iceberg tables (bronze / dims / gold)
        -> Trino semantic serving
        -> Metabase operations dashboard (health metrics + recommendation preview)
```

## Deferred Scope (M3 Reference)

1. Operational action-queue execution is deferred to M3.
2. T+1 reconciliation implementation is deferred to M3.
3. Canonical future plan:
   - `docs/milestone/m3_scope.md`

## Reliability Controls

1. SLA target: event-to-preview freshness latency `P95 < 3 minutes`.
2. Freshness response:
   - freshness `P95 > 3m` for 5 minutes: mark serving status as degraded and require manual review before operational use
   - freshness `> 10m` or ingestion outage: mark serving status as stale and block sign-off until healthy again
3. Batch reliability controls:
   - scheduled batch runs must complete with published freshness/completeness expectations
   - semantic/dbt quality checks must pass before publishing batch-derived outputs
4. Recovery requires sustained healthy windows before promotion.
5. Late-event impact is tracked via watermark/drop counters and quality monitoring outputs.
6. Release guard:
   - `WARN`: manual review required before promoting new `rule_version`
   - `CRIT`: block promotion until freshness/quality checks return to healthy
   - automated blocking workflow is deferred to M3

## Scope Boundaries (M1 + M2)

In scope:

1. Realtime decision preview for `BOOST`, `REVIEW`, `RESCUE`
2. Batch metrics expansion: `retention`, `engagement`, `sessionization`
3. Semantic serving contracts and dbt quality layer
4. AWS cloud deployment and scale benchmark artifacts (`MSK + Spark + S3 + Glue + Trino/Athena + dbt Core`)
5. Rule version traceability and freshness-response observability

Out of scope:

1. T+1 reconciliation implementation and operationalization
2. Operational `rt_action_queue` execution and queue-consumer automation
3. Automated policy optimization loop in production
4. Automated degraded-mode switching and automated rollout blocking workflow

## Platform Completion Criteria (M1 + M2)

M2 scope is considered complete when:

1. Semantic serving views, health metrics, and recommendation preview are generated and queryable on 1-minute cadence.
2. Batch metrics for retention/engagement/sessionization are implemented and documented for analytics consumption.
3. Semantic + dbt quality checks are documented and operationally testable.
4. Cloud benchmark evidence is available for resume/interview storytelling.
5. Realtime recommendations remain deterministic and auditable with explicit `rule_version` and threshold context.

## Documentation Map (Source of Truth)

1. [Docs Overview](docs/README.md)
2. [Milestone 2 Scope (platform scope anchor)](docs/milestone/m2_scope.md)
3. [Milestone 3 Scope (future plan anchor)](docs/milestone/m3_scope.md)
4. [Milestone 1 Scope (delivered reference)](docs/milestone/m1_scope.md)
5. [Business Decision PRD & KPI Tree](docs/product/business-decision-prd-kpi-tree.md)
6. [Realtime Decisioning Contracts](docs/architecture/realtime-decisioning/README.md)
7. [Metric Contract](docs/architecture/realtime-decisioning/metric-contract.md)
8. [Acceptance Criteria](docs/architecture/realtime-decisioning/acceptance-criteria.md)
9. [Streaming Execution Contract](docs/architecture/streaming/spark-realtime-jobs-contract-m1.md)
10. [Kafka Contract](docs/architecture/messaging/kafka-topic-schema-retention-contract-m1.md)
11. [Data Model Contract](docs/architecture/data-model/m1-data-model-v1.md)
12. [Trino Semantic Layer and Serving Contract](docs/architecture/serving/trino-semantic-layer-serving-contract-m1-s2.md)
13. [Generator Contract and Scenario Matrix](docs/architecture/generator/mock-event-generator-contract-scenario-matrix-m1.md)

Note:

1. Simulation-backed KPIs are reported as simulation evidence, not causal proof of production lift.
