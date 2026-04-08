# Documentation Architecture (v2)

This `docs/` tree is the new documentation layout for production-style specs.

Legacy docs remain in `legacy_docs/` and are intentionally untouched during this phase.

## Goals

1. Keep business decisions, metric contracts, and acceptance criteria separate.
2. Make docs easy to review, version, and audit.
3. Present one coherent delivered/current platform story for delivery and communication.

## Current Scope

Delivered + current platform scope:

1. Delivered scope is captured as reference.
2. Current scope covers batch metrics expansion, semantic + dbt quality, and cloud deployment + scale benchmark.
3. Deferred/conflicting items are centralized in future plan.

## Big Picture

This documentation set covers end-to-end platform capabilities and impact:

1. Realtime decision preview path that is executable and auditable.
2. Batch metrics for retention, engagement, and sessionization to support analytics-grade use cases.
3. Semantic serving and dbt quality controls to improve trust and reuse across BI/analytics workloads.
4. Cloud deployment and scale benchmark evidence for production-style readiness and portfolio storytelling.

## Folder Structure

1. `architecture/`
   - Long-lived architecture and domain-level design specs.
2. `architecture/realtime-decisioning/`
   - Realtime metric definitions, policy rules, reconciliation, and acceptance criteria.
3. `architecture/data-model/`
   - Core model contracts and table-grain definitions.
4. `architecture/messaging/`
   - Kafka topic, schema, and retention contracts.
5. `architecture/streaming/`
   - Spark Structured Streaming job contracts.
6. `architecture/serving/`
   - Trino semantic layer and BI serving contracts.
7. `architecture/generator/`
   - Mock event generator run contract and scenario matrix.
8. `product/`
   - Business-layer PRD and KPI definitions that drive technical contracts.

## Ownership

1. Primary owner: Data Engineering (project owner)
2. Change policy: any threshold or rule update must increment `rule_version`.

## Entry Points

1. Business objective and KPI tree:
   - `docs/product/business-decision-prd-kpi-tree.md`
2. Realtime technical contracts:
   - `docs/architecture/realtime-decisioning/`
3. Data model baseline contracts:
   - `docs/architecture/data-model/data-model-contract.md`
4. Messaging contract for streaming interface:
   - `docs/architecture/messaging/kafka-topic-schema-retention-contract.md`
5. Spark realtime execution contract:
   - `docs/architecture/streaming/spark-realtime-jobs-contract.md`
6. Trino semantic layer and serving contract:
   - `docs/architecture/serving/trino-realtime-semantic-serving-contract.md`
7. Mock event generator contract and scenario matrix:
   - `docs/architecture/generator/mock-event-generator-contract-and-scenario-matrix.md`
