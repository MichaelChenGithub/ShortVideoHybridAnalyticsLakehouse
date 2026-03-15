# Documentation Architecture (v2)

This `docs/` tree is the new documentation layout for production-style specs.

Legacy docs remain in `legacy_docs/` and are intentionally untouched during this phase.

## Goals

1. Keep business decisions, metric contracts, and acceptance criteria separate.
2. Make docs easy to review, version, and audit.
3. Present one coherent M1+M2 platform story for delivery and communication.

## Current Scope

M1 + M2 platform scope:

1. M1 delivered scope captured as reference.
2. M2 scope covers batch metrics expansion, semantic + dbt quality, and cloud deployment + scale benchmark.
3. Deferred/conflicting items are centralized in M3 future plan.

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
   - Realtime metric definitions, policy rules, reconciliation, acceptance criteria, and M3 queue references.
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
9. `milestone/`
   - Milestone scope anchors (`m1_scope`, `m2_scope`, `m3_scope`) for delivered/current/future boundaries.

## Ownership

1. Primary owner: Data Engineering (project owner)
2. Change policy: any threshold or rule update must increment `rule_version`.

## Entry Points

1. M2 scope anchor:
   - `docs/milestone/m2_scope.md`
2. M3 future plan anchor:
   - `docs/milestone/m3_scope.md`
3. M1 delivered scope anchor:
   - `docs/milestone/m1_scope.md`
4. Business objective and KPI tree:
   - `docs/product/business-decision-prd-kpi-tree.md`
5. Realtime technical contracts:
   - `docs/architecture/realtime-decisioning/`
6. Data model baseline contracts:
   - `docs/architecture/data-model/m1-data-model-v1.md`
7. Messaging contract for streaming interface:
   - `docs/architecture/messaging/kafka-topic-schema-retention-contract-m1.md`
8. Spark realtime execution contract:
   - `docs/architecture/streaming/spark-realtime-jobs-contract-m1.md`
9. Trino semantic layer and serving contract:
   - `docs/architecture/serving/trino-semantic-layer-serving-contract-m1-s2.md`
10. Mock event generator contract and scenario matrix:
   - `docs/architecture/generator/mock-event-generator-contract-scenario-matrix-m1.md`
11. Deferred M3 queue scope reference:
   - `docs/architecture/realtime-decisioning/m3-action-queue-reference.md`
