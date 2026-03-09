# Documentation Architecture (v2)

This `docs/` tree is the new documentation layout for production-style specs.

Legacy docs remain in `legacy_docs/` and are intentionally untouched during this phase.

## Goals

1. Keep business decisions, metric contracts, and acceptance criteria separate.
2. Make docs easy to review, version, and audit.
3. Support incremental rollout by milestone.

## Current Scope

Milestone 1 foundational docs for product, model, messaging, and realtime execution contracts.

## Folder Structure

1. `architecture/`
   - Long-lived architecture and domain-level design specs.
2. `architecture/realtime-decisioning/`
   - Realtime metric definitions, policy rules, queue contract, reconciliation, and acceptance criteria.
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
3. Milestone 1 data model baseline:
   - `docs/architecture/data-model/m1-data-model-v1.md`
4. Messaging contract for streaming interface:
   - `docs/architecture/messaging/kafka-topic-schema-retention-contract-m1.md`
5. Spark realtime execution contract:
   - `docs/architecture/streaming/spark-realtime-jobs-contract-m1.md`
6. Trino semantic layer and serving contract (M1 Sprint 2 prerequisite):
   - `docs/architecture/serving/trino-semantic-layer-serving-contract-m1-s2.md`
7. Mock event generator contract and scenario matrix:
   - `docs/architecture/generator/mock-event-generator-contract-scenario-matrix-m1.md`
