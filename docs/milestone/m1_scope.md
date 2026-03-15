# Milestone 1 Scope (Reference)

## 1. Purpose

Capture delivered M1 scope as a reference baseline.

This document is historical/reference-oriented.  
Current active scope through M2 is defined in `docs/milestone/m2_scope.md`.

## 2. M1 Scope Summary (Delivered)

1. Realtime decision path is executable and auditable:
   - `generator -> Kafka -> Spark -> Iceberg Gold -> Trino/BI`
2. Demo and storytelling artifacts are ready for portfolio/interview usage.
3. M1 acceptance criteria are testable and traceable via runbooks, verifier logs, and sign-off artifacts.

## 3. M1 In-Scope Themes (Reference)

1. Realtime decision preview for `BOOST`, `REVIEW`, `RESCUE`.
2. Serving and dashboard visibility for recommendation preview and platform health metrics.
3. Contract-driven realtime modeling across messaging, streaming, data model, and serving layers.
4. Deterministic acceptance flows with reproducible artifact capture.

## 4. M1 Out-of-Scope Themes (Reference)

1. Operational action queue execution and queue consumer automation.
2. Full batch expansion and production-scale cloud deployment.
3. Full dbt semantic/quality production workflow.
4. Advanced optimization and automated rollout gating workflows.

## 5. Reference Documents

1. `README.md`
2. `docs/product/business-decision-prd-kpi-tree.md`
3. `docs/architecture/realtime-decisioning/acceptance-criteria.md`
4. `docs/architecture/data-model/m1-data-model-v1.md`
5. `docs/architecture/streaming/spark-realtime-jobs-contract-m1.md`
6. `docs/architecture/serving/trino-semantic-layer-serving-contract-m1-s2.md`
7. `docs/architecture/streaming/reference/mic-38-signoff-acceptance.md`

