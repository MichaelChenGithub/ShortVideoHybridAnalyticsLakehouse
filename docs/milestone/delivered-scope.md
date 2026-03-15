# Delivered Scope Reference

## 1. Purpose

Capture delivered scope as a reference baseline.

This document is historical/reference-oriented.  
Current active scope current scope is defined in `docs/milestone/current-scope.md`.

## 2. Delivered Scope Summary

1. Realtime decision path is executable and auditable:
   - `generator -> Kafka -> Spark -> Iceberg Gold -> Trino/BI`
2. Demo and storytelling artifacts are ready for portfolio/interview usage.
3. Acceptance criteria are testable and traceable via runbooks, verifier logs, and sign-off artifacts.

## 3. In-Scope Themes (Reference)

1. Realtime decision preview for `BOOST`, `REVIEW`, `RESCUE`.
2. Serving and dashboard visibility for recommendation preview and platform health metrics.
3. Contract-driven realtime modeling across messaging, streaming, data model, and serving layers.
4. Deterministic acceptance flows with reproducible artifact capture.
5. Watermark-based late-event handling in realtime processing.

## 4. Out-of-Scope Themes (Reference)

1. Operational action queue execution and queue consumer automation.
2. Full batch expansion and production-scale cloud deployment.
3. Full dbt semantic/quality production workflow.
4. Advanced optimization and automated rollout gating workflows.

## 5. Reference Documents

1. `README.md`
2. `docs/product/business-decision-prd-kpi-tree.md`
3. `docs/architecture/realtime-decisioning/acceptance-domain-realtime.md`
4. `docs/architecture/data-model/data-model-contract.md`
5. `docs/architecture/streaming/spark-realtime-jobs-contract.md`
6. `docs/architecture/serving/trino-realtime-semantic-serving-contract.md`
7. `docs/architecture/streaming/reference/realtime-signoff-acceptance.md`
