# Spark Realtime Jobs Contract (M1)

## 1. Purpose

Define execution contracts for Milestone 1 Spark realtime processing.

In scope jobs:

1. `spark_rt_content_events_aggregator`
2. `spark_rt_video_cdc_upsert`

This document locks runtime behavior for trigger cadence, dedup policy, checkpointing, and degraded operation.

---

## 2. Upstream and Downstream References

1. Business spec: `docs/product/business-decision-prd-kpi-tree.md`
2. Data model: `docs/architecture/data-model/m1-data-model-v1.md`
3. Messaging contract: `docs/architecture/messaging/kafka-topic-schema-retention-contract-m1.md`
4. Realtime decision contracts: `docs/architecture/realtime-decisioning/*`

---

## 3. Job Topology (M1)

### 3.1 `spark_rt_content_events_aggregator`

Input topic:

1. `content_events`

Output tables:

1. `lakehouse.bronze.raw_events` (raw append)
2. `lakehouse.gold.rt_video_stats_1min` (1-minute tumbling aggregate)
3. `lakehouse.bronze.invalid_events_content` (quarantine)

### 3.2 `spark_rt_video_cdc_upsert`

Input topic:

1. `cdc.content.videos`

Output tables:

1. `lakehouse.dims.dim_videos` (Type-1 current snapshot via MERGE upsert)
2. `lakehouse.bronze.invalid_events_cdc_videos` (quarantine)

Isolation principle:

1. jobs run as separate Spark applications for failure isolation and operational clarity.

---

## 4. Runtime Cadence and Event-Time Policy

### 4.1 Trigger cadence

1. `raw_events` sink query: `processingTime = 10 seconds`
2. `rt_video_stats_1min` sink query: `processingTime = 1 minute`
3. `dim_videos` CDC upsert query: `processingTime = 1 minute`

### 4.2 Windowing

1. event-time tumbling window: `1 minute`
2. group grain: `video_id + window_start`

### 4.3 Watermark

1. fixed watermark baseline for `content_events` aggregation: `2 minutes` in M1
2. lag-prone deployments should use `5 minutes` watermark for `content_events` aggregation
3. no dynamic watermark adjustment in M1

### 4.4 Starting offsets (M1)

1. `content_events`: `latest`
2. `cdc.content.videos`: `latest`

Bootstrap note:

1. initial dimension backfill (if needed) is handled by a separate one-time bootstrap job, not by changing streaming offsets in this contract.

---

## 5. Dedup and Conflict Rules

### 5.1 Event stream dedup (`content_events`)

1. dedup key: `event_id`
2. raw table remains append-only source log
3. decision/metric path must apply `event_id` dedup before aggregate write

### 5.2 CDC dedup (`cdc.content.videos`)

1. per micro-batch, keep latest record by `video_id` using highest `ts_ms`
2. conflict tiebreak for same `ts_ms`: keep latest ingest order record

---

## 6. Checkpoint Contract

Checkpoint naming pattern:

1. `s3a://checkpoints/jobs/<job_name>/<sink_name>/v1`

M1 checkpoint paths:

1. `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1`
2. `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1`
3. `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1`
4. `s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1`
5. `s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/invalid_events_cdc_videos/v1`

Rules:

1. each streaming query must have its own unique checkpoint path
2. no checkpoint path sharing across queries or jobs
3. checkpoint reset requires explicit runbook execution (not ad-hoc in production flow)

---

## 7. Invalid Record Handling

`content_events` invalid records:

1. write to `lakehouse.bronze.invalid_events_content`

`cdc.content.videos` invalid records:

1. write to `lakehouse.bronze.invalid_events_cdc_videos`

Invalid rows must include:

1. raw payload
2. source metadata (`topic`, `partition`, `offset`)
3. `schema_version`
4. `error_code` and `error_reason`
5. `ingested_at`

---

## 8. Failure and Degraded Behavior

### 8.1 Content job degradation

Trigger:

1. `spark_rt_content_events_aggregator` unhealthy for > 5 minutes

Behavior:

1. freeze action outputs (no new `BOOST/REVIEW/RESCUE` actions emitted)

### 8.2 CDC job degradation

Trigger:

1. `spark_rt_video_cdc_upsert` unhealthy for > 5 minutes
2. or CDC freshness breach > 10 minutes

Behavior:

1. enter `REVIEW-only` mode
2. pause `BOOST` and `RESCUE` until CDC health recovers

Recovery:

1. require 15 consecutive healthy minutes to exit degraded mode

---

## 9. Operational Monitoring (M1 Lite)

Minimum runtime checks:

1. query active state per job
2. query progress lag / micro-batch latency
3. consumer lag by topic
4. invalid record rates in both quarantine tables
5. dropped-by-watermark rows from query progress metrics (`numRowsDroppedByWatermark` or equivalent)

Observability note:

1. Prometheus/Grafana deep instrumentation is deferred to M2.
2. M1 lite checks must still expose dropped-by-watermark counters via logs or periodic query progress sampling.

---

## 10. Acceptance Criteria (M1)

1. both jobs run independently with isolated checkpoints
2. output tables and grains match `m1-data-model-v1.md`
3. trigger cadence and watermark match this contract
4. dedup and CDC conflict rules are implemented as specified
5. invalid records route to split quarantine tables
6. degraded mode behavior matches contract (`freeze` or `REVIEW-only`)
7. replay/restart remains deterministic for same inputs and versions
8. late-event handling check passes for deterministic late profile (`121s..210s`) and aligns with configured watermark policy
