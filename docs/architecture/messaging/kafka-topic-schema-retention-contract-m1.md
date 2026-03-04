# Kafka Topic, Schema, and Retention Contract (M1)

## 1. Purpose

Define Kafka as a governed data interface for Milestone 1 realtime decisioning.

This contract covers:

1. topic inventory and naming
2. keying and partition strategy
3. schema and compatibility rules
4. retention and failure-path behavior

---

## 2. Scope (M1)

In scope:

1. event stream topic for realtime metrics
2. video CDC topic for realtime metadata updates

Out of scope (M2+):

1. full user CDC contract (`cdc.users.profiles`)
2. Kafka DLQ topic workflow and automated replay pipeline
3. Prometheus/Grafana-based messaging observability
4. advanced multi-cluster replication and cross-region failover

---

## 3. Topic Inventory

### 3.1 `content_events`

Role:

1. high-volume user interaction events for realtime scoring

Message key:

1. `video_id`

Consumer:

1. Spark realtime job: `spark_rt_content_events_aggregator`
2. Implementation file: `src/spark/rt_content_events_aggregator.py`
3. Invalid-record quarantine sink: `lakehouse.bronze.invalid_events_content`

### 3.2 `cdc.content.videos`

Role:

1. video metadata CDC stream for `dim_videos` upsert

Message key:

1. `video_id`

Consumer:

1. Spark realtime job: `spark_rt_video_cdc_upsert`
2. Implementation file: `src/spark/rt_video_cdc_upsert.py`
3. Invalid-record quarantine sink: `lakehouse.bronze.invalid_events_cdc_videos`

### 3.3 Consumer Naming and Consumer Group IDs (M1)

To avoid naming ambiguity, this project uses explicit logical job names.

Logical consumer jobs:

1. `spark_rt_content_events_aggregator`
2. `spark_rt_video_cdc_upsert`

Recommended consumer group IDs:

1. `cg_rt_content_events_aggregator_v1`
2. `cg_rt_video_cdc_upsert_v1`

---

## 4. Partition and Throughput Strategy

### 4.1 Partitioning

1. `content_events`: partition by `video_id` to keep per-video ordering for 1-minute aggregations.
2. `cdc.content.videos`: partition by `video_id` to preserve update order for dimension merges.

### 4.2 M1 Baseline Configuration (local/dev)

1. `content_events`: 6 partitions
2. `cdc.content.videos`: 3 partitions
3. replication factor: 1 (single-broker local environment)

Note:

1. production-like setups should increase replication factor and partition count based on capacity testing.

---

## 5. Schema Contract

## 5.1 `content_events` schema (header + body)

Required header fields:

1. `event_id` STRING (non-null)
2. `event_timestamp` TIMESTAMP (UTC, non-null)
3. `video_id` STRING (non-null)
4. `user_id` STRING (non-null)
5. `event_type` STRING (non-null)
6. `schema_version` STRING (non-null)

Body:

1. `payload_json` STRING (flexible extension area)

Allowed `event_type` values (M1):

1. `impression`
2. `play_start`
3. `play_finish`
4. `like`
5. `share`
6. `skip`

### 5.2 `cdc.content.videos` schema

Required fields:

1. `op` STRING (`c`, `u`, optional `d` when enabled)
2. `ts_ms` BIGINT
3. `schema_version` STRING
4. `after.video_id` STRING (required for `c`/`u`)
5. `after.category` STRING
6. `after.region` STRING
7. `after.upload_time` TIMESTAMP
8. `after.status` STRING

---

## 6. Schema Evolution Policy

1. additive-first changes only in M1
2. no breaking rename/removal without version bump and coordinated consumer update
3. every message must include `schema_version`
4. incompatible messages are quarantined to source-specific invalid tables in M1

Compatibility target:

1. consumers must tolerate unknown fields in `payload_json`
2. invalid records in M1 are quarantined to data-table workflow, not Kafka DLQ topics
3. quarantine routing in M1:
   - `content_events` -> `bronze.invalid_events_content`
   - `cdc.content.videos` -> `bronze.invalid_events_cdc_videos`

---

## 7. Retention Policy (M1)

### 7.1 `content_events`

1. `cleanup.policy=delete`
2. retention: 7 days

Rationale:

1. supports replay/debug for recent incidents without excessive local storage pressure

### 7.2 `cdc.content.videos`

1. `cleanup.policy=delete`
2. retention: 14 days

Rationale:

1. dimension update streams are lower volume and benefit from longer replay window

## 8. Producer and Consumer Reliability Contract

Producer expectations:

1. include stable key (`video_id`)
2. provide idempotent-friendly `event_id` for events
3. emit UTC timestamps

Consumer expectations:

1. checkpoint offsets externally via Spark checkpointing
2. enforce schema validation before downstream write
3. quarantine invalid records to source-specific tables with error code and reason:
   - `bronze.invalid_events_content`
   - `bronze.invalid_events_cdc_videos`

---

## 9. Monitoring and Alerts (M1)

Minimum monitoring:

1. producer error rate
2. consumer lag by topic
3. invalid-record count from `bronze.invalid_events_content`
4. invalid-record count from `bronze.invalid_events_cdc_videos`

Alert intent:

1. sustained lag threatens realtime SLA (`P95 < 3m`)
2. invalid-record spikes indicate schema contract breakage by source stream

Monitoring implementation note:

1. M1 supports lightweight monitoring (logs + periodic checks).
2. Prometheus/Grafana integration is planned for M2.

---

## 10. Acceptance Criteria (M1)

1. topic inventory and names are consistent across docs and code
2. keying strategy is defined and implemented (`video_id`)
3. required schema fields and allowed enums are explicit
4. schema evolution and invalid-record quarantine policy are documented
5. retention settings are explicit for each in-scope topic
6. monitoring requirements cover lag and contract violations

---

## 11. Linked Docs

1. `docs/product/business-decision-prd-kpi-tree.md`
2. `docs/architecture/data-model/m1-data-model-v1.md`
3. `docs/architecture/realtime-decisioning/metric-contract.md`
4. `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`

## 12. M2 Upgrade Notes

1. introduce Kafka DLQ topics and replay workflow
2. integrate Prometheus/Grafana observability for producer/consumer/Kafka metrics
