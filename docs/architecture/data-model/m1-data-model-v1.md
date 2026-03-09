# Data Model v1 (Milestone 1)

## 1. Purpose

Define the minimum data model required to deliver Milestone 1 realtime decisioning:

1. `BOOST`
2. `REVIEW`
3. `RESCUE`

This is a semantic-first model. Storage and optimization details are intentionally deferred to Milestone 2.

---

## 2. Scope (M1)

In scope tables:

1. `lakehouse.bronze.raw_events`
2. `lakehouse.bronze.invalid_events_content`
3. `lakehouse.bronze.invalid_events_cdc_videos`
4. `lakehouse.dims.dim_videos` (current snapshot, Type-1)
5. `lakehouse.dims.rt_rule_quantile_baselines`
6. `lakehouse.gold.rt_video_stats_1min`
7. `lakehouse.gold.rt_action_queue`
8. `lakehouse.qa.run_manifest`
9. `lakehouse.qa.expected_actions`

Out of scope (M2+):

1. full `dim_users` model
2. full Silver canonical model
3. global SCD Type-2 rollout across dimensions
4. storage optimization tuning (partition tuning, compression strategy, compaction policy details)
5. QA dashboarding and automated alert workflow

---

## 3. Modeling Principles

1. Semantic stability first, physical optimization later.
2. Realtime contracts are auditable and deterministic.
3. Additive schema evolution preferred; avoid breaking changes in M1.

---

## 4. Time Semantics (M1 Standard)

1. `event_timestamp`: event time in UTC.
2. `ingested_at` / `processed_at`: system processing timestamps.
3. Realtime aggregations use event-time 1-minute windows (`window_start`, `window_end`).

---

## 5. Table Contracts

### 5.1 `lakehouse.bronze.raw_events`

Role:

1. immutable raw event log for replay/audit
2. schema-evolution buffer via header/body pattern

Grain:

1. logical event grain = `event_id`

Required fields (minimum):

1. `event_id` STRING
2. `event_timestamp` TIMESTAMP
3. `video_id` STRING
4. `user_id` STRING
5. `event_type` STRING
6. `payload_json` STRING
7. `schema_version` STRING
8. `source_topic` STRING
9. `source_partition` INT
10. `source_offset` BIGINT
11. `ingested_at` TIMESTAMP

Header/body note:

1. Header holds routing and contract fields (`event_id`, `event_timestamp`, keys, event_type, schema_version, source metadata).
2. Body (`payload_json`) preserves flexible attributes for forward compatibility.

Data contract notes:

1. append-only
2. `event_id` non-null
3. `event_type` in approved enum set

---

### 5.2 `lakehouse.bronze.invalid_events_content`

Role:

1. quarantine sink for contract-violating or non-parseable records from `content_events`
2. keeps invalid data auditable without blocking realtime content pipeline writes

Grain:

1. `invalid_event_id` (or deterministic composite key from source metadata)

Required fields (minimum):

1. `invalid_event_id` STRING
2. `raw_value` STRING
3. `source_topic` STRING
4. `source_partition` INT
5. `source_offset` BIGINT
6. `schema_version` STRING
7. `error_code` STRING
8. `error_reason` STRING
9. `ingested_at` TIMESTAMP

Data contract notes:

1. append-only
2. no replay from this table in M1 (triage and analysis only)

---

### 5.3 `lakehouse.bronze.invalid_events_cdc_videos`

Role:

1. quarantine sink for contract-violating or non-parseable records from `cdc.content.videos`
2. isolates CDC invalid-write risk from event-stream invalid-write risk

Grain:

1. `invalid_event_id` (or deterministic composite key from source metadata)

Required fields (minimum):

1. `invalid_event_id` STRING
2. `raw_value` STRING
3. `source_topic` STRING
4. `source_partition` INT
5. `source_offset` BIGINT
6. `schema_version` STRING
7. `error_code` STRING
8. `error_reason` STRING
9. `ingested_at` TIMESTAMP

Data contract notes:

1. append-only
2. no replay from this table in M1 (triage and analysis only)

---

### 5.4 `lakehouse.dims.dim_videos` (M1 current snapshot)

Role:

1. latest video metadata for realtime joins and rescue logic

Storage behavior:

1. CDC micro-batch upsert with `MERGE`
2. Type-1 current snapshot in M1 (no full history table requirement in M1)

Grain:

1. `video_id` (current row per video)

Required fields (minimum):

1. `video_id` STRING
2. `category` STRING
3. `region` STRING
4. `upload_time` TIMESTAMP
5. `status` STRING
6. `updated_at` TIMESTAMP
7. `source_ts_ms` BIGINT

M1 usage:

1. `upload_time` for `upload_age <= 60m`
2. `category + region` for under-exposure cohort baseline
3. `status` for decision eligibility guardrails

---

### 5.5 `lakehouse.gold.rt_video_stats_1min`

Role:

1. canonical realtime metric fact for decision scoring

Grain:

1. `video_id + window_start`

Required fields (minimum):

1. `video_id` STRING
2. `window_start` TIMESTAMP
3. `window_end` TIMESTAMP
4. `impressions` BIGINT
5. `play_start` BIGINT
6. `play_finish` BIGINT
7. `likes` BIGINT
8. `shares` BIGINT
9. `skips` BIGINT
10. `watch_time_sum_ms` BIGINT
11. `processed_at` TIMESTAMP

Contract:

1. unique key: `video_id + window_start`
2. counts are non-negative

---

### 5.6 `lakehouse.gold.rt_action_queue`

Role:

1. execution-ready decision output for Ops workflows
2. decision audit trail

Primary grain:

1. `action_id`

Business constraint:

1. max 1 action per `video_id` per 60 minutes (cooldown)

Schema and policy source:

1. See `docs/architecture/realtime-decisioning/action-queue-contract.md`

---

### 5.7 `lakehouse.qa.run_manifest`

Role:

1. run-level reproducibility and audit metadata for simulation validation

Grain:

1. `run_id`

Required fields (minimum):

1. `run_id` STRING
2. `seed` BIGINT
3. `rule_version` STRING
4. `duration_minutes` INT
5. `events_per_sec` INT
6. `late_event_ratio` DOUBLE
7. `scenario_mix_json` STRING
8. `started_at` TIMESTAMP
9. `ended_at` TIMESTAMP
10. `status` STRING
11. `artifact_path` STRING

Data contract notes:

1. validation-only table, not consumer-facing serving table
2. one row per run

---

### 5.8 `lakehouse.qa.expected_actions`

Role:

1. deterministic ground truth for decision-accuracy checks

Grain:

1. `run_id + video_id + window_start`

Required fields (minimum):

1. `run_id` STRING
2. `video_id` STRING
3. `window_start` TIMESTAMP
4. `window_end` TIMESTAMP
5. `scenario_id` STRING
6. `expected_action` STRING
7. `expected_reason_codes` ARRAY<STRING>
8. `generated_at` TIMESTAMP

Data contract notes:

1. validation-only table, not consumer-facing serving table
2. partition recommendation: `days(window_start)`, `bucket(16, video_id)`

---

### 5.9 `lakehouse.dims.rt_rule_quantile_baselines`

Role:

1. published threshold registry for decision quantile baselines
2. traceable binding between quantile thresholds and `rule_version`

Grain:

1. `rule_version + effective_from + metric_name + percentile + cohort_category + cohort_region`

Required fields (minimum):

1. `rule_version` STRING
2. `effective_from` DATE
3. `effective_to` DATE
4. `metric_name` STRING (`velocity_30m` or `impressions_30m`)
5. `percentile` INT (`90` or `40`)
6. `cohort_category` STRING (nullable for global baseline)
7. `cohort_region` STRING (nullable for global baseline)
8. `threshold_value` DOUBLE
9. `sample_size` BIGINT
10. `is_fallback` BOOLEAN
11. `computed_at` TIMESTAMP

Data contract notes:

1. refresh cadence: once daily after T+1 completion
2. no intraday drift for published thresholds in M1
3. rows are immutable after publish for a given `rule_version + effective_from`
4. any threshold logic change must publish a new `rule_version`
5. cohort fallback behavior and publish guards are governed by `docs/architecture/realtime-decisioning/metric-contract.md`

---

## 6. Join Contract (M1)

Primary realtime join path:

1. `rt_video_stats_1min.video_id = dim_videos.video_id`

Join intent:

1. add `category`, `region`, `upload_time`, `status` context for decision logic

Guardrails:

1. do not explode grain (join must preserve `video_id + window_start`)
2. missing `dim_videos` should default to conservative decision handling (no unsafe boost)

Validation join path:

1. `gold.rt_action_queue` / `gold.rt_video_stats_1min` joined with `qa.expected_actions` by `video_id + window_start` under a specific `run_id`

---

## 7. Generator Contract Alignment

`user_id` policy for M1:

1. use stable synthetic IDs (for example `u_000001` style pool), not per-event random IDs
2. this keeps future `dim_users` and retention analysis feasible without replay redesign
3. run metadata and ground truth are tracked in `lakehouse.qa.*`, not in consumer-facing `gold` outputs

---

## 8. Deferred to Milestone 2

1. `dim_users` table and user-level modeling contracts
2. Type-2 historical dimensions where business requires point-in-time attribution
3. Silver-layer compression and storage optimization strategy
4. expanded canonical model and lineage for batch marts

---

## 9. Acceptance Criteria (Model v1)

1. table grains and keys are explicit and testable
2. required columns for each in-scope table are defined and non-ambiguous
3. realtime join path is defined and grain-safe
4. model supports current M1 decision contracts without additional schema dependencies
5. invalid-record quarantine paths are split and defined:
   - `lakehouse.bronze.invalid_events_content`
   - `lakehouse.bronze.invalid_events_cdc_videos`
6. QA validation tables are defined and linkable to decision outputs by `run_id` and `video_id + window_start`
