# Data Model v1

## 1. Purpose

Define the baseline realtime data model delivered in initial realtime scope and retained as current reference:

1. `BOOST`
2. `REVIEW`
3. `RESCUE`

This is a semantic-first baseline model. Advanced optimization and extended future-scope modeling are deferred to `docs/milestone/future-plan.md`.

---

## 2. Scope (Baseline Reference)

In scope tables:

1. `lakehouse.bronze.raw_events`
2. `lakehouse.bronze.invalid_events_content`
3. `lakehouse.bronze.invalid_events_cdc_videos`
4. `lakehouse.silver.events_conformed`
5. `lakehouse.silver.user_activity_sessions_30m`
6. `lakehouse.dims.dim_videos` (current snapshot, Type-1)
7. `lakehouse.dims.dim_users_scd2`
8. `lakehouse.dims.dim_videos_scd2`
9. `lakehouse.dims.rt_rule_quantile_baselines`
10. `lakehouse.gold.rt_video_stats_1min`
11. `lakehouse.gold.batch_retention_daily`
12. `lakehouse.gold.batch_engagement_daily`
13. `lakehouse.gold.batch_sessionization_daily`
14. `lakehouse.gold.batch_publish_manifest`
15. `lakehouse.qa.run_manifest`
16. `lakehouse.qa.expected_actions`

Out of scope (deferred):

1. full Silver canonical model
2. storage optimization tuning (partition tuning, compression strategy, compaction policy details)
3. QA dashboarding and automated alert/notification workflow
4. operational `rt_action_queue` execution and queue-consumer automation

---

## 3. Modeling Principles

1. Semantic stability first, physical optimization later.
2. Realtime contracts are auditable and deterministic.
3. Additive schema evolution preferred; avoid breaking changes to baseline contracts.
4. Batch tables must declare mutability policy (`append-only`, `partition-overwrite`, or `versioned publish`) explicitly.
5. Backfill/replay behavior must be deterministic and traceable through publish metadata.

---

## 4. Time Semantics (Baseline Standard)

1. `event_timestamp`: event time in UTC.
2. `ingested_at` / `processed_at`: system processing timestamps.
3. Realtime aggregations use event-time 1-minute windows (`window_start`, `window_end`).
4. Batch outputs use `data_date` (business date) and `published_at` (publish timestamp).
5. Batch SLA alignment uses `America/New_York` for daily `D-1` publish readiness checks.
6. Cohort outputs must include explicit cohort anchor date (`cohort_date`) and age index (`day_n`) when applicable.

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
2. no replay from this table in current scope (triage and analysis only)

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
2. no replay from this table in current scope (triage and analysis only)

---

### 5.4 `lakehouse.dims.dim_videos` (current snapshot baseline)

Role:

1. latest video metadata for realtime joins and rescue logic

Storage behavior:

1. CDC micro-batch upsert with `MERGE`
2. Type-1 current snapshot baseline (no full history table requirement in current scope)

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

Usage:

1. `upload_time` for `upload_age <= 60m`
2. `category + region` for under-exposure cohort baseline
3. `status` for decision eligibility guardrails

Track boundary (dual-track coexistence):

1. `dim_videos` remains the realtime snapshot dimension for current realtime decision paths.
2. batch historical attribution must use `dim_videos_scd2` (not snapshot `dim_videos`).

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

This table is deferred to future plan and is not part of current delivery scope.

Reference:

1. `docs/architecture/realtime-decisioning/action-queue-future-plan.md`

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

1. refresh cadence: once daily after batch publish completion
2. no intraday drift for published thresholds in current scope
3. rows are immutable after publish for a given `rule_version + effective_from`
4. any threshold logic change must publish a new `rule_version`
5. cohort fallback behavior and publish guards are governed by `docs/architecture/realtime-decisioning/metric-contract.md`

---

### 5.10 Batch Tables

This subsection defines table contracts aligned with current batch scope and PRD targets.

#### 5.10.1 `lakehouse.silver.events_conformed`

Role:

1. canonicalized event layer for batch metric derivation (retention/engagement/sessionization)
2. primary batch source table derived from `lakehouse.bronze.raw_events` parsing and conformance checks

Grain:

1. `event_id`

Required fields (minimum):

1. `event_id` STRING
2. `event_timestamp` TIMESTAMP
3. `event_date_et` DATE
4. `data_date` DATE
5. `video_id` STRING
6. `user_id` STRING
7. `event_type` STRING
8. `watch_time_ms` BIGINT
9. `category` STRING
10. `region` STRING

Data contract notes:

1. `event_id` is the uniqueness key in `events_conformed`.
2. batch reprocessing must not introduce duplicate `event_id` rows for the same logical event.
3. `event_type` allowed enum follows the authoritative messaging contract:
   - `docs/architecture/messaging/kafka-topic-schema-retention-contract.md` (section `5.1 content_events schema`)
4. `event_date_et` is derived from `event_timestamp` converted to `America/New_York`.
5. batch partitioning and `D-1` publish checks should align on `event_date_et` semantics.
6. physical layout baseline: `partition by event_date_et`, `bucket(64, user_id)`.
7. bucket count may be tuned by benchmark evidence in cloud/scale contract.
8. `watch_time_ms` is parsed from `raw_events.payload_json.watch_time_ms` and defaults to `0` when missing/null.
9. `events_conformed` standardizes as-of join inputs (`user_id`, `video_id`, `event_timestamp`) and does not execute SCD2 attribution joins in this layer.
10. downstream batch fact jobs must apply as-of attribution with left-closed/right-open windows: `event_timestamp >= valid_from AND event_timestamp < valid_to`.

#### 5.10.2 `lakehouse.silver.user_activity_sessions_30m`

Role:

1. sessionized user activity using 30-minute inactivity gap rule
2. derived from `lakehouse.silver.events_conformed`

Grain:

1. `session_id`

Required fields (minimum):

1. `session_id` STRING
2. `user_id` STRING
3. `session_start_ts` TIMESTAMP
4. `session_end_ts` TIMESTAMP
5. `category` STRING
6. `region` STRING
7. `new_vs_returning_user` STRING
8. `session_duration_sec` BIGINT
9. `event_count` BIGINT
10. `watch_time_sum_ms` BIGINT
11. `data_date` DATE

Data contract notes:

1. session split rule uses 30-minute inactivity gap.
2. `session_id` must be deterministic (for example hash of `user_id + session_start_ts`) to keep replay/backfill stable.
3. event ordering inside a user stream is `event_timestamp ASC`, then `event_id ASC`.
4. session attribution fields are deterministic and derived after sessionization under the governed event ordering.
5. `category` and `region` are selected as the dominant `(category, region)` pair in the session using highest `SUM(watch_time_ms)`.
6. ties for dominant `(category, region)` are broken by `COUNT(*)`, then max event timestamp, then max event id, all descending.
7. `new_vs_returning_user` comes from the `dim_users_scd2` as-of row matched at the session max event timestamp; use `unknown` when no match exists.
8. physical layout baseline: `partition by data_date`, `bucket(64, user_id)`.

#### 5.10.3 `lakehouse.dims.dim_users_scd2`

Role:

1. historical user attributes for point-in-time batch attribution

Grain:

1. `user_sk` (surrogate key)

Required fields (minimum):

1. `user_sk` STRING (deterministic hash surrogate key)
2. `user_id` STRING
3. `region` STRING
4. `new_vs_returning_user` STRING (`new`, `returning`, `unknown`; platform-lifetime definition with explicit fallback)
5. `valid_from` TIMESTAMP
6. `valid_to` TIMESTAMP
7. `is_current` BOOLEAN

Data contract notes:

1. `dim_users_scd2` is the canonical user-history dimension.
2. Current scope does not require a separate realtime `dim_users` snapshot table.
3. `region` captures the user's current region snapshot history via CDC and is distinct from `dim_videos_scd2.region`.
4. `new_vs_returning_user` is persisted in the dimension and consumed by batch joins; current scope does not rely on ad-hoc query-time derivation.
5. current/open row convention: `valid_to = 9999-12-31 00:00:00 UTC`.
6. physical layout baseline: `partition by date(valid_from)`, `bucket(64, user_id)`.

#### 5.10.4 `lakehouse.dims.dim_videos_scd2`

Role:

1. historical video attributes for point-in-time batch attribution

Grain:

1. `video_sk` (surrogate key)

Required fields (minimum):

1. `video_sk` STRING (deterministic hash surrogate key)
2. `video_id` STRING
3. `category` STRING
4. `region` STRING
5. `status` STRING
6. `valid_from` TIMESTAMP
7. `valid_to` TIMESTAMP
8. `is_current` BOOLEAN

Data contract notes:

1. `dim_videos_scd2` is the canonical batch historical video dimension for point-in-time attribution.
2. realtime decision paths continue to use snapshot `lakehouse.dims.dim_videos`.
3. current/open row convention: `valid_to = 9999-12-31 00:00:00 UTC`.
4. physical layout baseline: `partition by date(valid_from)`, `bucket(64, video_id)`.

#### 5.10.5 `lakehouse.gold.batch_retention_daily`

Role:

1. publishable retention metrics for D1/D7 cohort analysis
2. derived from `lakehouse.silver.events_conformed` with user/video dimension attribution joins

Grain:

1. `cohort_date + day_n + category + region + new_vs_returning_user`

Required fields (minimum):

1. `cohort_date` DATE
2. `day_n` INT
3. `category` STRING
4. `region` STRING
5. `new_vs_returning_user` STRING
6. `cohort_users` BIGINT
7. `retained_users` BIGINT
8. `retention_rate` DOUBLE
9. `data_date` DATE
10. `published_at` TIMESTAMP

Data contract notes:

1. `new_vs_returning_user` is a required segmentation dimension in batch gold outputs.
2. when user-state attribution is unavailable, use explicit `unknown` value instead of dropping the dimension.
3. downstream global/cohort views may aggregate over `new_vs_returning_user` when segment split is not needed.
4. physical partition baseline: `partition by data_date`.
5. `day_n` domain is restricted to `{1, 7}` in current scope.
6. retention formula semantics are governed by `docs/architecture/batch-analytics/batch-metrics-contract.md`.

#### 5.10.6 `lakehouse.gold.batch_engagement_daily`

Role:

1. daily engagement KPI + lightweight funnel outputs
2. derived from `lakehouse.silver.events_conformed` with user/video dimension attribution joins

Grain:

1. `data_date + category + region + new_vs_returning_user`

Required fields (minimum):

1. `data_date` DATE
2. `category` STRING
3. `region` STRING
4. `new_vs_returning_user` STRING
5. `impressions` BIGINT
6. `play_start` BIGINT
7. `play_finish` BIGINT
8. `likes` BIGINT
9. `shares` BIGINT
10. `skips` BIGINT
11. `play_start_rate` DOUBLE
12. `completion_rate` DOUBLE
13. `interaction_rate` DOUBLE
14. `skip_rate` DOUBLE
15. `published_at` TIMESTAMP

Data contract notes:

1. `new_vs_returning_user` is a required segmentation dimension in batch gold outputs.
2. when user-state attribution is unavailable, use explicit `unknown` value instead of dropping the dimension.
3. downstream global views may aggregate over `new_vs_returning_user` when segment split is not needed.
4. physical partition baseline: `partition by data_date`.
5. engagement formula semantics are governed by `docs/architecture/batch-analytics/batch-metrics-contract.md`.

#### 5.10.7 `lakehouse.gold.batch_sessionization_daily`

Role:

1. daily session behavior outputs for stickiness analysis
2. derived from `lakehouse.silver.user_activity_sessions_30m` with user/video dimension attribution joins

Grain:

1. `data_date + category + region + new_vs_returning_user`

Required fields (minimum):

1. `data_date` DATE
2. `category` STRING
3. `region` STRING
4. `new_vs_returning_user` STRING
5. `sessions` BIGINT
6. `sessions_per_user` DOUBLE
7. `avg_session_duration_sec` DOUBLE
8. `events_per_session` DOUBLE
9. `watch_time_per_session_ms` DOUBLE
10. `published_at` TIMESTAMP

Data contract notes:

1. `new_vs_returning_user` is a required segmentation dimension in batch gold outputs.
2. when user-state attribution is unavailable, use explicit `unknown` value instead of dropping the dimension.
3. downstream global views may aggregate over `new_vs_returning_user` when segment split is not needed.
4. physical partition baseline: `partition by data_date`.
5. sessionization formula semantics are governed by `docs/architecture/batch-analytics/batch-metrics-contract.md`.

#### 5.10.8 `lakehouse.gold.batch_publish_manifest`

Role:

1. publish-level audit contract for batch SLA and quality-gate traceability in current scope

Grain:

1. `data_date + publish_run_id`

Required fields (minimum):

1. `publish_run_id` STRING
2. `data_date` DATE
3. `target_ready_by_et` TIMESTAMP
4. `published_at` TIMESTAMP
5. `is_on_time` BOOLEAN
6. `quality_gate_passed` BOOLEAN
7. `quality_summary_json` STRING
8. `status` STRING

Data contract notes:

1. `quality_gate_passed = true` only when all current batch gold outputs pass required quality gates:
   - `lakehouse.gold.batch_retention_daily`
   - `lakehouse.gold.batch_engagement_daily`
   - `lakehouse.gold.batch_sessionization_daily`
2. manifest rows are append-only by `publish_run_id` for publish traceability.
3. `target_ready_by_et` is fixed to `08:00` (`America/New_York`) for the corresponding `data_date` publish window.
4. `is_on_time = true` when `published_at <= target_ready_by_et`.
5. current scope does not introduce per-table batch metric version columns; metric-logic notes are captured in `quality_summary_json`.
6. explicit batch metric versioning contract is deferred to future plan.

---

## 6. Join Contract (Baseline)

Primary realtime join path:

1. `rt_video_stats_1min.video_id = dim_videos.video_id`

Join intent:

1. add `category`, `region`, `upload_time`, `status` context for decision logic

Guardrails:

1. do not explode grain (join must preserve `video_id + window_start`)
2. missing `dim_videos` should default to conservative decision handling (no unsafe boost)

Validation join path:

1. `serving.v_rt_video_decision_context_30m_1m` / `gold.rt_video_stats_1min` joined with `qa.expected_actions` by `video_id + window_start` under a specific `run_id`

### 6.1 Batch Join (As-Of Attribution)

Batch attribution path:

1. batch facts join `dim_users_scd2` on `fact.user_id = dim_users_scd2.user_id`
2. batch facts join `dim_videos_scd2` on `fact.video_id = dim_videos_scd2.video_id`
3. as-of condition (left-closed, right-open): `event_ts >= valid_from AND event_ts < valid_to`
4. `event_ts` maps to the fact event-time column (for example `fact.event_timestamp`)

Guardrails:

1. one fact row maps to exactly one user-dimension version and one video-dimension version
2. unmatched dimension rows are counted and surfaced in quality checks before publish

---

## 7. Generator Contract Alignment

`user_id` policy for baseline scope:

1. use stable synthetic IDs (for example `u_000001` style pool), not per-event random IDs
2. this keeps future `dim_users` and retention analysis feasible without replay redesign
3. run metadata and ground truth are tracked in `lakehouse.qa.*`, not in consumer-facing `gold` outputs

### 7.1 Identity and Segmentation

1. `new_vs_returning_user` uses platform-lifetime definition:
   - `new`: first-seen activity date for `user_id`
   - `returning`: any activity after first-seen date
   - `unknown`: fallback when authoritative user-state attribution is unavailable during batch derivation
2. Identity mapping policy must keep batch joins stable across replay/backfill runs.
3. Surrogate keys use deterministic hash strategy (not sequence identity) to preserve cross-run reproducibility.
4. Final derivation logic is defined jointly with batch metrics contract; this file anchors required model fields only.

---

## 8. Future Plan (Deferred)

1. Silver-layer compression and storage optimization strategy
2. explicit batch metric versioning contract for retention/engagement/sessionization outputs
3. canonical deferred-scope reference: `docs/milestone/future-plan.md`

---

## 9. Acceptance Criteria (Model v1)

1. table grains and keys are explicit and testable
2. required columns for each in-scope table are defined and non-ambiguous
3. realtime join path is defined and grain-safe
4. model supports current decision contracts without additional schema dependencies
5. invalid-record quarantine paths are split and defined:
   - `lakehouse.bronze.invalid_events_content`
   - `lakehouse.bronze.invalid_events_cdc_videos`
6. QA validation tables are defined and linkable to decision outputs by `run_id` and `video_id + window_start`
7. Batch tables declare grain, required fields, and publish metadata for SLA/quality traceability.
8. Batch segmentation supports at minimum `date x category x region x new_vs_returning_user`.
