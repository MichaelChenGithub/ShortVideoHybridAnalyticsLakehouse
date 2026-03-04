# Mock Event Generator Contract and Scenario Matrix (M1)

## 1. Purpose

Define a deterministic simulation contract for Milestone 1 realtime decisioning.

This contract is designed to validate:

1. `Viral Velocity + Quality Gate`
2. `Cold-start Rescue`

The generator is treated as a governed upstream interface, not ad-hoc test data.

---

## 2. Scope (M1)

In scope:

1. bounded simulation run model (`run_id`, `seed`, fixed duration)
2. deterministic synthetic ID and registry generation
3. CDC bootstrap then event streaming sequence
4. scenario matrix for decision validation
5. QA validation tables for run metadata and expected actions

Out of scope (M2+):

1. DLQ replay workflow
2. Prometheus/Grafana observability for generator
3. automated adaptive watermark tuning
4. policy auto-optimization loop

---

## 3. Run Model

### 3.1 Run definition

One run is a bounded streaming simulation window with explicit start and end.

Recommended M1 default:

1. duration: 30 minutes
2. event throughput target: 120 events/second

### 3.2 Run lifecycle

1. initialize run config and deterministic pools from `seed`
2. emit CDC bootstrap events to `cdc.content.videos`
3. wait for CDC readiness gate (1-2 CDC micro-batches)
4. emit `content_events` stream according to scenario mix
5. finalize run artifacts and QA table writes

---

## 4. Run Config Contract

Required fields:

1. `run_id` STRING
2. `seed` BIGINT
3. `duration_minutes` INT
4. `events_per_sec` INT
5. `scenario_mix` MAP<STRING, DOUBLE>
6. `late_event_ratio` DOUBLE
7. `rule_version` STRING
8. `started_at` TIMESTAMP

Validation rules:

1. all `scenario_mix` values must be non-negative
2. sum(`scenario_mix`) must equal 1.0 (+/- 1e-6)
3. `late_event_ratio` must be in `[0, 0.2]` for M1
4. `duration_minutes >= 10`

Example:

```json
{
  "run_id": "m1_20260304_seed42_r001",
  "seed": 42,
  "duration_minutes": 30,
  "events_per_sec": 120,
  "scenario_mix": {
    "normal_baseline": 0.55,
    "viral_high_quality": 0.20,
    "viral_low_quality": 0.10,
    "cold_start_under_exposed": 0.10,
    "invalid_payload_burst": 0.05
  },
  "late_event_ratio": 0.02,
  "rule_version": "m1_rtv1",
  "started_at": "2026-03-04T14:00:00Z"
}
```

---

## 5. Deterministic Identity and Registry Contract

### 5.1 Identity rules

1. `video_id` and `user_id` are generated deterministically from `seed` and sequence index
2. `event_id` must be unique and deterministic inside a run
3. identical `seed + config + duration` must reproduce equivalent scenario-level distributions

### 5.2 Registry policy

Generator maintains a deterministic in-memory registry and writes run artifacts:

1. `artifacts/generator_runs/<run_id>/run_config.json`
2. `artifacts/generator_runs/<run_id>/video_registry.parquet`
3. `artifacts/generator_runs/<run_id>/expected_actions.parquet`

`content_events` must only use valid `video_id` from registry, except intentional invalid scenarios.

### 5.3 Upstream dependency policy

1. generator must not read Iceberg tables to decide IDs in M1
2. validity is guaranteed by CDC bootstrap ordering, not by downstream lookups

---

## 6. Topic Emission Contract

### 6.1 Topics

1. `content_events`
2. `cdc.content.videos`

### 6.2 Emission sequence

1. emit `cdc.content.videos` create events first
2. enforce readiness gate
3. start `content_events` stream

### 6.3 Partitioning and ordering

1. message key for `content_events`: `video_id`
2. message key for `cdc.content.videos`: `video_id`
3. per-video ordering must be preserved by keying strategy

### 6.4 Late-event simulation

1. late events are simulated by offsetting `event_timestamp`
2. M1 keeps watermark static (`10 seconds`) and does not auto-tune

---

## 7. Scenario Matrix (M1)

`x` = `completion_rate` threshold in quality gate  
`y` = `skip_rate` threshold in quality gate

| scenario_id | mix | impressions_30m | like_rate | share_rate | completion_rate | skip_rate | expected_action |
|---|---:|---:|---:|---:|---:|---:|---|
| `normal_baseline` | 0.55 | 300-1200 | 0.02-0.06 | 0.003-0.012 | `x-0.10` to `x+0.05` | `y-0.05` to `y+0.10` | `NONE` |
| `viral_high_quality` | 0.20 | 4000-20000 | 0.10-0.22 | 0.03-0.10 | `x+0.10` to `x+0.25` | `y-0.20` to `y-0.05` | `BOOST` |
| `viral_low_quality` | 0.10 | 4000-20000 | 0.08-0.18 | 0.02-0.06 | `x-0.35` to `x-0.10` | `y+0.10` to `y+0.35` | `REVIEW` |
| `cold_start_under_exposed` | 0.10 | 40-180 | 0.08-0.20 | 0.02-0.08 | `x+0.10` to `x+0.25` | `y-0.20` to `y-0.05` | `RESCUE` |
| `invalid_payload_burst` | 0.05 | N/A | N/A | N/A | N/A | N/A | invalid sink |

### 7.1 Scenario Decision-Target Predicates

To keep expected actions deterministic, each scenario must satisfy the target decision predicates in the metric contract:

1. `viral_high_quality`: enforce `candidate = true` and `quality_gate = pass`
2. `viral_low_quality`: enforce `candidate = true` and `quality_gate = fail`
3. `cold_start_under_exposed`: enforce `candidate = false`, `quality_gate = pass`, `upload_age <= 60m`, `under_exposed = true`
4. `normal_baseline`: enforce no decision predicate path reaches actionable output

Implementation note:

1. if percentile movement risks violating expected scenario action, generator should rebalance within-range engagement ratios to preserve the target predicate outcome.

---

## 8. QA Validation Layer Contract (M1 Minimal)

QA tables are validation-only and are not consumer-facing serving tables.

### 8.1 `lakehouse.qa.run_manifest`

Role:

1. run-level audit and reproducibility metadata

Grain:

1. `run_id`

Required fields:

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

### 8.2 `lakehouse.qa.expected_actions`

Role:

1. deterministic ground truth for decision accuracy checks

Grain:

1. `run_id + video_id + window_start`

Required fields:

1. `run_id` STRING
2. `video_id` STRING
3. `window_start` TIMESTAMP
4. `window_end` TIMESTAMP
5. `scenario_id` STRING
6. `expected_action` STRING
7. `expected_reason_codes` ARRAY<STRING>
8. `generated_at` TIMESTAMP

Storage guidance:

1. use Iceberg tables under `lakehouse.qa` namespace
2. `expected_actions` recommended partitioning: `days(window_start)` and `bucket(16, video_id)`

---

## 9. Acceptance Criteria (M1)

1. same `seed + run_config` produces stable scenario-level distributions
2. generator always emits CDC bootstrap before content events
3. valid scenarios achieve `event -> dim_videos` join coverage >= 99.5%
4. expected action hit rate >= 90% against decision outputs
5. invalid scenario routes malformed records to invalid sinks with success rate >= 99%
6. every run is traceable through `lakehouse.qa.run_manifest` and `lakehouse.qa.expected_actions`

---

## 10. Linked Docs

1. `docs/product/business-decision-prd-kpi-tree.md`
2. `docs/architecture/realtime-decisioning/metric-contract.md`
3. `docs/architecture/realtime-decisioning/action-queue-contract.md`
4. `docs/architecture/messaging/kafka-topic-schema-retention-contract-m1.md`
5. `docs/architecture/streaming/spark-realtime-jobs-contract-m1.md`
6. `docs/architecture/data-model/m1-data-model-v1.md`
