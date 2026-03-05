# Generator Contracts

This folder contains synthetic data generator contracts used to validate realtime decisioning behavior.

## Current Specs

1. `mock-event-generator-contract-scenario-matrix-m1.md`

## MIC-34 Implementation Entry

CLI entrypoint:

1. `python src/generator/m1_bounded_run.py --config docs/architecture/generator/examples/m1_run_config.example.json --sink dry-run`

Notes:

1. `--sink dry-run` writes local artifacts without Kafka dependency.
2. `--sink kafka --bootstrap-servers localhost:9092` bootstraps topics before emission:
   - missing topics are created with M1 local/dev defaults:
     - `content_events`: 6 partitions, replication factor 1
     - `cdc.content.videos`: 3 partitions, replication factor 1
3. startup validation runs immediately after bootstrap:
   - required topics must be present: `content_events`, `cdc.content.videos`
   - partition minimums are enforced (defaults: `content_events >= 6`, `cdc.content.videos >= 3`)
   - replication-factor minimums are enforced (defaults: both topics `>= 1`)
   - misconfigured existing topics fail fast with clear preflight error output
4. `--sink kafka --bootstrap-servers localhost:9092` emits to Kafka topics:
   - `content_events`
   - `cdc.content.videos`
5. partition minimums can be overridden with:
   - `--content-events-min-partitions`
   - `--cdc-videos-min-partitions`
6. default execution uses simulated clock (fast), not wall-clock sleeping.
7. use `--real-time` only when you intentionally want true wait behavior.

Kafka bootstrap + validation example:

1. `python src/generator/m1_bounded_run.py --config docs/architecture/generator/examples/m1_run_config.example.json --sink kafka --bootstrap-servers localhost:9092 --content-events-min-partitions 6 --cdc-videos-min-partitions 3`

Artifacts per run:

1. `artifacts/generator_runs/<run_id>/run_config.json`
2. `artifacts/generator_runs/<run_id>/video_registry.parquet`
3. `artifacts/generator_runs/<run_id>/expected_actions.parquet`
4. `artifacts/generator_runs/<run_id>/run_summary.json`

Scope guard:

1. MIC-34 does not write `lakehouse.qa.*` tables.

Acceptance report template:

1. `docs/architecture/generator/m1_acceptance_report_template.md`
