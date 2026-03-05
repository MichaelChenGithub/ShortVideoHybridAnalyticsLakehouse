# Generator Contracts

This folder contains synthetic data generator contracts used to validate realtime decisioning behavior.

## Current Specs

1. `mock-event-generator-contract-scenario-matrix-m1.md`

## MIC-34 Implementation Entry

CLI entrypoint:

1. `python src/generator/m1_bounded_run.py --config docs/architecture/generator/examples/m1_run_config.example.json --sink dry-run`

Notes:

1. `--sink dry-run` writes local artifacts without Kafka dependency.
2. `--sink kafka --bootstrap-servers localhost:9092` emits to Kafka topics:
   - `content_events`
   - `cdc.content.videos`
3. default execution uses simulated clock (fast), not wall-clock sleeping.
4. use `--real-time` only when you intentionally want true wait behavior.

Artifacts per run:

1. `artifacts/generator_runs/<run_id>/run_config.json`
2. `artifacts/generator_runs/<run_id>/video_registry.parquet`
3. `artifacts/generator_runs/<run_id>/expected_actions.parquet`
4. `artifacts/generator_runs/<run_id>/run_summary.json`

Scope guard:

1. MIC-34 does not write `lakehouse.qa.*` tables.

Acceptance report template:

1. `docs/architecture/generator/m1_acceptance_report_template.md`
