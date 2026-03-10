# MIC-34 Acceptance Report Template (M1 Bounded Generator)

Use this template to record one bounded-run acceptance check for MIC-34.

## 1. Run Metadata

- `run_id`:
- `seed`:
- `rule_version`:
- `schema_version`:
- `sink_mode` (`dry-run` / `kafka`):
- `started_at` (config):
- `executor`:
- `date`:

## 2. Commands Executed

- Unit tests:

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```

- Generator run:

```bash
python3 src/generator/m1_bounded_run.py \
  --config docs/architecture/generator/examples/m1_run_config.example.json \
  --run-id <run_id> \
  --sink dry-run
```

## 3. Artifacts Produced

Expected paths:

1. `artifacts/generator_runs/<run_id>/run_config.json`
2. `artifacts/generator_runs/<run_id>/video_registry.parquet`
3. `artifacts/generator_runs/<run_id>/expected_actions.parquet`
4. `artifacts/generator_runs/<run_id>/run_summary.json`

Actual paths:

- `run_config`:
- `video_registry`:
- `expected_actions`:
- `run_summary`:

## 4. Acceptance Criteria Check

### 4.1 Stable Output Distributions

Source: `run_summary.json` (`acceptance`, `scenario_mix_abs_error`, `scenario_mix_realized`)

- `scenario_abs_error_max <= 0.02`: `PASS / FAIL`
- `total_volume_error_ratio <= 0.05`: `PASS / FAIL`
- Notes:

### 4.2 Run Starts/Stops Within Configured Duration

Source: `run_summary.json` (`lifecycle`)

- `content_started_at - cdc_bootstrap_emitted_at == 300s`: `PASS / FAIL`
- `content_ended_at - content_started_at == duration_minutes * 60`: `PASS / FAIL`
- Notes:

### 4.3 Run Config Traceability

Source: `run_config.json` + `run_summary.json`

- Config file exists and matches executed run: `PASS / FAIL`
- `run_summary` references artifact paths for this `run_id`: `PASS / FAIL`
- Notes:

## 5. Contract Checks (M1)

### 5.1 CDC Contract

- CDC emitted before content stream: `PASS / FAIL`
- CDC key equals `after.video_id`: `PASS / FAIL`
- Required CDC fields present (`op`, `ts_ms`, `schema_version`, `after.*`): `PASS / FAIL`
- Notes:

### 5.2 Content Event Contract

- Required fields present (`event_id`, `event_timestamp`, `video_id`, `user_id`, `event_type`, `schema_version`, `payload_json`) except intentional invalid payload scenario: `PASS / FAIL`
- Event types only in allowed enum: `PASS / FAIL`
- Notes:

### 5.3 Late Event Contract

- `late_offset_min_seconds >= 121`: `PASS / FAIL`
- `late_offset_max_seconds <= 210`: `PASS / FAIL`
- Late histogram exists (`121_150`, `151_210`): `PASS / FAIL`
- Notes:

## 6. Scope Guard (MIC-34)

- No writes to `lakehouse.qa.run_manifest`: `PASS / FAIL`
- No writes to `lakehouse.qa.expected_actions`: `PASS / FAIL`
- `qa_table_writes_attempted == false`: `PASS / FAIL`
- Notes:

## 7. Final Verdict

- Overall result: `PASS / FAIL`
- Blockers:
- Follow-up issues:
- Reviewer:
- Sign-off date:
