# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Collaboration First

If any concern, ambiguity, or scope conflict appears, discuss with maintainers before implementing. `docs/` is the source of truth for scope and data contracts — validate against relevant contract docs before coding.

## Commands

```bash
# Setup
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# Tests
.venv/bin/python -m pytest                                            # full suite
.venv/bin/python -m pytest tests/test_rt_action_decisioning.py       # single file

# Infrastructure
docker compose up -d                                                  # full stack
docker compose up -d spark                                            # single service

# Acceptance runners (require docker stack)
bash src/scripts/run_realtime_signoff_acceptance.sh
bash src/scripts/run_content_aggregator_acceptance.sh
bash src/scripts/run_cdc_upsert_acceptance.sh
bash src/scripts/prepare_airflow_batch_demo_env.sh
```

Before any PR: run `pytest` and the relevant acceptance script for the touched domain.

## Architecture

This is a **contract-driven analytics platform** for short-video operations, built on Apache Iceberg + Spark + Trino. It has two parallel processing paths:

### Realtime Path (1-minute cadence)
```
Kafka (content_events + cdc.videos)
  → Spark Structured Streaming (contract enforcement)
  → Iceberg tables (bronze → dims → gold)
  → Trino semantic views
  → Metabase dashboard
  → BOOST | REVIEW | RESCUE decisions
```

### Batch Path (Daily at 8 AM ET, processes D-1)
```
Bronze Iceberg tables
  → Spark batch jobs (SCD2 dims, sessions, retention, engagement)
  → Quality gates verification
  → Trino semantic serving views
```

### Key modules

- **`dags/batch_publish_daily.py`** — Airflow DAG (daily 8 AM ET). Task groups: resolve data date → conformed-events → sessionization → batch-gold-metrics → quality-gates → publish-and-evidence.
- **`src/orchestration/airflow_batch_tasks.py`** — `SPARK_BATCH_SPECS` registry mapping job names to Spark scripts; helpers that build `docker exec` commands from Airflow.
- **`src/spark/`** — Realtime Spark jobs (prefix `rt_`) and batch Spark jobs (prefix `bt_`). Each file is scoped to one contract surface.
- **`src/generator/bounded_run/`** — Synthetic event/CDC generator used in acceptance testing.
- **`src/scripts/`** — Acceptance runners (`run_*.sh`) and contract verifiers (`verify_*.py`).
- **`src/trino/`** — Semantic serving SQL for both realtime (`rt_video_metrics_serving.sql`) and batch (`bt_semantic_serving.sql`).
- **`tests/`** — Mirrors `src/` structure; uses `pytest` with `unittest`-style classes. Deterministic (fixed seeds, explicit timestamps).
- **`docs/architecture/`** — Contracts for each domain (realtime-decisioning, data-model, messaging, streaming, serving). Read before changing behavior.
- **`artifacts/`** — Generated run evidence and acceptance outputs (not committed).

### Local infrastructure (docker-compose)

| Service | Port | Role |
|---------|------|------|
| MinIO | 9000/9001 | S3-compatible storage (`warehouse/`, `checkpoints/`) |
| iceberg-rest | 8181 | Iceberg REST catalog (PostgreSQL backend) |
| Kafka | 9092 | Event streaming |
| Spark | 4040/8080 | Processing engine |
| Trino | 8081 | SQL query layer over Iceberg |
| Airflow | 8082 | Batch DAG orchestration (SQLite, SequentialExecutor) |
| Metabase | 3001 | BI dashboard |
| Grafana | 3000 | Monitoring (Trino datasource) |

Airflow invokes Spark jobs via `docker exec` into the running `spark` container.

## Coding Conventions

- Python 3.10, 4-space indentation, explicit type-safe logic.
- File naming: `snake_case.py`; tests: `test_<module_or_behavior>.py`; scripts: `run_<domain>_acceptance.sh` / `verify_<contract>.py`.
- Each module covers one contract surface — do not mix CDC, aggregation, and decisioning concerns in a single file.
- Commit style: `type: concise summary` (e.g., `feat:`, `fix:`, `docs:`, `refactor:`). Scope commits to one change theme; include docs/tests when behavior changes.
- PRs must include: what/why, linked issue, verification evidence (commands + results), artifact paths if serving outputs changed.
