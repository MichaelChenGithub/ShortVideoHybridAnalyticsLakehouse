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

# Local infrastructure — use the Makefile targets (source of truth)
make up                 # Full reset + seed in one shot (reset-infra then seed-bronze)
make reset-infra        # Tear down, clean state, and rebuild the core pipeline stack
make seed-bronze        # Start streaming jobs, run generator, drain, and verify MinIO
make integration-test   # make up + run all 6 acceptance scripts end-to-end
make down               # Stop all containers and remove named volumes
make clean              # reset-infra + wipe ivy_cache (use when deps are corrupted)
make help               # List all available targets

# Airflow Docker image (build from repo root)
docker build -f short-video-lakehouse-airflow/Dockerfile \
  -t short-video-lakehouse-airflow .

# AWS — push Airflow image to ECR
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin \
    026177432704.dkr.ecr.us-east-1.amazonaws.com
docker tag short-video-lakehouse-airflow:latest \
  026177432704.dkr.ecr.us-east-1.amazonaws.com/short-video-lakehouse-airflow:latest
docker push \
  026177432704.dkr.ecr.us-east-1.amazonaws.com/short-video-lakehouse-airflow:latest

# AWS — deploy stack (run from terraform/)
cd terraform
terraform init
terraform apply \
  -var 'airflow_image=026177432704.dkr.ecr.us-east-1.amazonaws.com/short-video-lakehouse-airflow:latest'

# AWS — shut down (between demos to avoid idle cost)
# Destroys idle-cost resources: MSK (~$54/mo), NAT Gateway (~$32/mo), Metabase ECS (~$32/mo)
# S3 data and Glue catalog are preserved (force_destroy=false)
terraform destroy \
  -target=aws_msk_serverless_cluster.main \
  -target=aws_nat_gateway.main \
  -target=aws_eip.nat \
  -target=aws_ecs_service.metabase
```

Before any PR: build `.venv` first (`python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`), then run `pytest`. For changes touching acceptance scripts or Spark jobs, run `make integration-test`.

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
  → dbt quality gates (run + test against WAP branch via Trino)
  → Trino semantic serving views
```

### Key modules

- **`dbt_project.yml` / `models/batch/semantic/`** — dbt project run by the `quality-gates` DAG task. Executes `dbt run` + `dbt test` against the current WAP branch via Trino (`ICEBERG_WAP_BRANCH` env var is injected at runtime so all queries target the run branch, not main). `warn`-severity tests are non-blocking; `error`-severity tests fail the DAG task.
- **`dags/batch_publish_daily.py`** — Airflow DAG (daily 8 AM ET). Task groups: resolve data date → conformed-events → sessionization → batch-gold-metrics → quality-gates → publish-and-evidence.
- **`src/orchestration/airflow_batch_tasks.py`** — `SPARK_BATCH_SPECS` registry mapping job names to Spark scripts. Local dev: `docker exec` into Spark container. AWS: boto3 EMR Serverless `start_job_run` when `EMR_APPLICATION_ID` env var is set.
- **`src/spark/`** — Realtime Spark jobs (prefix `rt_`) and batch Spark jobs (prefix `bt_`). Each file is scoped to one contract surface.
- **`src/generator/bounded_run/`** — Synthetic event/CDC generator used in acceptance testing.
- **`src/scripts/`** — 6 integration acceptance scripts (`run_*_acceptance.sh`), contract verifiers (`verify_*.py`), and `common.sh` (shared `resolve_bounded_run_started_at` utility). Scripts assume infra is up (`make reset-infra`); use `make integration-test` to run them all.
- **`src/trino/`** — Semantic serving SQL for both realtime (`rt_video_metrics_serving.sql`) and batch (`bt_semantic_serving.sql`).
- **`src/metabase/realtime-metrics-sql-pack.sql`** — Pre-built SQL queries for the Metabase realtime metrics dashboard.
- **`tests/`** — Mirrors `src/` structure; uses `pytest` with `unittest`-style classes. Deterministic (fixed seeds, explicit timestamps).
- **`docs/architecture/`** — Contracts for each domain (realtime-decisioning, data-model, messaging, streaming, serving). Read before changing behavior.
- **`artifacts/`** — Generated run evidence and acceptance outputs (not committed).

### Local infrastructure (docker-compose)

| Service | Port | Role |
|---------|------|------|
| MinIO | 9000/9001 | S3-compatible storage (`warehouse/`, `checkpoints/`) |
| iceberg-rest | 8181 | Iceberg REST catalog (PostgreSQL backend) |
| Kafka | 9092 | Event streaming |
| Spark | 9090/8080 | Processing engine |
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
