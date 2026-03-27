# Local Airflow Dev Runtime

Status: Draft

## 1. Purpose

Define the local `docker compose` proving ground for the Airflow batch DAG before the same orchestration contract is promoted to `MWAA`.

This document covers the MIC-163 runtime boundary only:

1. local Airflow startup
2. DAG loading
3. canonical `data_date` proof for `D-1` in `America/New_York`
4. local batch execution wiring for the daily publish path

## 2. Prerequisites

1. Docker Desktop or equivalent local Docker runtime
2. repo checkout at the branch containing MIC-163
3. ability to run `docker compose` from the repo root
4. local `lakehouse-spark` service available because Airflow executes batch jobs via `docker exec`

## 3. Start the Runtime

From the repo root:

```bash
docker compose up -d spark
docker compose up -d airflow
```

Local access:

1. Airflow UI: `http://localhost:8082`
2. username: `admin`
3. password: `admin`

The local runtime mounts:

1. `./dags` to `/opt/airflow/dags`
2. `./src` to `/home/iceberg/local/src`
3. `/var/run/docker.sock` to `/var/run/docker.sock`

The container `PYTHONPATH` includes both paths so DAG code can import repo helpers directly.

## 4. DAG Location and Verification

The local batch DAG lives at:

1. `dags/batch_publish_daily.py`

Verify that Airflow loaded the DAG:

1. open the Airflow UI and confirm `batch_publish_daily` appears
2. or inspect the container logs:

```bash
docker logs --tail 200 lakehouse-airflow
```

The local DAG should load with this TaskGroup layout from the Airflow orchestration spec:

1. `conformed-events`
2. `sessionization`
3. `batch-gold-metrics`
4. `quality-gates`
5. `publish-and-evidence`

## 5. Canonical `data_date` Proof

The DAG resolves one canonical batch `data_date` at the start of each run.

Rule:

1. business timezone is `America/New_York`
2. canonical `data_date` is ET business date minus one day (`D-1`)

The resolution task logs both the Airflow logical date and the canonical `data_date`.

To verify:

1. trigger the DAG from the UI
2. open task logs for `resolve_data_date`
3. confirm a log line like:

```text
[AIRFLOW-BATCH] logical_date=... canonical_data_date=2026-03-20
```

## 6. Local Execution Path

The local DAG executes these batch tasks for one shared canonical `data_date`:

1. `src/spark/bt_events_conformed.py`
2. `src/spark/bt_dim_users_scd2.py`
3. `src/spark/bt_dim_videos_scd2.py`
4. `src/spark/bt_user_activity_sessions_30m.py`
5. `src/spark/bt_retention_daily.py`
6. `src/spark/bt_engagement_daily.py`
7. `src/spark/bt_sessionization_daily.py`
8. gold quality gates via:
   - `src/scripts/verify_bt_retention_daily.py`
   - `src/scripts/verify_bt_engagement_daily.py`
   - `src/scripts/verify_bt_sessionization_daily.py`

The DAG starts at the batch bronze-to-silver normalization boundary (`bt_events_conformed.py`) and
builds the required SCD2 dimension tables needed by downstream silver/gold jobs.
It does not start or manage upstream bronze ingestion services such as CDC/realtime producers.
This local runtime uses Docker socket access as its submission mechanism; future cloud execution can replace
the underlying command path without changing the DAG dependency graph.

## 7. Runtime Boundary

Current local scope still does not implement:

1. `lakehouse.gold.batch_publish_manifest` writes
2. publish-ready signal emission
3. evidence packaging beyond deferred placeholder tasks
4. a real in-repo `dbt` project; local quality gates currently use verifier scripts instead

Those publish/evidence behaviors are reserved for `MIC-165` and follow-on work.

## 8. Task Timeouts

The local DAG uses explicit Airflow `execution_timeout` values for external-process tasks:

1. `resolve_data_date`: `5` minutes
2. Spark batch build tasks: `30` minutes
3. gold quality gates: `10` minutes
4. deferred publish/evidence placeholders: `2` minutes
