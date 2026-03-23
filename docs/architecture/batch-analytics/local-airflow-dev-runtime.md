# Local Airflow Dev Runtime

Status: Draft

## 1. Purpose

Define the local `docker compose` proving ground for the Airflow batch DAG before the same orchestration contract is promoted to `MWAA`.

This document covers the MIC-163 runtime boundary only:

1. local Airflow startup
2. DAG loading
3. canonical `data_date` proof for `D-1` in `America/New_York`

Real batch execution wiring remains deferred to `MIC-164`.

## 2. Prerequisites

1. Docker Desktop or equivalent local Docker runtime
2. repo checkout at the branch containing MIC-163
3. ability to run `docker compose` from the repo root

## 3. Start the Runtime

From the repo root:

```bash
docker compose up -d airflow
```

Local access:

1. Airflow UI: `http://localhost:8082`
2. username: `admin`
3. password: `admin`

The local runtime mounts:

1. `./dags` to `/opt/airflow/dags`
2. `./src` to `/home/iceberg/local/src`

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

This MIC-163 scaffold should load one DAG with the TaskGroup layout from the Airflow orchestration spec:

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

## 6. Runtime Boundary

MIC-163 intentionally does not implement:

1. real Spark `spark-submit` execution from Airflow
2. dbt quality gate execution
3. `lakehouse.gold.batch_publish_manifest` writes
4. publish-ready signal emission
5. evidence packaging beyond scaffold tasks

Those execution behaviors are reserved for `MIC-164`.
