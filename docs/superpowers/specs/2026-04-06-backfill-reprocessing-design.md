# Batch Backfill & Parameterized Reprocessing — Design Spec

**Date:** 2026-04-06  
**Branch:** `mic-batch-backfill-reprocessing`  
**Status:** Approved

---

## Overview

Add a `batch_backfill` Airflow DAG that allows operators to reprocess any contiguous range of historical dates through the full batch pipeline. The daily DAG (`batch_publish_daily`) is unchanged.

---

## Architecture

### New DAG: `dags/batch_backfill.py`

- **Schedule:** `None` — manually triggered only (Airflow UI or CLI)
- **`max_active_runs=1`** — prevents concurrent backfills from racing on cumulative/SCD2 tables
- **Params:**
  - `start_date` — `YYYY-MM-DD`, first date to reprocess (inclusive)
  - `end_date` — `YYYY-MM-DD`, last date to reprocess (inclusive)

### Task Graph

```
validate_date_range → run_backfill_sequential
```

**`validate_date_range`**  
Parses and validates the `start_date` / `end_date` params, generates a chronologically ordered list of date strings, and pushes it to XCom.

**`run_backfill_sequential`**  
Pulls the date list from XCom and processes each date in order, running the full pipeline per date. Uses existing helpers from `airflow_batch_tasks.py` — no logic is duplicated.

### New date utility: `airflow_batch_dates.py`

Add `date_range(start_date: str, end_date: str) -> list[str]` — generates an ordered list of `YYYY-MM-DD` strings between start and end inclusive. Validation lives here too.

---

## Per-Date Pipeline

For each date the sequential loop executes these steps in order, identical to the daily DAG:

```
create_iceberg_branch(run_id=f"backfill_{date}")
  → run_spark_batch_job("events_conformed",            data_date, wap_branch)
  → run_spark_batch_job("dim_users_scd2",              data_date, wap_branch)
  → run_spark_batch_job("dim_videos_scd2",             data_date, wap_branch)
  → run_spark_batch_job("user_activity_sessions_30m",  data_date, wap_branch)
  → run_spark_batch_job("batch_sessionization_daily",  data_date, wap_branch)
  → run_spark_batch_job("batch_retention_daily",       data_date, wap_branch)
  → run_spark_batch_job("batch_engagement_daily",      data_date, wap_branch)
  → run_dbt_quality_gates(data_date, wap_branch)
  → merge_coordinator_task(branch_name)
  → cleanup_branch_task(branch_name)   ← always runs via try/finally
```

**WAP branch naming:** `backfill_{date}` where hyphens in the date are replaced with underscores (e.g. `2026-03-03` → `backfill_2026_03_03`) — distinct from the daily DAG's `run_*` prefix, no collision risk.

---

## Error Handling & Idempotency

**Idempotency:** All existing Spark jobs use DELETE + INSERT per `data_date`. Reprocessing a date twice produces the same result — no changes needed in the Spark layer.

**Cleanup guarantee:** `cleanup_branch_task` is called inside a `try/finally` block within the loop. Stale WAP branches never accumulate, even if a Spark job or dbt gate fails.

**Failure recovery:** If the backfill fails on date N, dates before N are already merged to main and safe. Re-trigger the DAG with `start_date=N` to resume from the failure point.

**Validation guardrails** (enforced in `validate_date_range`):

| Check | Behaviour |
|---|---|
| `start_date > end_date` | Raises `ValueError` |
| `end_date >= today` | Raises `ValueError` (no future dates) |
| Range > 90 days | Raises `ValueError` (prevents accidental mass reprocessing) |

---

## Testing

### `tests/test_airflow_batch_dates.py`

Unit tests for the new `date_range` utility:

- Valid range generates correct ordered list of dates
- Single date (start == end) returns list of one
- `start_date > end_date` raises `ValueError`
- Range > 90 days raises `ValueError`
- Future `end_date` raises `ValueError`

### `tests/test_batch_backfill.py`

Unit tests for the DAG task callables (all helpers mocked — no Docker or Spark):

- `validate_date_range` pushes correct date list to XCom
- `run_backfill_sequential` calls pipeline helpers in correct order for each date
- `cleanup_branch_task` is called even when a Spark job raises
- Loop stops on first failing date and re-raises the exception

---

## Files Changed

| File | Change |
|---|---|
| `dags/batch_backfill.py` | New — backfill DAG |
| `src/orchestration/airflow_batch_dates.py` | Add `date_range()` utility + validation |
| `tests/test_airflow_batch_dates.py` | New — unit tests for `date_range` |
| `tests/test_batch_backfill.py` | New — unit tests for backfill DAG tasks |

No changes to `batch_publish_daily.py`, `airflow_batch_tasks.py`, or any Spark job.
