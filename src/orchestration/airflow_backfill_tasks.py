"""Task callables for the batch_backfill DAG."""

from __future__ import annotations

from orchestration.airflow_batch_dates import date_range


def build_trigger_confs(start_date: str, end_date: str) -> list[dict]:
    """Validate the date range and return one TriggerDagRunOperator conf per date.

    Raises ValueError (propagated from date_range) if start_date > end_date,
    end_date is not in the past, or the range exceeds 90 days.
    """
    dates = date_range(start_date, end_date)
    print(f"[BACKFILL] {len(dates)} date(s): {start_date} → {end_date}")
    return [{"data_date": d} for d in dates]
