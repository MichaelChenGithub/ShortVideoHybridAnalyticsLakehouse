"""Verify batch user_activity_sessions_30m contract health for a target data_date."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Mapping, Sequence

DEFAULT_TABLE = "lakehouse.silver.user_activity_sessions_30m"
ALLOWED_USER_STATES = ("new", "returning", "unknown")
REQUIRED_COLUMNS = (
    "session_id",
    "user_id",
    "session_start_ts",
    "session_end_ts",
    "category",
    "region",
    "new_vs_returning_user",
    "session_duration_sec",
    "event_count",
    "watch_time_sum_ms",
    "data_date",
)


def validate_user_activity_sessions_30m_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    expected_data_date: str,
    min_row_count: int,
) -> list[str]:
    errors: list[str] = []

    if len(rows) < min_row_count:
        errors.append(
            f"Expected at least {min_row_count} rows for data_date={expected_data_date}, found {len(rows)}"
        )
        return errors

    seen_session_ids: set[str] = set()
    duplicate_session_ids: set[str] = set()

    for row in rows:
        session_id = row.get("session_id")
        user_id = row.get("user_id")
        session_start_ts = row.get("session_start_ts")
        session_end_ts = row.get("session_end_ts")
        category = row.get("category")
        region = row.get("region")
        new_vs_returning_user = row.get("new_vs_returning_user")
        session_duration_sec = row.get("session_duration_sec")
        event_count = row.get("event_count")
        watch_time_sum_ms = row.get("watch_time_sum_ms")
        data_date = row.get("data_date")

        if session_id is None:
            errors.append("session_id must be non-null")
        else:
            session_id_str = str(session_id)
            if session_id_str in seen_session_ids:
                duplicate_session_ids.add(session_id_str)
            seen_session_ids.add(session_id_str)

        if user_id is None:
            errors.append("user_id must be non-null")
        if session_start_ts is None:
            errors.append(f"session_start_ts must be non-null for session_id={session_id}")
        if session_end_ts is None:
            errors.append(f"session_end_ts must be non-null for session_id={session_id}")
        if category is None:
            errors.append(f"category must be non-null for session_id={session_id}")
        if region is None:
            errors.append(f"region must be non-null for session_id={session_id}")

        if new_vs_returning_user is None:
            errors.append(f"new_vs_returning_user must be non-null for session_id={session_id}")
        elif str(new_vs_returning_user) not in ALLOWED_USER_STATES:
            errors.append(
                "new_vs_returning_user must be one of "
                f"{ALLOWED_USER_STATES} for session_id={session_id}, got={new_vs_returning_user}"
            )

        if session_start_ts is not None and session_end_ts is not None and session_end_ts < session_start_ts:
            errors.append(
                f"session_end_ts must be >= session_start_ts for session_id={session_id}"
            )

        if session_duration_sec is None:
            errors.append(f"session_duration_sec must be non-null for session_id={session_id}")
        elif int(session_duration_sec) < 0:
            errors.append(
                f"session_duration_sec must be non-negative for session_id={session_id}, got={session_duration_sec}"
            )

        if event_count is None:
            errors.append(f"event_count must be non-null for session_id={session_id}")
        elif int(event_count) < 1:
            errors.append(f"event_count must be >= 1 for session_id={session_id}, got={event_count}")

        if watch_time_sum_ms is None:
            errors.append(f"watch_time_sum_ms must be non-null for session_id={session_id}")
        elif int(watch_time_sum_ms) < 0:
            errors.append(
                f"watch_time_sum_ms must be non-negative for session_id={session_id}, got={watch_time_sum_ms}"
            )

        if data_date is None:
            errors.append(f"data_date must be non-null for session_id={session_id}")
        elif str(data_date) != expected_data_date:
            errors.append(
                f"data_date mismatch for session_id={session_id}: expected={expected_data_date}, actual={data_date}"
            )

    if duplicate_session_ids:
        sample = sorted(duplicate_session_ids)[:5]
        errors.append("Duplicate session_id rows found (sample up to 5): " + ", ".join(sample))

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify user_activity_sessions_30m contract health")
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--data-date", required=True)
    parser.add_argument("--min-row-count", type=int, default=1)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit

    args = _parse_args(argv)
    spark = SparkSession.builder.appName("verify_bt_user_activity_sessions_30m").getOrCreate()

    table_df = spark.read.format("iceberg").load(args.table)
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in table_df.columns]
    if missing_columns:
        print("FAIL: user_activity_sessions_30m verification failed")
        print(" - Missing required columns: " + ", ".join(missing_columns))
        return 1

    rows = [
        row.asDict(recursive=True)
        for row in table_df.filter(col("data_date") == lit(args.data_date)).collect()
    ]

    errors = validate_user_activity_sessions_30m_rows(
        rows,
        expected_data_date=args.data_date,
        min_row_count=args.min_row_count,
    )

    if errors:
        print("FAIL: user_activity_sessions_30m verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    printable = {
        "table": args.table,
        "data_date": args.data_date,
        "row_count": len(rows),
        "min_row_count": args.min_row_count,
        "allowed_user_states": list(ALLOWED_USER_STATES),
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }
    print("PASS: user_activity_sessions_30m verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
