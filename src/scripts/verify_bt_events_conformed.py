"""Verify batch events_conformed contract health for a target data_date."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Mapping, Sequence

DEFAULT_TABLE = "lakehouse.silver.events_conformed"
ET_TIMEZONE = "America/New_York"
ALLOWED_EVENT_TYPES = (
    "impression",
    "play_start",
    "play_finish",
    "like",
    "share",
    "skip",
)


def validate_events_conformed_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    expected_data_date: str,
    min_row_count: int,
) -> list[str]:
    errors: list[str] = []

    if len(rows) < min_row_count:
        errors.append(
            f"Expected at least {min_row_count} rows for data_date={expected_data_date}, "
            f"found {len(rows)}"
        )
        return errors

    seen_event_ids: set[str] = set()
    duplicate_event_ids: set[str] = set()

    for row in rows:
        event_id = row.get("event_id")
        event_timestamp = row.get("event_timestamp")
        event_date_et = row.get("event_date_et")
        data_date = row.get("data_date")
        video_id = row.get("video_id")
        user_id = row.get("user_id")
        event_type = row.get("event_type")
        watch_time_ms = row.get("watch_time_ms")
        category = row.get("category")
        region = row.get("region")

        if event_id is None:
            errors.append("event_id must be non-null")
        else:
            event_id_str = str(event_id)
            if event_id_str in seen_event_ids:
                duplicate_event_ids.add(event_id_str)
            seen_event_ids.add(event_id_str)

        if event_timestamp is None:
            errors.append("event_timestamp must be non-null")
        if event_date_et is None:
            errors.append("event_date_et must be non-null")
        if data_date is None:
            errors.append("data_date must be non-null")
        if video_id is None:
            errors.append("video_id must be non-null")
        if user_id is None:
            errors.append("user_id must be non-null")
        if category is None:
            errors.append("category must be non-null")
        if region is None:
            errors.append("region must be non-null")

        if event_type is None:
            errors.append("event_type must be non-null")
        else:
            normalized_event_type = str(event_type).strip().lower()
            if normalized_event_type not in ALLOWED_EVENT_TYPES:
                errors.append(f"event_type must be one of {ALLOWED_EVENT_TYPES}, got={event_type}")

        if watch_time_ms is None:
            errors.append("watch_time_ms must be non-null")
        elif int(watch_time_ms) < 0:
            errors.append(f"watch_time_ms must be non-negative for event_id={event_id}, got={watch_time_ms}")

        if data_date is not None and str(data_date) != expected_data_date:
            errors.append(
                f"data_date mismatch for event_id={event_id}: "
                f"expected={expected_data_date}, actual={data_date}"
            )

        if event_date_et is not None and data_date is not None and str(event_date_et) != str(data_date):
            errors.append(
                f"event_date_et/data_date mismatch for event_id={event_id}: "
                f"event_date_et={event_date_et}, data_date={data_date}"
            )

    if duplicate_event_ids:
        sample = sorted(duplicate_event_ids)[:5]
        errors.append(
            "Duplicate event_id rows found (sample up to 5): "
            + ", ".join(sample)
        )

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify events_conformed contract health")
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument(
        "--data-date",
        help="Target business data_date in YYYY-MM-DD. Defaults to ET D-1 inside Spark.",
    )
    parser.add_argument("--min-row-count", type=int, default=1)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit

    args = _parse_args(argv)
    spark = SparkSession.builder.appName("verify_bt_events_conformed").getOrCreate()

    if args.data_date:
        target_data_date = args.data_date
    else:
        target_data_date = spark.sql(
            "SELECT CAST(date_sub(to_date(from_utc_timestamp(current_timestamp(), "
            f"'{ET_TIMEZONE}')), 1) AS STRING) AS data_date"
        ).collect()[0]["data_date"]

    table_df = spark.read.format("iceberg").load(args.table)
    rows = [
        row.asDict(recursive=True)
        for row in table_df.filter(col("data_date") == lit(target_data_date)).collect()
    ]

    errors = validate_events_conformed_rows(
        rows,
        expected_data_date=target_data_date,
        min_row_count=args.min_row_count,
    )

    if errors:
        print("FAIL: events_conformed verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    printable = {
        "table": args.table,
        "data_date": target_data_date,
        "row_count": len(rows),
        "min_row_count": args.min_row_count,
        "allowed_event_types": list(ALLOWED_EVENT_TYPES),
        "timezone": ET_TIMEZONE,
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }
    print("PASS: events_conformed verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
