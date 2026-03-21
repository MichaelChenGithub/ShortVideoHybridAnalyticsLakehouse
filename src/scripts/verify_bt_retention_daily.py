"""Verify batch retention_daily contract health for a target data_date."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from math import isclose
from typing import Any, Mapping, Sequence

DEFAULT_TABLE = "lakehouse.gold.batch_retention_daily"
ET_TIMEZONE = "America/New_York"
ALLOWED_DAY_N = (1, 7)
ALLOWED_NEW_VS_RETURNING_USER = ("new", "returning", "unknown")


def validate_batch_retention_daily_rows(
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

    seen_keys: set[tuple[str, int, str, str, str]] = set()
    duplicate_keys: set[tuple[str, int, str, str, str]] = set()

    for row in rows:
        cohort_date = row.get("cohort_date")
        day_n = row.get("day_n")
        category = row.get("category")
        region = row.get("region")
        new_vs_returning_user = row.get("new_vs_returning_user")
        cohort_users = row.get("cohort_users")
        retained_users = row.get("retained_users")
        retention_rate = row.get("retention_rate")
        data_date = row.get("data_date")
        published_at = row.get("published_at")

        if cohort_date is None:
            errors.append("cohort_date must be non-null")
        if day_n is None:
            errors.append("day_n must be non-null")
        if category is None:
            errors.append("category must be non-null")
        if region is None:
            errors.append("region must be non-null")
        if new_vs_returning_user is None:
            errors.append("new_vs_returning_user must be non-null")
        if cohort_users is None:
            errors.append("cohort_users must be non-null")
        if retained_users is None:
            errors.append("retained_users must be non-null")
        if data_date is None:
            errors.append("data_date must be non-null")
        if published_at is None:
            errors.append("published_at must be non-null")

        normalized_day_n: int | None = None
        if day_n is not None:
            normalized_day_n = int(day_n)
            if normalized_day_n not in ALLOWED_DAY_N:
                errors.append(f"day_n must be one of {ALLOWED_DAY_N}, got={day_n}")

        normalized_segment: str | None = None
        if new_vs_returning_user is not None:
            normalized_segment = str(new_vs_returning_user).strip().lower()
            if normalized_segment not in ALLOWED_NEW_VS_RETURNING_USER:
                errors.append(
                    "new_vs_returning_user must be one of "
                    f"{ALLOWED_NEW_VS_RETURNING_USER}, got={new_vs_returning_user}"
                )

        if (
            cohort_date is not None
            and normalized_day_n is not None
            and category is not None
            and region is not None
            and normalized_segment is not None
        ):
            key = (
                str(cohort_date),
                normalized_day_n,
                str(category),
                str(region),
                normalized_segment,
            )
            if key in seen_keys:
                duplicate_keys.add(key)
            seen_keys.add(key)

        normalized_cohort_users: int | None = None
        normalized_retained_users: int | None = None
        if cohort_users is not None:
            normalized_cohort_users = int(cohort_users)
            if normalized_cohort_users < 0:
                errors.append(f"cohort_users must be >= 0, got={cohort_users}")
        if retained_users is not None:
            normalized_retained_users = int(retained_users)
            if normalized_retained_users < 0:
                errors.append(f"retained_users must be >= 0, got={retained_users}")

        if normalized_cohort_users is not None and normalized_retained_users is not None:
            if normalized_retained_users > normalized_cohort_users:
                errors.append(
                    "retained_users must be <= cohort_users for key="
                    f"{cohort_date}|{day_n}|{category}|{region}|{new_vs_returning_user}"
                )

            if normalized_cohort_users == 0:
                if retention_rate is not None:
                    errors.append(
                        "retention_rate must be NULL when cohort_users = 0 for key="
                        f"{cohort_date}|{day_n}|{category}|{region}|{new_vs_returning_user}"
                    )
            elif retention_rate is None:
                errors.append(
                    "retention_rate must be non-null when cohort_users > 0 for key="
                    f"{cohort_date}|{day_n}|{category}|{region}|{new_vs_returning_user}"
                )
            else:
                expected_rate = normalized_retained_users / normalized_cohort_users
                if not isclose(float(retention_rate), expected_rate, rel_tol=1e-9, abs_tol=1e-9):
                    errors.append(
                        "retention_rate mismatch for key="
                        f"{cohort_date}|{day_n}|{category}|{region}|{new_vs_returning_user}: "
                        f"expected={expected_rate}, actual={retention_rate}"
                    )

        if data_date is not None and str(data_date) != expected_data_date:
            errors.append(
                f"data_date mismatch for key={cohort_date}|{day_n}|{category}|{region}|"
                f"{new_vs_returning_user}: expected={expected_data_date}, actual={data_date}"
            )

    if duplicate_keys:
        sample = sorted(duplicate_keys)[:5]
        errors.append(
            "Duplicate retention grain rows found (sample up to 5): "
            + ", ".join("|".join(map(str, key)) for key in sample)
        )

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify batch retention_daily contract health")
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
    spark = SparkSession.builder.appName("verify_bt_retention_daily").getOrCreate()

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

    errors = validate_batch_retention_daily_rows(
        rows,
        expected_data_date=target_data_date,
        min_row_count=args.min_row_count,
    )

    if errors:
        print("FAIL: batch_retention_daily verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    printable = {
        "table": args.table,
        "data_date": target_data_date,
        "row_count": len(rows),
        "min_row_count": args.min_row_count,
        "allowed_day_n": list(ALLOWED_DAY_N),
        "allowed_new_vs_returning_user": list(ALLOWED_NEW_VS_RETURNING_USER),
        "timezone": ET_TIMEZONE,
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }
    print("PASS: batch_retention_daily verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
