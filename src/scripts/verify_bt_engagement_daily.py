"""Verify batch engagement_daily contract health for a target data_date."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Mapping, Sequence

DEFAULT_TABLE = "lakehouse.gold.batch_engagement_daily"
ET_TIMEZONE = "America/New_York"
ALLOWED_USER_STATES = ("new", "returning", "unknown")
FLOAT_TOLERANCE = 1e-9


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    return float(numerator) / float(max(denominator, 1))


def validate_batch_engagement_daily_rows(
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

    seen_grains: set[tuple[str, str, str, str]] = set()
    duplicate_grains: set[tuple[str, str, str, str]] = set()

    for row in rows:
        data_date = row.get("data_date")
        category = row.get("category")
        region = row.get("region")
        new_vs_returning_user = row.get("new_vs_returning_user")
        published_at = row.get("published_at")

        if data_date is None:
            errors.append("data_date must be non-null")
        if category is None:
            errors.append("category must be non-null")
        if region is None:
            errors.append("region must be non-null")
        if new_vs_returning_user is None:
            errors.append("new_vs_returning_user must be non-null")
        if published_at is None:
            errors.append("published_at must be non-null")

        if data_date is not None and str(data_date) != expected_data_date:
            errors.append(
                "data_date mismatch for grain="
                f"({category}, {region}, {new_vs_returning_user}): "
                f"expected={expected_data_date}, actual={data_date}"
            )

        if None not in (data_date, category, region, new_vs_returning_user):
            grain = (str(data_date), str(category), str(region), str(new_vs_returning_user))
            if grain in seen_grains:
                duplicate_grains.add(grain)
            seen_grains.add(grain)

        if new_vs_returning_user is not None and str(new_vs_returning_user) not in ALLOWED_USER_STATES:
            errors.append(
                "new_vs_returning_user must be one of "
                f"{ALLOWED_USER_STATES}, got={new_vs_returning_user}"
            )

        impressions = row.get("impressions")
        play_start = row.get("play_start")
        play_finish = row.get("play_finish")
        likes = row.get("likes")
        shares = row.get("shares")
        skips = row.get("skips")
        play_start_rate = row.get("play_start_rate")
        completion_rate = row.get("completion_rate")
        interaction_rate = row.get("interaction_rate")
        skip_rate = row.get("skip_rate")

        required_count_fields = {
            "impressions": impressions,
            "play_start": play_start,
            "play_finish": play_finish,
            "likes": likes,
            "shares": shares,
            "skips": skips,
        }
        for field_name, value in required_count_fields.items():
            if value is None:
                errors.append(f"{field_name} must be non-null")

        required_rate_fields = {
            "play_start_rate": play_start_rate,
            "completion_rate": completion_rate,
            "interaction_rate": interaction_rate,
            "skip_rate": skip_rate,
        }
        for field_name, value in required_rate_fields.items():
            if value is None:
                errors.append(f"{field_name} must be non-null")
            elif float(value) < 0.0 or float(value) > 1.0:
                errors.append(f"{field_name} must be within [0, 1], got={value}")

        if all(value is not None for value in required_count_fields.values()) and all(
            value is not None for value in required_rate_fields.values()
        ):
            expected_play_start_rate = _safe_ratio(play_start, impressions)
            expected_completion_rate = _safe_ratio(play_finish, play_start)
            expected_interaction_rate = _safe_ratio(likes + shares, play_finish)
            expected_skip_rate = _safe_ratio(skips, play_start)

            rate_checks = (
                ("play_start_rate", float(play_start_rate), expected_play_start_rate),
                ("completion_rate", float(completion_rate), expected_completion_rate),
                ("interaction_rate", float(interaction_rate), expected_interaction_rate),
                ("skip_rate", float(skip_rate), expected_skip_rate),
            )
            for field_name, actual_value, expected_value in rate_checks:
                if abs(actual_value - expected_value) > FLOAT_TOLERANCE:
                    errors.append(
                        f"{field_name} mismatch for grain=({category}, {region}, "
                        f"{new_vs_returning_user}): expected={expected_value}, actual={actual_value}"
                    )

    if duplicate_grains:
        sample = sorted(duplicate_grains)[:5]
        errors.append(f"Duplicate grain rows found (sample up to 5): {sample}")

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify batch engagement_daily contract health")
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
    spark = SparkSession.builder.appName("verify_bt_engagement_daily").getOrCreate()

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

    errors = validate_batch_engagement_daily_rows(
        rows,
        expected_data_date=target_data_date,
        min_row_count=args.min_row_count,
    )

    if errors:
        print("FAIL: batch_engagement_daily verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    unknown_region_rows = sum(1 for row in rows if row.get("region") == "unknown")
    unknown_user_state_rows = sum(1 for row in rows if row.get("new_vs_returning_user") == "unknown")
    printable = {
        "table": args.table,
        "data_date": target_data_date,
        "row_count": len(rows),
        "min_row_count": args.min_row_count,
        "unknown_region_rows": unknown_region_rows,
        "unknown_user_state_rows": unknown_user_state_rows,
        "timezone": ET_TIMEZONE,
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }
    print("PASS: batch_engagement_daily verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
