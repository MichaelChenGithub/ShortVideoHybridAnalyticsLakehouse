"""Verify MIC-43 CDC invalid quarantine table contract fields."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any

DEFAULT_TABLE = "lakehouse.bronze.invalid_events_cdc_videos"


def validate_invalid_quarantine(
    *,
    row_count: int,
    min_row_count: int,
    null_counts: dict[str, int],
    actual_error_codes: set[str],
    expected_error_codes: set[str] | None = None,
) -> tuple[list[str], dict[str, Any]]:
    errors: list[str] = []

    if row_count < min_row_count:
        errors.append(
            f"row count {row_count} is below minimum {min_row_count} for invalid CDC quarantine"
        )

    for field, null_count in sorted(null_counts.items()):
        if null_count > 0:
            errors.append(f"field {field} has {null_count} null value(s)")

    if expected_error_codes:
        missing_codes = sorted(expected_error_codes - actual_error_codes)
        if missing_codes:
            errors.append(
                "missing expected error_code value(s): " + ", ".join(missing_codes)
            )

    metrics = {
        "row_count": row_count,
        "min_row_count": min_row_count,
        "null_counts": null_counts,
        "actual_error_codes": sorted(actual_error_codes),
        "expected_error_codes": sorted(expected_error_codes) if expected_error_codes else [],
    }
    return errors, metrics


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify MIC-43 invalid CDC quarantine data")
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--lookback-minutes", type=int, default=30)
    parser.add_argument("--min-row-count", type=int, default=1)
    parser.add_argument(
        "--expect-error-codes",
        default="",
        help="Comma-separated error_code values that must appear in lookback window",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, sum as spark_sum, when

    args = _parse_args(argv)
    expected_error_codes = {code.strip() for code in args.expect_error_codes.split(",") if code.strip()}

    spark = SparkSession.builder.appName("verify_invalid_cdc_quarantine").getOrCreate()
    table_df = spark.read.format("iceberg").load(args.table)

    lookback_start = datetime.now(timezone.utc) - timedelta(minutes=args.lookback_minutes)
    scoped = table_df.filter(col("ingested_at") >= lit(lookback_start))
    row_count = scoped.count()

    tracked_fields = [
        "invalid_event_id",
        "raw_value",
        "source_topic",
        "source_partition",
        "source_offset",
        "schema_version",
        "error_code",
        "error_reason",
        "ingested_at",
    ]
    null_aggregates = [
        spark_sum(when(col(field).isNull(), 1).otherwise(0)).alias(field)
        for field in tracked_fields
    ]
    null_row = scoped.agg(*null_aggregates).collect()[0].asDict()
    null_counts = {field: int(null_row[field] or 0) for field in tracked_fields}

    actual_error_codes = {
        row["error_code"]
        for row in scoped.select("error_code").distinct().collect()
        if row["error_code"] is not None
    }

    errors, metrics = validate_invalid_quarantine(
        row_count=row_count,
        min_row_count=args.min_row_count,
        null_counts=null_counts,
        actual_error_codes=actual_error_codes,
        expected_error_codes=expected_error_codes if expected_error_codes else None,
    )
    metrics["lookback_minutes"] = args.lookback_minutes
    metrics["checked_at"] = datetime.now(timezone.utc).isoformat()

    if errors:
        print("FAIL: invalid CDC quarantine verification failed")
        for err in errors:
            print(f" - {err}")
        print(json.dumps(metrics, sort_keys=True))
        return 1

    print("PASS: invalid CDC quarantine verification succeeded")
    print(json.dumps(metrics, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
