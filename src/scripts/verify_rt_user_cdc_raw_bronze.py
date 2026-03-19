"""Verify raw user CDC bronze landing for valid user CDC records."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Mapping, Sequence

DEFAULT_TABLE = "lakehouse.bronze.raw_cdc_users"


def validate_raw_cdc_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    user_id: str,
    min_row_count: int,
    expected_state: str | None = None,
    expected_region: str | None = None,
    expected_latest_ts_ms: int | None = None,
    min_source_ts_ms: int | None = None,
) -> list[str]:
    errors: list[str] = []

    if len(rows) < min_row_count:
        errors.append(
            f"raw_cdc_users row count below threshold for user_id={user_id}: "
            f"row_count={len(rows)}, min_row_count={min_row_count}"
        )
        return errors

    required_fields = (
        "op",
        "ts_ms",
        "schema_version",
        "user_id",
        "new_vs_returning_user",
        "region",
        "source_topic",
        "source_partition",
        "source_offset",
        "kafka_timestamp",
        "raw_value",
        "ingested_at",
    )
    missing_required_rows = 0
    latest_row: Mapping[str, Any] | None = None
    latest_key: tuple[int, int] | None = None

    for row in rows:
        if row.get("user_id") != user_id:
            errors.append(
                f"row user_id mismatch: expected={user_id}, actual={row.get('user_id')}"
            )

        missing_fields = [field for field in required_fields if row.get(field) is None]
        if missing_fields:
            missing_required_rows += 1

        ts_ms_raw = row.get("ts_ms")
        source_offset_raw = row.get("source_offset")
        try:
            ts_ms = int(ts_ms_raw)
            source_offset = int(source_offset_raw)
        except (TypeError, ValueError):
            errors.append(
                "raw user CDC row has non-numeric ordering fields: "
                f"ts_ms={ts_ms_raw}, source_offset={source_offset_raw}"
            )
            continue

        if min_source_ts_ms is not None and ts_ms < min_source_ts_ms:
            continue

        key = (ts_ms, source_offset)
        if latest_key is None or key > latest_key:
            latest_key = key
            latest_row = row

    if missing_required_rows > 0:
        errors.append(
            f"raw user CDC required fields contain nulls: missing_required_rows={missing_required_rows}"
        )

    if latest_row is None:
        errors.append(
            "no raw user CDC row remained after applying source-ts scope"
            if min_source_ts_ms is not None
            else "no raw user CDC row available for latest-state validation"
        )
        return errors

    if expected_state is not None and latest_row.get("new_vs_returning_user") != expected_state:
        errors.append(
            "latest raw user CDC state mismatch: "
            f"expected={expected_state}, actual={latest_row.get('new_vs_returning_user')}"
        )

    if expected_region is not None and latest_row.get("region") != expected_region:
        errors.append(
            f"latest raw user CDC region mismatch: expected={expected_region}, actual={latest_row.get('region')}"
        )

    latest_ts_ms = latest_row.get("ts_ms")
    try:
        latest_ts_ms_int = int(latest_ts_ms)
    except (TypeError, ValueError):
        errors.append(f"latest raw user CDC ts_ms is not numeric: {latest_ts_ms}")
    else:
        if expected_latest_ts_ms is not None and latest_ts_ms_int != expected_latest_ts_ms:
            errors.append(
                "latest raw user CDC ts_ms mismatch: "
                f"expected={expected_latest_ts_ms}, actual={latest_ts_ms_int}"
            )

    if latest_row.get("op") not in {"c", "u"}:
        errors.append(f"latest raw user CDC op must be c/u, got {latest_row.get('op')}")

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify raw user CDC bronze landing for user CDC")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--min-row-count", type=int, default=1)
    parser.add_argument("--expect-state")
    parser.add_argument("--expect-region")
    parser.add_argument("--expect-latest-ts-ms", type=int)
    parser.add_argument("--min-source-ts-ms", type=int, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    args = _parse_args(argv)
    spark = SparkSession.builder.appName("verify_rt_user_cdc_raw_bronze").getOrCreate()

    table_df = spark.read.format("iceberg").load(args.table)
    rows = [
        row.asDict(recursive=True)
        for row in table_df.filter(col("user_id") == args.user_id).collect()
    ]

    errors = validate_raw_cdc_rows(
        rows,
        user_id=args.user_id,
        min_row_count=args.min_row_count,
        expected_state=args.expect_state,
        expected_region=args.expect_region,
        expected_latest_ts_ms=args.expect_latest_ts_ms,
        min_source_ts_ms=args.min_source_ts_ms,
    )

    if errors:
        print("FAIL: raw user CDC bronze verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    latest_row = max(
        rows,
        key=lambda row: (int(row["ts_ms"]), int(row["source_offset"])),
    )
    printable = {
        "user_id": latest_row.get("user_id"),
        "row_count": len(rows),
        "latest_op": latest_row.get("op"),
        "latest_state": latest_row.get("new_vs_returning_user"),
        "latest_region": latest_row.get("region"),
        "latest_ts_ms": latest_row.get("ts_ms"),
        "latest_source_offset": latest_row.get("source_offset"),
        "source_topic": latest_row.get("source_topic"),
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }
    print("PASS: raw user CDC bronze verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
