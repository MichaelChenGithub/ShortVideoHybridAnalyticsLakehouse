"""Verify raw CDC bronze landing for valid video CDC records."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Mapping, Sequence

DEFAULT_TABLE = "lakehouse.bronze.raw_cdc_videos"


def validate_raw_cdc_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    video_id: str,
    min_row_count: int,
    expected_status: str | None = None,
    expected_latest_ts_ms: int | None = None,
    min_source_ts_ms: int | None = None,
) -> list[str]:
    errors: list[str] = []

    if len(rows) < min_row_count:
        errors.append(
            f"raw_cdc_videos row count below threshold for video_id={video_id}: "
            f"row_count={len(rows)}, min_row_count={min_row_count}"
        )
        return errors

    required_fields = (
        "op",
        "ts_ms",
        "schema_version",
        "video_id",
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
        if row.get("video_id") != video_id:
            errors.append(
                f"row video_id mismatch: expected={video_id}, actual={row.get('video_id')}"
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
                "raw CDC row has non-numeric ordering fields: "
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
            f"raw CDC required fields contain nulls: missing_required_rows={missing_required_rows}"
        )

    if latest_row is None:
        errors.append(
            "no raw CDC row remained after applying source-ts scope"
            if min_source_ts_ms is not None
            else "no raw CDC row available for latest-state validation"
        )
        return errors

    if expected_status is not None and latest_row.get("status") != expected_status:
        errors.append(
            f"latest raw CDC status mismatch: expected={expected_status}, actual={latest_row.get('status')}"
        )

    latest_ts_ms = latest_row.get("ts_ms")
    try:
        latest_ts_ms_int = int(latest_ts_ms)
    except (TypeError, ValueError):
        errors.append(f"latest raw CDC ts_ms is not numeric: {latest_ts_ms}")
    else:
        if expected_latest_ts_ms is not None and latest_ts_ms_int != expected_latest_ts_ms:
            errors.append(
                "latest raw CDC ts_ms mismatch: "
                f"expected={expected_latest_ts_ms}, actual={latest_ts_ms_int}"
            )

    if latest_row.get("op") not in {"c", "u"}:
        errors.append(f"latest raw CDC op must be c/u, got {latest_row.get('op')}")

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify raw CDC bronze landing for video CDC")
    parser.add_argument("--video-id", required=True)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--min-row-count", type=int, default=1)
    parser.add_argument("--expect-status")
    parser.add_argument("--expect-latest-ts-ms", type=int)
    parser.add_argument("--min-source-ts-ms", type=int, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    args = _parse_args(argv)
    spark = SparkSession.builder.appName("verify_rt_video_cdc_raw_bronze").getOrCreate()

    table_df = spark.read.format("iceberg").load(args.table)
    rows = [
        row.asDict(recursive=True)
        for row in table_df.filter(col("video_id") == args.video_id).collect()
    ]

    errors = validate_raw_cdc_rows(
        rows,
        video_id=args.video_id,
        min_row_count=args.min_row_count,
        expected_status=args.expect_status,
        expected_latest_ts_ms=args.expect_latest_ts_ms,
        min_source_ts_ms=args.min_source_ts_ms,
    )

    if errors:
        print("FAIL: raw CDC bronze verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    latest_row = max(
        rows,
        key=lambda row: (int(row["ts_ms"]), int(row["source_offset"])),
    )
    printable = {
        "video_id": latest_row.get("video_id"),
        "row_count": len(rows),
        "latest_op": latest_row.get("op"),
        "latest_status": latest_row.get("status"),
        "latest_ts_ms": latest_row.get("ts_ms"),
        "latest_source_offset": latest_row.get("source_offset"),
        "source_topic": latest_row.get("source_topic"),
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }
    print("PASS: raw CDC bronze verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
