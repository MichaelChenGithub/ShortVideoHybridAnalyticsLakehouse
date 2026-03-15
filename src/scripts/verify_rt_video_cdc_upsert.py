"""Verify MIC-37 CDC upsert health for dim_videos."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from typing import Any, Mapping, Sequence

DEFAULT_TABLE = "lakehouse.dims.dim_videos"


def utc_now_ms() -> int:
    return int(time.time() * 1_000)


def validate_video_snapshot(
    rows: Sequence[Mapping[str, Any]],
    *,
    video_id: str,
    now_ms: int,
    max_freshness_minutes: int,
    expected_status: str | None = None,
    expected_source_ts_ms: int | None = None,
) -> list[str]:
    errors: list[str] = []

    if not rows:
        return [f"No dim_videos row found for video_id={video_id}"]

    if len(rows) > 1:
        errors.append(f"Expected one dim_videos row for video_id={video_id}, found {len(rows)}")

    row = rows[0]
    if row.get("video_id") != video_id:
        errors.append(
            f"Row video_id mismatch: expected {video_id}, got {row.get('video_id')}"
        )

    source_ts_raw = row.get("source_ts_ms")
    if source_ts_raw is None:
        errors.append("source_ts_ms is null")
    else:
        try:
            source_ts_ms = int(source_ts_raw)
        except (TypeError, ValueError):
            errors.append(f"source_ts_ms is not numeric: {source_ts_raw}")
        else:
            age_ms = now_ms - source_ts_ms
            if age_ms < 0:
                errors.append(
                    f"source_ts_ms is in the future (source_ts_ms={source_ts_ms}, now_ms={now_ms})"
                )
            max_age_ms = max_freshness_minutes * 60 * 1_000
            if age_ms > max_age_ms:
                errors.append(
                    "source_ts_ms freshness breach: "
                    f"age_ms={age_ms} exceeds max_age_ms={max_age_ms}"
                )
            if expected_source_ts_ms is not None and source_ts_ms != expected_source_ts_ms:
                errors.append(
                    "source_ts_ms mismatch: "
                    f"expected={expected_source_ts_ms}, actual={source_ts_ms}"
                )

    if expected_status is not None and row.get("status") != expected_status:
        errors.append(
            f"status mismatch: expected={expected_status}, actual={row.get('status')}"
        )

    if row.get("updated_at") is None:
        errors.append("updated_at is null")

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify MIC-37 dim_videos upsert health")
    parser.add_argument("--video-id", required=True)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--max-freshness-minutes", type=int, default=10)
    parser.add_argument("--expect-status")
    parser.add_argument("--expect-source-ts-ms", type=int)
    parser.add_argument("--min-row-count", type=int, default=1)
    parser.add_argument("--now-ms", type=int, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    args = _parse_args(argv)
    spark = SparkSession.builder.appName("verify_rt_video_cdc_upsert").getOrCreate()

    table_df = spark.read.format("iceberg").load(args.table)
    total_rows = table_df.count()
    if total_rows < args.min_row_count:
        print(
            f"FAIL: table {args.table} row count {total_rows} is below minimum {args.min_row_count}"
        )
        return 1

    rows = [
        row.asDict(recursive=True)
        for row in table_df.filter(col("video_id") == args.video_id).limit(2).collect()
    ]

    reference_now_ms = args.now_ms if args.now_ms is not None else utc_now_ms()

    errors = validate_video_snapshot(
        rows,
        video_id=args.video_id,
        now_ms=reference_now_ms,
        max_freshness_minutes=args.max_freshness_minutes,
        expected_status=args.expect_status,
        expected_source_ts_ms=args.expect_source_ts_ms,
    )

    if errors:
        print("FAIL: MIC-37 verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    row = rows[0]
    printable = {
        "video_id": row.get("video_id"),
        "category": row.get("category"),
        "region": row.get("region"),
        "status": row.get("status"),
        "upload_time": row.get("upload_time"),
        "updated_at": row.get("updated_at"),
        "source_ts_ms": row.get("source_ts_ms"),
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }
    print("PASS: MIC-37 verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
