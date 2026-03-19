"""Verify batch dim_videos_scd2 interval and current-row contract."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any, Mapping, Sequence

DEFAULT_TABLE = "lakehouse.dims.dim_videos_scd2"
OPEN_END_VALID_TO = "9999-12-31 00:00:00"


def validate_scd2_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    video_id: str,
    expect_category: str | None = None,
    expect_region: str | None = None,
    expect_status: str | None = None,
) -> list[str]:
    errors: list[str] = []

    if not rows:
        return [f"No dim_videos_scd2 rows found for video_id={video_id}"]

    ordered = sorted(rows, key=lambda row: row.get("valid_from"))

    current_rows = [row for row in ordered if bool(row.get("is_current"))]
    if len(current_rows) != 1:
        errors.append(
            "Expected exactly one current row "
            f"for video_id={video_id}, found {len(current_rows)}"
        )

    for idx, row in enumerate(ordered):
        if row.get("video_id") != video_id:
            errors.append(
                f"Row video_id mismatch: expected={video_id}, actual={row.get('video_id')}"
            )

        valid_from = row.get("valid_from")
        valid_to = row.get("valid_to")
        if valid_from is None or valid_to is None:
            errors.append("valid_from/valid_to must be non-null")
            continue
        if valid_from >= valid_to:
            errors.append(
                "Invalid interval ordering: "
                f"valid_from={valid_from}, valid_to={valid_to}"
            )

        if idx + 1 < len(ordered):
            next_valid_from = ordered[idx + 1].get("valid_from")
            if next_valid_from is not None and next_valid_from < valid_to:
                errors.append(
                    "Overlapping intervals detected: "
                    f"valid_to={valid_to} exceeds next_valid_from={next_valid_from}"
                )

    if current_rows:
        latest = current_rows[0]
        if str(latest.get("valid_to")) != OPEN_END_VALID_TO:
            errors.append(
                "Current row valid_to must match open-end sentinel: "
                f"expected={OPEN_END_VALID_TO}, actual={latest.get('valid_to')}"
            )
        if expect_category is not None and latest.get("category") != expect_category:
            errors.append(
                "Latest category mismatch: "
                f"expected={expect_category}, actual={latest.get('category')}"
            )
        if expect_region is not None and latest.get("region") != expect_region:
            errors.append(
                "Latest region mismatch: "
                f"expected={expect_region}, actual={latest.get('region')}"
            )
        if expect_status is not None and latest.get("status") != expect_status:
            errors.append(
                "Latest status mismatch: "
                f"expected={expect_status}, actual={latest.get('status')}"
            )

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify dim_videos_scd2 contract health")
    parser.add_argument("--video-id", required=True)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--expect-category")
    parser.add_argument("--expect-region")
    parser.add_argument("--expect-status")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    args = _parse_args(argv)
    spark = SparkSession.builder.appName("verify_bt_dim_videos_scd2").getOrCreate()

    table_df = spark.read.format("iceberg").load(args.table)
    rows = [
        row.asDict(recursive=True)
        for row in table_df.filter(col("video_id") == args.video_id).collect()
    ]

    errors = validate_scd2_rows(
        rows,
        video_id=args.video_id,
        expect_category=args.expect_category,
        expect_region=args.expect_region,
        expect_status=args.expect_status,
    )

    if errors:
        print("FAIL: dim_videos_scd2 verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    ordered = sorted(rows, key=lambda row: row.get("valid_from"))
    current_row = next(row for row in ordered if bool(row.get("is_current")))
    printable = {
        "video_id": args.video_id,
        "row_count": len(rows),
        "first_valid_from": ordered[0].get("valid_from"),
        "current_valid_to": current_row.get("valid_to"),
        "current_valid_to_expected": OPEN_END_VALID_TO,
        "current_category": current_row.get("category"),
        "current_region": current_row.get("region"),
        "current_status": current_row.get("status"),
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }
    print("PASS: dim_videos_scd2 verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
