"""Verify MIC-40 content events aggregator health for Bronze and Gold outputs."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from typing import Any, Mapping

DEFAULT_RAW_TABLE = "lakehouse.bronze.raw_events"
DEFAULT_GOLD_TABLE = "lakehouse.gold.rt_video_stats_1min"



def utc_now_ms() -> int:
    return int(time.time() * 1_000)



def validate_aggregator_snapshot(
    snapshot: Mapping[str, Any],
    *,
    now_ms: int,
    min_raw_rows: int,
    min_gold_rows: int,
    max_freshness_minutes: int,
    min_processed_at_ms: int | None = None,
    min_watermark_drop_ratio: float | None = None,
    max_watermark_drop_ratio: float | None = None,
) -> list[str]:
    errors: list[str] = []

    raw_count = int(snapshot.get("raw_count") or 0)
    gold_count = int(snapshot.get("gold_count") or 0)
    duplicate_key_rows = int(snapshot.get("duplicate_key_rows") or 0)
    null_required_rows = int(snapshot.get("null_required_rows") or 0)
    negative_metric_rows = int(snapshot.get("negative_metric_rows") or 0)
    raw_unique_event_count = int(snapshot.get("raw_unique_event_count") or 0)
    gold_event_total_count = int(snapshot.get("gold_event_total_count") or 0)

    watermark_drop_ratio_raw = snapshot.get("watermark_drop_ratio")
    if watermark_drop_ratio_raw is None:
        if raw_unique_event_count > 0:
            watermark_drop_ratio = max(raw_unique_event_count - gold_event_total_count, 0) / raw_unique_event_count
        else:
            watermark_drop_ratio = 0.0
    else:
        try:
            watermark_drop_ratio = float(watermark_drop_ratio_raw)
        except (TypeError, ValueError):
            errors.append(f"watermark_drop_ratio is not numeric: {watermark_drop_ratio_raw}")
            watermark_drop_ratio = None

    if raw_count < min_raw_rows:
        errors.append(
            f"raw_count below threshold: raw_count={raw_count}, min_raw_rows={min_raw_rows}"
        )
    if gold_count < min_gold_rows:
        errors.append(
            f"gold_count below threshold: gold_count={gold_count}, min_gold_rows={min_gold_rows}"
        )
    if duplicate_key_rows > 0:
        errors.append(
            f"gold uniqueness violated: duplicate_key_rows={duplicate_key_rows} for video_id+window_start"
        )
    if null_required_rows > 0:
        errors.append(
            f"gold required fields contain nulls: null_required_rows={null_required_rows}"
        )
    if negative_metric_rows > 0:
        errors.append(
            f"gold metrics contain negative values: negative_metric_rows={negative_metric_rows}"
        )
    if watermark_drop_ratio is not None:
        if watermark_drop_ratio < 0 or watermark_drop_ratio > 1:
            errors.append(
                "watermark_drop_ratio outside [0, 1]: "
                f"watermark_drop_ratio={watermark_drop_ratio}"
            )
        if (
            min_watermark_drop_ratio is not None
            and watermark_drop_ratio < min_watermark_drop_ratio
        ):
            errors.append(
                "watermark_drop_ratio below threshold: "
                f"watermark_drop_ratio={watermark_drop_ratio:.6f}, "
                f"min_watermark_drop_ratio={min_watermark_drop_ratio:.6f}"
            )
        if (
            max_watermark_drop_ratio is not None
            and watermark_drop_ratio > max_watermark_drop_ratio
        ):
            errors.append(
                "watermark_drop_ratio above threshold: "
                f"watermark_drop_ratio={watermark_drop_ratio:.6f}, "
                f"max_watermark_drop_ratio={max_watermark_drop_ratio:.6f}"
            )

    max_processed_at_ms = snapshot.get("max_processed_at_ms")
    if max_processed_at_ms is None:
        errors.append("max_processed_at is null; no fresh gold output detected")
    else:
        try:
            max_processed_at_ms_int = int(max_processed_at_ms)
        except (TypeError, ValueError):
            errors.append(f"max_processed_at_ms is not numeric: {max_processed_at_ms}")
        else:
            age_ms = now_ms - max_processed_at_ms_int
            if age_ms < 0:
                errors.append(
                    "max_processed_at is in the future: "
                    f"max_processed_at_ms={max_processed_at_ms_int}, now_ms={now_ms}"
                )
            max_age_ms = max_freshness_minutes * 60 * 1_000
            if age_ms > max_age_ms:
                errors.append(
                    "freshness breach on gold output: "
                    f"age_ms={age_ms} exceeds max_age_ms={max_age_ms}"
                )
            if min_processed_at_ms is not None and max_processed_at_ms_int < min_processed_at_ms:
                errors.append(
                    "max_processed_at predates this acceptance run: "
                    f"max_processed_at_ms={max_processed_at_ms_int}, "
                    f"min_processed_at_ms={min_processed_at_ms}"
                )

    return errors



def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify MIC-40 Bronze/Gold aggregator health")
    parser.add_argument("--raw-table", default=DEFAULT_RAW_TABLE)
    parser.add_argument("--gold-table", default=DEFAULT_GOLD_TABLE)
    parser.add_argument("--min-raw-rows", type=int, default=1)
    parser.add_argument("--min-gold-rows", type=int, default=1)
    parser.add_argument("--max-freshness-minutes", type=int, default=10)
    parser.add_argument("--min-processed-at-ms", type=int, default=None)
    parser.add_argument("--min-watermark-drop-ratio", type=float, default=None)
    parser.add_argument("--max-watermark-drop-ratio", type=float, default=None)
    return parser.parse_args(argv)



def main(argv: list[str] | None = None) -> int:
    from pyspark.sql.functions import (
        col,
        coalesce,
        lit,
        max as spark_max,
        sum as spark_sum,
    )
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    if args.min_watermark_drop_ratio is not None and (
        args.min_watermark_drop_ratio < 0 or args.min_watermark_drop_ratio > 1
    ):
        raise ValueError("--min-watermark-drop-ratio must be in [0, 1]")
    if args.max_watermark_drop_ratio is not None and (
        args.max_watermark_drop_ratio < 0 or args.max_watermark_drop_ratio > 1
    ):
        raise ValueError("--max-watermark-drop-ratio must be in [0, 1]")
    if (
        args.min_watermark_drop_ratio is not None
        and args.max_watermark_drop_ratio is not None
        and args.min_watermark_drop_ratio > args.max_watermark_drop_ratio
    ):
        raise ValueError("--min-watermark-drop-ratio must be <= --max-watermark-drop-ratio")

    spark = SparkSession.builder.appName("verify_rt_content_events_aggregator").getOrCreate()

    raw_df = spark.read.format("iceberg").load(args.raw_table)
    gold_df = spark.read.format("iceberg").load(args.gold_table)

    scoped_raw_df = raw_df
    scoped_gold_df = gold_df
    if args.min_processed_at_ms is not None:
        scoped_raw_df = raw_df.filter((col("ingested_at").cast("long") * 1000) >= args.min_processed_at_ms)
        scoped_gold_df = gold_df.filter(
            (col("processed_at").cast("long") * 1000) >= args.min_processed_at_ms
        )

    raw_count = scoped_raw_df.count()
    gold_count = scoped_gold_df.count()

    duplicate_key_rows = (
        scoped_gold_df.groupBy("video_id", "window_start")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    null_required_rows = scoped_gold_df.filter(
        col("window_start").isNull() | col("window_end").isNull() | col("processed_at").isNull()
    ).count()

    negative_metric_rows = scoped_gold_df.filter(
        (col("impressions") < 0)
        | (col("play_start") < 0)
        | (col("play_finish") < 0)
        | (col("likes") < 0)
        | (col("shares") < 0)
        | (col("skips") < 0)
        | (col("watch_time_sum_ms") < 0)
    ).count()

    raw_unique_event_count = (
        scoped_raw_df.filter(col("event_id").isNotNull())
        .select("event_id")
        .distinct()
        .count()
    )
    gold_event_total_count = int(
        scoped_gold_df.agg(
            spark_sum(
                coalesce(col("impressions"), lit(0))
                + coalesce(col("play_start"), lit(0))
                + coalesce(col("play_finish"), lit(0))
                + coalesce(col("likes"), lit(0))
                + coalesce(col("shares"), lit(0))
                + coalesce(col("skips"), lit(0))
            ).alias("gold_event_total_count")
        ).collect()[0]["gold_event_total_count"]
        or 0
    )
    watermark_drop_ratio = 0.0
    if raw_unique_event_count > 0:
        watermark_drop_ratio = max(raw_unique_event_count - gold_event_total_count, 0) / raw_unique_event_count

    max_processed_at = scoped_gold_df.agg(spark_max("processed_at").alias("max_processed_at")).collect()[0][
        "max_processed_at"
    ]
    max_processed_at_ms = None
    if max_processed_at is not None:
        max_processed_at_ms = int(max_processed_at.timestamp() * 1_000)

    snapshot = {
        "raw_count": raw_count,
        "gold_count": gold_count,
        "duplicate_key_rows": duplicate_key_rows,
        "null_required_rows": null_required_rows,
        "negative_metric_rows": negative_metric_rows,
        "raw_unique_event_count": raw_unique_event_count,
        "gold_event_total_count": gold_event_total_count,
        "watermark_drop_ratio": watermark_drop_ratio,
        "max_processed_at_ms": max_processed_at_ms,
    }

    errors = validate_aggregator_snapshot(
        snapshot,
        now_ms=utc_now_ms(),
        min_raw_rows=args.min_raw_rows,
        min_gold_rows=args.min_gold_rows,
        max_freshness_minutes=args.max_freshness_minutes,
        min_processed_at_ms=args.min_processed_at_ms,
        min_watermark_drop_ratio=args.min_watermark_drop_ratio,
        max_watermark_drop_ratio=args.max_watermark_drop_ratio,
    )

    if errors:
        print("FAIL: MIC-40 verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    printable = {
        **snapshot,
        "max_processed_at": str(max_processed_at),
        "checked_at": datetime.utcnow().isoformat() + "Z",
        "raw_table": args.raw_table,
        "gold_table": args.gold_table,
    }
    print("PASS: MIC-40 verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
