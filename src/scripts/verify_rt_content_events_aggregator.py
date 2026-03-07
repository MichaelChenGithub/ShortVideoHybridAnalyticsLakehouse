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
) -> list[str]:
    errors: list[str] = []

    raw_count = int(snapshot.get("raw_count") or 0)
    gold_count = int(snapshot.get("gold_count") or 0)
    duplicate_key_rows = int(snapshot.get("duplicate_key_rows") or 0)
    null_required_rows = int(snapshot.get("null_required_rows") or 0)
    negative_metric_rows = int(snapshot.get("negative_metric_rows") or 0)

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
    return parser.parse_args(argv)



def main(argv: list[str] | None = None) -> int:
    from pyspark.sql.functions import col, max as spark_max
    from pyspark.sql import SparkSession

    args = _parse_args(argv)
    spark = SparkSession.builder.appName("verify_rt_content_events_aggregator").getOrCreate()

    raw_df = spark.read.format("iceberg").load(args.raw_table)
    gold_df = spark.read.format("iceberg").load(args.gold_table)

    raw_count = raw_df.count()
    gold_count = gold_df.count()

    duplicate_key_rows = (
        gold_df.groupBy("video_id", "window_start")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    null_required_rows = gold_df.filter(
        col("window_start").isNull() | col("window_end").isNull() | col("processed_at").isNull()
    ).count()

    negative_metric_rows = gold_df.filter(
        (col("impressions") < 0)
        | (col("play_start") < 0)
        | (col("play_finish") < 0)
        | (col("likes") < 0)
        | (col("shares") < 0)
        | (col("skips") < 0)
        | (col("watch_time_sum_ms") < 0)
    ).count()

    max_processed_at = gold_df.agg(spark_max("processed_at").alias("max_processed_at")).collect()[0][
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
        "max_processed_at_ms": max_processed_at_ms,
    }

    errors = validate_aggregator_snapshot(
        snapshot,
        now_ms=utc_now_ms(),
        min_raw_rows=args.min_raw_rows,
        min_gold_rows=args.min_gold_rows,
        max_freshness_minutes=args.max_freshness_minutes,
        min_processed_at_ms=args.min_processed_at_ms,
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
