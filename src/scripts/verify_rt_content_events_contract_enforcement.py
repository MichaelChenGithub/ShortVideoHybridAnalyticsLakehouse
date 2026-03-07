"""Verify MIC-39 content contract enforcement for valid and quarantine sinks."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from typing import Any, Mapping

DEFAULT_RAW_TABLE = "lakehouse.bronze.raw_events"
DEFAULT_GOLD_TABLE = "lakehouse.gold.rt_video_stats_1min"
DEFAULT_INVALID_TABLE = "lakehouse.bronze.invalid_events_content"


def utc_now_ms() -> int:
    return int(time.time() * 1_000)


def validate_content_contract_snapshot(
    snapshot: Mapping[str, Any],
    *,
    now_ms: int,
    min_raw_rows: int,
    min_gold_rows: int,
    min_invalid_rows: int,
    max_invalid_rate: float,
    max_freshness_minutes: int,
    min_ingested_at_ms: int | None = None,
) -> list[str]:
    errors: list[str] = []

    raw_count = int(snapshot.get("raw_count") or 0)
    gold_count = int(snapshot.get("gold_count") or 0)
    invalid_count = int(snapshot.get("invalid_count") or 0)
    invalid_required_null_rows = int(snapshot.get("invalid_required_null_rows") or 0)

    if raw_count < min_raw_rows:
        errors.append(
            f"raw_count below threshold: raw_count={raw_count}, min_raw_rows={min_raw_rows}"
        )
    if gold_count < min_gold_rows:
        errors.append(
            f"gold_count below threshold: gold_count={gold_count}, min_gold_rows={min_gold_rows}"
        )
    if invalid_count < min_invalid_rows:
        errors.append(
            "invalid_count below threshold: "
            f"invalid_count={invalid_count}, min_invalid_rows={min_invalid_rows}"
        )

    denominator = raw_count + invalid_count
    invalid_rate = (invalid_count / denominator) if denominator > 0 else 0.0
    if invalid_rate > max_invalid_rate:
        errors.append(
            "invalid_rate above threshold: "
            f"invalid_rate={invalid_rate:.6f}, max_invalid_rate={max_invalid_rate:.6f}"
        )

    if invalid_required_null_rows > 0:
        errors.append(
            "invalid sink has null required fields: "
            f"invalid_required_null_rows={invalid_required_null_rows}"
        )

    max_invalid_ingested_at_ms = snapshot.get("max_invalid_ingested_at_ms")
    if max_invalid_ingested_at_ms is None:
        errors.append("max_invalid_ingested_at is null; no fresh invalid output detected")
    else:
        try:
            max_invalid_ingested_at_ms_int = int(max_invalid_ingested_at_ms)
        except (TypeError, ValueError):
            errors.append(
                "max_invalid_ingested_at_ms is not numeric: "
                f"{max_invalid_ingested_at_ms}"
            )
        else:
            age_ms = now_ms - max_invalid_ingested_at_ms_int
            if age_ms < 0:
                errors.append(
                    "max_invalid_ingested_at is in the future: "
                    f"max_invalid_ingested_at_ms={max_invalid_ingested_at_ms_int}, now_ms={now_ms}"
                )
            max_age_ms = max_freshness_minutes * 60 * 1_000
            if age_ms > max_age_ms:
                errors.append(
                    "invalid freshness breach: "
                    f"age_ms={age_ms} exceeds max_age_ms={max_age_ms}"
                )
            if min_ingested_at_ms is not None and max_invalid_ingested_at_ms_int < min_ingested_at_ms:
                errors.append(
                    "max_invalid_ingested_at predates this acceptance run: "
                    f"max_invalid_ingested_at_ms={max_invalid_ingested_at_ms_int}, "
                    f"min_ingested_at_ms={min_ingested_at_ms}"
                )

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify MIC-39 content contract enforcement")
    parser.add_argument("--raw-table", default=DEFAULT_RAW_TABLE)
    parser.add_argument("--gold-table", default=DEFAULT_GOLD_TABLE)
    parser.add_argument("--invalid-table", default=DEFAULT_INVALID_TABLE)
    parser.add_argument("--min-raw-rows", type=int, default=1)
    parser.add_argument("--min-gold-rows", type=int, default=1)
    parser.add_argument("--min-invalid-rows", type=int, default=1)
    parser.add_argument("--max-invalid-rate", type=float, default=0.20)
    parser.add_argument("--max-freshness-minutes", type=int, default=10)
    parser.add_argument("--min-ingested-at-ms", type=int, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, max as spark_max

    args = _parse_args(argv)
    if args.max_invalid_rate < 0 or args.max_invalid_rate > 1:
        raise ValueError("--max-invalid-rate must be in [0, 1]")

    spark = SparkSession.builder.appName("verify_rt_content_events_contract_enforcement").getOrCreate()

    raw_df = spark.read.format("iceberg").load(args.raw_table)
    gold_df = spark.read.format("iceberg").load(args.gold_table)
    invalid_df = spark.read.format("iceberg").load(args.invalid_table)

    scoped_raw_df = raw_df
    scoped_invalid_df = invalid_df
    if args.min_ingested_at_ms is not None:
        scoped_raw_df = raw_df.filter((col("ingested_at").cast("long") * 1000) >= args.min_ingested_at_ms)
        scoped_invalid_df = invalid_df.filter(
            (col("ingested_at").cast("long") * 1000) >= args.min_ingested_at_ms
        )

    raw_count = scoped_raw_df.count()
    gold_count = gold_df.count()
    invalid_count = scoped_invalid_df.count()

    invalid_required_null_rows = scoped_invalid_df.filter(
        col("invalid_event_id").isNull()
        | col("raw_value").isNull()
        | col("source_topic").isNull()
        | col("source_partition").isNull()
        | col("source_offset").isNull()
        | col("schema_version").isNull()
        | col("error_code").isNull()
        | col("error_reason").isNull()
        | col("ingested_at").isNull()
    ).count()

    invalid_max_row = (
        scoped_invalid_df.select((col("ingested_at").cast("long") * 1000).alias("ingested_at_ms"))
        .agg(spark_max("ingested_at_ms").alias("max_invalid_ingested_at_ms"))
        .collect()[0]
    )
    max_invalid_ingested_at_ms = invalid_max_row["max_invalid_ingested_at_ms"]

    denominator = raw_count + invalid_count
    invalid_rate = (invalid_count / denominator) if denominator > 0 else 0.0

    snapshot = {
        "raw_count": raw_count,
        "gold_count": gold_count,
        "invalid_count": invalid_count,
        "invalid_required_null_rows": invalid_required_null_rows,
        "max_invalid_ingested_at_ms": max_invalid_ingested_at_ms,
    }

    errors = validate_content_contract_snapshot(
        snapshot,
        now_ms=utc_now_ms(),
        min_raw_rows=args.min_raw_rows,
        min_gold_rows=args.min_gold_rows,
        min_invalid_rows=args.min_invalid_rows,
        max_invalid_rate=args.max_invalid_rate,
        max_freshness_minutes=args.max_freshness_minutes,
        min_ingested_at_ms=args.min_ingested_at_ms,
    )

    if errors:
        print("FAIL: MIC-39 verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    printable = {
        **snapshot,
        "invalid_rate": invalid_rate,
        "checked_at": datetime.utcnow().isoformat() + "Z",
        "raw_table": args.raw_table,
        "gold_table": args.gold_table,
        "invalid_table": args.invalid_table,
        "min_ingested_at_ms": args.min_ingested_at_ms,
    }
    print("PASS: MIC-39 verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
