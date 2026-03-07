"""Check MIC-43 CDC health signals: freshness and invalid-rate."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any

DEFAULT_DIM_TABLE = "lakehouse.dims.dim_videos"
DEFAULT_INVALID_TABLE = "lakehouse.bronze.invalid_events_cdc_videos"


def utc_now_ms() -> int:
    return int(time.time() * 1_000)


def validate_cdc_health(
    *,
    latest_source_ts_ms: int | None,
    now_ms: int,
    max_freshness_minutes: int,
    invalid_count_lookback: int,
    lookback_minutes: int,
    max_invalid_rate: float | None = None,
) -> tuple[list[str], dict[str, Any]]:
    errors: list[str] = []

    if lookback_minutes <= 0:
        errors.append("lookback_minutes must be > 0")
        lookback_minutes = 1

    if latest_source_ts_ms is None:
        errors.append("latest source_ts_ms is null (dim_videos has no CDC timestamp)")
        freshness_age_ms = None
    else:
        freshness_age_ms = now_ms - latest_source_ts_ms
        if freshness_age_ms < 0:
            errors.append(
                f"latest source_ts_ms is in the future (source_ts_ms={latest_source_ts_ms}, now_ms={now_ms})"
            )
        max_age_ms = max_freshness_minutes * 60 * 1_000
        if freshness_age_ms > max_age_ms:
            errors.append(
                "freshness breach: "
                f"age_ms={freshness_age_ms} exceeds max_age_ms={max_age_ms}"
            )

    invalid_rate = float(invalid_count_lookback) / float(lookback_minutes)
    if max_invalid_rate is not None and invalid_rate > max_invalid_rate:
        errors.append(
            "invalid-rate breach: "
            f"invalid_rate={invalid_rate:.6f} exceeds max_invalid_rate={max_invalid_rate:.6f}"
        )

    metrics = {
        "latest_source_ts_ms": latest_source_ts_ms,
        "freshness_age_ms": freshness_age_ms,
        "max_freshness_minutes": max_freshness_minutes,
        "lookback_minutes": lookback_minutes,
        "invalid_count_lookback": invalid_count_lookback,
        "invalid_rate_per_minute": invalid_rate,
        "max_invalid_rate": max_invalid_rate,
    }
    return errors, metrics


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check MIC-43 CDC freshness and invalid-rate")
    parser.add_argument("--dim-table", default=DEFAULT_DIM_TABLE)
    parser.add_argument("--invalid-table", default=DEFAULT_INVALID_TABLE)
    parser.add_argument("--max-freshness-minutes", type=int, default=10)
    parser.add_argument("--lookback-minutes", type=int, default=10)
    parser.add_argument("--max-invalid-rate", type=float)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit

    args = _parse_args(argv)
    spark = SparkSession.builder.appName("check_rt_video_cdc_health").getOrCreate()

    dim_df = spark.read.format("iceberg").load(args.dim_table)
    latest_source_ts_ms = dim_df.selectExpr("max(source_ts_ms) AS latest_source_ts_ms").collect()[0][
        "latest_source_ts_ms"
    ]
    if latest_source_ts_ms is not None:
        latest_source_ts_ms = int(latest_source_ts_ms)

    invalid_df = spark.read.format("iceberg").load(args.invalid_table)
    lookback_start = datetime.now(timezone.utc) - timedelta(minutes=args.lookback_minutes)
    invalid_count_lookback = invalid_df.filter(col("ingested_at") >= lit(lookback_start)).count()

    errors, metrics = validate_cdc_health(
        latest_source_ts_ms=latest_source_ts_ms,
        now_ms=utc_now_ms(),
        max_freshness_minutes=args.max_freshness_minutes,
        invalid_count_lookback=invalid_count_lookback,
        lookback_minutes=args.lookback_minutes,
        max_invalid_rate=args.max_invalid_rate,
    )

    metrics["checked_at"] = datetime.now(timezone.utc).isoformat()

    if errors:
        print("FAIL: MIC-43 CDC health check failed")
        for err in errors:
            print(f" - {err}")
        print(json.dumps(metrics, sort_keys=True))
        return 1

    print("PASS: MIC-43 CDC health check passed")
    print(json.dumps(metrics, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
