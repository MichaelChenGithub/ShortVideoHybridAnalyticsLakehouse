"""Verify batch dim_users_scd2 interval, governed-state, and as-of join contract."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

DEFAULT_TABLE = "lakehouse.dims.dim_users_scd2"
OPEN_END_VALID_TO = "9999-12-31 00:00:00"
GOVERNED_USER_STATES = {"new", "returning", "unknown"}


def _as_of_match_count(rows: Sequence[Mapping[str, Any]], event_ts: datetime) -> int:
    count = 0
    for row in rows:
        valid_from = row.get("valid_from")
        valid_to = row.get("valid_to")
        if valid_from is None or valid_to is None:
            continue
        if valid_from <= event_ts < valid_to:
            count += 1
    return count


def _as_of_row(rows: Sequence[Mapping[str, Any]], event_ts: datetime) -> Mapping[str, Any] | None:
    for row in rows:
        valid_from = row.get("valid_from")
        valid_to = row.get("valid_to")
        if valid_from is None or valid_to is None:
            continue
        if valid_from <= event_ts < valid_to:
            return row
    return None


def validate_scd2_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    user_id: str,
    expect_latest_state: str | None = None,
    expect_latest_region: str | None = None,
    probe_old_ts_ms: int | None = None,
    expect_state_at_old: str | None = None,
    probe_new_ts_ms: int | None = None,
    expect_state_at_new: str | None = None,
) -> list[str]:
    errors: list[str] = []

    if not rows:
        return [f"No dim_users_scd2 rows found for user_id={user_id}"]

    ordered = sorted(rows, key=lambda row: row.get("valid_from"))

    current_rows = [row for row in ordered if bool(row.get("is_current"))]
    if len(current_rows) != 1:
        errors.append(
            "Expected exactly one current row "
            f"for user_id={user_id}, found {len(current_rows)}"
        )

    for idx, row in enumerate(ordered):
        if row.get("user_id") != user_id:
            errors.append(
                f"Row user_id mismatch: expected={user_id}, actual={row.get('user_id')}"
            )

        state_value = row.get("new_vs_returning_user")
        if state_value not in GOVERNED_USER_STATES:
            errors.append(
                "new_vs_returning_user must be governed value "
                f"{sorted(GOVERNED_USER_STATES)}, actual={state_value}"
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
        if expect_latest_state is not None and latest.get("new_vs_returning_user") != expect_latest_state:
            errors.append(
                "Latest new_vs_returning_user mismatch: "
                f"expected={expect_latest_state}, actual={latest.get('new_vs_returning_user')}"
            )
        if expect_latest_region is not None and latest.get("region") != expect_latest_region:
            errors.append(
                "Latest region mismatch: "
                f"expected={expect_latest_region}, actual={latest.get('region')}"
            )

    if probe_old_ts_ms is not None and expect_state_at_old is not None:
        probe_old_ts = datetime.fromtimestamp(
            probe_old_ts_ms / 1000.0, tz=timezone.utc
        ).replace(
            tzinfo=None
        )
        match_count = _as_of_match_count(ordered, probe_old_ts)
        if match_count != 1:
            errors.append(
                "As-of old probe must match exactly one row: "
                f"probe_old_ts_ms={probe_old_ts_ms}, match_count={match_count}"
            )
        probe_row = _as_of_row(ordered, probe_old_ts)
        if probe_row is not None and probe_row.get("new_vs_returning_user") != expect_state_at_old:
            errors.append(
                "As-of old probe state mismatch: "
                f"expected={expect_state_at_old}, actual={probe_row.get('new_vs_returning_user')}"
            )

    if probe_new_ts_ms is not None and expect_state_at_new is not None:
        probe_new_ts = datetime.fromtimestamp(
            probe_new_ts_ms / 1000.0, tz=timezone.utc
        ).replace(
            tzinfo=None
        )
        match_count = _as_of_match_count(ordered, probe_new_ts)
        if match_count != 1:
            errors.append(
                "As-of new probe must match exactly one row: "
                f"probe_new_ts_ms={probe_new_ts_ms}, match_count={match_count}"
            )
        probe_row = _as_of_row(ordered, probe_new_ts)
        if probe_row is not None and probe_row.get("new_vs_returning_user") != expect_state_at_new:
            errors.append(
                "As-of new probe state mismatch: "
                f"expected={expect_state_at_new}, actual={probe_row.get('new_vs_returning_user')}"
            )

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify dim_users_scd2 contract health")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--expect-latest-state")
    parser.add_argument("--expect-latest-region")
    parser.add_argument("--probe-old-ts-ms", type=int)
    parser.add_argument("--expect-state-at-old")
    parser.add_argument("--probe-new-ts-ms", type=int)
    parser.add_argument("--expect-state-at-new")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    args = _parse_args(argv)
    spark = SparkSession.builder.appName("verify_bt_dim_users_scd2").getOrCreate()

    table_df = spark.read.format("iceberg").load(args.table)
    rows = [
        row.asDict(recursive=True)
        for row in table_df.filter(col("user_id") == args.user_id).collect()
    ]

    errors = validate_scd2_rows(
        rows,
        user_id=args.user_id,
        expect_latest_state=args.expect_latest_state,
        expect_latest_region=args.expect_latest_region,
        probe_old_ts_ms=args.probe_old_ts_ms,
        expect_state_at_old=args.expect_state_at_old,
        probe_new_ts_ms=args.probe_new_ts_ms,
        expect_state_at_new=args.expect_state_at_new,
    )

    if errors:
        print("FAIL: dim_users_scd2 verification failed")
        for err in errors:
            print(f" - {err}")
        return 1

    ordered = sorted(rows, key=lambda row: row.get("valid_from"))
    current_row = next(row for row in ordered if bool(row.get("is_current")))
    printable = {
        "user_id": args.user_id,
        "row_count": len(rows),
        "first_valid_from": ordered[0].get("valid_from"),
        "current_valid_to": current_row.get("valid_to"),
        "current_valid_to_expected": OPEN_END_VALID_TO,
        "current_new_vs_returning_user": current_row.get("new_vs_returning_user"),
        "current_region": current_row.get("region"),
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }
    print("PASS: dim_users_scd2 verification succeeded")
    print(json.dumps(printable, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
