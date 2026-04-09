"""Verify Iceberg compaction after a bulk_arrival adversarial run.

Runs EXECUTE optimize on the affected bronze partition, then asserts the
resulting file count is within the configured bound.

Usage:
    python verify_bulk_arrival_compaction.py \\
        --data-date 2026-04-07 \\
        --max-file-count 20 \\
        --trino-host localhost \\
        --trino-port 8081
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional


# ---------------------------------------------------------------------------
# Pure validation logic (no I/O — unit-testable)
# ---------------------------------------------------------------------------


def validate_compaction_result(file_count: int, max_file_count: int) -> list[str]:
    """Return a list of error strings. Empty list means PASS."""
    errors: list[str] = []
    if file_count > max_file_count:
        errors.append(
            f"file_count={file_count} exceeds max_file_count={max_file_count} after compaction; "
            "small files from burst were not fully merged"
        )
    return errors


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Iceberg compaction on a bronze partition and assert file count"
    )
    parser.add_argument(
        "--data-date",
        required=True,
        help="Partition date to compact (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--max-file-count",
        type=int,
        default=20,
        help="Maximum allowed file count after compaction (default: 20)",
    )
    parser.add_argument(
        "--trino-host",
        default="localhost",
        help="Trino host (default: localhost)",
    )
    parser.add_argument(
        "--trino-port",
        type=int,
        default=8081,
        help="Trino port (default: 8081)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    import trino  # type: ignore[import]

    args = _parse_args(argv)

    conn = trino.dbapi.connect(
        host=args.trino_host,
        port=args.trino_port,
        user="verify_bulk_arrival_compaction",
    )
    cur = conn.cursor()

    # Step 1: Run compaction on the burst partition.
    # Merges small files written during the burst micro-batches into larger files.
    print(
        f"[BULK-ARRIVAL-COMPACTION] Running optimize on "
        f"bronze.raw_events event_date={args.data_date} ..."
    )
    cur.execute(
        f"""
        ALTER TABLE lakehouse.bronze.raw_events
        EXECUTE optimize(file_size_threshold => '128MB')
        WHERE event_date = DATE '{args.data_date}'
        """
    )
    cur.fetchall()  # consume result

    # Step 2: Count data files remaining after compaction.
    cur.execute('SELECT count(*) FROM "lakehouse"."bronze"."raw_events$files"')
    rows = cur.fetchall()
    file_count = int(rows[0][0]) if rows else 0

    conn.close()

    print(
        f"[BULK-ARRIVAL-COMPACTION] post-compaction file_count={file_count} "
        f"max_file_count={args.max_file_count}"
    )

    errors = validate_compaction_result(file_count, args.max_file_count)

    summary = {
        "data_date": args.data_date,
        "file_count_after_compaction": file_count,
        "max_file_count": args.max_file_count,
    }

    if errors:
        print("[BULK-ARRIVAL-COMPACTION] FAIL: compaction verification failed")
        for err in errors:
            print(f"  - {err}")
        print(json.dumps(summary, sort_keys=True))
        return 1

    print("[BULK-ARRIVAL-COMPACTION] PASS: compaction verification succeeded")
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
