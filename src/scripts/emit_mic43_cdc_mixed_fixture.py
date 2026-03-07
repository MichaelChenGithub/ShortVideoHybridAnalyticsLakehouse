"""Emit deterministic mixed CDC fixtures for MIC-43 contract validation."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from typing import List

DEFAULT_SCHEMA_VERSION = "m1_v1"
DEFAULT_BASE_TS_MS = 1_772_588_800_000  # 2026-03-01T00:00:00Z
DEFAULT_TOPIC = "cdc.content.videos"


@dataclass(frozen=True)
class FixtureRecord:
    key: str
    raw_value: str
    is_valid: bool


@dataclass(frozen=True)
class FixtureSummary:
    video_id: str
    total_records: int
    valid_records: int
    invalid_records: int
    expected_status: str
    expected_source_ts_ms: int


def build_mixed_fixture_records(
    video_id: str,
    *,
    schema_version: str = DEFAULT_SCHEMA_VERSION,
    base_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> tuple[List[FixtureRecord], FixtureSummary]:
    records: List[FixtureRecord] = []

    def _valid_payload(op: str, ts_ms: int, status: str) -> str:
        return json.dumps(
            {
                "op": op,
                "ts_ms": ts_ms,
                "schema_version": schema_version,
                "after": {
                    "video_id": video_id,
                    "category": "Comedy",
                    "region": "US",
                    "upload_time": "2026-03-05T00:00:00Z",
                    "status": status,
                },
            },
            sort_keys=True,
        )

    records.append(FixtureRecord(video_id, _valid_payload("c", base_ts_ms, "active"), True))
    records.append(FixtureRecord(video_id, "not-json-{mic43", False))
    records.append(
        FixtureRecord(
            video_id,
            json.dumps(
                {
                    "op": "d",
                    "ts_ms": base_ts_ms + 1_000,
                    "schema_version": schema_version,
                    "after": {
                        "video_id": video_id,
                        "category": "Comedy",
                        "region": "US",
                        "upload_time": "2026-03-05T00:00:00Z",
                        "status": "deleted",
                    },
                },
                sort_keys=True,
            ),
            False,
        )
    )
    records.append(
        FixtureRecord(
            video_id,
            json.dumps(
                {
                    "op": "u",
                    "ts_ms": base_ts_ms + 1_500,
                    "after": {
                        "video_id": video_id,
                        "category": "Comedy",
                        "region": "US",
                        "upload_time": "2026-03-05T00:00:00Z",
                        "status": "review_hold",
                    },
                },
                sort_keys=True,
            ),
            False,
        )
    )
    records.append(
        FixtureRecord(
            video_id,
            json.dumps(
                {
                    "op": "u",
                    "ts_ms": base_ts_ms + 1_700,
                    "schema_version": schema_version,
                    "after": {
                        "category": "Comedy",
                        "region": "US",
                        "upload_time": "2026-03-05T00:00:00Z",
                        "status": "review_hold",
                    },
                },
                sort_keys=True,
            ),
            False,
        )
    )
    records.append(
        FixtureRecord(
            video_id,
            _valid_payload("u", base_ts_ms + 2_000, "copyright_strike"),
            True,
        )
    )

    valid_records = sum(1 for record in records if record.is_valid)
    summary = FixtureSummary(
        video_id=video_id,
        total_records=len(records),
        valid_records=valid_records,
        invalid_records=len(records) - valid_records,
        expected_status="copyright_strike",
        expected_source_ts_ms=base_ts_ms + 2_000,
    )
    return records, summary


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit deterministic mixed CDC fixture for MIC-43")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--video-id", required=True)
    parser.add_argument("--schema-version", default=DEFAULT_SCHEMA_VERSION)
    parser.add_argument("--base-ts-ms", type=int, default=DEFAULT_BASE_TS_MS)
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _emit_to_kafka(topic: str, bootstrap_servers: str, records: List[FixtureRecord], sleep_ms: int) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers, "client.id": "mic43-cdc-mixed-emitter"})
    for record in records:
        producer.produce(topic, key=record.key, value=record.raw_value)
        producer.poll(0)
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1_000.0)
    producer.flush()


def main(argv: List[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    records, summary = build_mixed_fixture_records(
        args.video_id,
        schema_version=args.schema_version,
        base_ts_ms=args.base_ts_ms,
    )

    if args.dry_run:
        for index, record in enumerate(records):
            print(
                json.dumps(
                    {
                        "index": index,
                        "key": record.key,
                        "raw_value": record.raw_value,
                        "is_valid": record.is_valid,
                    },
                    sort_keys=True,
                )
            )
    else:
        _emit_to_kafka(args.topic, args.bootstrap_servers, records, args.sleep_ms)

    print(
        json.dumps(
            {
                "video_id": summary.video_id,
                "topic": args.topic,
                "bootstrap_servers": args.bootstrap_servers,
                "total_records": summary.total_records,
                "valid_records": summary.valid_records,
                "invalid_records": summary.invalid_records,
                "expected_status": summary.expected_status,
                "expected_source_ts_ms": summary.expected_source_ts_ms,
                "dry_run": args.dry_run,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
