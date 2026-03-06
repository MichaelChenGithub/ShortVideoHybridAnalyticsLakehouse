"""Emit deterministic CDC fixtures for MIC-37 insert/update/tie-break checks."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from typing import Dict, List

DEFAULT_SCHEMA_VERSION = "m1_v1"
DEFAULT_BASE_TS_MS = 1_762_435_200_000  # 2025-11-03T00:00:00Z
DEFAULT_TOPIC = "cdc.content.videos"


@dataclass(frozen=True)
class FixtureEvent:
    key: str
    payload: Dict[str, object]


@dataclass(frozen=True)
class FixtureSummary:
    video_id: str
    scenario: str
    event_count: int
    expected_status: str
    expected_source_ts_ms: int


def build_fixture_events(
    video_id: str,
    scenario: str,
    schema_version: str = DEFAULT_SCHEMA_VERSION,
    base_ts_ms: int = DEFAULT_BASE_TS_MS,
) -> List[FixtureEvent]:
    if scenario not in {"insert-update", "same-ts-tiebreak", "full"}:
        raise ValueError(f"Unsupported scenario: {scenario}")

    def _event(op: str, ts_ms: int, category: str, region: str, status: str) -> FixtureEvent:
        payload = {
            "op": op,
            "ts_ms": ts_ms,
            "schema_version": schema_version,
            "after": {
                "video_id": video_id,
                "category": category,
                "region": region,
                "upload_time": "2026-03-05T00:00:00Z",
                "status": status,
            },
        }
        return FixtureEvent(key=video_id, payload=payload)

    events: List[FixtureEvent] = []

    if scenario in {"insert-update", "full"}:
        events.append(_event("c", base_ts_ms, "Education", "US", "active"))
        events.append(_event("u", base_ts_ms + 1_000, "Gaming", "US", "review_hold"))

    if scenario in {"same-ts-tiebreak", "full"}:
        events.append(_event("u", base_ts_ms + 2_000, "Comedy", "US", "active"))
        events.append(_event("u", base_ts_ms + 2_000, "Comedy", "US", "copyright_strike"))

    return events


def expected_final_state(events: List[FixtureEvent]) -> FixtureSummary:
    if not events:
        raise ValueError("events must be non-empty")

    winner_index = max(
        range(len(events)),
        key=lambda idx: (int(events[idx].payload["ts_ms"]), idx),
    )
    winner = events[winner_index]
    after = winner.payload["after"]

    return FixtureSummary(
        video_id=str(after["video_id"]),
        scenario="derived",
        event_count=len(events),
        expected_status=str(after["status"]),
        expected_source_ts_ms=int(winner.payload["ts_ms"]),
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit deterministic CDC fixtures for MIC-37")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--video-id", required=True)
    parser.add_argument(
        "--scenario",
        choices=["insert-update", "same-ts-tiebreak", "full"],
        default="full",
    )
    parser.add_argument("--schema-version", default=DEFAULT_SCHEMA_VERSION)
    parser.add_argument("--base-ts-ms", type=int, default=DEFAULT_BASE_TS_MS)
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _emit_to_kafka(topic: str, bootstrap_servers: str, events: List[FixtureEvent], sleep_ms: int) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers, "client.id": "mic37-fixture-emitter"})

    for event in events:
        producer.produce(topic, key=event.key, value=json.dumps(event.payload))
        producer.poll(0)
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1_000.0)

    producer.flush()


def main(argv: List[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    events = build_fixture_events(
        video_id=args.video_id,
        scenario=args.scenario,
        schema_version=args.schema_version,
        base_ts_ms=args.base_ts_ms,
    )

    if args.dry_run:
        for idx, event in enumerate(events):
            print(json.dumps({"index": idx, "key": event.key, "value": event.payload}, sort_keys=True))
    else:
        _emit_to_kafka(args.topic, args.bootstrap_servers, events, args.sleep_ms)

    summary = expected_final_state(events)
    print(
        json.dumps(
            {
                "video_id": summary.video_id,
                "scenario": args.scenario,
                "event_count": summary.event_count,
                "expected_status": summary.expected_status,
                "expected_source_ts_ms": summary.expected_source_ts_ms,
                "topic": args.topic,
                "bootstrap_servers": args.bootstrap_servers,
                "dry_run": args.dry_run,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
