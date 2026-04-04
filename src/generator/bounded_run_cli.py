"""CLI entrypoint for bounded-run generator."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

# Support direct invocation via:
# python src/generator/bounded_run_cli.py
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from generator.bounded_run.clock import RealClock, SimulatedClock
from generator.bounded_run.config import ConfigError, load_run_config
from generator.bounded_run.constants import DEFAULT_SCHEMA_VERSION
from generator.bounded_run.preflight import (
    bootstrap_kafka_topics,
    build_default_topic_expectations,
    run_kafka_preflight,
)
from generator.bounded_run.runner import BoundedRunGenerator
from generator.bounded_run.sink import InMemoryEventSink, KafkaEventSink


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run bounded mock event generator")
    parser.add_argument("--config", required=True, help="Path to run_config JSON")
    parser.add_argument("--run-id", help="Override run_id")
    parser.add_argument("--seed", type=int, help="Override seed")
    parser.add_argument("--duration-minutes", type=int, help="Override duration_minutes")
    parser.add_argument("--events-per-sec", type=int, help="Override events_per_sec")
    parser.add_argument("--late-event-ratio", type=float, help="Override late_event_ratio")
    parser.add_argument("--rule-version", help="Override rule_version")
    parser.add_argument("--started-at", help="Override started_at ISO-8601")
    parser.add_argument(
        "--scenario-mix",
        help="Override scenario_mix as JSON object string",
    )
    parser.add_argument(
        "--schema-version",
        default=DEFAULT_SCHEMA_VERSION,
        help=f"Schema version to stamp on emitted events (default: {DEFAULT_SCHEMA_VERSION})",
    )
    parser.add_argument(
        "--sink",
        choices=["dry-run", "kafka"],
        default="dry-run",
        help="Emission target. dry-run keeps events in-memory and only writes artifacts.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers when --sink kafka",
    )
    parser.add_argument(
        "--msk-iam",
        action="store_true",
        help="Use AWS MSK IAM (SASL/OAUTHBEARER) auth. Required for MSK Serverless.",
    )
    parser.add_argument(
        "--aws-region",
        default="us-east-1",
        help="AWS region for MSK IAM token signing (default: us-east-1)",
    )
    parser.add_argument(
        "--content-events-min-partitions",
        type=int,
        default=6,
        help="Minimum partitions required for content_events topic preflight checks",
    )
    parser.add_argument(
        "--cdc-videos-min-partitions",
        type=int,
        default=3,
        help="Minimum partitions required for cdc.content.videos topic preflight checks",
    )
    parser.add_argument(
        "--artifacts-root",
        default="artifacts/generator_runs",
        help="Root path for run artifacts",
    )
    parser.add_argument(
        "--real-time",
        action="store_true",
        help="Use wall clock sleeping for gate/content windows. Default is simulated clock (fast).",
    )
    return parser


def _build_overrides(args: argparse.Namespace) -> Dict[str, Any]:
    overrides: Dict[str, Any] = {
        "run_id": args.run_id,
        "seed": args.seed,
        "duration_minutes": args.duration_minutes,
        "events_per_sec": args.events_per_sec,
        "late_event_ratio": args.late_event_ratio,
        "rule_version": args.rule_version,
        "started_at": args.started_at,
    }
    if args.scenario_mix is not None:
        overrides["scenario_mix"] = args.scenario_mix
    return overrides


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        config = load_run_config(args.config, _build_overrides(args))
    except ConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    try:
        if args.sink == "kafka":
            topic_expectations = build_default_topic_expectations(
                content_events_min_partitions=args.content_events_min_partitions,
                cdc_videos_min_partitions=args.cdc_videos_min_partitions,
                replication_factor=3 if args.msk_iam else 1,
            )
            bootstrap_kafka_topics(
                bootstrap_servers=args.bootstrap_servers,
                expectations=topic_expectations,
                use_iam_auth=args.msk_iam,
                aws_region=args.aws_region,
            )
            run_kafka_preflight(
                bootstrap_servers=args.bootstrap_servers,
                expectations=topic_expectations,
                use_iam_auth=args.msk_iam,
                aws_region=args.aws_region,
            )
            sink = KafkaEventSink(
                bootstrap_servers=args.bootstrap_servers,
                use_iam_auth=args.msk_iam,
                aws_region=args.aws_region,
            )
        else:
            sink = InMemoryEventSink()

        clock = RealClock() if args.real_time else SimulatedClock(config.started_at)

        runner = BoundedRunGenerator(
            config=config,
            sink=sink,
            artifacts_root=Path(args.artifacts_root),
            schema_version=args.schema_version,
            clock=clock,
        )

        result = runner.run()
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"Run failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result.summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
