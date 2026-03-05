"""CLI entrypoint for MIC-34 bounded-run generator."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

# Support direct invocation via:
# python src/generator/m1_bounded_run.py
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from generator.m1.clock import RealClock, SimulatedClock
from generator.m1.config import ConfigError, load_run_config
from generator.m1.constants import DEFAULT_SCHEMA_VERSION
from generator.m1.runner import BoundedRunGenerator
from generator.m1.sink import InMemoryEventSink, KafkaEventSink


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run MIC-34 bounded mock event generator")
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

    if args.sink == "kafka":
        sink = KafkaEventSink(bootstrap_servers=args.bootstrap_servers)
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

    try:
        result = runner.run()
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"Run failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result.summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
