"""CLI entrypoint for adversarial run generator."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from generator.bounded_run.adversarial import ADVERSARIAL_SCENARIO_KEYS
from generator.bounded_run.adversarial_config import AdversarialConfigError, load_adversarial_run_config
from generator.bounded_run.clock import RealClock, SimulatedClock
from generator.bounded_run.sink import InMemoryEventSink, KafkaEventSink
from generator.bounded_run.adversarial_runner import AdversarialRunner


def build_arg_parser() -> argparse.ArgumentParser:
    valid_scenarios = ", ".join(ADVERSARIAL_SCENARIO_KEYS)
    parser = argparse.ArgumentParser(description="Run adversarial scenario generator")
    parser.add_argument("--config", required=True, help="Path to adversarial run config JSON")
    parser.add_argument("--run-id", help="Override run_id")
    parser.add_argument("--seed", type=int, help="Override seed")
    parser.add_argument("--duration-minutes", type=int, help="Override duration_minutes")
    parser.add_argument("--events-per-sec", type=int, help="Override events_per_sec")
    parser.add_argument("--started-at", help="Override started_at ISO-8601")
    parser.add_argument(
        "--adversarial-scenario",
        choices=list(ADVERSARIAL_SCENARIO_KEYS),
        help=f"Override adversarial_scenario. Valid: {valid_scenarios}",
    )
    parser.add_argument(
        "--sink",
        choices=["dry-run", "kafka"],
        default="dry-run",
        help="Emission target. dry-run keeps events in-memory only.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers when --sink kafka",
    )
    parser.add_argument(
        "--msk-iam",
        action="store_true",
        help="Use AWS MSK IAM auth.",
    )
    parser.add_argument(
        "--aws-region",
        default="us-east-1",
        help="AWS region for MSK IAM token signing",
    )
    parser.add_argument(
        "--artifacts-root",
        default="artifacts/adversarial_runs",
        help="Root path for run artifacts",
    )
    parser.add_argument(
        "--real-time",
        action="store_true",
        help="Use wall clock for phase gaps. Default is simulated clock.",
    )
    return parser


def _build_overrides(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "run_id": args.run_id,
        "seed": args.seed,
        "duration_minutes": args.duration_minutes,
        "events_per_sec": args.events_per_sec,
        "started_at": args.started_at,
        "adversarial_scenario": args.adversarial_scenario,
    }


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        config = load_adversarial_run_config(args.config, _build_overrides(args))
    except AdversarialConfigError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    try:
        if args.sink == "kafka":
            sink = KafkaEventSink(
                bootstrap_servers=args.bootstrap_servers,
                use_iam_auth=args.msk_iam,
                aws_region=args.aws_region,
            )
        else:
            sink = InMemoryEventSink()

        clock = RealClock() if args.real_time else SimulatedClock(config.started_at)

        runner = AdversarialRunner(
            config=config,
            sink=sink,
            artifacts_root=Path(args.artifacts_root),
            clock=clock,
            logger=lambda msg: print(msg, file=sys.stderr),
        )

        result = runner.run()
    except NotImplementedError as exc:
        print(f"Scenario not yet implemented: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"Run failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result.summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
