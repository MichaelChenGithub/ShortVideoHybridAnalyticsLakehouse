from __future__ import annotations

import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import generator.bounded_run_cli as bounded_run_cli
from generator.bounded_run.preflight import KafkaPreflightError


class BoundedRunCliArgsTests(unittest.TestCase):
    def test_partition_minimum_flags_default_values(self) -> None:
        parser = bounded_run_cli.build_arg_parser()
        args = parser.parse_args(["--config", "run.json"])

        self.assertEqual(args.content_events_min_partitions, 6)
        self.assertEqual(args.cdc_videos_min_partitions, 3)

    def test_partition_minimum_flags_support_override(self) -> None:
        parser = bounded_run_cli.build_arg_parser()
        args = parser.parse_args(
            [
                "--config",
                "run.json",
                "--content-events-min-partitions",
                "8",
                "--cdc-videos-min-partitions",
                "4",
            ]
        )

        self.assertEqual(args.content_events_min_partitions, 8)
        self.assertEqual(args.cdc_videos_min_partitions, 4)


class BoundedRunCliBehaviorTests(unittest.TestCase):
    def _fake_config(self):
        return SimpleNamespace(started_at=datetime(2026, 3, 5, tzinfo=timezone.utc))

    def _fake_runner(self):
        return SimpleNamespace(run=lambda: SimpleNamespace(summary={"result": "ok"}))

    def test_main_dry_run_skips_preflight(self) -> None:
        with (
            patch.object(bounded_run_cli, "load_run_config", return_value=self._fake_config()),
            patch.object(bounded_run_cli, "bootstrap_kafka_topics") as bootstrap_mock,
            patch.object(bounded_run_cli, "run_kafka_preflight") as preflight_mock,
            patch.object(bounded_run_cli, "InMemoryEventSink", return_value=object()) as in_mem_sink_mock,
            patch.object(bounded_run_cli, "BoundedRunGenerator", return_value=self._fake_runner()),
            patch.object(bounded_run_cli, "SimulatedClock", return_value=object()),
        ):
            exit_code = bounded_run_cli.main(["--config", "run.json", "--sink", "dry-run"])

        self.assertEqual(exit_code, 0)
        bootstrap_mock.assert_not_called()
        preflight_mock.assert_not_called()
        in_mem_sink_mock.assert_called_once()

    def test_main_kafka_runs_bootstrap_then_preflight_before_sink(self) -> None:
        call_order = []

        def _bootstrap_side_effect(**kwargs):
            call_order.append("bootstrap")
            expectations = kwargs["expectations"]
            self.assertEqual(expectations[0].topic, "content_events")
            self.assertEqual(expectations[0].min_partitions, 8)
            self.assertEqual(expectations[1].topic, "cdc.content.videos")
            self.assertEqual(expectations[1].min_partitions, 4)

        def _preflight_side_effect(**kwargs):
            call_order.append("preflight")
            expectations = kwargs["expectations"]
            self.assertEqual(expectations[0].topic, "content_events")
            self.assertEqual(expectations[0].min_partitions, 8)
            self.assertEqual(expectations[1].topic, "cdc.content.videos")
            self.assertEqual(expectations[1].min_partitions, 4)

        def _sink_side_effect(*args, **kwargs):
            del args, kwargs
            call_order.append("sink")
            return object()

        with (
            patch.object(bounded_run_cli, "load_run_config", return_value=self._fake_config()),
            patch.object(bounded_run_cli, "bootstrap_kafka_topics", side_effect=_bootstrap_side_effect),
            patch.object(bounded_run_cli, "run_kafka_preflight", side_effect=_preflight_side_effect),
            patch.object(bounded_run_cli, "KafkaEventSink", side_effect=_sink_side_effect),
            patch.object(bounded_run_cli, "BoundedRunGenerator", return_value=self._fake_runner()),
            patch.object(bounded_run_cli, "SimulatedClock", return_value=object()),
        ):
            exit_code = bounded_run_cli.main(
                [
                    "--config",
                    "run.json",
                    "--sink",
                    "kafka",
                    "--content-events-min-partitions",
                    "8",
                    "--cdc-videos-min-partitions",
                    "4",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(call_order[:3], ["bootstrap", "preflight", "sink"])

    def test_main_returns_failure_when_preflight_fails(self) -> None:
        with (
            patch.object(bounded_run_cli, "load_run_config", return_value=self._fake_config()),
            patch.object(bounded_run_cli, "bootstrap_kafka_topics", return_value={}),
            patch.object(
                bounded_run_cli,
                "run_kafka_preflight",
                side_effect=KafkaPreflightError("missing topic"),
            ),
            patch.object(bounded_run_cli, "KafkaEventSink") as kafka_sink_mock,
            patch.object(bounded_run_cli, "BoundedRunGenerator", return_value=self._fake_runner()),
            patch.object(bounded_run_cli, "SimulatedClock", return_value=object()),
        ):
            exit_code = bounded_run_cli.main(["--config", "run.json", "--sink", "kafka"])

        self.assertEqual(exit_code, 1)
        kafka_sink_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
