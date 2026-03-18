from __future__ import annotations

import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from generator.bounded_run.clock import SimulatedClock
from generator.bounded_run.config import load_run_config
from generator.bounded_run.runner import BoundedRunGenerator
from generator.bounded_run.sink import InMemoryEventSink

from common import build_config, write_config


def _parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class RunnerLifecycleTests(unittest.TestCase):
    def test_gate_and_duration_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), build_config())
            config = load_run_config(cfg_path)
            sink = InMemoryEventSink()

            result = BoundedRunGenerator(
                config=config,
                sink=sink,
                artifacts_root=Path(td) / "artifacts",
                clock=SimulatedClock(config.started_at),
                cdc_gate_seconds=300,
            ).run()

            lifecycle = result.summary["lifecycle"]
            video_cdc_end = _parse_iso(lifecycle["video_cdc_bootstrap_emitted_at"])
            user_cdc_end = _parse_iso(lifecycle["user_cdc_bootstrap_emitted_at"])
            content_start = _parse_iso(lifecycle["content_started_at"])
            content_end = _parse_iso(lifecycle["content_ended_at"])

            self.assertEqual((content_start - video_cdc_end).total_seconds(), 300)
            self.assertEqual((content_start - user_cdc_end).total_seconds(), 300)
            self.assertEqual(
                (content_end - content_start).total_seconds(),
                config.duration_minutes * 60,
            )

            first_video_cdc_time = sink.video_cdc_events[0].emitted_at
            first_user_cdc_time = sink.user_cdc_events[0].emitted_at
            first_content_time = sink.content_events[0].emitted_at
            self.assertLess(first_video_cdc_time, first_content_time)
            self.assertLess(first_user_cdc_time, first_content_time)

            self.assertEqual(result.summary["planned_total_events"], config.total_content_events)
            self.assertEqual(result.summary["emitted_total_events"], config.total_content_events)
            self.assertGreater(result.summary["video_cdc_update_events"], 0)
            self.assertEqual(
                result.summary["video_cdc_total_events"],
                result.summary["video_cdc_bootstrap_events"] + result.summary["video_cdc_update_events"],
            )
            self.assertEqual(len(sink.video_cdc_events), result.summary["video_cdc_total_events"])
            self.assertEqual(
                result.summary["user_cdc_total_events"],
                result.summary["user_cdc_bootstrap_events"] + result.summary["user_cdc_update_events"],
            )
            self.assertEqual(len(sink.user_cdc_events), result.summary["user_cdc_total_events"])
            self.assertTrue(result.summary["acceptance"]["total_volume_pass"])


if __name__ == "__main__":
    unittest.main()
