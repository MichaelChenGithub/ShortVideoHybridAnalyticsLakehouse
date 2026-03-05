from __future__ import annotations

import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import tempfile
import unittest
from pathlib import Path

from generator.m1.clock import SimulatedClock
from generator.m1.config import load_run_config
from generator.m1.runner import BoundedRunGenerator
from generator.m1.sink import InMemoryEventSink

from common import build_config, write_config


class LateEventTests(unittest.TestCase):
    def test_late_event_ratio_and_offset_bounds(self) -> None:
        overrides = {
            "events_per_sec": 2,
            "late_event_ratio": 0.2,
        }
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), build_config(overrides))
            config = load_run_config(cfg_path)
            sink = InMemoryEventSink()

            result = BoundedRunGenerator(
                config=config,
                sink=sink,
                artifacts_root=Path(td) / "artifacts",
                clock=SimulatedClock(config.started_at),
            ).run()

            summary = result.summary
            expected_late = round(config.total_content_events * config.late_event_ratio)
            self.assertEqual(summary["late_event_count"], expected_late)

            self.assertGreaterEqual(summary["late_offset_min_seconds"], 11)
            self.assertLessEqual(summary["late_offset_max_seconds"], 90)

            hist = summary["late_event_histogram"]
            hist_total = hist["11_30"] + hist["31_90"]
            self.assertEqual(hist_total, summary["late_event_count"])

            if hist_total > 0:
                near_ratio = hist["11_30"] / hist_total
                self.assertGreaterEqual(near_ratio, 0.65)
                self.assertLessEqual(near_ratio, 0.95)


if __name__ == "__main__":
    unittest.main()
