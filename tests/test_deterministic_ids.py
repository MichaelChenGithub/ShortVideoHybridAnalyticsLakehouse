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
from generator.m1.deterministic import DeterministicIdFactory
from generator.m1.runner import BoundedRunGenerator
from generator.m1.sink import InMemoryEventSink

from common import build_config, write_config


class DeterministicIdTests(unittest.TestCase):
    def test_id_factory_is_stable_for_same_run(self) -> None:
        left = DeterministicIdFactory("run_a")
        right = DeterministicIdFactory("run_a")

        for _ in range(20):
            self.assertEqual(left.next_video_id(), right.next_video_id())
            self.assertEqual(left.next_user_id(), right.next_user_id())
            self.assertEqual(left.next_event_id(), right.next_event_id())

    def test_id_factory_changes_when_run_id_changes(self) -> None:
        left = DeterministicIdFactory("run_a")
        right = DeterministicIdFactory("run_b")
        self.assertNotEqual(left.next_video_id(), right.next_video_id())

    def test_full_run_same_seed_same_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), build_config())
            config = load_run_config(cfg_path)

            sink_a = InMemoryEventSink()
            sink_b = InMemoryEventSink()

            run_a = BoundedRunGenerator(
                config=config,
                sink=sink_a,
                artifacts_root=Path(td) / "a",
                clock=SimulatedClock(config.started_at),
            )
            run_b = BoundedRunGenerator(
                config=config,
                sink=sink_b,
                artifacts_root=Path(td) / "b",
                clock=SimulatedClock(config.started_at),
            )

            run_a.run()
            run_b.run()

            ids_a = [record.value["event_id"] for record in sink_a.content_events[:100]]
            ids_b = [record.value["event_id"] for record in sink_b.content_events[:100]]

            self.assertEqual(ids_a, ids_b)

    def test_full_run_different_seed_changes_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = build_config()
            cfg_path_a = write_config(Path(td), base)
            config_a = load_run_config(cfg_path_a)

            cfg_path_b = write_config(Path(td), build_config({"seed": 7, "run_id": "m1_test_seed7_r001"}))
            config_b = load_run_config(cfg_path_b)

            sink_a = InMemoryEventSink()
            sink_b = InMemoryEventSink()

            BoundedRunGenerator(
                config=config_a,
                sink=sink_a,
                artifacts_root=Path(td) / "a",
                clock=SimulatedClock(config_a.started_at),
            ).run()
            BoundedRunGenerator(
                config=config_b,
                sink=sink_b,
                artifacts_root=Path(td) / "b",
                clock=SimulatedClock(config_b.started_at),
            ).run()

            ids_a = [record.value["event_id"] for record in sink_a.content_events[:30]]
            ids_b = [record.value["event_id"] for record in sink_b.content_events[:30]]
            self.assertNotEqual(ids_a, ids_b)


if __name__ == "__main__":
    unittest.main()
