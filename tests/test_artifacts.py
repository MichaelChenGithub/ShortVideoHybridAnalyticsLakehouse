from __future__ import annotations

import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import json
import tempfile
import unittest
from pathlib import Path

from generator.m1.clock import SimulatedClock
from generator.m1.config import load_run_config
from generator.m1.runner import BoundedRunGenerator
from generator.m1.sink import InMemoryEventSink

from common import build_config, write_config


class ArtifactTests(unittest.TestCase):
    def test_required_artifacts_exist(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), build_config())
            config = load_run_config(cfg_path)

            result = BoundedRunGenerator(
                config=config,
                sink=InMemoryEventSink(),
                artifacts_root=Path(td) / "artifacts",
                clock=SimulatedClock(config.started_at),
            ).run()

            artifacts = result.summary["artifacts"]
            run_config_path = Path(artifacts["run_config"])
            video_registry_path = Path(artifacts["video_registry"])
            expected_actions_path = Path(artifacts["expected_actions"])

            self.assertTrue(run_config_path.exists())
            self.assertTrue(video_registry_path.exists())
            self.assertTrue(expected_actions_path.exists())

            run_cfg = json.loads(run_config_path.read_text(encoding="utf-8"))
            self.assertEqual(run_cfg["run_id"], config.run_id)

            expected_format = artifacts["expected_actions_format"]
            self.assertIn(expected_format, {"parquet", "jsonl_fallback"})
            if expected_format == "jsonl_fallback":
                lines = expected_actions_path.read_text(encoding="utf-8").strip().splitlines()
                self.assertGreater(len(lines), 0)
                first = json.loads(lines[0])
                self.assertIn("expected_action", first)
                self.assertIn("video_id", first)


if __name__ == "__main__":
    unittest.main()
