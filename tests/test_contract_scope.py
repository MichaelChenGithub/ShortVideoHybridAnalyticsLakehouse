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
from generator.m1.constants import ALLOWED_EVENT_TYPES
from generator.m1.runner import BoundedRunGenerator
from generator.m1.sink import InMemoryEventSink

from common import build_config, write_config


class ContractAndScopeTests(unittest.TestCase):
    def test_content_event_contract_and_scope_guard(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), build_config())
            config = load_run_config(cfg_path)
            sink = InMemoryEventSink()

            result = BoundedRunGenerator(
                config=config,
                sink=sink,
                artifacts_root=Path(td) / "artifacts",
                clock=SimulatedClock(config.started_at),
            ).run()

            invalid_count = 0
            for record in sink.content_events:
                payload = record.value
                if "event_type" not in payload:
                    invalid_count += 1
                    continue

                self.assertIn(payload["event_type"], ALLOWED_EVENT_TYPES)
                self.assertIn("event_id", payload)
                self.assertIn("event_timestamp", payload)
                self.assertIn("video_id", payload)
                self.assertIn("user_id", payload)
                self.assertIn("schema_version", payload)
                self.assertIn("payload_json", payload)

            self.assertEqual(invalid_count, result.summary["invalid_payload_events"])
            self.assertFalse(result.summary["qa_table_writes_attempted"])
            for _, path_value in result.summary["artifacts"].items():
                if isinstance(path_value, str):
                    self.assertNotIn("lakehouse.qa", path_value)


if __name__ == "__main__":
    unittest.main()
