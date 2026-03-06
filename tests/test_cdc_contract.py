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


class CdcContractTests(unittest.TestCase):
    def _run_once(self, tmp_dir: str):
        cfg_path = write_config(Path(tmp_dir), build_config())
        config = load_run_config(cfg_path)
        sink = InMemoryEventSink()

        result = BoundedRunGenerator(
            config=config,
            sink=sink,
            artifacts_root=Path(tmp_dir) / "artifacts",
            clock=SimulatedClock(config.started_at),
        ).run()
        return result, sink

    @staticmethod
    def _cdc_signature(sink: InMemoryEventSink):
        signature = []
        for record in sink.cdc_events:
            payload = record.value
            after = payload["after"]
            signature.append(
                (
                    record.key,
                    payload["op"],
                    payload["ts_ms"],
                    after["video_id"],
                    after["category"],
                    after["region"],
                    after["upload_time"],
                    after["status"],
                )
            )
        return signature

    def test_cdc_schema_and_key_contract(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            result, sink = self._run_once(td)

            self.assertGreater(result.summary["cdc_bootstrap_events"], 0)
            self.assertGreater(result.summary["cdc_update_events"], 0)
            self.assertEqual(
                result.summary["cdc_total_events"],
                result.summary["cdc_bootstrap_events"] + result.summary["cdc_update_events"],
            )
            self.assertEqual(len(sink.cdc_events), result.summary["cdc_total_events"])

            c_count = 0
            u_count = 0
            ops_seen = set()
            events_by_video = {}
            for record in sink.cdc_events:
                payload = record.value
                self.assertIn("op", payload)
                self.assertIn(payload["op"], {"c", "u"})
                self.assertIn("ts_ms", payload)
                self.assertIsInstance(payload["ts_ms"], int)
                self.assertIn("schema_version", payload)
                self.assertIn("after", payload)

                after = payload["after"]
                for key in ("video_id", "category", "region", "upload_time", "status"):
                    self.assertIn(key, after)

                self.assertEqual(record.key, after["video_id"])

                op = payload["op"]
                ops_seen.add(op)
                if op == "c":
                    c_count += 1
                elif op == "u":
                    u_count += 1
                    self.assertTrue(after["category"].endswith("_u"))
                events_by_video.setdefault(after["video_id"], []).append(payload)

            self.assertEqual(ops_seen, {"c", "u"})
            self.assertEqual(c_count, result.summary["cdc_bootstrap_events"])
            self.assertEqual(u_count, result.summary["cdc_update_events"])
            self.assertEqual(c_count, u_count)

            for video_id, events in events_by_video.items():
                self.assertEqual(len(events), 2, msg=f"video_id={video_id} expected exactly c+u events")
                self.assertEqual(events[0]["op"], "c")
                self.assertEqual(events[1]["op"], "u")
                self.assertGreater(events[1]["ts_ms"], events[0]["ts_ms"])

    def test_cdc_bootstrap_emits_before_content_stream(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            _, sink = self._run_once(td)

            self.assertGreater(len(sink.cdc_events), 0)
            self.assertGreater(len(sink.content_events), 0)
            self.assertLess(sink.cdc_events[0].emitted_at, sink.content_events[0].emitted_at)

    def test_video_registry_artifact_written_without_external_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            result, _ = self._run_once(td)

            artifacts = result.summary["artifacts"]
            registry_path = Path(artifacts["video_registry"])
            self.assertTrue(registry_path.exists())

            registry_format = artifacts["video_registry_format"]
            self.assertIn(registry_format, {"parquet", "jsonl_fallback"})

            if registry_format == "jsonl_fallback":
                rows = [json.loads(line) for line in registry_path.read_text(encoding="utf-8").splitlines() if line.strip()]
                self.assertGreater(len(rows), 0)
                sample = rows[0]
                for field in ("video_id", "scenario_id", "category", "region", "upload_time", "status"):
                    self.assertIn(field, sample)

    def test_cdc_emission_is_deterministic_for_same_config_and_seed(self) -> None:
        with tempfile.TemporaryDirectory() as left_dir, tempfile.TemporaryDirectory() as right_dir:
            _, left_sink = self._run_once(left_dir)
            _, right_sink = self._run_once(right_dir)

            self.assertEqual(self._cdc_signature(left_sink), self._cdc_signature(right_sink))


if __name__ == "__main__":
    unittest.main()
