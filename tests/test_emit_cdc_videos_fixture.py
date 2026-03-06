from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.emit_cdc_videos_fixture import (  # noqa: E402
    DEFAULT_BASE_TS_MS,
    build_fixture_events,
    expected_final_state,
)


class EmitCdcVideosFixtureTests(unittest.TestCase):
    def test_insert_update_scenario(self) -> None:
        events = build_fixture_events("mic37_vid_001", "insert-update")
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].payload["op"], "c")
        self.assertEqual(events[1].payload["op"], "u")
        summary = expected_final_state(events)
        self.assertEqual(summary.expected_status, "review_hold")
        self.assertEqual(summary.expected_source_ts_ms, DEFAULT_BASE_TS_MS + 1_000)

    def test_same_ts_tiebreak_prefers_latest_emitted_event(self) -> None:
        events = build_fixture_events("mic37_vid_002", "same-ts-tiebreak")
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].payload["ts_ms"], events[1].payload["ts_ms"])
        summary = expected_final_state(events)
        self.assertEqual(summary.expected_status, "copyright_strike")

    def test_full_scenario_contains_four_events(self) -> None:
        events = build_fixture_events("mic37_vid_003", "full")
        self.assertEqual(len(events), 4)
        summary = expected_final_state(events)
        self.assertEqual(summary.expected_status, "copyright_strike")
        self.assertEqual(summary.expected_source_ts_ms, DEFAULT_BASE_TS_MS + 2_000)


if __name__ == "__main__":
    unittest.main()
