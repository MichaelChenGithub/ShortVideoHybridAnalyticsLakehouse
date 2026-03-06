from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_rt_video_cdc_upsert import validate_video_snapshot  # noqa: E402


class VerifyRtVideoCdcUpsertTests(unittest.TestCase):
    def test_validation_passes_for_fresh_matching_row(self) -> None:
        now_ms = 1_000_000
        rows = [
            {
                "video_id": "mic37_vid_001",
                "status": "copyright_strike",
                "updated_at": "2026-03-05T00:00:00Z",
                "source_ts_ms": now_ms - 30_000,
            }
        ]

        errors = validate_video_snapshot(
            rows,
            video_id="mic37_vid_001",
            now_ms=now_ms,
            max_freshness_minutes=2,
            expected_status="copyright_strike",
            expected_source_ts_ms=now_ms - 30_000,
        )

        self.assertEqual(errors, [])

    def test_validation_fails_when_row_missing(self) -> None:
        errors = validate_video_snapshot(
            [],
            video_id="missing_video",
            now_ms=1_000_000,
            max_freshness_minutes=2,
        )
        self.assertEqual(errors, ["No dim_videos row found for video_id=missing_video"])

    def test_validation_fails_for_stale_or_mismatched_values(self) -> None:
        now_ms = 1_000_000
        rows = [
            {
                "video_id": "mic37_vid_002",
                "status": "active",
                "updated_at": None,
                "source_ts_ms": now_ms - 600_000,
            }
        ]

        errors = validate_video_snapshot(
            rows,
            video_id="mic37_vid_002",
            now_ms=now_ms,
            max_freshness_minutes=1,
            expected_status="copyright_strike",
            expected_source_ts_ms=now_ms - 30_000,
        )

        joined = "\n".join(errors)
        self.assertIn("freshness breach", joined)
        self.assertIn("status mismatch", joined)
        self.assertIn("source_ts_ms mismatch", joined)
        self.assertIn("updated_at is null", joined)


if __name__ == "__main__":
    unittest.main()
