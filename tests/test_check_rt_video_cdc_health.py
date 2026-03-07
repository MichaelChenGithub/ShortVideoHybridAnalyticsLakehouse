from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.check_rt_video_cdc_health import validate_cdc_health  # noqa: E402


class CheckRtVideoCdcHealthTests(unittest.TestCase):
    def test_validation_passes_for_fresh_data_and_optional_threshold(self) -> None:
        now_ms = 2_000_000
        errors, metrics = validate_cdc_health(
            latest_source_ts_ms=now_ms - 20_000,
            now_ms=now_ms,
            max_freshness_minutes=2,
            invalid_count_lookback=4,
            lookback_minutes=10,
            max_invalid_rate=1.0,
        )
        self.assertEqual(errors, [])
        self.assertEqual(metrics["freshness_age_ms"], 20_000)
        self.assertAlmostEqual(metrics["invalid_rate_per_minute"], 0.4)

    def test_validation_fails_for_stale_source_and_rate_breach(self) -> None:
        now_ms = 2_000_000
        errors, _ = validate_cdc_health(
            latest_source_ts_ms=now_ms - 900_000,
            now_ms=now_ms,
            max_freshness_minutes=5,
            invalid_count_lookback=30,
            lookback_minutes=10,
            max_invalid_rate=2.0,
        )
        joined = "\n".join(errors)
        self.assertIn("freshness breach", joined)
        self.assertIn("invalid-rate breach", joined)

    def test_validation_fails_when_source_timestamp_missing(self) -> None:
        errors, _ = validate_cdc_health(
            latest_source_ts_ms=None,
            now_ms=2_000_000,
            max_freshness_minutes=5,
            invalid_count_lookback=0,
            lookback_minutes=10,
            max_invalid_rate=None,
        )
        self.assertIn("latest source_ts_ms is null", "\n".join(errors))

    def test_validation_handles_invalid_lookback_input(self) -> None:
        errors, metrics = validate_cdc_health(
            latest_source_ts_ms=1_999_000,
            now_ms=2_000_000,
            max_freshness_minutes=10,
            invalid_count_lookback=3,
            lookback_minutes=0,
            max_invalid_rate=None,
        )
        self.assertIn("lookback_minutes must be > 0", "\n".join(errors))
        self.assertAlmostEqual(metrics["invalid_rate_per_minute"], 3.0)


if __name__ == "__main__":
    unittest.main()
