"""Unit tests for the batch_backfill task callable."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from orchestration.airflow_backfill_tasks import build_trigger_confs  # noqa: E402


class TestBuildTriggerConfs(unittest.TestCase):
    def test_returns_one_conf_per_date(self):
        result = build_trigger_confs("2026-03-01", "2026-03-03")
        self.assertEqual(result, [
            {"data_date": "2026-03-01"},
            {"data_date": "2026-03-02"},
            {"data_date": "2026-03-03"},
        ])

    def test_single_date_range(self):
        result = build_trigger_confs("2026-03-15", "2026-03-15")
        self.assertEqual(result, [{"data_date": "2026-03-15"}])

    def test_propagates_value_error_for_inverted_range(self):
        with self.assertRaises(ValueError):
            build_trigger_confs("2026-03-05", "2026-03-01")

    def test_propagates_value_error_for_range_exceeding_90_days(self):
        with self.assertRaises(ValueError):
            build_trigger_confs("2025-01-01", "2025-04-10")  # 99 days

    def test_conf_keys_match_daily_dag_param(self):
        result = build_trigger_confs("2026-03-01", "2026-03-02")
        for conf in result:
            self.assertIn("data_date", conf)
            self.assertEqual(list(conf.keys()), ["data_date"])


if __name__ == "__main__":
    unittest.main()
