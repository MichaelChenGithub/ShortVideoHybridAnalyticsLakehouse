from __future__ import annotations

import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import tempfile
import unittest
from pathlib import Path

from generator.m1.config import ConfigError, load_run_config

from common import build_config, write_config


class RunConfigValidationTests(unittest.TestCase):
    def test_valid_config_loads(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), build_config())
            config = load_run_config(cfg_path)
        self.assertEqual(config.run_id, "m1_test_seed42_r001")
        self.assertEqual(config.duration_minutes, 10)

    def test_missing_required_field(self) -> None:
        payload = build_config()
        payload.pop("run_id")
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), payload)
            with self.assertRaises(ConfigError):
                load_run_config(cfg_path)

    def test_missing_scenario_key_rejected(self) -> None:
        mix = build_config()["scenario_mix"]
        del mix["viral_low_quality"]
        payload = build_config({"scenario_mix": mix})
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), payload)
            with self.assertRaises(ConfigError):
                load_run_config(cfg_path)

    def test_unknown_scenario_key_rejected(self) -> None:
        mix = build_config()["scenario_mix"]
        mix["not_a_valid_scenario"] = 0.0
        payload = build_config({"scenario_mix": mix})
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), payload)
            with self.assertRaises(ConfigError):
                load_run_config(cfg_path)

    def test_scenario_mix_sum_must_be_one(self) -> None:
        payload = build_config(
            {
                "scenario_mix": {
                    "normal_baseline": 0.60,
                    "viral_high_quality": 0.20,
                    "viral_low_quality": 0.10,
                    "cold_start_under_exposed": 0.10,
                    "invalid_payload_burst": 0.10,
                }
            }
        )
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), payload)
            with self.assertRaises(ConfigError):
                load_run_config(cfg_path)

    def test_late_event_ratio_range(self) -> None:
        payload = build_config({"late_event_ratio": 0.25})
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), payload)
            with self.assertRaises(ConfigError):
                load_run_config(cfg_path)

    def test_duration_minimum(self) -> None:
        payload = build_config({"duration_minutes": 9})
        with tempfile.TemporaryDirectory() as td:
            cfg_path = write_config(Path(td), payload)
            with self.assertRaises(ConfigError):
                load_run_config(cfg_path)


if __name__ == "__main__":
    unittest.main()
