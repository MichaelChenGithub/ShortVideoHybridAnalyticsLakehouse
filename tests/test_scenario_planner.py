from __future__ import annotations

import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


import unittest

from generator.m1.constants import SCENARIO_KEYS
from generator.m1.scenario import build_scenario_sequence, plan_event_counts, realized_mix


class ScenarioPlannerTests(unittest.TestCase):
    def test_largest_remainder_distribution_within_threshold(self) -> None:
        mix = {
            "normal_baseline": 0.55,
            "viral_high_quality": 0.20,
            "viral_low_quality": 0.10,
            "cold_start_under_exposed": 0.10,
            "invalid_payload_burst": 0.05,
        }
        total_events = 600

        planned = plan_event_counts(total_events, mix)
        self.assertEqual(sum(planned.values()), total_events)

        realized = realized_mix(planned)
        for scenario in SCENARIO_KEYS:
            self.assertLessEqual(abs(realized[scenario] - mix[scenario]), 0.02)

    def test_sequence_determinism(self) -> None:
        planned = {
            "normal_baseline": 330,
            "viral_high_quality": 120,
            "viral_low_quality": 60,
            "cold_start_under_exposed": 60,
            "invalid_payload_burst": 30,
        }
        seq_a = build_scenario_sequence(planned, seed=42)
        seq_b = build_scenario_sequence(planned, seed=42)
        seq_c = build_scenario_sequence(planned, seed=99)

        self.assertEqual(seq_a, seq_b)
        self.assertNotEqual(seq_a, seq_c)

    def test_rerun_drift_zero_for_same_input(self) -> None:
        mix = {
            "normal_baseline": 0.55,
            "viral_high_quality": 0.20,
            "viral_low_quality": 0.10,
            "cold_start_under_exposed": 0.10,
            "invalid_payload_burst": 0.05,
        }
        planned_a = plan_event_counts(600, mix)
        planned_b = plan_event_counts(600, mix)

        realized_a = realized_mix(planned_a)
        realized_b = realized_mix(planned_b)

        for scenario in SCENARIO_KEYS:
            self.assertLessEqual(abs(realized_a[scenario] - realized_b[scenario]), 0.01)


if __name__ == "__main__":
    unittest.main()
