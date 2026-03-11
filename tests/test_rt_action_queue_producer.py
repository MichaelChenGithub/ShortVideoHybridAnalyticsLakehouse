from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_action_queue_producer import (  # noqa: E402
    ActionQueueCandidate,
    DecisionContextRow,
    build_action_queue_candidates,
)


class RtActionQueueProducerTests(unittest.TestCase):
    def _row(self, *, video_id: str, **overrides: object) -> DecisionContextRow:
        base = dict(
            video_id=video_id,
            window_start=datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 3, 10, 12, 30, tzinfo=timezone.utc),
            rule_version="m1_rtv1",
            velocity_30m=0.8,
            impressions_30m=150,
            completion_rate_30m=0.65,
            skip_rate_30m=0.2,
            play_start_30m=50,
            upload_age_minutes=120,
            velocity_p90_threshold=0.7,
            global_p40_impressions_threshold=90,
        )
        base.update(overrides)
        return DecisionContextRow(**base)

    def test_build_candidates_suppresses_no_action(self) -> None:
        rows = [
            self._row(video_id="v_boost"),
            self._row(video_id="v_review", completion_rate_30m=0.4, skip_rate_30m=0.5),
            self._row(
                video_id="v_rescue",
                velocity_30m=0.66,
                impressions_30m=80,
                upload_age_minutes=30,
            ),
            self._row(
                video_id="v_no_action",
                velocity_30m=0.5,
                impressions_30m=130,
                completion_rate_30m=0.4,
                skip_rate_30m=0.6,
                play_start_30m=20,
                upload_age_minutes=180,
            ),
        ]

        candidates = build_action_queue_candidates(rows)

        self.assertEqual([c.video_id for c in candidates], ["v_boost", "v_review", "v_rescue"])
        self.assertEqual([c.decision_type for c in candidates], ["BOOST", "REVIEW", "RESCUE"])

    def test_candidate_fields_keep_decision_evidence(self) -> None:
        row = self._row(video_id="v1")
        candidates = build_action_queue_candidates([row])

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertIsInstance(candidate, ActionQueueCandidate)
        self.assertEqual(candidate.video_id, "v1")
        self.assertEqual(candidate.rule_version, "m1_rtv1")
        self.assertEqual(candidate.velocity_30m, 0.8)
        self.assertEqual(candidate.impressions_30m, 150)
        self.assertEqual(candidate.completion_rate_30m, 0.65)
        self.assertEqual(candidate.skip_rate_30m, 0.2)
        self.assertEqual(candidate.reason_codes, ("HIGH_VELOCITY_P90", "GATE_PASS"))
        self.assertEqual(candidate.window_start, datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc))
        self.assertEqual(candidate.window_end, datetime(2026, 3, 10, 12, 30, tzinfo=timezone.utc))
        self.assertEqual(candidate.decision_type, "BOOST")

    def test_review_reason_codes(self) -> None:
        row = self._row(video_id="v_review", completion_rate_30m=0.4, skip_rate_30m=0.5)
        candidates = build_action_queue_candidates([row])
        self.assertEqual(candidates[0].reason_codes, ("HIGH_VELOCITY_P90", "LOW_COMPLETION", "HIGH_SKIP"))

    def test_rescue_reason_codes(self) -> None:
        row = self._row(
            video_id="v_rescue",
            velocity_30m=0.66,
            impressions_30m=80,
            upload_age_minutes=30,
        )
        candidates = build_action_queue_candidates([row])
        self.assertEqual(candidates[0].reason_codes, ("NEW_UPLOAD_LT_60M", "UNDER_EXPOSED_P40", "GATE_PASS"))

    def test_empty_input_returns_empty(self) -> None:
        self.assertEqual(build_action_queue_candidates([]), [])

    def test_all_no_action_returns_empty(self) -> None:
        rows = [
            self._row(
                video_id=f"v{i}",
                velocity_30m=0.5,
                impressions_30m=130,
                completion_rate_30m=0.4,
                skip_rate_30m=0.6,
                play_start_30m=20,
                upload_age_minutes=180,
            )
            for i in range(3)
        ]
        self.assertEqual(build_action_queue_candidates(rows), [])

    def test_input_order_preserved_in_output(self) -> None:
        rows = [
            self._row(video_id="v_rescue", velocity_30m=0.66, impressions_30m=80, upload_age_minutes=30),
            self._row(video_id="v_boost"),
            self._row(video_id="v_review", completion_rate_30m=0.4, skip_rate_30m=0.5),
        ]
        candidates = build_action_queue_candidates(rows)
        self.assertEqual([c.video_id for c in candidates], ["v_rescue", "v_boost", "v_review"])

    def test_velocity_p90_threshold_controls_decision(self) -> None:
        # Raising threshold above row velocity → NO_ACTION
        row_no_action = self._row(video_id="v_high_threshold", velocity_p90_threshold=0.99)
        self.assertEqual(build_action_queue_candidates([row_no_action]), [])

        # Lowering threshold below row velocity → BOOST
        row_boost = self._row(video_id="v_low_threshold", velocity_p90_threshold=0.5)
        candidates = build_action_queue_candidates([row_boost])
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].decision_type, "BOOST")

    def test_global_p40_threshold_controls_rescue(self) -> None:
        rescue_base = dict(
            velocity_30m=0.66,
            impressions_30m=80,
            upload_age_minutes=30,
        )
        # p40 threshold above impressions → under-exposed → RESCUE
        row_rescue = self._row(video_id="v_under_exposed", global_p40_impressions_threshold=100, **rescue_base)
        candidates = build_action_queue_candidates([row_rescue])
        self.assertEqual(candidates[0].decision_type, "RESCUE")

        # p40 threshold below impressions → over-exposed → NO_ACTION
        row_no_action = self._row(video_id="v_over_exposed", global_p40_impressions_threshold=79, **rescue_base)
        self.assertEqual(build_action_queue_candidates([row_no_action]), [])


if __name__ == "__main__":
    unittest.main()
