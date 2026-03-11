from __future__ import annotations

import sys
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import unittest

from spark.rt_action_decisioning import (
    ACTION_BOOST,
    ACTION_NO_ACTION,
    ACTION_RESCUE,
    ACTION_REVIEW,
    DECISION_DOMAIN,
    DecisionFlags,
    DecisionInputs,
    assert_decision_domain,
    derive_decision,
    derive_flags,
    is_valid_decision,
    resolve_decision,
    suppress_no_action,
)


class RtActionDecisioningTests(unittest.TestCase):
    def test_decision_domain_is_exact_contract_set(self) -> None:
        self.assertEqual(set(DECISION_DOMAIN), {"BOOST", "REVIEW", "RESCUE", "NO_ACTION"})

    def test_precedence_overlap_resolves_to_boost(self) -> None:
        decision = resolve_decision(DecisionFlags(boost=True, review=True, rescue=True))
        self.assertEqual(decision, ACTION_BOOST)

    def test_precedence_overlap_resolves_to_review(self) -> None:
        decision = resolve_decision(DecisionFlags(boost=False, review=True, rescue=True))
        self.assertEqual(decision, ACTION_REVIEW)

    def test_precedence_overlap_resolves_to_rescue(self) -> None:
        decision = resolve_decision(DecisionFlags(boost=False, review=False, rescue=True))
        self.assertEqual(decision, ACTION_RESCUE)

    def test_boost_mapping_case(self) -> None:
        decision = derive_decision(
            DecisionInputs(
                velocity_30m=0.81,
                impressions_30m=150,
                completion_rate_30m=0.63,
                skip_rate_30m=0.20,
                play_start_30m=40,
                upload_age_minutes=120,
                velocity_p90_threshold=0.70,
                global_p40_impressions_threshold=90,
            )
        )
        self.assertEqual(decision, ACTION_BOOST)

    def test_review_mapping_case(self) -> None:
        decision = derive_decision(
            DecisionInputs(
                velocity_30m=0.75,
                impressions_30m=180,
                completion_rate_30m=0.40,
                skip_rate_30m=0.42,
                play_start_30m=60,
                upload_age_minutes=120,
                velocity_p90_threshold=0.70,
                global_p40_impressions_threshold=90,
            )
        )
        self.assertEqual(decision, ACTION_REVIEW)

    def test_rescue_mapping_case(self) -> None:
        decision = derive_decision(
            DecisionInputs(
                velocity_30m=0.66,
                impressions_30m=80,
                completion_rate_30m=0.71,
                skip_rate_30m=0.19,
                play_start_30m=44,
                upload_age_minutes=21,
                velocity_p90_threshold=0.70,
                global_p40_impressions_threshold=90,
            )
        )
        self.assertEqual(decision, ACTION_RESCUE)

    def test_no_action_mapping_case(self) -> None:
        decision = derive_decision(
            DecisionInputs(
                velocity_30m=0.50,
                impressions_30m=120,
                completion_rate_30m=0.44,
                skip_rate_30m=0.52,
                play_start_30m=20,
                upload_age_minutes=180,
                velocity_p90_threshold=0.70,
                global_p40_impressions_threshold=90,
            )
        )
        self.assertEqual(decision, ACTION_NO_ACTION)

    def test_no_action_is_suppressed_before_queue_write(self) -> None:
        rows = [
            (ACTION_BOOST, {"video_id": "v1"}),
            (ACTION_NO_ACTION, {"video_id": "v2"}),
            (ACTION_REVIEW, {"video_id": "v3"}),
            (ACTION_RESCUE, {"video_id": "v4"}),
        ]
        filtered = suppress_no_action(rows)
        self.assertEqual([item[0] for item in filtered], [ACTION_BOOST, ACTION_REVIEW, ACTION_RESCUE])

    def test_all_mapping_outputs_are_in_domain(self) -> None:
        decisions = [
            derive_decision(
                DecisionInputs(
                    velocity_30m=0.90,
                    impressions_30m=200,
                    completion_rate_30m=0.70,
                    skip_rate_30m=0.10,
                    play_start_30m=80,
                    upload_age_minutes=140,
                    velocity_p90_threshold=0.70,
                    global_p40_impressions_threshold=90,
                )
            ),
            derive_decision(
                DecisionInputs(
                    velocity_30m=0.73,
                    impressions_30m=180,
                    completion_rate_30m=0.30,
                    skip_rate_30m=0.60,
                    play_start_30m=80,
                    upload_age_minutes=140,
                    velocity_p90_threshold=0.70,
                    global_p40_impressions_threshold=90,
                )
            ),
            derive_decision(
                DecisionInputs(
                    velocity_30m=0.68,
                    impressions_30m=70,
                    completion_rate_30m=0.80,
                    skip_rate_30m=0.10,
                    play_start_30m=80,
                    upload_age_minutes=40,
                    velocity_p90_threshold=0.70,
                    global_p40_impressions_threshold=90,
                )
            ),
            derive_decision(
                DecisionInputs(
                    velocity_30m=0.68,
                    impressions_30m=150,
                    completion_rate_30m=0.40,
                    skip_rate_30m=0.60,
                    play_start_30m=10,
                    upload_age_minutes=140,
                    velocity_p90_threshold=0.70,
                    global_p40_impressions_threshold=90,
                )
            ),
        ]

        for decision in decisions:
            self.assertTrue(is_valid_decision(decision))


# ---------------------------------------------------------------------------
# resolve_decision — all-false path
# ---------------------------------------------------------------------------

class ResolveDecisionTests(unittest.TestCase):
    def test_all_false_flags_resolve_to_no_action(self) -> None:
        decision = resolve_decision(DecisionFlags(boost=False, review=False, rescue=False))
        self.assertEqual(decision, ACTION_NO_ACTION)


# ---------------------------------------------------------------------------
# is_valid_decision
# ---------------------------------------------------------------------------

class IsValidDecisionTests(unittest.TestCase):
    def test_unknown_string_returns_false(self) -> None:
        self.assertFalse(is_valid_decision("UNKNOWN"))

    def test_empty_string_returns_false(self) -> None:
        self.assertFalse(is_valid_decision(""))

    def test_lowercase_returns_false(self) -> None:
        self.assertFalse(is_valid_decision("boost"))


# ---------------------------------------------------------------------------
# assert_decision_domain
# ---------------------------------------------------------------------------

class AssertDecisionDomainTests(unittest.TestCase):
    def test_valid_values_do_not_raise(self) -> None:
        assert_decision_domain(["BOOST", "REVIEW", "RESCUE", "NO_ACTION"])

    def test_invalid_value_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            assert_decision_domain(["BOOST", "INVALID"])

    def test_all_invalid_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            assert_decision_domain(["X", "Y"])

    def test_empty_sequence_does_not_raise(self) -> None:
        assert_decision_domain([])


# ---------------------------------------------------------------------------
# derive_flags — direct unit tests
# ---------------------------------------------------------------------------

class DeriveFlagsTests(unittest.TestCase):
    def _base(self, **overrides) -> DecisionInputs:
        defaults = dict(
            velocity_30m=0.81,
            impressions_30m=150,
            completion_rate_30m=0.63,
            skip_rate_30m=0.20,
            play_start_30m=40,
            upload_age_minutes=120,
            velocity_p90_threshold=0.70,
            global_p40_impressions_threshold=90,
        )
        defaults.update(overrides)
        return DecisionInputs(**defaults)

    def test_boost_flags(self) -> None:
        flags = derive_flags(self._base())
        self.assertTrue(flags.boost)
        self.assertFalse(flags.review)
        self.assertFalse(flags.rescue)

    def test_review_flags(self) -> None:
        flags = derive_flags(self._base(completion_rate_30m=0.40, skip_rate_30m=0.42))
        self.assertFalse(flags.boost)
        self.assertTrue(flags.review)
        self.assertFalse(flags.rescue)

    def test_rescue_flags(self) -> None:
        flags = derive_flags(self._base(
            velocity_30m=0.66,
            impressions_30m=80,
            completion_rate_30m=0.71,
            skip_rate_30m=0.19,
            play_start_30m=44,
            upload_age_minutes=21,
        ))
        self.assertFalse(flags.boost)
        self.assertFalse(flags.review)
        self.assertTrue(flags.rescue)

    def test_no_action_flags(self) -> None:
        flags = derive_flags(self._base(
            velocity_30m=0.50,
            impressions_30m=120,
            completion_rate_30m=0.44,
            skip_rate_30m=0.52,
            play_start_30m=20,
            upload_age_minutes=180,
        ))
        self.assertFalse(flags.boost)
        self.assertFalse(flags.review)
        self.assertFalse(flags.rescue)


# ---------------------------------------------------------------------------
# suppress_no_action — edge cases
# ---------------------------------------------------------------------------

class SuppressNoActionTests(unittest.TestCase):
    def test_empty_list_returns_empty(self) -> None:
        self.assertEqual(suppress_no_action([]), [])

    def test_all_no_action_returns_empty(self) -> None:
        rows = [(ACTION_NO_ACTION, {"video_id": "v1"}), (ACTION_NO_ACTION, {"video_id": "v2"})]
        self.assertEqual(suppress_no_action(rows), [])

    def test_no_no_action_returns_all(self) -> None:
        rows = [
            (ACTION_BOOST, {"video_id": "v1"}),
            (ACTION_REVIEW, {"video_id": "v2"}),
            (ACTION_RESCUE, {"video_id": "v3"}),
        ]
        self.assertEqual(suppress_no_action(rows), rows)


# ---------------------------------------------------------------------------
# RESCUE isolation — one gate fails at a time → NO_ACTION
# ---------------------------------------------------------------------------

class RescueIsolationTests(unittest.TestCase):
    def _rescue_base(self, **overrides) -> DecisionInputs:
        defaults = dict(
            velocity_30m=0.66,
            impressions_30m=80,
            completion_rate_30m=0.71,
            skip_rate_30m=0.19,
            play_start_30m=44,
            upload_age_minutes=21,
            velocity_p90_threshold=0.70,
            global_p40_impressions_threshold=90,
        )
        defaults.update(overrides)
        return DecisionInputs(**defaults)

    def test_rescue_blocked_by_old_upload(self) -> None:
        decision = derive_decision(self._rescue_base(upload_age_minutes=61))
        self.assertEqual(decision, ACTION_NO_ACTION)

    def test_rescue_blocked_by_over_exposure(self) -> None:
        decision = derive_decision(self._rescue_base(impressions_30m=91))
        self.assertEqual(decision, ACTION_NO_ACTION)

    def test_rescue_blocked_by_gate_fail(self) -> None:
        decision = derive_decision(self._rescue_base(completion_rate_30m=0.40, skip_rate_30m=0.50))
        self.assertEqual(decision, ACTION_NO_ACTION)


# ---------------------------------------------------------------------------
# Boundary values — exact threshold equality
# ---------------------------------------------------------------------------

class BoundaryValueTests(unittest.TestCase):
    def _base(self, **overrides) -> DecisionInputs:
        defaults = dict(
            velocity_30m=0.70,
            impressions_30m=100,
            completion_rate_30m=0.55,
            skip_rate_30m=0.35,
            play_start_30m=30,
            upload_age_minutes=60,
            velocity_p90_threshold=0.70,
            global_p40_impressions_threshold=90,
        )
        defaults.update(overrides)
        return DecisionInputs(**defaults)

    def test_velocity_at_p90_threshold_is_candidate(self) -> None:
        # velocity_30m == p90 → is_candidate (>= boundary)
        decision = derive_decision(self._base(impressions_30m=200))
        self.assertEqual(decision, ACTION_BOOST)

    def test_impressions_at_100_is_candidate(self) -> None:
        # impressions_30m == 100 → is_candidate (>= boundary)
        decision = derive_decision(self._base(velocity_30m=0.80, impressions_30m=100))
        self.assertEqual(decision, ACTION_BOOST)

    def test_completion_rate_at_055_passes_gate(self) -> None:
        decision = derive_decision(self._base(velocity_30m=0.80, skip_rate_30m=0.20, play_start_30m=40))
        self.assertEqual(decision, ACTION_BOOST)

    def test_skip_rate_at_035_passes_gate(self) -> None:
        decision = derive_decision(self._base(velocity_30m=0.80, completion_rate_30m=0.60, play_start_30m=40))
        self.assertEqual(decision, ACTION_BOOST)

    def test_play_start_at_30_passes_gate(self) -> None:
        decision = derive_decision(self._base(velocity_30m=0.80, completion_rate_30m=0.60, skip_rate_30m=0.20))
        self.assertEqual(decision, ACTION_BOOST)

    def test_upload_age_at_60_qualifies_as_new_upload(self) -> None:
        # Non-candidate so must go through RESCUE path
        decision = derive_decision(self._base(
            velocity_30m=0.66,
            impressions_30m=80,
            completion_rate_30m=0.60,
            skip_rate_30m=0.20,
            play_start_30m=40,
            upload_age_minutes=60,
            global_p40_impressions_threshold=90,
        ))
        self.assertEqual(decision, ACTION_RESCUE)

    def test_impressions_at_p40_threshold_is_under_exposed(self) -> None:
        # impressions_30m == p40 → under-exposed (<= boundary)
        decision = derive_decision(self._base(
            velocity_30m=0.66,
            impressions_30m=90,
            completion_rate_30m=0.60,
            skip_rate_30m=0.20,
            play_start_30m=40,
            upload_age_minutes=30,
            global_p40_impressions_threshold=90,
        ))
        self.assertEqual(decision, ACTION_RESCUE)


if __name__ == "__main__":
    unittest.main()
