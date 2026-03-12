from __future__ import annotations

import sys
import unittest
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_action_queue_producer import (  # noqa: E402
    ACTION_QUEUE_INITIAL_STATE,
    ACTION_QUEUE_REQUIRED_FIELDS,
    REJECT_EMPTY_REASON_CODES,
    REJECT_INVALID_DECISION_TYPE,
    REJECT_INVALID_INITIAL_STATE,
    REJECT_INVALID_TIME_ORDER,
    REJECT_MISSING_REQUIRED_FIELD,
    REJECT_NULL_REQUIRED_FIELD,
    ActionQueueCandidate,
    ActionQueueWriteRow,
    DecisionContextRow,
    build_and_prepare_action_queue_write_rows,
    build_action_queue_candidates,
    build_action_queue_write_rows,
    prepare_action_queue_write_rows,
    validate_action_queue_write_row,
    validate_action_queue_write_rows,
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


class RtActionQueueProducerContractValidationTests(unittest.TestCase):
    def _candidate(self, **overrides: object) -> ActionQueueCandidate:
        base = dict(
            video_id="v_contract",
            decision_type="BOOST",
            window_start=datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 3, 10, 12, 30, tzinfo=timezone.utc),
            rule_version="rt_rules_v1",
            velocity_30m=0.81,
            impressions_30m=180,
            completion_rate_30m=0.67,
            skip_rate_30m=0.22,
            reason_codes=("HIGH_VELOCITY_P90", "GATE_PASS"),
        )
        base.update(overrides)
        return ActionQueueCandidate(**base)

    def _valid_write_row(self) -> ActionQueueWriteRow:
        decided_at = datetime(2026, 3, 10, 12, 31, tzinfo=timezone.utc)
        rows = build_action_queue_write_rows(
            [self._candidate()],
            decided_at_factory=lambda: decided_at,
            action_id_factory=lambda _candidate, _decided_at: "act_001",
        )
        self.assertEqual(len(rows), 1)
        return rows[0]

    def _context_row(self, **overrides: object) -> DecisionContextRow:
        base = dict(
            video_id="v1",
            window_start=datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 3, 10, 12, 30, tzinfo=timezone.utc),
            rule_version="rt_rules_v1",
            velocity_30m=0.82,
            impressions_30m=180,
            completion_rate_30m=0.66,
            skip_rate_30m=0.21,
            play_start_30m=70,
            upload_age_minutes=120,
            velocity_p90_threshold=0.7,
            global_p40_impressions_threshold=90,
        )
        base.update(overrides)
        return DecisionContextRow(**base)

    def test_write_row_contains_required_contract_fields(self) -> None:
        row = self._valid_write_row()
        for field in ACTION_QUEUE_REQUIRED_FIELDS:
            self.assertIsNotNone(getattr(row, field))
        self.assertEqual(row.state, ACTION_QUEUE_INITIAL_STATE)

    def test_validate_row_rejects_missing_required_field(self) -> None:
        row = asdict(self._valid_write_row())
        del row["rule_version"]
        is_valid, rejects = validate_action_queue_write_row(row, row_index=4)
        self.assertFalse(is_valid)
        self.assertIn(
            (REJECT_MISSING_REQUIRED_FIELD, "rule_version", 4),
            {(r.code, r.field, r.row_index) for r in rejects},
        )

    def test_validate_row_rejects_null_required_field(self) -> None:
        row = asdict(self._valid_write_row())
        row["rule_version"] = None
        is_valid, rejects = validate_action_queue_write_row(row, row_index=1)
        self.assertFalse(is_valid)
        self.assertIn(
            (REJECT_NULL_REQUIRED_FIELD, "rule_version", 1),
            {(r.code, r.field, r.row_index) for r in rejects},
        )

    def test_validate_row_rejects_invalid_decision_domain(self) -> None:
        row = asdict(self._valid_write_row())
        row["decision_type"] = "NO_ACTION"
        is_valid, rejects = validate_action_queue_write_row(row)
        self.assertFalse(is_valid)
        self.assertIn(REJECT_INVALID_DECISION_TYPE, {r.code for r in rejects})

    def test_validate_row_rejects_non_pending_initial_state(self) -> None:
        row = asdict(self._valid_write_row())
        row["state"] = "ACKED"
        is_valid, rejects = validate_action_queue_write_row(row)
        self.assertFalse(is_valid)
        self.assertIn(REJECT_INVALID_INITIAL_STATE, {r.code for r in rejects})

    def test_validate_row_rejects_invalid_time_order(self) -> None:
        row = asdict(self._valid_write_row())
        row["expires_at"] = row["decided_at"]
        is_valid, rejects = validate_action_queue_write_row(row)
        self.assertFalse(is_valid)
        self.assertIn(REJECT_INVALID_TIME_ORDER, {r.code for r in rejects})

    def test_validate_row_rejects_empty_reason_codes(self) -> None:
        row = asdict(self._valid_write_row())
        row["reason_codes"] = ()
        is_valid, rejects = validate_action_queue_write_row(row)
        self.assertFalse(is_valid)
        self.assertIn(REJECT_EMPTY_REASON_CODES, {r.code for r in rejects})

    def test_validate_row_accepts_valid_row(self) -> None:
        is_valid, rejects = validate_action_queue_write_row(self._valid_write_row(), row_index=9)
        self.assertTrue(is_valid)
        self.assertEqual(rejects, [])

    def test_write_row_ttl_mapping_matches_contract(self) -> None:
        decided_at = datetime(2026, 3, 10, 12, 31, tzinfo=timezone.utc)
        boost_row = build_action_queue_write_rows(
            [self._candidate(decision_type="BOOST")],
            decided_at_factory=lambda: decided_at,
            action_id_factory=lambda _candidate, _decided_at: "act_boost",
        )[0]
        review_row = build_action_queue_write_rows(
            [self._candidate(decision_type="REVIEW")],
            decided_at_factory=lambda: decided_at,
            action_id_factory=lambda _candidate, _decided_at: "act_review",
        )[0]
        rescue_row = build_action_queue_write_rows(
            [self._candidate(decision_type="RESCUE")],
            decided_at_factory=lambda: decided_at,
            action_id_factory=lambda _candidate, _decided_at: "act_rescue",
        )[0]

        self.assertEqual(
            (boost_row.expires_at - boost_row.decided_at).total_seconds(),
            15 * 60,
        )
        self.assertEqual(
            (review_row.expires_at - review_row.decided_at).total_seconds(),
            30 * 60,
        )
        self.assertEqual(
            (rescue_row.expires_at - rescue_row.decided_at).total_seconds(),
            30 * 60,
        )

    def test_write_row_priority_mapping_matches_urgency_contract(self) -> None:
        decided_at = datetime(2026, 3, 10, 12, 31, tzinfo=timezone.utc)
        rows = build_action_queue_write_rows(
            [
                self._candidate(decision_type="RESCUE"),
                self._candidate(decision_type="REVIEW"),
                self._candidate(decision_type="BOOST"),
            ],
            decided_at_factory=lambda: decided_at,
            action_id_factory=lambda _candidate, _decided_at: f"act_{_candidate.decision_type.lower()}",
        )
        priority_by_decision = {row.decision_type: row.priority for row in rows}
        self.assertEqual(priority_by_decision["RESCUE"], 1)
        self.assertEqual(priority_by_decision["REVIEW"], 2)
        self.assertEqual(priority_by_decision["BOOST"], 3)

    def test_batch_validator_splits_valid_and_reject_rows(self) -> None:
        valid = self._valid_write_row()
        invalid = ActionQueueWriteRow(
            **{
                **asdict(self._valid_write_row()),
                "action_id": "act_invalid",
                "decision_type": "NO_ACTION",
            }
        )
        valid_rows, rejects = validate_action_queue_write_rows([valid, invalid], strict=False)
        self.assertEqual(len(valid_rows), 1)
        self.assertEqual(valid_rows[0].action_id, "act_001")
        self.assertEqual(len(rejects), 1)
        self.assertEqual(rejects[0].row_index, 1)
        self.assertEqual(rejects[0].code, REJECT_INVALID_DECISION_TYPE)

    def test_batch_validator_strict_mode_raises_on_reject(self) -> None:
        invalid = ActionQueueWriteRow(
            **{
                **asdict(self._valid_write_row()),
                "decision_type": "NO_ACTION",
            }
        )
        with self.assertRaises(ValueError):
            validate_action_queue_write_rows([invalid], strict=True)

    def test_prepare_action_queue_write_rows_returns_valid_rows(self) -> None:
        fixed_ts = datetime(2026, 3, 10, 12, 31, tzinfo=timezone.utc)
        valid_rows, rejects = prepare_action_queue_write_rows(
            [self._candidate()],
            strict=False,
            decided_at_factory=lambda: fixed_ts,
            action_id_factory=lambda _candidate, _decided_at: "act_prepared",
        )
        self.assertEqual(len(valid_rows), 1)
        self.assertEqual(len(rejects), 0)
        self.assertEqual(valid_rows[0].state, ACTION_QUEUE_INITIAL_STATE)
        self.assertEqual(valid_rows[0].action_id, "act_prepared")

    def test_build_and_prepare_flow_returns_reject_for_null_action_id(self) -> None:
        row = self._context_row()
        valid_rows, rejects = build_and_prepare_action_queue_write_rows(
            [row],
            strict=False,
            decided_at_factory=lambda: datetime(2026, 3, 10, 12, 31, tzinfo=timezone.utc),
            action_id_factory=lambda _candidate, _decided_at: None,  # type: ignore[return-value]
        )
        self.assertEqual(len(valid_rows), 0)
        self.assertEqual(len(rejects), 1)
        self.assertEqual(rejects[0].code, REJECT_NULL_REQUIRED_FIELD)
        self.assertEqual(rejects[0].field, "action_id")

    def test_build_and_prepare_flow_strict_mode_raises_on_reject(self) -> None:
        with self.assertRaises(ValueError):
            build_and_prepare_action_queue_write_rows(
                [self._context_row()],
                strict=True,
                decided_at_factory=lambda: datetime(2026, 3, 10, 12, 31, tzinfo=timezone.utc),
                action_id_factory=lambda _candidate, _decided_at: None,  # type: ignore[return-value]
            )


if __name__ == "__main__":
    unittest.main()
