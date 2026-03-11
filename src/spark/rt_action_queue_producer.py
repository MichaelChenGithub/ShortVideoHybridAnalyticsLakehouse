"""Producer-side action queue candidate derivation for realtime decisioning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List

from spark.rt_action_decisioning import (
    ACTION_BOOST,
    ACTION_RESCUE,
    ACTION_REVIEW,
    DecisionInputs,
    derive_decision,
)


@dataclass(frozen=True)
class DecisionContextRow:
    """Minimal producer input row from decision context metrics."""

    video_id: str
    window_start: datetime
    window_end: datetime
    rule_version: str
    velocity_30m: float
    impressions_30m: int
    completion_rate_30m: float
    skip_rate_30m: float
    play_start_30m: int
    upload_age_minutes: int
    velocity_p90_threshold: float
    global_p40_impressions_threshold: int


@dataclass(frozen=True)
class ActionQueueCandidate:
    """Write-ready queue candidate excluding dedupe/cooldown winner logic."""

    video_id: str
    decision_type: str
    window_start: datetime
    window_end: datetime
    rule_version: str
    velocity_30m: float
    impressions_30m: int
    completion_rate_30m: float
    skip_rate_30m: float
    reason_codes: tuple[str, ...]


_REASON_CODES = {
    ACTION_BOOST: ("HIGH_VELOCITY_P90", "GATE_PASS"),
    ACTION_REVIEW: ("HIGH_VELOCITY_P90", "LOW_COMPLETION", "HIGH_SKIP"),
    ACTION_RESCUE: ("NEW_UPLOAD_LT_60M", "UNDER_EXPOSED_P40", "GATE_PASS"),
}


def _to_decision_inputs(row: DecisionContextRow) -> DecisionInputs:
    return DecisionInputs(
        velocity_30m=row.velocity_30m,
        impressions_30m=row.impressions_30m,
        completion_rate_30m=row.completion_rate_30m,
        skip_rate_30m=row.skip_rate_30m,
        play_start_30m=row.play_start_30m,
        upload_age_minutes=row.upload_age_minutes,
        velocity_p90_threshold=row.velocity_p90_threshold,
        global_p40_impressions_threshold=row.global_p40_impressions_threshold,
    )


def build_action_queue_candidates(rows: Iterable[DecisionContextRow]) -> List[ActionQueueCandidate]:
    """Build queue-write candidates and suppress `NO_ACTION` before write path."""

    candidates: List[ActionQueueCandidate] = []
    for row in rows:
        decision = derive_decision(_to_decision_inputs(row))
        if decision not in _REASON_CODES:
            # Contract: `NO_ACTION` must not be persisted to rt_action_queue.
            continue

        candidates.append(
            ActionQueueCandidate(
                video_id=row.video_id,
                decision_type=decision,
                window_start=row.window_start,
                window_end=row.window_end,
                rule_version=row.rule_version,
                velocity_30m=row.velocity_30m,
                impressions_30m=row.impressions_30m,
                completion_rate_30m=row.completion_rate_30m,
                skip_rate_30m=row.skip_rate_30m,
                reason_codes=_REASON_CODES[decision],
            )
        )

    return candidates
