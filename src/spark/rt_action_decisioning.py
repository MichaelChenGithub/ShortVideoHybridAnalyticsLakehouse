"""Decision mapping and NO_ACTION suppression for rt_action_queue producer path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Literal, Sequence, Tuple, TypeAlias

DecisionType: TypeAlias = Literal["BOOST", "REVIEW", "RESCUE", "NO_ACTION"]

ACTION_BOOST: DecisionType = "BOOST"
ACTION_REVIEW: DecisionType = "REVIEW"
ACTION_RESCUE: DecisionType = "RESCUE"
ACTION_NO_ACTION: DecisionType = "NO_ACTION"

DECISION_DOMAIN: Tuple[DecisionType, ...] = (
    ACTION_BOOST,
    ACTION_REVIEW,
    ACTION_RESCUE,
    ACTION_NO_ACTION,
)


@dataclass(frozen=True)
class DecisionInputs:
    """Contract-aligned inputs from realtime metric context and rule baselines."""

    velocity_30m: float
    impressions_30m: int
    completion_rate_30m: float
    skip_rate_30m: float
    play_start_30m: int
    upload_age_minutes: int
    velocity_p90_threshold: float
    global_p40_impressions_threshold: int


@dataclass(frozen=True)
class DecisionFlags:
    """Intermediate policy flags before precedence resolution."""

    boost: bool
    review: bool
    rescue: bool


def resolve_decision(flags: DecisionFlags) -> DecisionType:
    """Enforce contract precedence: BOOST > REVIEW > RESCUE > NO_ACTION."""

    if flags.boost:
        return ACTION_BOOST
    if flags.review:
        return ACTION_REVIEW
    if flags.rescue:
        return ACTION_RESCUE
    return ACTION_NO_ACTION


def derive_flags(inputs: DecisionInputs) -> DecisionFlags:
    """Derive policy flags from metric/rule inputs defined in the metric contract."""

    is_candidate = (
        inputs.velocity_30m >= inputs.velocity_p90_threshold
        and inputs.impressions_30m >= 100
    )
    gate_pass = (
        inputs.completion_rate_30m >= 0.55
        and inputs.skip_rate_30m <= 0.35
        and inputs.play_start_30m >= 30
    )
    is_under_exposed = (
        inputs.impressions_30m <= inputs.global_p40_impressions_threshold
    )
    is_new_upload = inputs.upload_age_minutes <= 60

    return DecisionFlags(
        boost=is_candidate and gate_pass,
        review=is_candidate and (not gate_pass),
        rescue=(not is_candidate) and gate_pass and is_new_upload and is_under_exposed,
    )


def derive_decision(inputs: DecisionInputs) -> DecisionType:
    """Map contract inputs to decision type within the fixed domain."""

    return resolve_decision(derive_flags(inputs))


def suppress_no_action(records: Iterable[Tuple[DecisionType, object]]) -> List[Tuple[DecisionType, object]]:
    """Filter `NO_ACTION` before queue write path."""

    return [record for record in records if record[0] != ACTION_NO_ACTION]


def is_valid_decision(value: str) -> bool:
    return value in DECISION_DOMAIN


def assert_decision_domain(values: Sequence[str]) -> None:
    invalid = [value for value in values if value not in DECISION_DOMAIN]
    if invalid:
        raise ValueError(f"invalid decision values: {invalid}")
