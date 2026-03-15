"""Scenario definitions and deterministic planning logic."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping

from .constants import (
    ALLOWED_EVENT_TYPES,
    SCENARIO_COLD_START_UNDER_EXPOSED,
    SCENARIO_INVALID_PAYLOAD_BURST,
    SCENARIO_KEYS,
    SCENARIO_NORMAL_BASELINE,
    SCENARIO_VIRAL_HIGH_QUALITY,
    SCENARIO_VIRAL_LOW_QUALITY,
)
from .deterministic import make_rng


@dataclass(frozen=True)
class ScenarioTemplate:
    scenario_id: str
    category: str
    region: str
    event_type_weights: Mapping[str, float]
    invalid_payload: bool = False


SCENARIO_TEMPLATES: Dict[str, ScenarioTemplate] = {
    SCENARIO_NORMAL_BASELINE: ScenarioTemplate(
        scenario_id=SCENARIO_NORMAL_BASELINE,
        category="comedy",
        region="US",
        event_type_weights={
            "impression": 0.62,
            "play_start": 0.20,
            "play_finish": 0.07,
            "like": 0.05,
            "share": 0.01,
            "skip": 0.05,
        },
    ),
    SCENARIO_VIRAL_HIGH_QUALITY: ScenarioTemplate(
        scenario_id=SCENARIO_VIRAL_HIGH_QUALITY,
        category="gaming",
        region="US",
        event_type_weights={
            "impression": 0.46,
            "play_start": 0.20,
            "play_finish": 0.15,
            "like": 0.11,
            "share": 0.05,
            "skip": 0.03,
        },
    ),
    SCENARIO_VIRAL_LOW_QUALITY: ScenarioTemplate(
        scenario_id=SCENARIO_VIRAL_LOW_QUALITY,
        category="sports",
        region="US",
        event_type_weights={
            "impression": 0.45,
            "play_start": 0.22,
            "play_finish": 0.05,
            "like": 0.09,
            "share": 0.03,
            "skip": 0.16,
        },
    ),
    SCENARIO_COLD_START_UNDER_EXPOSED: ScenarioTemplate(
        scenario_id=SCENARIO_COLD_START_UNDER_EXPOSED,
        category="education",
        region="US",
        event_type_weights={
            "impression": 0.36,
            "play_start": 0.24,
            "play_finish": 0.16,
            "like": 0.11,
            "share": 0.07,
            "skip": 0.06,
        },
    ),
    SCENARIO_INVALID_PAYLOAD_BURST: ScenarioTemplate(
        scenario_id=SCENARIO_INVALID_PAYLOAD_BURST,
        category="unknown",
        region="US",
        event_type_weights={
            "impression": 1.0,
        },
        invalid_payload=True,
    ),
}


def validate_templates() -> None:
    for key in SCENARIO_KEYS:
        if key not in SCENARIO_TEMPLATES:
            raise ValueError(f"Missing template for scenario: {key}")
    for template in SCENARIO_TEMPLATES.values():
        if abs(sum(template.event_type_weights.values()) - 1.0) > 1e-9:
            raise ValueError(f"Event type weights must sum to 1.0 for {template.scenario_id}")
        for event_type in template.event_type_weights:
            if event_type not in ALLOWED_EVENT_TYPES:
                raise ValueError(f"Unsupported event type '{event_type}' in {template.scenario_id}")


def plan_event_counts(total_events: int, scenario_mix: Mapping[str, float]) -> Dict[str, int]:
    if total_events <= 0:
        raise ValueError("total_events must be > 0")

    raw = {scenario: scenario_mix[scenario] * total_events for scenario in SCENARIO_KEYS}
    base = {scenario: int(math.floor(value)) for scenario, value in raw.items()}
    remainder = total_events - sum(base.values())

    remainders = sorted(
        ((raw[scenario] - base[scenario], scenario) for scenario in SCENARIO_KEYS),
        key=lambda pair: (-pair[0], pair[1]),
    )

    for index in range(remainder):
        _, scenario = remainders[index]
        base[scenario] += 1

    return {scenario: base[scenario] for scenario in SCENARIO_KEYS}


def realized_mix(counts: Mapping[str, int]) -> Dict[str, float]:
    total = sum(counts.values())
    if total == 0:
        return {scenario: 0.0 for scenario in SCENARIO_KEYS}
    return {scenario: counts.get(scenario, 0) / total for scenario in SCENARIO_KEYS}


def build_scenario_sequence(counts: Mapping[str, int], seed: int) -> List[str]:
    sequence: List[str] = []
    for scenario in SCENARIO_KEYS:
        count = int(counts.get(scenario, 0))
        if count < 0:
            raise ValueError(f"Scenario count must be >= 0 for {scenario}")
        sequence.extend([scenario] * count)

    rng = make_rng(seed, "scenario-sequence")
    rng.shuffle(sequence)
    return sequence


def weighted_choice(weights: Mapping[str, float], rng_seed: int, index: int) -> str:
    """Deterministic weighted event type selection for a given index."""
    rng = make_rng(rng_seed + index, "event-type")
    threshold = rng.random()

    cumulative = 0.0
    fallback = None
    for event_type, weight in weights.items():
        cumulative += weight
        fallback = event_type
        if threshold <= cumulative:
            return event_type
    assert fallback is not None
    return fallback


def summarize_counts(counts: Mapping[str, int]) -> str:
    return ", ".join(f"{scenario}:{counts[scenario]}" for scenario in SCENARIO_KEYS)


validate_templates()
