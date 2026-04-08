"""Adversarial scenario registry and templates.

Each AdversarialTemplate declares:
  - scenario_id:            unique key used in config and dispatch
  - description:            human-readable summary of what it tests
  - required_params:        keys that must appear in scenario_params
  - default_params:         merged with scenario_params at config load time

To add a new adversarial scenario:
  1. Add a ADVERSARIAL_<NAME> constant below.
  2. Add it to ADVERSARIAL_SCENARIO_KEYS.
  3. Add an AdversarialTemplate entry in ADVERSARIAL_REGISTRY.
  4. Add a _run_<name> method to AdversarialRunner (adversarial_runner.py).
  5. Add a config JSON under config/adversarial/.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Sequence, Tuple

# ---------------------------------------------------------------------------
# Scenario key constants
# ---------------------------------------------------------------------------

ADVERSARIAL_LATE_BULK_ARRIVAL = "late_bulk_arrival"
ADVERSARIAL_DUPLICATE_STORM = "duplicate_event_storm"
ADVERSARIAL_SCHEMA_MISMATCH = "schema_mismatch"
ADVERSARIAL_LATE_ARRIVAL_REPROCESS = "late_arrival_reprocess"

ADVERSARIAL_SCENARIO_KEYS: Tuple[str, ...] = (
    ADVERSARIAL_LATE_BULK_ARRIVAL,
    ADVERSARIAL_DUPLICATE_STORM,
    ADVERSARIAL_SCHEMA_MISMATCH,
    ADVERSARIAL_LATE_ARRIVAL_REPROCESS,
)


# ---------------------------------------------------------------------------
# Template definition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AdversarialTemplate:
    scenario_id: str
    description: str
    required_params: Tuple[str, ...]
    default_params: Dict[str, object] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

ADVERSARIAL_REGISTRY: Dict[str, AdversarialTemplate] = {
    ADVERSARIAL_LATE_ARRIVAL_REPROCESS: AdversarialTemplate(
        scenario_id=ADVERSARIAL_LATE_ARRIVAL_REPROCESS,
        description=(
            "Two-phase batch late-arrival scenario. Phase A emits a normal "
            "event stream (D-1 started_at) representing the pre-batch window. "
            "Phase B emits a smaller burst with the same started_at, simulating "
            "mobile clients flushing buffered events after the batch job has "
            "already published D-1 metrics. Phase separation and orchestration "
            "live in the acceptance script — each phase is an independent "
            "generator run."
        ),
        required_params=("phase",),
        default_params={
            "phase": "A",
            "paired_run_id": "",
        },
    ),
    ADVERSARIAL_LATE_BULK_ARRIVAL: AdversarialTemplate(
        scenario_id=ADVERSARIAL_LATE_BULK_ARRIVAL,
        description=(
            "Two-phase emission: phase A advances the Spark watermark with "
            "on-time events; after phase_gap_seconds, phase B bulk-emits "
            "events backdated past the watermark to force late-drop evaluation."
        ),
        required_params=("phase_gap_seconds",),
        default_params={
            "phase_gap_seconds": 75,
            "phase_b_ratio": 0.3,
            "late_offset_seconds": 150,
            "expected_drop_floor": 0.90,
        },
    ),
    ADVERSARIAL_DUPLICATE_STORM: AdversarialTemplate(
        scenario_id=ADVERSARIAL_DUPLICATE_STORM,
        description=(
            "Phase A emits a baseline event stream; phase B replays a "
            "duplicate_ratio fraction of the emitted event_ids to test "
            "pipeline deduplication at the Spark layer."
        ),
        required_params=("duplicate_ratio",),
        default_params={
            "duplicate_ratio": 0.20,
            "replay_delay_seconds": 30,
            "expected_dedup_floor": 0.99,
        },
    ),
    ADVERSARIAL_SCHEMA_MISMATCH: AdversarialTemplate(
        scenario_id=ADVERSARIAL_SCHEMA_MISMATCH,
        description=(
            "Injects structured schema violations into the event stream to "
            "test DLQ routing and schema enforcement. Each violation class "
            "is injected at a controlled ratio, leaving the remainder as "
            "valid baseline events."
        ),
        required_params=("mismatch_ratio",),
        default_params={
            "mismatch_ratio": 0.10,
            "mismatch_types": [
                "wrong_schema_version",
                "missing_required_field",
                "null_event_type",
                "malformed_payload_json",
            ],
            "expected_dlq_floor": 0.99,
        },
    ),
}


def validate_registry() -> None:
    for key in ADVERSARIAL_SCENARIO_KEYS:
        if key not in ADVERSARIAL_REGISTRY:
            raise ValueError(f"Missing registry entry for adversarial scenario: {key}")


validate_registry()
