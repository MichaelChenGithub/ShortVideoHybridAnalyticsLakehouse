"""Config model and loader for adversarial runs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from .adversarial import ADVERSARIAL_REGISTRY, ADVERSARIAL_SCENARIO_KEYS
from .constants import DEFAULT_SCHEMA_VERSION


class AdversarialConfigError(ValueError):
    """Raised when an adversarial run config is invalid."""


@dataclass(frozen=True)
class AdversarialRunConfig:
    run_id: str
    seed: int
    started_at: datetime
    duration_minutes: int
    events_per_sec: int
    schema_version: str
    adversarial_scenario: str
    scenario_params: Dict[str, Any]

    @property
    def duration_seconds(self) -> int:
        return self.duration_minutes * 60

    @property
    def total_events(self) -> int:
        return self.events_per_sec * self.duration_seconds

    def to_serializable(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "seed": self.seed,
            "started_at": self.started_at.isoformat().replace("+00:00", "Z"),
            "duration_minutes": self.duration_minutes,
            "events_per_sec": self.events_per_sec,
            "schema_version": self.schema_version,
            "adversarial_scenario": self.adversarial_scenario,
            "scenario_params": self.scenario_params,
        }


def _default_started_at() -> datetime:
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    return datetime(yesterday.year, yesterday.month, yesterday.day, 12, 0, 0, tzinfo=timezone.utc)


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise AdversarialConfigError(f"Invalid started_at timestamp: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _validate_config(config: Dict[str, Any]) -> None:
    required = {"run_id", "seed", "duration_minutes", "events_per_sec", "adversarial_scenario"}
    missing = sorted(required - set(config.keys()))
    if missing:
        raise AdversarialConfigError(f"Missing required config fields: {', '.join(missing)}")

    if not str(config["run_id"]).strip():
        raise AdversarialConfigError("run_id must be non-empty")

    try:
        int(config["seed"])
    except (TypeError, ValueError) as exc:
        raise AdversarialConfigError("seed must be an integer") from exc

    try:
        duration = int(config["duration_minutes"])
    except (TypeError, ValueError) as exc:
        raise AdversarialConfigError("duration_minutes must be an integer") from exc
    if duration < 10:
        raise AdversarialConfigError("duration_minutes must be >= 10")

    try:
        eps = int(config["events_per_sec"])
    except (TypeError, ValueError) as exc:
        raise AdversarialConfigError("events_per_sec must be an integer") from exc
    if eps <= 0:
        raise AdversarialConfigError("events_per_sec must be > 0")

    scenario = str(config["adversarial_scenario"]).strip()
    if scenario not in ADVERSARIAL_SCENARIO_KEYS:
        valid = ", ".join(ADVERSARIAL_SCENARIO_KEYS)
        raise AdversarialConfigError(
            f"Unknown adversarial_scenario '{scenario}'. Valid: {valid}"
        )

    template = ADVERSARIAL_REGISTRY[scenario]
    params = config.get("scenario_params", {})
    if not isinstance(params, Mapping):
        raise AdversarialConfigError("scenario_params must be a map/object")

    missing_params = sorted(p for p in template.required_params if p not in params)
    if missing_params:
        raise AdversarialConfigError(
            f"scenario_params missing required keys for '{scenario}': "
            + ", ".join(missing_params)
        )


def load_adversarial_run_config(
    config_path: str | Path,
    overrides: Optional[Mapping[str, Any]] = None,
) -> AdversarialRunConfig:
    path = Path(config_path)
    if not path.exists():
        raise AdversarialConfigError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        base = json.load(handle)

    if not isinstance(base, dict):
        raise AdversarialConfigError("Config root must be a JSON object")

    merged: Dict[str, Any] = dict(base)
    for key, value in (overrides or {}).items():
        if value is None:
            continue
        merged[key] = value

    if "started_at" not in merged:
        merged["started_at"] = _default_started_at()

    _validate_config(merged)

    scenario = str(merged["adversarial_scenario"]).strip()
    template = ADVERSARIAL_REGISTRY[scenario]

    # Merge template defaults under provided params so explicit values win.
    params: Dict[str, Any] = {**template.default_params, **merged.get("scenario_params", {})}

    return AdversarialRunConfig(
        run_id=str(merged["run_id"]).strip(),
        seed=int(merged["seed"]),
        started_at=_parse_timestamp(merged["started_at"]),
        duration_minutes=int(merged["duration_minutes"]),
        events_per_sec=int(merged["events_per_sec"]),
        schema_version=str(merged.get("schema_version", DEFAULT_SCHEMA_VERSION)).strip(),
        adversarial_scenario=scenario,
        scenario_params=params,
    )
