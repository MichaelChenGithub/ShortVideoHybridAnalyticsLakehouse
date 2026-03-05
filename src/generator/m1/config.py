"""Run config model and validation for MIC-34 bounded runs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from .constants import SCENARIO_KEYS


class ConfigError(ValueError):
    """Raised when run config is invalid."""


@dataclass(frozen=True)
class RunConfig:
    run_id: str
    seed: int
    duration_minutes: int
    events_per_sec: int
    scenario_mix: Dict[str, float]
    late_event_ratio: float
    rule_version: str
    started_at: datetime

    @property
    def duration_seconds(self) -> int:
        return self.duration_minutes * 60

    @property
    def total_content_events(self) -> int:
        return self.events_per_sec * self.duration_seconds

    def to_serializable(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "seed": self.seed,
            "duration_minutes": self.duration_minutes,
            "events_per_sec": self.events_per_sec,
            "scenario_mix": {key: self.scenario_mix[key] for key in SCENARIO_KEYS},
            "late_event_ratio": self.late_event_ratio,
            "rule_version": self.rule_version,
            "started_at": self.started_at.isoformat().replace("+00:00", "Z"),
        }


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
            raise ConfigError(f"Invalid started_at timestamp: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_scenario_mix(raw: Mapping[str, Any]) -> Dict[str, float]:
    mix: Dict[str, float] = {}
    for key, value in raw.items():
        try:
            mix[key] = float(value)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"scenario_mix value for '{key}' must be numeric") from exc
    return mix


def _validate_config_values(config: Dict[str, Any]) -> None:
    required = {
        "run_id",
        "seed",
        "duration_minutes",
        "events_per_sec",
        "scenario_mix",
        "late_event_ratio",
        "rule_version",
        "started_at",
    }
    missing = sorted(required - set(config.keys()))
    if missing:
        raise ConfigError(f"Missing required config fields: {', '.join(missing)}")

    if not str(config["run_id"]).strip():
        raise ConfigError("run_id must be non-empty")
    if not str(config["rule_version"]).strip():
        raise ConfigError("rule_version must be non-empty")

    try:
        int(config["seed"])
    except (TypeError, ValueError) as exc:
        raise ConfigError("seed must be an integer") from exc

    try:
        duration_minutes = int(config["duration_minutes"])
    except (TypeError, ValueError) as exc:
        raise ConfigError("duration_minutes must be an integer") from exc
    if duration_minutes < 10:
        raise ConfigError("duration_minutes must be >= 10")

    try:
        events_per_sec = int(config["events_per_sec"])
    except (TypeError, ValueError) as exc:
        raise ConfigError("events_per_sec must be an integer") from exc
    if events_per_sec <= 0:
        raise ConfigError("events_per_sec must be > 0")

    try:
        late_event_ratio = float(config["late_event_ratio"])
    except (TypeError, ValueError) as exc:
        raise ConfigError("late_event_ratio must be numeric") from exc
    if not 0.0 <= late_event_ratio <= 0.2:
        raise ConfigError("late_event_ratio must be in [0, 0.2]")

    raw_mix = config["scenario_mix"]
    if not isinstance(raw_mix, Mapping):
        raise ConfigError("scenario_mix must be a map/object")

    mix = _normalize_scenario_mix(raw_mix)

    expected_keys = set(SCENARIO_KEYS)
    actual_keys = set(mix.keys())
    missing_keys = sorted(expected_keys - actual_keys)
    unknown_keys = sorted(actual_keys - expected_keys)
    if missing_keys:
        raise ConfigError(f"scenario_mix missing keys: {', '.join(missing_keys)}")
    if unknown_keys:
        raise ConfigError(f"scenario_mix has unknown keys: {', '.join(unknown_keys)}")

    negatives = sorted([key for key, value in mix.items() if value < 0])
    if negatives:
        raise ConfigError(f"scenario_mix values must be non-negative: {', '.join(negatives)}")

    total = sum(mix.values())
    if abs(total - 1.0) > 1e-6:
        raise ConfigError(f"scenario_mix values must sum to 1.0 (+/- 1e-6); got {total}")

    _parse_timestamp(config["started_at"])


def load_run_config(config_path: str | Path, overrides: Optional[Mapping[str, Any]] = None) -> RunConfig:
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        base = json.load(handle)

    if not isinstance(base, dict):
        raise ConfigError("Config root must be a JSON object")

    merged: Dict[str, Any] = dict(base)
    for key, value in (overrides or {}).items():
        if value is None:
            continue
        merged[key] = value

    if isinstance(merged.get("scenario_mix"), str):
        try:
            merged["scenario_mix"] = json.loads(merged["scenario_mix"])
        except json.JSONDecodeError as exc:
            raise ConfigError("scenario_mix override must be valid JSON") from exc

    _validate_config_values(merged)

    scenario_mix = _normalize_scenario_mix(merged["scenario_mix"])

    return RunConfig(
        run_id=str(merged["run_id"]).strip(),
        seed=int(merged["seed"]),
        duration_minutes=int(merged["duration_minutes"]),
        events_per_sec=int(merged["events_per_sec"]),
        scenario_mix={key: float(scenario_mix[key]) for key in SCENARIO_KEYS},
        late_event_ratio=float(merged["late_event_ratio"]),
        rule_version=str(merged["rule_version"]).strip(),
        started_at=_parse_timestamp(merged["started_at"]),
    )
