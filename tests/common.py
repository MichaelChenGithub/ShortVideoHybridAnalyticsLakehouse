from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def base_config() -> Dict[str, Any]:
    return {
        "run_id": "m1_test_seed42_r001",
        "seed": 42,
        "duration_minutes": 10,
        "events_per_sec": 1,
        "scenario_mix": {
            "normal_baseline": 0.55,
            "viral_high_quality": 0.20,
            "viral_low_quality": 0.10,
            "cold_start_under_exposed": 0.10,
            "invalid_payload_burst": 0.05,
        },
        "late_event_ratio": 0.02,
        "rule_version": "m1_rtv1",
        "started_at": "2026-03-04T14:00:00Z",
    }


def build_config(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = copy.deepcopy(base_config())
    for key, value in (overrides or {}).items():
        if key == "scenario_mix" and isinstance(value, dict):
            payload[key] = value
        else:
            payload[key] = value
    return payload


def write_config(tmp_path: Path, payload: Dict[str, Any]) -> Path:
    cfg_path = tmp_path / "run_config.json"
    cfg_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return cfg_path
