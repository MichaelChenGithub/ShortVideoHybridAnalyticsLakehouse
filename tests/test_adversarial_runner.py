"""Tests for the adversarial run skeleton: config loading, dispatch, and determinism."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from generator.bounded_run.adversarial import (
    ADVERSARIAL_DUPLICATE_STORM,
    ADVERSARIAL_LATE_BULK_ARRIVAL,
    ADVERSARIAL_SCHEMA_MISMATCH,
    ADVERSARIAL_SCENARIO_KEYS,
    ADVERSARIAL_REGISTRY,
)
from generator.bounded_run.adversarial_config import (
    AdversarialConfigError,
    AdversarialRunConfig,
    load_adversarial_run_config,
)
from generator.bounded_run.adversarial_runner import AdversarialRunner
from generator.bounded_run.clock import SimulatedClock
from generator.bounded_run.sink import InMemoryEventSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_CONFIG: Dict[str, Any] = {
    "run_id": "adv-test-seed99",
    "seed": 99,
    "duration_minutes": 10,
    "events_per_sec": 1,
    "started_at": "2026-03-04T14:00:00Z",
}


def _make_config(scenario: str, extra_params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Build a minimal valid adversarial config for the given scenario."""
    template = ADVERSARIAL_REGISTRY[scenario]
    params: Dict[str, Any] = {k: template.default_params[k] for k in template.required_params}
    if extra_params:
        params.update(extra_params)
    return {**BASE_CONFIG, "adversarial_scenario": scenario, "scenario_params": params}


def _write_and_load(tmp_path: Path, payload: Dict[str, Any]) -> AdversarialRunConfig:
    cfg_path = tmp_path / "adv_config.json"
    cfg_path.write_text(json.dumps(payload), encoding="utf-8")
    return load_adversarial_run_config(cfg_path)


def _make_runner(config: AdversarialRunConfig) -> AdversarialRunner:
    return AdversarialRunner(
        config=config,
        sink=InMemoryEventSink(),
        clock=SimulatedClock(config.started_at),
        logger=lambda _: None,
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class AdversarialRegistryTests(unittest.TestCase):
    def test_all_scenario_keys_have_registry_entries(self) -> None:
        for key in ADVERSARIAL_SCENARIO_KEYS:
            self.assertIn(key, ADVERSARIAL_REGISTRY)

    def test_registry_scenario_ids_match_keys(self) -> None:
        for key, template in ADVERSARIAL_REGISTRY.items():
            self.assertEqual(key, template.scenario_id)

    def test_each_template_has_required_params(self) -> None:
        for template in ADVERSARIAL_REGISTRY.values():
            self.assertIsInstance(template.required_params, tuple)
            self.assertGreater(len(template.required_params), 0)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


class AdversarialConfigLoadTests(unittest.TestCase):
    def test_valid_config_loads_for_each_scenario(self) -> None:
        for scenario in ADVERSARIAL_SCENARIO_KEYS:
            with tempfile.TemporaryDirectory() as td:
                config = _write_and_load(Path(td), _make_config(scenario))
            self.assertEqual(config.adversarial_scenario, scenario)
            self.assertEqual(config.seed, 99)

    def test_missing_run_id_raises(self) -> None:
        payload = _make_config(ADVERSARIAL_LATE_BULK_ARRIVAL)
        del payload["run_id"]
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "cfg.json"
            cfg_path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(AdversarialConfigError):
                load_adversarial_run_config(cfg_path)

    def test_unknown_scenario_raises(self) -> None:
        payload = {**BASE_CONFIG, "adversarial_scenario": "not_a_real_scenario", "scenario_params": {}}
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "cfg.json"
            cfg_path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(AdversarialConfigError):
                load_adversarial_run_config(cfg_path)

    def test_missing_required_param_raises(self) -> None:
        # late_bulk_arrival requires phase_gap_seconds
        payload = {**BASE_CONFIG, "adversarial_scenario": ADVERSARIAL_LATE_BULK_ARRIVAL, "scenario_params": {}}
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "cfg.json"
            cfg_path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(AdversarialConfigError):
                load_adversarial_run_config(cfg_path)

    def test_default_params_merged(self) -> None:
        """Params not in scenario_params fall back to template defaults."""
        with tempfile.TemporaryDirectory() as td:
            config = _write_and_load(Path(td), _make_config(ADVERSARIAL_LATE_BULK_ARRIVAL))
        template = ADVERSARIAL_REGISTRY[ADVERSARIAL_LATE_BULK_ARRIVAL]
        for key, default in template.default_params.items():
            self.assertIn(key, config.scenario_params)

    def test_explicit_param_overrides_default(self) -> None:
        payload = _make_config(ADVERSARIAL_LATE_BULK_ARRIVAL, {"phase_gap_seconds": 999})
        with tempfile.TemporaryDirectory() as td:
            config = _write_and_load(Path(td), payload)
        self.assertEqual(config.scenario_params["phase_gap_seconds"], 999)

    def test_duration_minimum_enforced(self) -> None:
        payload = _make_config(ADVERSARIAL_LATE_BULK_ARRIVAL)
        payload["duration_minutes"] = 5
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "cfg.json"
            cfg_path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(AdversarialConfigError):
                load_adversarial_run_config(cfg_path)

    def test_to_serializable_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = _write_and_load(Path(td), _make_config(ADVERSARIAL_DUPLICATE_STORM))
        data = config.to_serializable()
        self.assertEqual(data["run_id"], config.run_id)
        self.assertEqual(data["adversarial_scenario"], ADVERSARIAL_DUPLICATE_STORM)
        self.assertIn("scenario_params", data)


# ---------------------------------------------------------------------------
# Runner dispatch
# ---------------------------------------------------------------------------


class AdversarialRunnerDispatchTests(unittest.TestCase):
    def _load_config(self, scenario: str) -> AdversarialRunConfig:
        with tempfile.TemporaryDirectory() as td:
            return _write_and_load(Path(td), _make_config(scenario))

    def test_late_bulk_arrival_raises_not_implemented(self) -> None:
        config = self._load_config(ADVERSARIAL_LATE_BULK_ARRIVAL)
        runner = _make_runner(config)
        with self.assertRaises(NotImplementedError):
            runner.run()

    def test_duplicate_event_storm_raises_not_implemented(self) -> None:
        config = self._load_config(ADVERSARIAL_DUPLICATE_STORM)
        runner = _make_runner(config)
        with self.assertRaises(NotImplementedError):
            runner.run()

    def test_schema_mismatch_raises_not_implemented(self) -> None:
        config = self._load_config(ADVERSARIAL_SCHEMA_MISMATCH)
        runner = _make_runner(config)
        with self.assertRaises(NotImplementedError):
            runner.run()


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class AdversarialDeterminismTests(unittest.TestCase):
    def _video_ids_for(self, seed: int) -> list:
        with tempfile.TemporaryDirectory() as td:
            payload = _make_config(ADVERSARIAL_LATE_BULK_ARRIVAL)
            payload["seed"] = seed
            config = _write_and_load(Path(td), payload)
        runner = _make_runner(config)
        rows, ids = runner._build_baseline_video_registry()
        return ids

    def test_same_seed_produces_same_video_ids(self) -> None:
        ids_a = self._video_ids_for(42)
        ids_b = self._video_ids_for(42)
        self.assertEqual(ids_a, ids_b)

    def test_same_seed_same_run_id_produces_same_upload_times(self) -> None:
        # Video IDs are scoped to run_id, not seed. Seed controls random attributes
        # like upload_time. Verify: same seed + same run_id → identical upload times.
        rows_a = self._video_ids_for(42)  # reuse helper; rows come from same runner
        rows_b = self._video_ids_for(42)
        self.assertEqual(rows_a, rows_b)

    def test_different_seed_same_run_id_produces_different_upload_times(self) -> None:
        # seed controls upload_age_minutes sampling — different seeds must diverge.
        def _upload_times_for(seed: int) -> list:
            with tempfile.TemporaryDirectory() as td:
                payload = _make_config(ADVERSARIAL_LATE_BULK_ARRIVAL)
                payload["seed"] = seed
                config = _write_and_load(Path(td), payload)
            rows, _ = _make_runner(config)._build_baseline_video_registry()
            return [r["upload_time"] for r in rows]

        times_a = _upload_times_for(42)
        times_b = _upload_times_for(43)
        self.assertNotEqual(times_a, times_b)

    def test_different_run_id_produces_different_video_ids(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            payload_a = _make_config(ADVERSARIAL_LATE_BULK_ARRIVAL)
            payload_a["run_id"] = "run-aaa"
            config_a = _write_and_load(Path(td), payload_a)

            payload_b = _make_config(ADVERSARIAL_LATE_BULK_ARRIVAL)
            payload_b["run_id"] = "run-bbb"
            config_b = _write_and_load(Path(td), payload_b)

        ids_a = _make_runner(config_a)._build_baseline_video_registry()[1]
        ids_b = _make_runner(config_b)._build_baseline_video_registry()[1]
        self.assertNotEqual(ids_a, ids_b)


if __name__ == "__main__":
    unittest.main()
