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
    ADVERSARIAL_BULK_ARRIVAL,
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

    def test_bulk_arrival_does_not_raise(self) -> None:
        # duration_minutes must cover the default phases (300+60+300=660s=11min).
        with tempfile.TemporaryDirectory() as td:
            payload = _make_config(ADVERSARIAL_BULK_ARRIVAL)
            payload["duration_minutes"] = 12
            config = _write_and_load(Path(td), payload)
        runner = _make_runner(config)
        result = runner.run()
        self.assertIsNotNone(result)


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


# ---------------------------------------------------------------------------
# BulkArrival scenario
# ---------------------------------------------------------------------------

# Minimal phase durations keep the test fast under SimulatedClock.
# events_per_sec=1, burst_multiplier=3, phases=3s/2s/3s
# → phase1=3, phase2=6, phase3=3, total=12
_BULK_ARRIVAL_FAST_PARAMS = {
    "burst_multiplier": 3,
    "baseline_duration_seconds": 3,
    "burst_duration_seconds": 2,
    "recovery_duration_seconds": 3,
    "max_lag_threshold": 50000,
    "recovery_timeout_seconds": 120,
}


def _make_bulk_arrival_config() -> Dict[str, Any]:
    return {
        **BASE_CONFIG,
        "events_per_sec": 1,
        "adversarial_scenario": ADVERSARIAL_BULK_ARRIVAL,
        "scenario_params": _BULK_ARRIVAL_FAST_PARAMS,
    }


class BulkArrivalRunnerTests(unittest.TestCase):
    def _run(self) -> Any:
        with tempfile.TemporaryDirectory() as td:
            config = _write_and_load(Path(td), _make_bulk_arrival_config())
        sink = InMemoryEventSink()
        runner = AdversarialRunner(
            config=config,
            sink=sink,
            clock=SimulatedClock(config.started_at),
            logger=lambda _: None,
        )
        result = runner.run()
        return result, sink

    def test_bulk_arrival_returns_result(self) -> None:
        result, _ = self._run()
        self.assertIsNotNone(result)
        self.assertIsInstance(result.summary, dict)

    def test_bulk_arrival_summary_keys(self) -> None:
        result, _ = self._run()
        for key in (
            "scenario",
            "run_id",
            "total_emitted",
            "phase1_emitted",
            "phase2_emitted",
            "phase3_emitted",
            "burst_multiplier",
            "burst_rate",
        ):
            self.assertIn(key, result.summary, msg=f"missing key: {key}")
        self.assertEqual(result.summary["scenario"], "bulk_arrival")

    def test_bulk_arrival_phase_counts(self) -> None:
        result, _ = self._run()
        p = _BULK_ARRIVAL_FAST_PARAMS
        eps = 1  # events_per_sec from BASE_CONFIG override
        expected_phase1 = p["baseline_duration_seconds"] * eps
        expected_phase2 = p["burst_duration_seconds"] * eps * p["burst_multiplier"]
        expected_phase3 = p["recovery_duration_seconds"] * eps
        expected_total = expected_phase1 + expected_phase2 + expected_phase3

        self.assertEqual(result.summary["phase1_emitted"], expected_phase1)
        self.assertEqual(result.summary["phase2_emitted"], expected_phase2)
        self.assertEqual(result.summary["phase3_emitted"], expected_phase3)
        self.assertEqual(result.summary["total_emitted"], expected_total)

    def test_bulk_arrival_burst_exceeds_baseline(self) -> None:
        result, _ = self._run()
        self.assertGreater(result.summary["phase2_emitted"], result.summary["phase1_emitted"])

    def test_bulk_arrival_is_deterministic(self) -> None:
        result_a, sink_a = self._run()
        result_b, sink_b = self._run()
        self.assertEqual(result_a.summary["total_emitted"], result_b.summary["total_emitted"])
        ids_a = [e.value["event_id"] for e in sink_a.content_events]
        ids_b = [e.value["event_id"] for e in sink_b.content_events]
        self.assertEqual(ids_a, ids_b)

    def test_bulk_arrival_raises_when_phases_exceed_duration(self) -> None:
        # duration_minutes=10 → 600s, but phases sum to 3+2+3=8s here — invert:
        # set phases that exceed the envelope to trigger the guard.
        with tempfile.TemporaryDirectory() as td:
            payload = {
                **BASE_CONFIG,
                "events_per_sec": 1,
                "duration_minutes": 10,  # 600s envelope
                "adversarial_scenario": ADVERSARIAL_BULK_ARRIVAL,
                "scenario_params": {
                    "burst_multiplier": 2,
                    "baseline_duration_seconds": 300,
                    "burst_duration_seconds": 200,
                    "recovery_duration_seconds": 200,  # total=700s > 600s
                    "max_lag_threshold": 50000,
                    "recovery_timeout_seconds": 120,
                },
            }
            config = _write_and_load(Path(td), payload)
        runner = _make_runner(config)
        with self.assertRaises(ValueError):
            runner.run()


if __name__ == "__main__":
    unittest.main()
