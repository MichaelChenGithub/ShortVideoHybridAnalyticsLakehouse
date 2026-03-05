"""Bounded-run orchestrator for MIC-34 generator."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Tuple

from .artifacts import write_run_artifacts
from .clock import Clock, RealClock, SimulatedClock
from .config import RunConfig
from .constants import (
    ACTION_REASON_CODES,
    DEFAULT_CDC_GATE_SECONDS,
    DEFAULT_SCHEMA_VERSION,
    SCENARIO_INVALID_PAYLOAD_BURST,
    SCENARIO_KEYS,
    SCENARIO_TO_ACTION,
)
from .deterministic import DeterministicIdFactory, make_rng
from .scenario import (
    SCENARIO_TEMPLATES,
    build_scenario_sequence,
    plan_event_counts,
    realized_mix,
    weighted_choice,
)
from .sink import EventSink


def _to_utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


@dataclass
class RunResult:
    summary: Dict[str, Any]


class BoundedRunGenerator:
    def __init__(
        self,
        config: RunConfig,
        sink: EventSink,
        artifacts_root: str | Path = "artifacts/generator_runs",
        schema_version: str = DEFAULT_SCHEMA_VERSION,
        cdc_gate_seconds: int = DEFAULT_CDC_GATE_SECONDS,
        clock: Clock | None = None,
        logger: Callable[[str], None] = print,
    ) -> None:
        self.config = config
        self.sink = sink
        self.schema_version = schema_version
        self.cdc_gate_seconds = cdc_gate_seconds
        self.artifacts_root = Path(artifacts_root)
        self.logger = logger

        self.clock = clock or SimulatedClock(config.started_at)
        self.id_factory = DeterministicIdFactory(config.run_id)

    def _log(self, message: str) -> None:
        self.logger(message)

    def _build_user_pool(self, total_events: int) -> List[str]:
        pool_size = max(200, min(5000, max(1, total_events // 60)))
        return [self.id_factory.next_user_id() for _ in range(pool_size)]

    def _video_count_for_scenario(self, scenario: str, event_count: int) -> int:
        if scenario == SCENARIO_INVALID_PAYLOAD_BURST:
            return 0
        return max(1, min(200, int(math.ceil(event_count / 1000.0))))

    def _build_registry(self, planned_counts: Mapping[str, int]) -> Tuple[List[Dict[str, Any]], Dict[str, List[str]]]:
        rows: List[Dict[str, Any]] = []
        by_scenario: Dict[str, List[str]] = {scenario: [] for scenario in SCENARIO_KEYS}

        rng = make_rng(self.config.seed, "video-registry")

        for scenario in SCENARIO_KEYS:
            template = SCENARIO_TEMPLATES[scenario]
            for _ in range(self._video_count_for_scenario(scenario, planned_counts[scenario])):
                video_id = self.id_factory.next_video_id()
                if scenario == "cold_start_under_exposed":
                    upload_age_minutes = rng.randint(1, 60)
                else:
                    upload_age_minutes = rng.randint(61, 24 * 60)
                upload_time = self.config.started_at - timedelta(minutes=upload_age_minutes)

                row = {
                    "video_id": video_id,
                    "scenario_id": scenario,
                    "category": template.category,
                    "region": template.region,
                    "upload_time": _to_utc_iso(upload_time),
                    "status": "active",
                }
                rows.append(row)
                by_scenario[scenario].append(video_id)

        return rows, by_scenario

    def _emit_cdc_bootstrap(self, registry_rows: List[Dict[str, Any]]) -> int:
        cdc_count = 0
        for idx, row in enumerate(registry_rows):
            event = {
                "op": "c",
                "ts_ms": int((self.config.started_at + timedelta(milliseconds=idx)).timestamp() * 1000),
                "schema_version": self.schema_version,
                "after": {
                    "video_id": row["video_id"],
                    "category": row["category"],
                    "region": row["region"],
                    "upload_time": row["upload_time"],
                    "status": row["status"],
                },
            }
            emitted_at = self.clock.now()
            self.sink.emit_cdc_event(row["video_id"], event, emitted_at)
            cdc_count += 1
        return cdc_count

    def _build_late_offsets(self, total_events: int) -> Dict[int, int]:
        late_count = int(round(total_events * self.config.late_event_ratio))
        if late_count <= 0:
            return {}

        index_rng = make_rng(self.config.seed, "late-indexes")
        offset_rng = make_rng(self.config.seed, "late-offsets")

        indexes = index_rng.sample(range(total_events), late_count)
        indexes.sort()

        offsets: Dict[int, int] = {}
        for event_index in indexes:
            if offset_rng.random() < 0.8:
                offsets[event_index] = offset_rng.randint(11, 30)
            else:
                offsets[event_index] = offset_rng.randint(31, 90)
        return offsets

    def _make_invalid_event(self, user_id: str, event_id: str, event_index: int) -> Dict[str, Any]:
        return {
            "event_id": event_id,
            "event_timestamp": "bad-timestamp",
            "video_id": f"invalid_{event_index}",
            "user_id": user_id,
            "schema_version": self.schema_version,
            "payload_json": "{not-valid-json",
            # Intentionally missing event_type for invalid payload scenario.
        }

    def _make_valid_event(
        self,
        scenario: str,
        video_id: str,
        user_id: str,
        event_id: str,
        event_timestamp: datetime,
        event_index: int,
    ) -> Dict[str, Any]:
        template = SCENARIO_TEMPLATES[scenario]
        event_type = weighted_choice(template.event_type_weights, self.config.seed, event_index)

        payload_rng = make_rng(self.config.seed + event_index, "payload")
        duration_ms = payload_rng.randint(5000, 90000)
        if event_type in ("impression", "play_start"):
            watch_time_ms = 0
        elif event_type == "play_finish":
            watch_time_ms = duration_ms
        elif event_type == "skip":
            watch_time_ms = payload_rng.randint(1, max(duration_ms - 1, 1))
        else:
            watch_time_ms = payload_rng.randint(0, duration_ms)

        payload = {
            "watch_time_ms": watch_time_ms,
            "device_os": payload_rng.choice(["iOS", "Android"]),
            "app_version": "14.2.0",
            "network_type": payload_rng.choice(["5G", "WiFi", "4G"]),
            "scenario_id": scenario,
        }

        return {
            "event_id": event_id,
            "event_timestamp": _to_utc_iso(event_timestamp),
            "video_id": video_id,
            "user_id": user_id,
            "event_type": event_type,
            "schema_version": self.schema_version,
            "payload_json": json.dumps(payload, separators=(",", ":")),
        }

    def _build_expected_actions(self, registry_rows: List[Dict[str, Any]], generated_at: datetime) -> List[Dict[str, Any]]:
        window_start = self.config.started_at.replace(second=0, microsecond=0)
        window_end = window_start + timedelta(minutes=30)

        rows: List[Dict[str, Any]] = []
        for row in registry_rows:
            scenario = row["scenario_id"]
            action = SCENARIO_TO_ACTION[scenario]
            rows.append(
                {
                    "run_id": self.config.run_id,
                    "video_id": row["video_id"],
                    "window_start": _to_utc_iso(window_start),
                    "window_end": _to_utc_iso(window_end),
                    "scenario_id": scenario,
                    "expected_action": action,
                    "expected_reason_codes": ACTION_REASON_CODES[action],
                    "generated_at": _to_utc_iso(generated_at),
                }
            )
        return rows

    def run(self) -> RunResult:
        total_events = self.config.total_content_events
        planned_counts = plan_event_counts(total_events, self.config.scenario_mix)
        scenario_sequence = build_scenario_sequence(planned_counts, self.config.seed)

        registry_rows, videos_by_scenario = self._build_registry(planned_counts)
        user_pool = self._build_user_pool(total_events)
        late_offsets = self._build_late_offsets(total_events)

        self._log("[m1] run init complete")

        lifecycle: Dict[str, str] = {
            "initialized_at": _to_utc_iso(self.clock.now()),
        }

        cdc_count = self._emit_cdc_bootstrap(registry_rows)
        lifecycle["cdc_bootstrap_emitted_at"] = _to_utc_iso(self.clock.now())

        self.clock.sleep(self.cdc_gate_seconds)
        lifecycle["content_started_at"] = _to_utc_iso(self.clock.now())

        scenario_emitted_counts = {scenario: 0 for scenario in SCENARIO_KEYS}
        invalid_payload_events = 0
        late_histogram = {"11_30": 0, "31_90": 0}

        user_rng = make_rng(self.config.seed, "user-selection")
        total_seconds = self.config.duration_seconds

        event_index = 0
        for _ in range(total_seconds):
            second_start = self.clock.now()
            for slot in range(self.config.events_per_sec):
                scenario = scenario_sequence[event_index]
                scenario_emitted_counts[scenario] += 1

                base_timestamp = second_start + timedelta(
                    microseconds=int((slot / self.config.events_per_sec) * 1_000_000)
                )
                late_offset = late_offsets.get(event_index, 0)
                if late_offset:
                    if late_offset <= 30:
                        late_histogram["11_30"] += 1
                    else:
                        late_histogram["31_90"] += 1
                event_timestamp = base_timestamp - timedelta(seconds=late_offset)

                event_id = self.id_factory.next_event_id()
                user_id = user_pool[user_rng.randint(0, len(user_pool) - 1)]

                if scenario == SCENARIO_INVALID_PAYLOAD_BURST:
                    event = self._make_invalid_event(user_id, event_id, event_index)
                    invalid_payload_events += 1
                else:
                    scenario_videos = videos_by_scenario[scenario]
                    video_id = scenario_videos[event_index % len(scenario_videos)]
                    event = self._make_valid_event(
                        scenario=scenario,
                        video_id=video_id,
                        user_id=user_id,
                        event_id=event_id,
                        event_timestamp=event_timestamp,
                        event_index=event_index,
                    )

                key = str(event.get("video_id") or f"invalid-{event_index}")
                self.sink.emit_content_event(key, event, self.clock.now())
                event_index += 1

            self.clock.sleep(1.0)

        lifecycle["content_ended_at"] = _to_utc_iso(self.clock.now())

        self.sink.flush()

        expected_action_rows = self._build_expected_actions(registry_rows, self.clock.now())

        realized = realized_mix(scenario_emitted_counts)
        abs_errors = {
            scenario: abs(realized[scenario] - self.config.scenario_mix[scenario])
            for scenario in SCENARIO_KEYS
        }
        total_error_ratio = abs(event_index - total_events) / total_events

        summary: Dict[str, Any] = {
            "run_id": self.config.run_id,
            "seed": self.config.seed,
            "schema_version": self.schema_version,
            "sink_mode": self.sink.mode,
            "planned_total_events": total_events,
            "emitted_total_events": event_index,
            "cdc_bootstrap_events": cdc_count,
            "invalid_payload_events": invalid_payload_events,
            "planned_scenario_counts": planned_counts,
            "emitted_scenario_counts": scenario_emitted_counts,
            "scenario_mix_target": self.config.scenario_mix,
            "scenario_mix_realized": realized,
            "scenario_mix_abs_error": abs_errors,
            "late_event_count": len(late_offsets),
            "late_event_histogram": late_histogram,
            "late_offset_min_seconds": min(late_offsets.values()) if late_offsets else 0,
            "late_offset_max_seconds": max(late_offsets.values()) if late_offsets else 0,
            "lifecycle": lifecycle,
            "acceptance": {
                "scenario_abs_error_max": max(abs_errors.values()),
                "scenario_abs_error_threshold": 0.02,
                "total_volume_error_ratio": total_error_ratio,
                "total_volume_error_threshold": 0.05,
                "scenario_distribution_pass": max(abs_errors.values()) <= 0.02,
                "total_volume_pass": total_error_ratio <= 0.05,
            },
            "qa_table_writes_attempted": False,
        }

        artifact_info = write_run_artifacts(
            artifacts_root=self.artifacts_root,
            config=self.config,
            video_registry_rows=registry_rows,
            expected_action_rows=expected_action_rows,
            run_summary=summary,
        )
        summary["artifacts"] = artifact_info

        self._log(
            "[m1] run complete: "
            f"content={event_index}, cdc={cdc_count}, invalid={invalid_payload_events}"
        )

        return RunResult(summary=summary)
