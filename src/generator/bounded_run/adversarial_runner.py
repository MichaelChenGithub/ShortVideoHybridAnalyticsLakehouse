"""Adversarial run orchestrator.

Each adversarial scenario is implemented as a private method on AdversarialRunner.
The public entry point is run(), which dispatches to the right method based on
config.adversarial_scenario.

Shared infrastructure (CDC bootstrap, baseline event emission, user/video
registries) lives here so every scenario starts from the same clean foundation.

To implement a new scenario:
  1. Register it in adversarial.py.
  2. Add a _run_<scenario_id> method here.
  3. Wire it into _DISPATCH at the bottom of __init__.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from .adversarial import (
    ADVERSARIAL_DUPLICATE_STORM,
    ADVERSARIAL_LATE_BULK_ARRIVAL,
    ADVERSARIAL_SCHEMA_MISMATCH,
)
from .adversarial_config import AdversarialRunConfig
from .clock import Clock, SimulatedClock
from .constants import DEFAULT_CDC_GATE_SECONDS, DEFAULT_SCHEMA_VERSION
from .deterministic import DeterministicIdFactory, make_rng
from .sink import EventSink


def _to_utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


@dataclass
class AdversarialRunResult:
    summary: Dict[str, Any]


class AdversarialRunner:
    """Runs a single adversarial scenario against the configured sink."""

    USER_REGIONS = ("NA", "LATAM", "EMEA", "APAC")
    USER_STATES = ("new", "returning", "unknown")

    def __init__(
        self,
        config: AdversarialRunConfig,
        sink: EventSink,
        artifacts_root: str | Path = "artifacts/adversarial_runs",
        cdc_gate_seconds: int = DEFAULT_CDC_GATE_SECONDS,
        clock: Optional[Clock] = None,
        logger: Callable[[str], None] = print,
    ) -> None:
        self.config = config
        self.sink = sink
        self.artifacts_root = Path(artifacts_root)
        self.cdc_gate_seconds = cdc_gate_seconds
        self.clock = clock or SimulatedClock(config.started_at)
        self.logger = logger
        self.id_factory = DeterministicIdFactory(config.run_id)

        self._dispatch: Dict[str, Callable[[], AdversarialRunResult]] = {
            ADVERSARIAL_LATE_BULK_ARRIVAL: self._run_late_bulk_arrival,
            ADVERSARIAL_DUPLICATE_STORM: self._run_duplicate_event_storm,
            ADVERSARIAL_SCHEMA_MISMATCH: self._run_schema_mismatch,
        }

    def _log(self, message: str) -> None:
        self.logger(message)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> AdversarialRunResult:
        scenario = self.config.adversarial_scenario
        handler = self._dispatch[scenario]
        self._log(f"[adversarial] starting scenario={scenario}")
        result = handler()
        self._log(f"[adversarial] complete scenario={scenario}")
        return result

    # ------------------------------------------------------------------
    # Shared infrastructure — used by every scenario
    # ------------------------------------------------------------------

    def _build_baseline_video_registry(self) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Build a flat normal_baseline video registry."""
        video_count = max(1, min(200, int(math.ceil(self.config.total_events / 1000.0))))
        rng = make_rng(self.config.seed, "adv-video-registry")
        rows: List[Dict[str, Any]] = []
        video_ids: List[str] = []
        for _ in range(video_count):
            video_id = self.id_factory.next_video_id()
            upload_age_minutes = rng.randint(61, 24 * 60)
            upload_time = self.config.started_at - timedelta(minutes=upload_age_minutes)
            rows.append(
                {
                    "video_id": video_id,
                    "category": "comedy",
                    "region": "US",
                    "upload_time": _to_utc_iso(upload_time),
                    "status": "active",
                }
            )
            video_ids.append(video_id)
        return rows, video_ids

    def _build_user_registry(self) -> List[Dict[str, Any]]:
        pool_size = max(200, min(5000, max(1, self.config.total_events // 60)))
        region_rng = make_rng(self.config.seed, "adv-user-regions")
        state_rng = make_rng(self.config.seed, "adv-user-states")
        rows: List[Dict[str, Any]] = []
        for idx in range(pool_size):
            rows.append(
                {
                    "user_id": self.id_factory.next_user_id(),
                    "new_vs_returning_user": state_rng.choice(self.USER_STATES),
                    "region": region_rng.choice(self.USER_REGIONS),
                    "registry_seq": idx,
                }
            )
        return rows

    def _emit_video_cdc_bootstrap(self, registry_rows: List[Dict[str, Any]]) -> int:
        for idx, row in enumerate(registry_rows):
            event = {
                "op": "c",
                "ts_ms": int(
                    (self.config.started_at + timedelta(milliseconds=idx)).timestamp() * 1000
                ),
                "schema_version": self.config.schema_version,
                "after": {
                    "video_id": row["video_id"],
                    "category": row["category"],
                    "region": row["region"],
                    "upload_time": row["upload_time"],
                    "status": row["status"],
                },
            }
            self.sink.emit_video_cdc_event(row["video_id"], event, self.clock.now())
        return len(registry_rows)

    def _emit_user_cdc_bootstrap(self, user_rows: List[Dict[str, Any]]) -> int:
        for idx, row in enumerate(user_rows):
            event = {
                "op": "c",
                "ts_ms": int(
                    (
                        self.config.started_at + timedelta(seconds=120, milliseconds=idx)
                    ).timestamp()
                    * 1000
                ),
                "schema_version": self.config.schema_version,
                "after": {
                    "user_id": row["user_id"],
                    "new_vs_returning_user": row["new_vs_returning_user"],
                    "region": row["region"],
                },
            }
            self.sink.emit_user_cdc_event(row["user_id"], event, self.clock.now())
        return len(user_rows)

    def _emit_baseline_event_stream(
        self,
        video_ids: List[str],
        user_rows: List[Dict[str, Any]],
        event_timestamp_fn: Callable[[int, datetime], datetime],
    ) -> List[Dict[str, Any]]:
        """Emit a full baseline event stream and return all emitted events.

        event_timestamp_fn(event_index, second_start) -> timestamp to stamp
        on the event. Each scenario can override this to backdate events.
        """
        user_rng = make_rng(self.config.seed, "adv-user-selection")
        payload_rng_seed = self.config.seed

        emitted: List[Dict[str, Any]] = []
        event_index = 0

        for _ in range(self.config.duration_seconds):
            second_start = self.clock.now()
            for slot in range(self.config.events_per_sec):
                event_timestamp = event_timestamp_fn(event_index, second_start)
                event_id = self.id_factory.next_event_id()
                user_id = user_rows[user_rng.randint(0, len(user_rows) - 1)]["user_id"]
                video_id = video_ids[event_index % len(video_ids)]

                p_rng = make_rng(payload_rng_seed + event_index, "adv-payload")
                event: Dict[str, Any] = {
                    "event_id": event_id,
                    "event_timestamp": _to_utc_iso(event_timestamp),
                    "video_id": video_id,
                    "user_id": user_id,
                    "event_type": p_rng.choice(["impression", "play_start", "play_finish"]),
                    "schema_version": self.config.schema_version,
                    "payload_json": json.dumps(
                        {
                            "watch_time_ms": p_rng.randint(0, 90000),
                            "device_os": p_rng.choice(["iOS", "Android"]),
                            "network_type": p_rng.choice(["5G", "WiFi", "4G"]),
                        },
                        separators=(",", ":"),
                    ),
                }
                key = video_id
                self.sink.emit_content_event(key, event, self.clock.now())
                emitted.append(event)
                event_index += 1

            self.clock.sleep(1.0)

        return emitted

    # ------------------------------------------------------------------
    # Scenario implementations (stubs — filled in per-scenario iteration)
    # ------------------------------------------------------------------

    def _run_late_bulk_arrival(self) -> AdversarialRunResult:
        """Two-phase late arrival storm.

        Phase A: emit on-time events to advance the Spark watermark.
        Gap:     wait phase_gap_seconds so the watermark advances.
        Phase B: bulk-emit events backdated past the watermark to force
                 late-drop evaluation.

        Acceptance: watermark_drop_ratio >= expected_drop_floor.
        """
        raise NotImplementedError(
            "late_bulk_arrival scenario is not yet implemented. "
            "Implement _run_late_bulk_arrival() in adversarial_runner.py."
        )

    def _run_duplicate_event_storm(self) -> AdversarialRunResult:
        """Duplicate event storm.

        Phase A: emit a baseline event stream.
        Phase B: replay duplicate_ratio fraction of phase A event_ids.

        Acceptance: dedup_rate (duplicates filtered) >= expected_dedup_floor.
        """
        raise NotImplementedError(
            "duplicate_event_storm scenario is not yet implemented. "
            "Implement _run_duplicate_event_storm() in adversarial_runner.py."
        )

    def _run_schema_mismatch(self) -> AdversarialRunResult:
        """Schema mismatch injection.

        Injects mismatch_ratio fraction of events with structured violations
        across mismatch_types classes. Remaining events are valid baseline.

        Acceptance: dlq_rate per violation class >= expected_dlq_floor.
        """
        raise NotImplementedError(
            "schema_mismatch scenario is not yet implemented. "
            "Implement _run_schema_mismatch() in adversarial_runner.py."
        )
