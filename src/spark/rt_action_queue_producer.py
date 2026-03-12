"""Producer-side action queue candidate derivation for realtime decisioning."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import Callable, Iterable, List, Mapping

from spark.rt_action_decisioning import (
    ACTION_BOOST,
    ACTION_RESCUE,
    ACTION_REVIEW,
    DecisionInputs,
    derive_decision,
)


@dataclass(frozen=True)
class DecisionContextRow:
    """Minimal producer input row from decision context metrics."""

    video_id: str
    window_start: datetime
    window_end: datetime
    rule_version: str
    velocity_30m: float
    impressions_30m: int
    completion_rate_30m: float
    skip_rate_30m: float
    play_start_30m: int
    upload_age_minutes: int
    velocity_p90_threshold: float
    global_p40_impressions_threshold: int


@dataclass(frozen=True)
class ActionQueueCandidate:
    """Write-ready queue candidate excluding dedupe/cooldown winner logic."""

    video_id: str
    decision_type: str
    window_start: datetime
    window_end: datetime
    rule_version: str
    velocity_30m: float
    impressions_30m: int
    completion_rate_30m: float
    skip_rate_30m: float
    reason_codes: tuple[str, ...]


_REASON_CODES = {
    ACTION_BOOST: ("HIGH_VELOCITY_P90", "GATE_PASS"),
    ACTION_REVIEW: ("HIGH_VELOCITY_P90", "LOW_COMPLETION", "HIGH_SKIP"),
    ACTION_RESCUE: ("NEW_UPLOAD_LT_60M", "UNDER_EXPOSED_P40", "GATE_PASS"),
}

ACTION_QUEUE_INITIAL_STATE = "PENDING"
ACTION_QUEUE_DECISION_DOMAIN = (ACTION_BOOST, ACTION_REVIEW, ACTION_RESCUE)
ACTION_QUEUE_STATE_DOMAIN = ("PENDING", "ACKED", "DONE", "EXPIRED", "HOLD")
ACTION_QUEUE_REQUIRED_FIELDS = (
    "action_id",
    "video_id",
    "decision_type",
    "priority",
    "state",
    "decided_at",
    "window_start",
    "window_end",
    "expires_at",
    "rule_version",
    "reason_codes",
    "velocity_30m",
    "completion_rate_30m",
    "skip_rate_30m",
    "impressions_30m",
    "created_at",
    "updated_at",
    "state_updated_at",
)

_PRIORITY_BY_DECISION = {
    ACTION_RESCUE: 1,
    ACTION_REVIEW: 2,
    ACTION_BOOST: 3,
}

_TTL_BY_DECISION = {
    ACTION_BOOST: timedelta(minutes=15),
    ACTION_REVIEW: timedelta(minutes=30),
    ACTION_RESCUE: timedelta(minutes=30),
}

REJECT_MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
REJECT_NULL_REQUIRED_FIELD = "NULL_REQUIRED_FIELD"
REJECT_INVALID_DECISION_TYPE = "INVALID_DECISION_TYPE"
REJECT_INVALID_INITIAL_STATE = "INVALID_INITIAL_STATE"
REJECT_INVALID_TIME_ORDER = "INVALID_TIME_ORDER"
REJECT_EMPTY_REASON_CODES = "EMPTY_REASON_CODES"


@dataclass(frozen=True)
class ActionQueueWriteRow:
    """Final producer write row aligned with `rt_action_queue` contract fields."""

    action_id: str
    video_id: str
    decision_type: str
    priority: int
    state: str
    decided_at: datetime
    window_start: datetime
    window_end: datetime
    expires_at: datetime
    rule_version: str
    velocity_30m: float
    completion_rate_30m: float
    skip_rate_30m: float
    impressions_30m: int
    reason_codes: tuple[str, ...]
    created_at: datetime
    updated_at: datetime
    state_updated_at: datetime


@dataclass(frozen=True)
class ActionQueueRowReject:
    """Structured reject record for producer-side contract violations."""

    row_index: int
    action_id: str | None
    video_id: str | None
    code: str
    field: str
    reason: str


DecisionTimeFactory = Callable[[], datetime]
ActionIdFactory = Callable[[ActionQueueCandidate, datetime], str]

_MISSING = object()


def _to_decision_inputs(row: DecisionContextRow) -> DecisionInputs:
    return DecisionInputs(
        velocity_30m=row.velocity_30m,
        impressions_30m=row.impressions_30m,
        completion_rate_30m=row.completion_rate_30m,
        skip_rate_30m=row.skip_rate_30m,
        play_start_30m=row.play_start_30m,
        upload_age_minutes=row.upload_age_minutes,
        velocity_p90_threshold=row.velocity_p90_threshold,
        global_p40_impressions_threshold=row.global_p40_impressions_threshold,
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _default_action_id(candidate: ActionQueueCandidate, decided_at: datetime) -> str:
    fingerprint = (
        f"{candidate.video_id}|{candidate.window_start.isoformat()}|"
        f"{candidate.window_end.isoformat()}|{candidate.decision_type}|"
        f"{candidate.rule_version}|{decided_at.isoformat()}"
    )
    return sha256(fingerprint.encode("utf-8")).hexdigest()


def build_action_queue_write_rows(
    candidates: Iterable[ActionQueueCandidate],
    *,
    decided_at_factory: DecisionTimeFactory = _utc_now,
    action_id_factory: ActionIdFactory = _default_action_id,
) -> List[ActionQueueWriteRow]:
    """Map candidates to contract-shaped write rows before validation/write."""

    rows: List[ActionQueueWriteRow] = []
    for candidate in candidates:
        decided_at = decided_at_factory()
        rows.append(
            ActionQueueWriteRow(
                action_id=action_id_factory(candidate, decided_at),
                video_id=candidate.video_id,
                decision_type=candidate.decision_type,
                priority=_PRIORITY_BY_DECISION[candidate.decision_type],
                state=ACTION_QUEUE_INITIAL_STATE,
                decided_at=decided_at,
                window_start=candidate.window_start,
                window_end=candidate.window_end,
                expires_at=decided_at + _TTL_BY_DECISION[candidate.decision_type],
                rule_version=candidate.rule_version,
                velocity_30m=candidate.velocity_30m,
                completion_rate_30m=candidate.completion_rate_30m,
                skip_rate_30m=candidate.skip_rate_30m,
                impressions_30m=candidate.impressions_30m,
                reason_codes=candidate.reason_codes,
                created_at=decided_at,
                updated_at=decided_at,
                state_updated_at=decided_at,
            )
        )
    return rows


def _read_field(row: object, field: str) -> object:
    if isinstance(row, Mapping):
        return row.get(field, _MISSING)
    return getattr(row, field, _MISSING)


def _read_optional_id(row: object, field: str) -> str | None:
    value = _read_field(row, field)
    if value in (_MISSING, None):
        return None
    return str(value)


def validate_action_queue_write_row(
    row: object,
    *,
    row_index: int = 0,
) -> tuple[bool, List[ActionQueueRowReject]]:
    """Validate a single queue write row against required fields/domains."""

    rejects: List[ActionQueueRowReject] = []
    action_id = _read_optional_id(row, "action_id")
    video_id = _read_optional_id(row, "video_id")

    for field in ACTION_QUEUE_REQUIRED_FIELDS:
        value = _read_field(row, field)
        if value is _MISSING:
            rejects.append(
                ActionQueueRowReject(
                    row_index=row_index,
                    action_id=action_id,
                    video_id=video_id,
                    code=REJECT_MISSING_REQUIRED_FIELD,
                    field=field,
                    reason=f"required field `{field}` is missing",
                )
            )
        elif value is None:
            rejects.append(
                ActionQueueRowReject(
                    row_index=row_index,
                    action_id=action_id,
                    video_id=video_id,
                    code=REJECT_NULL_REQUIRED_FIELD,
                    field=field,
                    reason=f"required field `{field}` is null",
                )
            )

    decision_type = _read_field(row, "decision_type")
    if decision_type not in (_MISSING, None) and decision_type not in ACTION_QUEUE_DECISION_DOMAIN:
        rejects.append(
            ActionQueueRowReject(
                row_index=row_index,
                action_id=action_id,
                video_id=video_id,
                code=REJECT_INVALID_DECISION_TYPE,
                field="decision_type",
                reason=f"unsupported decision_type `{decision_type}`",
            )
        )

    state = _read_field(row, "state")
    if state not in (_MISSING, None) and state != ACTION_QUEUE_INITIAL_STATE:
        rejects.append(
            ActionQueueRowReject(
                row_index=row_index,
                action_id=action_id,
                video_id=video_id,
                code=REJECT_INVALID_INITIAL_STATE,
                field="state",
                reason=f"producer write state must be `{ACTION_QUEUE_INITIAL_STATE}`, got `{state}`",
            )
        )

    decided_at = _read_field(row, "decided_at")
    expires_at = _read_field(row, "expires_at")
    if (
        decided_at not in (_MISSING, None)
        and expires_at not in (_MISSING, None)
        and expires_at <= decided_at
    ):
        rejects.append(
            ActionQueueRowReject(
                row_index=row_index,
                action_id=action_id,
                video_id=video_id,
                code=REJECT_INVALID_TIME_ORDER,
                field="expires_at",
                reason="expires_at must be greater than decided_at",
            )
        )

    reason_codes = _read_field(row, "reason_codes")
    if reason_codes not in (_MISSING, None) and len(reason_codes) == 0:
        rejects.append(
            ActionQueueRowReject(
                row_index=row_index,
                action_id=action_id,
                video_id=video_id,
                code=REJECT_EMPTY_REASON_CODES,
                field="reason_codes",
                reason="reason_codes must not be empty",
            )
        )

    return len(rejects) == 0, rejects


def validate_action_queue_write_rows(
    rows: Iterable[ActionQueueWriteRow],
    *,
    strict: bool = False,
) -> tuple[List[ActionQueueWriteRow], List[ActionQueueRowReject]]:
    """Batch validator that returns valid rows and rejects with explicit reasons."""

    valid_rows: List[ActionQueueWriteRow] = []
    rejects: List[ActionQueueRowReject] = []
    for index, row in enumerate(rows):
        is_valid, row_rejects = validate_action_queue_write_row(row, row_index=index)
        if is_valid:
            valid_rows.append(row)
        else:
            rejects.extend(row_rejects)

    if strict and rejects:
        summary = ", ".join(f"{r.code}:{r.field}@{r.row_index}" for r in rejects[:3])
        raise ValueError(
            f"action queue write-row contract validation failed; rejects={len(rejects)}; "
            f"sample={summary}"
        )

    return valid_rows, rejects


def prepare_action_queue_write_rows(
    candidates: Iterable[ActionQueueCandidate],
    *,
    strict: bool = False,
    decided_at_factory: DecisionTimeFactory = _utc_now,
    action_id_factory: ActionIdFactory = _default_action_id,
) -> tuple[List[ActionQueueWriteRow], List[ActionQueueRowReject]]:
    """Prepare and validate write rows, returning `(valid_rows, rejects)`."""

    write_rows = build_action_queue_write_rows(
        candidates,
        decided_at_factory=decided_at_factory,
        action_id_factory=action_id_factory,
    )
    return validate_action_queue_write_rows(write_rows, strict=strict)


def build_and_prepare_action_queue_write_rows(
    context_rows: Iterable[DecisionContextRow],
    *,
    strict: bool = False,
    decided_at_factory: DecisionTimeFactory = _utc_now,
    action_id_factory: ActionIdFactory = _default_action_id,
) -> tuple[List[ActionQueueWriteRow], List[ActionQueueRowReject]]:
    """End-to-end producer prep: decision candidates to validated write rows."""

    candidates = build_action_queue_candidates(context_rows)
    return prepare_action_queue_write_rows(
        candidates,
        strict=strict,
        decided_at_factory=decided_at_factory,
        action_id_factory=action_id_factory,
    )


def build_action_queue_candidates(rows: Iterable[DecisionContextRow]) -> List[ActionQueueCandidate]:
    """Build queue-write candidates and suppress `NO_ACTION` before write path."""

    candidates: List[ActionQueueCandidate] = []
    for row in rows:
        decision = derive_decision(_to_decision_inputs(row))
        if decision not in _REASON_CODES:
            # Contract: `NO_ACTION` must not be persisted to rt_action_queue.
            continue

        candidates.append(
            ActionQueueCandidate(
                video_id=row.video_id,
                decision_type=decision,
                window_start=row.window_start,
                window_end=row.window_end,
                rule_version=row.rule_version,
                velocity_30m=row.velocity_30m,
                impressions_30m=row.impressions_30m,
                completion_rate_30m=row.completion_rate_30m,
                skip_rate_30m=row.skip_rate_30m,
                reason_codes=_REASON_CODES[decision],
            )
        )

    return candidates
