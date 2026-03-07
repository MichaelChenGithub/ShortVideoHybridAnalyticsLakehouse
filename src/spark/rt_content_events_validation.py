"""Validation helpers for MIC-39 content contract enforcement."""

from __future__ import annotations

from typing import Sequence

ALLOWED_EVENT_TYPES = (
    "impression",
    "play_start",
    "play_finish",
    "like",
    "share",
    "skip",
)

REQUIRED_CONTENT_FIELDS = (
    "event_id",
    "video_id",
    "user_id",
    "schema_version",
    "payload_json",
)

UNKNOWN_SCHEMA_VERSION = "unknown"

PARSE_ERROR = "PARSE_ERROR"
MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
INVALID_EVENT_TIMESTAMP = "INVALID_EVENT_TIMESTAMP"
INVALID_EVENT_TYPE = "INVALID_EVENT_TYPE"
INVALID_PAYLOAD_JSON = "INVALID_PAYLOAD_JSON"


def classify_contract_error_code(
    *,
    parse_error: bool,
    missing_required_field: bool,
    invalid_event_timestamp: bool,
    invalid_event_type: bool,
    invalid_payload_json: bool,
) -> str | None:
    """Return the first matching MIC-39 contract error code by precedence."""
    if parse_error:
        return PARSE_ERROR
    if missing_required_field:
        return MISSING_REQUIRED_FIELD
    if invalid_event_timestamp:
        return INVALID_EVENT_TIMESTAMP
    if invalid_event_type:
        return INVALID_EVENT_TYPE
    if invalid_payload_json:
        return INVALID_PAYLOAD_JSON
    return None


def error_reason_for_code(error_code: str, missing_fields: Sequence[str] | None = None) -> str:
    """Return a stable human-readable reason for a MIC-39 error code."""
    if error_code == PARSE_ERROR:
        return "root payload is not valid JSON"
    if error_code == MISSING_REQUIRED_FIELD:
        if missing_fields:
            return "missing required field(s): " + ",".join(missing_fields)
        return "missing one or more required fields"
    if error_code == INVALID_EVENT_TIMESTAMP:
        return "event_timestamp is missing or not parseable as UTC timestamp"
    if error_code == INVALID_EVENT_TYPE:
        return "event_type is not in allowed enum"
    if error_code == INVALID_PAYLOAD_JSON:
        return "payload_json is not parseable JSON"
    return "unknown contract error"


def normalize_schema_version(schema_version: str | None) -> str:
    """Normalize null/blank schema versions to the quarantine fallback value."""
    if schema_version is None:
        return UNKNOWN_SCHEMA_VERSION
    normalized = schema_version.strip()
    if not normalized:
        return UNKNOWN_SCHEMA_VERSION
    return normalized


def build_invalid_event_id(
    source_topic: str | None,
    source_partition: int | None,
    source_offset: int | None,
) -> str:
    """Build deterministic invalid_event_id as topic:partition:offset."""
    topic = source_topic or "unknown"
    partition = source_partition if source_partition is not None else -1
    offset = source_offset if source_offset is not None else -1
    return f"{topic}:{partition}:{offset}"
