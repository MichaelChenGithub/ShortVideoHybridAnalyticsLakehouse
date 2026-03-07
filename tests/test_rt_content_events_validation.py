from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_content_events_validation import (  # noqa: E402
    INVALID_EVENT_TIMESTAMP,
    INVALID_EVENT_TYPE,
    INVALID_PAYLOAD_JSON,
    MISSING_REQUIRED_FIELD,
    PARSE_ERROR,
    build_invalid_event_id,
    classify_contract_error_code,
    error_reason_for_code,
    normalize_schema_version,
)


class RtContentEventsValidationTests(unittest.TestCase):
    def test_valid_flags_return_no_error(self) -> None:
        self.assertIsNone(
            classify_contract_error_code(
                parse_error=False,
                missing_required_field=False,
                invalid_event_timestamp=False,
                invalid_event_type=False,
                invalid_payload_json=False,
            )
        )

    def test_error_precedence_matches_mic39_contract(self) -> None:
        self.assertEqual(
            classify_contract_error_code(
                parse_error=True,
                missing_required_field=True,
                invalid_event_timestamp=True,
                invalid_event_type=True,
                invalid_payload_json=True,
            ),
            PARSE_ERROR,
        )
        self.assertEqual(
            classify_contract_error_code(
                parse_error=False,
                missing_required_field=True,
                invalid_event_timestamp=True,
                invalid_event_type=True,
                invalid_payload_json=True,
            ),
            MISSING_REQUIRED_FIELD,
        )
        self.assertEqual(
            classify_contract_error_code(
                parse_error=False,
                missing_required_field=False,
                invalid_event_timestamp=True,
                invalid_event_type=True,
                invalid_payload_json=True,
            ),
            INVALID_EVENT_TIMESTAMP,
        )
        self.assertEqual(
            classify_contract_error_code(
                parse_error=False,
                missing_required_field=False,
                invalid_event_timestamp=False,
                invalid_event_type=True,
                invalid_payload_json=True,
            ),
            INVALID_EVENT_TYPE,
        )
        self.assertEqual(
            classify_contract_error_code(
                parse_error=False,
                missing_required_field=False,
                invalid_event_timestamp=False,
                invalid_event_type=False,
                invalid_payload_json=True,
            ),
            INVALID_PAYLOAD_JSON,
        )

    def test_reason_and_schema_fallback_helpers(self) -> None:
        self.assertIn(
            "event_id",
            error_reason_for_code(MISSING_REQUIRED_FIELD, missing_fields=["event_id", "payload_json"]),
        )
        self.assertEqual(normalize_schema_version(None), "unknown")
        self.assertEqual(normalize_schema_version("   "), "unknown")
        self.assertEqual(normalize_schema_version("v1"), "v1")

    def test_invalid_event_id_is_deterministic(self) -> None:
        self.assertEqual(build_invalid_event_id("content_events", 3, 99), "content_events:3:99")
        self.assertEqual(build_invalid_event_id(None, None, None), "unknown:-1:-1")


if __name__ == "__main__":
    unittest.main()
