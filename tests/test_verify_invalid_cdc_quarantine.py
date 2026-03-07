from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_invalid_cdc_quarantine import validate_invalid_quarantine  # noqa: E402


class VerifyInvalidCdcQuarantineTests(unittest.TestCase):
    def test_validation_passes_when_contract_fields_present(self) -> None:
        errors, metrics = validate_invalid_quarantine(
            row_count=4,
            min_row_count=1,
            null_counts={
                "invalid_event_id": 0,
                "raw_value": 0,
                "source_topic": 0,
                "source_partition": 0,
                "source_offset": 0,
                "schema_version": 0,
                "error_code": 0,
                "error_reason": 0,
                "ingested_at": 0,
            },
            actual_error_codes={"CDC_PARSE_ERROR", "CDC_UNSUPPORTED_OP"},
            expected_error_codes={"CDC_PARSE_ERROR"},
        )
        self.assertEqual(errors, [])
        self.assertEqual(metrics["row_count"], 4)

    def test_validation_fails_for_nulls_and_missing_error_codes(self) -> None:
        errors, _ = validate_invalid_quarantine(
            row_count=1,
            min_row_count=2,
            null_counts={
                "invalid_event_id": 0,
                "raw_value": 1,
                "source_topic": 0,
                "source_partition": 0,
                "source_offset": 0,
                "schema_version": 0,
                "error_code": 0,
                "error_reason": 1,
                "ingested_at": 0,
            },
            actual_error_codes={"CDC_PARSE_ERROR"},
            expected_error_codes={"CDC_PARSE_ERROR", "CDC_UNSUPPORTED_OP"},
        )
        joined = "\n".join(errors)
        self.assertIn("row count 1 is below minimum 2", joined)
        self.assertIn("field raw_value has 1 null value(s)", joined)
        self.assertIn("field error_reason has 1 null value(s)", joined)
        self.assertIn("missing expected error_code value(s): CDC_UNSUPPORTED_OP", joined)


if __name__ == "__main__":
    unittest.main()
