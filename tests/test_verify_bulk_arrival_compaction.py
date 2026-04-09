"""Tests for verify_bulk_arrival_compaction pure validation logic."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_bulk_arrival_compaction import validate_compaction_result  # noqa: E402


class ValidateCompactionResultTests(unittest.TestCase):
    def test_passes_when_file_count_below_limit(self) -> None:
        errors = validate_compaction_result(file_count=5, max_file_count=20)
        self.assertEqual(errors, [])

    def test_passes_when_file_count_at_limit(self) -> None:
        errors = validate_compaction_result(file_count=20, max_file_count=20)
        self.assertEqual(errors, [])

    def test_fails_when_file_count_exceeds_limit(self) -> None:
        errors = validate_compaction_result(file_count=21, max_file_count=20)
        self.assertEqual(len(errors), 1)

    def test_error_message_includes_counts(self) -> None:
        errors = validate_compaction_result(file_count=42, max_file_count=20)
        self.assertTrue(any("42" in e for e in errors), msg=f"actual count missing from: {errors}")
        self.assertTrue(any("20" in e for e in errors), msg=f"max count missing from: {errors}")

    def test_fails_when_file_count_far_exceeds_limit(self) -> None:
        errors = validate_compaction_result(file_count=1000, max_file_count=20)
        self.assertGreater(len(errors), 0)

    def test_passes_with_single_file(self) -> None:
        errors = validate_compaction_result(file_count=1, max_file_count=20)
        self.assertEqual(errors, [])

    def test_passes_with_zero_files(self) -> None:
        errors = validate_compaction_result(file_count=0, max_file_count=20)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
