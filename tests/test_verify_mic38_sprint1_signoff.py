from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from scripts.verify_mic38_sprint1_signoff import (  # noqa: E402
    build_signoff_report,
    extract_last_json_object,
)


class VerifyMic38Sprint1SignoffTests(unittest.TestCase):
    def test_extract_last_json_object_uses_last_json_line(self) -> None:
        log_text = "\n".join(
            [
                "PASS: verifier passed",
                '{"first": 1}',
                "non-json line",
                '{"second": 2}',
            ]
        )
        parsed = extract_last_json_object(log_text)
        self.assertEqual(parsed, {"second": 2})

    def test_build_signoff_report_passes_for_healthy_inputs(self) -> None:
        now_ms = 2_000_000
        report = build_signoff_report(
            run_id="mic38_healthy",
            run_start_ms=1_900_000,
            verifier_status={
                "content_metrics": "PASS",
                "content_contract": "PASS",
                "cdc_upsert": "PASS",
                "cdc_invalid": "PASS",
                "cdc_health": "PASS",
            },
            content_metrics={
                "raw_count": 100,
                "gold_count": 20,
                "duplicate_key_rows": 0,
                "null_required_rows": 0,
                "negative_metric_rows": 0,
                "max_processed_at_ms": now_ms - 20_000,
            },
            content_contract={
                "raw_count": 100,
                "gold_count": 20,
                "invalid_count": 10,
                "invalid_required_null_rows": 0,
                "max_invalid_ingested_at_ms": now_ms - 10_000,
                "invalid_rate": 0.09,
            },
            cdc_upsert={
                "video_id": "mic38_healthy_cdc_vid_001",
                "source_ts_ms": now_ms - 30_000,
                "status": "copyright_strike",
                "updated_at": "2026-03-08T00:00:00Z",
            },
            cdc_invalid={
                "row_count": 4,
                "null_counts": {
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
            },
            cdc_health={
                "latest_source_ts_ms": now_ms - 30_000,
                "freshness_age_ms": 30_000,
                "invalid_rate_per_minute": 0.10,
            },
            runtime_start={
                "sample_at_ms": now_ms - 120_000,
                "content_pid_count": 1,
                "cdc_pid_count": 1,
                "content_log_size": 1_000,
                "cdc_log_size": 900,
                "content_max_batch_id": 1,
                "cdc_max_batch_id": 1,
                "content_exception_lines": 0,
                "cdc_exception_lines": 0,
            },
            runtime_end={
                "sample_at_ms": now_ms,
                "content_pid_count": 1,
                "cdc_pid_count": 1,
                "content_log_size": 2_100,
                "cdc_log_size": 2_050,
                "content_max_batch_id": 3,
                "cdc_max_batch_id": 4,
                "content_exception_lines": 0,
                "cdc_exception_lines": 0,
            },
            checkpoint_start={
                "paths": {
                    "content_raw": {"file_count": 10},
                    "content_gold": {"file_count": 10},
                    "content_invalid": {"file_count": 5},
                    "cdc_dim": {"file_count": 8},
                    "cdc_invalid": {"file_count": 3},
                }
            },
            checkpoint_end={
                "paths": {
                    "content_raw": {"file_count": 12},
                    "content_gold": {"file_count": 12},
                    "content_invalid": {"file_count": 7},
                    "cdc_dim": {"file_count": 10},
                    "cdc_invalid": {"file_count": 6},
                }
            },
            max_freshness_minutes=3,
            latency_threshold_minutes=3,
            max_content_invalid_rate=0.20,
            max_cdc_invalid_rate=0.20,
            input_errors=[],
            checked_at_ms=now_ms,
        )

        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["failure_reasons"], [])
        self.assertTrue(all(gate["passed"] for gate in report["gates"]))

    def test_build_signoff_report_fails_for_multiple_gate_breaches(self) -> None:
        now_ms = 2_000_000
        report = build_signoff_report(
            run_id="mic38_unhealthy",
            run_start_ms=1_900_000,
            verifier_status={
                "content_metrics": "PASS",
                "content_contract": "PASS",
                "cdc_upsert": "PASS",
                "cdc_invalid": "PASS",
                "cdc_health": "PASS",
            },
            content_metrics={
                "raw_count": 100,
                "gold_count": 20,
                "duplicate_key_rows": 0,
                "null_required_rows": 0,
                "negative_metric_rows": 0,
                "max_processed_at_ms": now_ms - 400_000,
            },
            content_contract={
                "raw_count": 100,
                "gold_count": 20,
                "invalid_count": 10,
                "invalid_required_null_rows": 0,
                "max_invalid_ingested_at_ms": now_ms - 350_000,
                "invalid_rate": 0.60,
            },
            cdc_upsert={
                "video_id": "mic38_unhealthy_cdc_vid_001",
                "source_ts_ms": now_ms - 450_000,
                "status": "copyright_strike",
                "updated_at": "2026-03-08T00:00:00Z",
            },
            cdc_invalid={
                "row_count": 4,
                "null_counts": {
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
            },
            cdc_health={
                "latest_source_ts_ms": now_ms - 450_000,
                "freshness_age_ms": 450_000,
                "invalid_rate_per_minute": 0.50,
            },
            runtime_start={
                "sample_at_ms": now_ms - 120_000,
                "content_pid_count": 1,
                "cdc_pid_count": 1,
                "content_log_size": 1_000,
                "cdc_log_size": 900,
                "content_max_batch_id": 3,
                "cdc_max_batch_id": 4,
                "content_exception_lines": 0,
                "cdc_exception_lines": 0,
            },
            runtime_end={
                "sample_at_ms": now_ms,
                "content_pid_count": 0,
                "cdc_pid_count": 0,
                "content_log_size": 1_000,
                "cdc_log_size": 900,
                "content_max_batch_id": 3,
                "cdc_max_batch_id": 4,
                "content_exception_lines": 1,
                "cdc_exception_lines": 2,
            },
            checkpoint_start={
                "paths": {
                    "content_raw": {"file_count": 10},
                    "content_gold": {"file_count": 10},
                    "content_invalid": {"file_count": 5},
                    "cdc_dim": {"file_count": 8},
                    "cdc_invalid": {"file_count": 3},
                }
            },
            checkpoint_end={
                "paths": {
                    "content_raw": {"file_count": 10},
                    "content_gold": {"file_count": 10},
                    "content_invalid": {"file_count": 5},
                    "cdc_dim": {"file_count": 8},
                    "cdc_invalid": {"file_count": 3},
                }
            },
            max_freshness_minutes=3,
            latency_threshold_minutes=3,
            max_content_invalid_rate=0.20,
            max_cdc_invalid_rate=0.20,
            input_errors=[],
            checked_at_ms=now_ms,
        )

        self.assertEqual(report["status"], "FAIL")
        failed_gate_names = {gate["name"] for gate in report["gates"] if not gate["passed"]}
        self.assertIn("invalid_rates_within_threshold", failed_gate_names)
        self.assertIn("query_health_active_no_exception", failed_gate_names)
        self.assertIn("query_progress_moving", failed_gate_names)
        self.assertIn("checkpoint_growth", failed_gate_names)
        self.assertIn("freshness_sla", failed_gate_names)
        self.assertIn("latency_p95_proxy_lt_threshold", failed_gate_names)

    def test_build_signoff_report_uses_verifier_checked_at_for_freshness(self) -> None:
        verifier_now_ms = 2_000_000
        final_now_ms = verifier_now_ms + 300_000
        checked_at = datetime.fromtimestamp(verifier_now_ms / 1_000, tz=timezone.utc).isoformat()

        report = build_signoff_report(
            run_id="mic38_checked_at",
            run_start_ms=1_800_000,
            verifier_status={
                "content_metrics": "PASS",
                "content_contract": "PASS",
                "cdc_upsert": "PASS",
                "cdc_invalid": "PASS",
                "cdc_health": "PASS",
            },
            content_metrics={
                "raw_count": 100,
                "gold_count": 20,
                "duplicate_key_rows": 0,
                "null_required_rows": 0,
                "negative_metric_rows": 0,
                "max_processed_at_ms": verifier_now_ms - 120_000,
                "checked_at": checked_at,
            },
            content_contract={
                "raw_count": 100,
                "gold_count": 20,
                "invalid_count": 10,
                "invalid_required_null_rows": 0,
                "max_invalid_ingested_at_ms": verifier_now_ms - 110_000,
                "invalid_rate": 0.09,
                "checked_at": checked_at,
            },
            cdc_upsert={
                "video_id": "mic38_checked_at_cdc_vid_001",
                "source_ts_ms": verifier_now_ms - 100_000,
                "status": "copyright_strike",
                "updated_at": "2026-03-08T00:00:00Z",
            },
            cdc_invalid={
                "row_count": 4,
                "null_counts": {
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
            },
            cdc_health={
                "latest_source_ts_ms": verifier_now_ms - 100_000,
                "freshness_age_ms": 100_000,
                "invalid_rate_per_minute": 0.10,
                "checked_at": checked_at,
            },
            runtime_start={
                "sample_at_ms": verifier_now_ms - 120_000,
                "content_pid_count": 1,
                "cdc_pid_count": 1,
                "content_log_size": 1_000,
                "cdc_log_size": 900,
                "content_max_batch_id": 1,
                "cdc_max_batch_id": 1,
                "content_exception_lines": 0,
                "cdc_exception_lines": 0,
            },
            runtime_end={
                "sample_at_ms": final_now_ms,
                "content_pid_count": 1,
                "cdc_pid_count": 1,
                "content_log_size": 2_100,
                "cdc_log_size": 2_050,
                "content_max_batch_id": 3,
                "cdc_max_batch_id": 4,
                "content_exception_lines": 0,
                "cdc_exception_lines": 0,
            },
            checkpoint_start={
                "paths": {
                    "content_raw": {"file_count": 10},
                    "content_gold": {"file_count": 10},
                    "content_invalid": {"file_count": 5},
                    "cdc_dim": {"file_count": 8},
                    "cdc_invalid": {"file_count": 3},
                }
            },
            checkpoint_end={
                "paths": {
                    "content_raw": {"file_count": 12},
                    "content_gold": {"file_count": 12},
                    "content_invalid": {"file_count": 7},
                    "cdc_dim": {"file_count": 10},
                    "cdc_invalid": {"file_count": 6},
                }
            },
            max_freshness_minutes=3,
            latency_threshold_minutes=3,
            max_content_invalid_rate=0.20,
            max_cdc_invalid_rate=0.20,
            input_errors=[],
            checked_at_ms=final_now_ms,
        )

        self.assertEqual(report["status"], "PASS")


if __name__ == "__main__":
    unittest.main()
