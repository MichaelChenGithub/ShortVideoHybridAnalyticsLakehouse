from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_content_events_aggregator_sql import (  # noqa: E402
    create_invalid_events_content_sql,
    create_raw_events_sql,
    create_rt_video_stats_sql,
    manual_alter_invalid_events_content_statements,
    manual_alter_raw_events_statements,
    manual_alter_rt_video_stats_statements,
    merge_rt_video_stats_sql,
    missing_invalid_events_content_columns,
    missing_raw_events_columns,
    missing_rt_video_stats_columns,
    required_invalid_events_content_columns,
    required_raw_events_columns,
    required_rt_video_stats_columns,
)


class RtContentEventsAggregatorSqlTests(unittest.TestCase):
    def test_required_columns_match_contract(self) -> None:
        self.assertEqual(
            required_raw_events_columns(),
            (
                ("event_id", "STRING"),
                ("event_timestamp", "TIMESTAMP"),
                ("video_id", "STRING"),
                ("user_id", "STRING"),
                ("event_type", "STRING"),
                ("payload_json", "STRING"),
                ("schema_version", "STRING"),
                ("source_topic", "STRING"),
                ("source_partition", "INT"),
                ("source_offset", "BIGINT"),
                ("ingested_at", "TIMESTAMP"),
            ),
        )
        self.assertEqual(
            required_rt_video_stats_columns(),
            (
                ("video_id", "STRING"),
                ("window_start", "TIMESTAMP"),
                ("window_end", "TIMESTAMP"),
                ("impressions", "BIGINT"),
                ("play_start", "BIGINT"),
                ("play_finish", "BIGINT"),
                ("likes", "BIGINT"),
                ("shares", "BIGINT"),
                ("skips", "BIGINT"),
                ("watch_time_sum_ms", "BIGINT"),
                ("processed_at", "TIMESTAMP"),
            ),
        )
        self.assertEqual(
            required_invalid_events_content_columns(),
            (
                ("invalid_event_id", "STRING"),
                ("raw_value", "STRING"),
                ("source_topic", "STRING"),
                ("source_partition", "INT"),
                ("source_offset", "BIGINT"),
                ("schema_version", "STRING"),
                ("error_code", "STRING"),
                ("error_reason", "STRING"),
                ("ingested_at", "TIMESTAMP"),
            ),
        )

    def test_create_sql_contains_required_fields(self) -> None:
        raw_sql = create_raw_events_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.bronze.raw_events", raw_sql)
        for column_name, data_type in required_raw_events_columns():
            self.assertIn(f"{column_name} {data_type}", raw_sql)

        gold_sql = create_rt_video_stats_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.gold.rt_video_stats_1min", gold_sql)
        for column_name, data_type in required_rt_video_stats_columns():
            self.assertIn(f"{column_name} {data_type}", gold_sql)

        invalid_sql = create_invalid_events_content_sql()
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS lakehouse.bronze.invalid_events_content",
            invalid_sql,
        )
        for column_name, data_type in required_invalid_events_content_columns():
            self.assertIn(f"{column_name} {data_type}", invalid_sql)

    def test_missing_columns_and_manual_alter_sql(self) -> None:
        raw_existing = ["event_id", "event_timestamp", "video_id"]
        raw_missing = missing_raw_events_columns(raw_existing)
        self.assertIn(("payload_json", "STRING"), raw_missing)
        raw_statements = manual_alter_raw_events_statements(raw_existing)
        self.assertIn(
            "ALTER TABLE lakehouse.bronze.raw_events ADD COLUMNS (payload_json STRING);",
            raw_statements,
        )

        gold_existing = ["video_id", "window_start", "impressions"]
        gold_missing = missing_rt_video_stats_columns(gold_existing)
        self.assertIn(("window_end", "TIMESTAMP"), gold_missing)
        gold_statements = manual_alter_rt_video_stats_statements(gold_existing)
        self.assertIn(
            "ALTER TABLE lakehouse.gold.rt_video_stats_1min ADD COLUMNS (window_end TIMESTAMP);",
            gold_statements,
        )

        invalid_existing = ["invalid_event_id", "raw_value"]
        invalid_missing = missing_invalid_events_content_columns(invalid_existing)
        self.assertIn(("error_code", "STRING"), invalid_missing)
        invalid_statements = manual_alter_invalid_events_content_statements(invalid_existing)
        self.assertIn(
            "ALTER TABLE lakehouse.bronze.invalid_events_content ADD COLUMNS (error_code STRING);",
            invalid_statements,
        )

    def test_sql_builders_support_custom_table_name(self) -> None:
        raw_table = "lakehouse.bronze.raw_events_canary"
        gold_table = "lakehouse.gold.rt_video_stats_1min_canary"
        invalid_table = "lakehouse.bronze.invalid_events_content_canary"
        self.assertIn(raw_table, create_raw_events_sql(raw_table))
        self.assertIn(gold_table, create_rt_video_stats_sql(gold_table))
        self.assertIn(invalid_table, create_invalid_events_content_sql(invalid_table))

    def test_merge_sql_builder_supports_custom_view_and_table(self) -> None:
        merge_sql = merge_rt_video_stats_sql(
            source_view="custom_updates",
            table_name="lakehouse.gold.rt_video_stats_1min_canary",
        )
        self.assertIn("MERGE INTO lakehouse.gold.rt_video_stats_1min_canary AS target", merge_sql)
        self.assertIn("USING custom_updates AS source", merge_sql)
        self.assertIn("ON target.video_id = source.video_id", merge_sql)
        self.assertIn("AND target.window_start = source.window_start", merge_sql)


if __name__ == "__main__":
    unittest.main()
