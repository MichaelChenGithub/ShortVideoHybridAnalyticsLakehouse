from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_video_cdc_upsert_sql import (  # noqa: E402
    create_dim_videos_sql,
    create_invalid_events_cdc_videos_sql,
    manual_alter_invalid_events_cdc_videos_statements,
    manual_alter_statements,
    merge_dim_videos_sql,
    missing_invalid_events_cdc_videos_columns,
    missing_required_columns,
    required_dim_videos_columns,
    required_invalid_events_cdc_videos_columns,
)


class RtVideoCdcUpsertSqlTests(unittest.TestCase):
    def test_required_columns_match_contract(self) -> None:
        expected = (
            ("video_id", "STRING"),
            ("category", "STRING"),
            ("region", "STRING"),
            ("upload_time", "TIMESTAMP"),
            ("status", "STRING"),
            ("updated_at", "TIMESTAMP"),
            ("source_ts_ms", "BIGINT"),
        )
        self.assertEqual(required_dim_videos_columns(), expected)

    def test_create_sql_contains_required_fields(self) -> None:
        sql = create_dim_videos_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.dims.dim_videos", sql)
        for column_name, data_type in required_dim_videos_columns():
            self.assertIn(f"{column_name} {data_type}", sql)

    def test_invalid_cdc_required_columns_match_contract(self) -> None:
        expected = (
            ("invalid_event_id", "STRING"),
            ("raw_value", "STRING"),
            ("source_topic", "STRING"),
            ("source_partition", "INT"),
            ("source_offset", "BIGINT"),
            ("schema_version", "STRING"),
            ("error_code", "STRING"),
            ("error_reason", "STRING"),
            ("ingested_at", "TIMESTAMP"),
        )
        self.assertEqual(required_invalid_events_cdc_videos_columns(), expected)

    def test_create_invalid_cdc_sql_contains_required_fields(self) -> None:
        sql = create_invalid_events_cdc_videos_sql()
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS lakehouse.bronze.invalid_events_cdc_videos",
            sql,
        )
        for column_name, data_type in required_invalid_events_cdc_videos_columns():
            self.assertIn(f"{column_name} {data_type}", sql)

    def test_missing_columns_and_manual_alter_sql(self) -> None:
        existing = ["video_id", "category", "upload_time"]
        missing = missing_required_columns(existing)
        self.assertEqual(
            missing,
            [
                ("region", "STRING"),
                ("status", "STRING"),
                ("updated_at", "TIMESTAMP"),
                ("source_ts_ms", "BIGINT"),
            ],
        )
        statements = manual_alter_statements(existing)
        self.assertEqual(
            statements,
            [
                "ALTER TABLE lakehouse.dims.dim_videos ADD COLUMNS (region STRING);",
                "ALTER TABLE lakehouse.dims.dim_videos ADD COLUMNS (status STRING);",
                "ALTER TABLE lakehouse.dims.dim_videos ADD COLUMNS (updated_at TIMESTAMP);",
                "ALTER TABLE lakehouse.dims.dim_videos ADD COLUMNS (source_ts_ms BIGINT);",
            ],
        )

    def test_merge_sql_enforces_ts_and_offset_ordering(self) -> None:
        sql = merge_dim_videos_sql("video_updates")
        self.assertIn("ORDER BY ts_ms DESC, source_offset DESC", sql)
        self.assertIn("MERGE INTO lakehouse.dims.dim_videos", sql)
        self.assertIn("WHEN MATCHED THEN UPDATE SET", sql)
        self.assertIn("WHEN NOT MATCHED THEN INSERT", sql)

    def test_sql_builders_support_custom_table_name(self) -> None:
        table_name = "lakehouse.dims.dim_videos_canary"
        self.assertIn(table_name, create_dim_videos_sql(table_name))
        self.assertIn(table_name, merge_dim_videos_sql("video_updates", table_name))
        statements = manual_alter_statements(["video_id"], table_name)
        self.assertTrue(all(table_name in statement for statement in statements))

    def test_invalid_cdc_missing_columns_and_manual_alter_sql(self) -> None:
        existing = ["invalid_event_id", "raw_value", "source_topic"]
        missing = missing_invalid_events_cdc_videos_columns(existing)
        self.assertEqual(
            missing,
            [
                ("source_partition", "INT"),
                ("source_offset", "BIGINT"),
                ("schema_version", "STRING"),
                ("error_code", "STRING"),
                ("error_reason", "STRING"),
                ("ingested_at", "TIMESTAMP"),
            ],
        )
        statements = manual_alter_invalid_events_cdc_videos_statements(existing)
        self.assertEqual(
            statements,
            [
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_videos ADD COLUMNS (source_partition INT);",
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_videos ADD COLUMNS (source_offset BIGINT);",
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_videos ADD COLUMNS (schema_version STRING);",
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_videos ADD COLUMNS (error_code STRING);",
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_videos ADD COLUMNS (error_reason STRING);",
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_videos ADD COLUMNS (ingested_at TIMESTAMP);",
            ],
        )


if __name__ == "__main__":
    unittest.main()
