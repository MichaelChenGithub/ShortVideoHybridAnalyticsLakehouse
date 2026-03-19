from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_user_cdc_raw_sql import (  # noqa: E402
    create_invalid_events_cdc_users_sql,
    create_raw_cdc_users_sql,
    manual_alter_invalid_events_cdc_users_statements,
    manual_alter_raw_cdc_users_statements,
    missing_invalid_events_cdc_users_columns,
    missing_raw_cdc_users_columns,
    required_invalid_events_cdc_users_columns,
    required_raw_cdc_users_columns,
)


class RtUserCdcRawSqlTests(unittest.TestCase):
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
        self.assertEqual(required_invalid_events_cdc_users_columns(), expected)

    def test_raw_cdc_required_columns_match_contract(self) -> None:
        expected = (
            ("op", "STRING"),
            ("ts_ms", "BIGINT"),
            ("schema_version", "STRING"),
            ("user_id", "STRING"),
            ("new_vs_returning_user", "STRING"),
            ("region", "STRING"),
            ("source_topic", "STRING"),
            ("source_partition", "INT"),
            ("source_offset", "BIGINT"),
            ("kafka_timestamp", "TIMESTAMP"),
            ("raw_value", "STRING"),
            ("ingested_at", "TIMESTAMP"),
        )
        self.assertEqual(required_raw_cdc_users_columns(), expected)

    def test_create_invalid_cdc_sql_contains_required_fields(self) -> None:
        sql = create_invalid_events_cdc_users_sql()
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS lakehouse.bronze.invalid_events_cdc_users",
            sql,
        )
        for column_name, data_type in required_invalid_events_cdc_users_columns():
            self.assertIn(f"{column_name} {data_type}", sql)

    def test_create_raw_cdc_sql_contains_required_fields(self) -> None:
        sql = create_raw_cdc_users_sql()
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS lakehouse.bronze.raw_cdc_users",
            sql,
        )
        for column_name, data_type in required_raw_cdc_users_columns():
            self.assertIn(f"{column_name} {data_type}", sql)

    def test_invalid_cdc_missing_columns_and_manual_alter_sql(self) -> None:
        existing = ["invalid_event_id", "raw_value", "source_topic"]
        missing = missing_invalid_events_cdc_users_columns(existing)
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
        statements = manual_alter_invalid_events_cdc_users_statements(existing)
        self.assertEqual(
            statements,
            [
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_users ADD COLUMNS (source_partition INT);",
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_users ADD COLUMNS (source_offset BIGINT);",
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_users ADD COLUMNS (schema_version STRING);",
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_users ADD COLUMNS (error_code STRING);",
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_users ADD COLUMNS (error_reason STRING);",
                "ALTER TABLE lakehouse.bronze.invalid_events_cdc_users ADD COLUMNS (ingested_at TIMESTAMP);",
            ],
        )

    def test_raw_cdc_missing_columns_and_manual_alter_sql(self) -> None:
        existing = ["op", "ts_ms", "user_id"]
        missing = missing_raw_cdc_users_columns(existing)
        self.assertEqual(
            missing,
            [
                ("schema_version", "STRING"),
                ("new_vs_returning_user", "STRING"),
                ("region", "STRING"),
                ("source_topic", "STRING"),
                ("source_partition", "INT"),
                ("source_offset", "BIGINT"),
                ("kafka_timestamp", "TIMESTAMP"),
                ("raw_value", "STRING"),
                ("ingested_at", "TIMESTAMP"),
            ],
        )
        statements = manual_alter_raw_cdc_users_statements(existing)
        self.assertEqual(
            statements,
            [
                "ALTER TABLE lakehouse.bronze.raw_cdc_users ADD COLUMNS (schema_version STRING);",
                "ALTER TABLE lakehouse.bronze.raw_cdc_users ADD COLUMNS (new_vs_returning_user STRING);",
                "ALTER TABLE lakehouse.bronze.raw_cdc_users ADD COLUMNS (region STRING);",
                "ALTER TABLE lakehouse.bronze.raw_cdc_users ADD COLUMNS (source_topic STRING);",
                "ALTER TABLE lakehouse.bronze.raw_cdc_users ADD COLUMNS (source_partition INT);",
                "ALTER TABLE lakehouse.bronze.raw_cdc_users ADD COLUMNS (source_offset BIGINT);",
                "ALTER TABLE lakehouse.bronze.raw_cdc_users ADD COLUMNS (kafka_timestamp TIMESTAMP);",
                "ALTER TABLE lakehouse.bronze.raw_cdc_users ADD COLUMNS (raw_value STRING);",
                "ALTER TABLE lakehouse.bronze.raw_cdc_users ADD COLUMNS (ingested_at TIMESTAMP);",
            ],
        )


if __name__ == "__main__":
    unittest.main()
