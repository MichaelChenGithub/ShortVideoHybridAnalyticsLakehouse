from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.bt_events_conformed_sql import (  # noqa: E402
    ET_TIMEZONE,
    create_events_conformed_sql,
    default_d_minus_1_data_date_sql,
    delete_events_conformed_data_date_sql,
    insert_events_conformed_for_data_date_sql,
    required_events_conformed_columns,
)


class BtEventsConformedSqlTests(unittest.TestCase):
    def test_required_columns_match_contract(self) -> None:
        expected = (
            ("event_id", "STRING"),
            ("event_timestamp", "TIMESTAMP"),
            ("event_date_et", "DATE"),
            ("data_date", "DATE"),
            ("video_id", "STRING"),
            ("user_id", "STRING"),
            ("event_type", "STRING"),
            ("category", "STRING"),
            ("region", "STRING"),
        )
        self.assertEqual(required_events_conformed_columns(), expected)

    def test_create_sql_contains_contract_schema_and_layout(self) -> None:
        sql = create_events_conformed_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.silver.events_conformed", sql)
        for column_name, data_type in required_events_conformed_columns():
            self.assertIn(f"{column_name} {data_type}", sql)
        self.assertIn("PARTITIONED BY (event_date_et, bucket(64, user_id))", sql)

    def test_default_data_date_sql_uses_et_d_minus_1(self) -> None:
        self.assertEqual(
            default_d_minus_1_data_date_sql(),
            f"date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)",
        )

    def test_delete_sql_targets_d_minus_1_slice(self) -> None:
        sql = delete_events_conformed_data_date_sql(target_table="lakehouse.silver.events_conformed_canary")
        self.assertIn("DELETE FROM lakehouse.silver.events_conformed_canary", sql)
        self.assertIn(
            f"WHERE data_date = date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)",
            sql,
        )

    def test_insert_sql_contains_filters_dedupe_and_fallbacks(self) -> None:
        sql = insert_events_conformed_for_data_date_sql(
            source_table="lakehouse.bronze.raw_events_canary",
            target_table="lakehouse.silver.events_conformed_canary",
        )
        self.assertIn("INSERT INTO lakehouse.silver.events_conformed_canary", sql)
        self.assertIn("FROM lakehouse.bronze.raw_events_canary", sql)
        self.assertIn("to_date(from_utc_timestamp(event_timestamp, 'America/New_York')) AS event_date_et", sql)
        self.assertIn("LOWER(TRIM(event_type)) IN ('impression', 'play_start', 'play_finish', 'like', 'share', 'skip')", sql)
        self.assertIn("WHERE event_date_et = date_sub(to_date(from_utc_timestamp(current_timestamp(), 'America/New_York')), 1)", sql)
        self.assertIn("PARTITION BY event_id", sql)
        self.assertIn("ORDER BY source_partition DESC NULLS LAST,", sql)
        self.assertIn("source_offset DESC NULLS LAST,", sql)
        self.assertIn("ingested_at DESC NULLS LAST", sql)
        self.assertIn("event_date_et AS data_date", sql)
        self.assertIn("COALESCE(NULLIF(TRIM(get_json_object(payload_json, '$.category')), ''), 'unknown') AS category", sql)
        self.assertIn("COALESCE(NULLIF(TRIM(get_json_object(payload_json, '$.region')), ''), 'unknown') AS region", sql)


if __name__ == "__main__":
    unittest.main()
