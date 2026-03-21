from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.bt_engagement_daily_sql import (  # noqa: E402
    create_batch_engagement_daily_sql,
    delete_batch_engagement_daily_data_date_sql,
    insert_batch_engagement_daily_for_data_date_sql,
    required_batch_engagement_daily_columns,
)


class BtEngagementDailySqlTests(unittest.TestCase):
    def test_required_columns_match_contract(self) -> None:
        expected = (
            ("data_date", "DATE"),
            ("category", "STRING"),
            ("region", "STRING"),
            ("new_vs_returning_user", "STRING"),
            ("impressions", "BIGINT"),
            ("play_start", "BIGINT"),
            ("play_finish", "BIGINT"),
            ("likes", "BIGINT"),
            ("shares", "BIGINT"),
            ("skips", "BIGINT"),
            ("play_start_rate", "DOUBLE"),
            ("completion_rate", "DOUBLE"),
            ("interaction_rate", "DOUBLE"),
            ("skip_rate", "DOUBLE"),
            ("published_at", "TIMESTAMP"),
        )
        self.assertEqual(required_batch_engagement_daily_columns(), expected)

    def test_create_sql_contains_contract_schema_and_layout(self) -> None:
        sql = create_batch_engagement_daily_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.gold.batch_engagement_daily", sql)
        for column_name, data_type in required_batch_engagement_daily_columns():
            self.assertIn(f"{column_name} {data_type}", sql)
        self.assertIn("PARTITIONED BY (data_date)", sql)

    def test_delete_sql_targets_one_data_date_slice(self) -> None:
        sql = delete_batch_engagement_daily_data_date_sql(
            target_table="lakehouse.gold.batch_engagement_daily_canary",
            data_date_sql="DATE '2026-03-20'",
        )
        self.assertIn("DELETE FROM lakehouse.gold.batch_engagement_daily_canary", sql)
        self.assertIn("WHERE data_date = DATE '2026-03-20'", sql)

    def test_insert_sql_contains_as_of_joins_and_contract_formulas(self) -> None:
        sql = insert_batch_engagement_daily_for_data_date_sql(
            source_table="lakehouse.silver.events_conformed_canary",
            users_dim_table="lakehouse.dims.dim_users_scd2_canary",
            videos_dim_table="lakehouse.dims.dim_videos_scd2_canary",
            target_table="lakehouse.gold.batch_engagement_daily_canary",
            data_date_sql="DATE '2026-03-20'",
        )
        self.assertIn("INSERT INTO lakehouse.gold.batch_engagement_daily_canary", sql)
        self.assertIn("FROM lakehouse.silver.events_conformed_canary", sql)
        self.assertIn("LEFT JOIN lakehouse.dims.dim_users_scd2_canary u", sql)
        self.assertIn("LEFT JOIN lakehouse.dims.dim_videos_scd2_canary v", sql)
        self.assertIn("e.event_timestamp >= u.valid_from", sql)
        self.assertIn("e.event_timestamp < u.valid_to", sql)
        self.assertIn("e.event_timestamp >= v.valid_from", sql)
        self.assertIn("e.event_timestamp < v.valid_to", sql)
        self.assertIn("COALESCE(category, 'unknown') AS category", sql)
        self.assertIn("COALESCE(region, 'unknown') AS region", sql)
        self.assertIn("COALESCE(new_vs_returning_user, 'unknown') AS new_vs_returning_user", sql)
        self.assertIn("CAST(play_start AS DOUBLE) / CAST(GREATEST(impressions, 1) AS DOUBLE)", sql)
        self.assertIn("CAST(play_finish AS DOUBLE) / CAST(GREATEST(play_start, 1) AS DOUBLE)", sql)
        self.assertIn(
            "CAST((likes + shares) AS DOUBLE) / CAST(GREATEST(play_finish, 1) AS DOUBLE)",
            sql,
        )
        self.assertIn("CAST(skips AS DOUBLE) / CAST(GREATEST(play_start, 1) AS DOUBLE)", sql)
        self.assertIn("current_timestamp() AS published_at", sql)

    def test_insert_sql_uses_user_region_for_public_region_field(self) -> None:
        sql = insert_batch_engagement_daily_for_data_date_sql()
        self.assertIn("u.region", sql)
        self.assertNotIn("v.region", sql)


if __name__ == "__main__":
    unittest.main()
