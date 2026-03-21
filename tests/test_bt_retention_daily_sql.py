from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.bt_retention_daily_sql import (  # noqa: E402
    ET_TIMEZONE,
    create_batch_retention_daily_sql,
    default_d_minus_1_data_date_sql,
    delete_batch_retention_daily_data_date_sql,
    insert_batch_retention_daily_for_data_date_sql,
    required_batch_retention_daily_columns,
)


class BtRetentionDailySqlTests(unittest.TestCase):
    def test_required_columns_match_contract(self) -> None:
        expected = (
            ("cohort_date", "DATE"),
            ("day_n", "INT"),
            ("category", "STRING"),
            ("region", "STRING"),
            ("new_vs_returning_user", "STRING"),
            ("cohort_users", "BIGINT"),
            ("retained_users", "BIGINT"),
            ("retention_rate", "DOUBLE"),
            ("data_date", "DATE"),
            ("published_at", "TIMESTAMP"),
        )
        self.assertEqual(required_batch_retention_daily_columns(), expected)

    def test_create_sql_contains_contract_schema_and_layout(self) -> None:
        sql = create_batch_retention_daily_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.gold.batch_retention_daily", sql)
        for column_name, data_type in required_batch_retention_daily_columns():
            self.assertIn(f"{column_name} {data_type}", sql)
        self.assertIn("PARTITIONED BY (data_date)", sql)
        self.assertIn("'format-version'='2'", sql)

    def test_default_data_date_sql_uses_et_d_minus_1(self) -> None:
        self.assertEqual(
            default_d_minus_1_data_date_sql(),
            f"date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)",
        )

    def test_delete_sql_targets_data_date_slice(self) -> None:
        sql = delete_batch_retention_daily_data_date_sql(
            target_table="lakehouse.gold.batch_retention_daily_canary"
        )
        self.assertIn("DELETE FROM lakehouse.gold.batch_retention_daily_canary", sql)
        self.assertIn(
            f"WHERE data_date = date_sub(to_date(from_utc_timestamp(current_timestamp(), '{ET_TIMEZONE}')), 1)",
            sql,
        )

    def test_delete_sql_uses_custom_data_date_when_provided(self) -> None:
        sql = delete_batch_retention_daily_data_date_sql(
            target_table="lakehouse.gold.batch_retention_daily_canary",
            data_date_sql="DATE '2026-03-19'",
        )
        self.assertIn("WHERE data_date = DATE '2026-03-19'", sql)

    def test_insert_sql_contains_horizons_attribution_and_fallbacks(self) -> None:
        sql = insert_batch_retention_daily_for_data_date_sql(
            source_table="lakehouse.silver.events_conformed_canary",
            user_dim_table="lakehouse.dims.dim_users_scd2_canary",
            video_dim_table="lakehouse.dims.dim_videos_scd2_canary",
            target_table="lakehouse.gold.batch_retention_daily_canary",
        )
        self.assertIn("INSERT INTO lakehouse.gold.batch_retention_daily_canary", sql)
        self.assertIn("SELECT 1 AS day_n", sql)
        self.assertIn("SELECT 7 AS day_n", sql)
        self.assertIn("FROM lakehouse.silver.events_conformed_canary e", sql)
        self.assertIn("ON e.event_date_et = date_sub(", sql)
        self.assertIn("LEFT JOIN lakehouse.dims.dim_videos_scd2_canary v", sql)
        self.assertIn("c.event_timestamp >= v.valid_from", sql)
        self.assertIn("c.event_timestamp < v.valid_to", sql)
        self.assertIn("LEFT JOIN lakehouse.dims.dim_users_scd2_canary u", sql)
        self.assertIn("c.event_timestamp >= u.valid_from", sql)
        self.assertIn("c.event_timestamp < u.valid_to", sql)
        self.assertIn("COALESCE(NULLIF(TRIM(v.category), ''), NULLIF(TRIM(c.event_category), ''), 'unknown') AS category", sql)
        self.assertIn("COALESCE(NULLIF(TRIM(u.region), ''), NULLIF(TRIM(c.event_region), ''), 'unknown') AS region", sql)
        self.assertIn("COALESCE(NULLIF(LOWER(TRIM(u.new_vs_returning_user)), ''), 'unknown') AS new_vs_returning_user", sql)
        self.assertIn("COUNT(DISTINCT user_id) AS cohort_users", sql)
        self.assertIn("COUNT(DISTINCT c.user_id) AS retained_users", sql)
        self.assertIn("WHERE event_date_et = date_sub(to_date(from_utc_timestamp(current_timestamp(), 'America/New_York')), 1)", sql)
        self.assertIn("CASE", sql)
        self.assertIn("ELSE NULL", sql)
        self.assertIn("current_timestamp() AS published_at", sql)

    def test_insert_sql_uses_custom_data_date_and_exact_day_retention_join_keys(self) -> None:
        sql = insert_batch_retention_daily_for_data_date_sql(
            source_table="lakehouse.silver.events_conformed_canary",
            user_dim_table="lakehouse.dims.dim_users_scd2_canary",
            video_dim_table="lakehouse.dims.dim_videos_scd2_canary",
            target_table="lakehouse.gold.batch_retention_daily_canary",
            data_date_sql="DATE '2026-03-19'",
        )
        self.assertIn("date_sub(DATE '2026-03-19', h.day_n) AS cohort_date", sql)
        self.assertIn("ON e.event_date_et = date_sub(DATE '2026-03-19', h.day_n)", sql)
        self.assertIn("WHERE event_date_et = DATE '2026-03-19'", sql)
        self.assertNotIn("BETWEEN", sql)
        self.assertIn("FROM cohort_users c", sql)
        self.assertIn("LEFT JOIN retained_users r", sql)
        self.assertIn("c.cohort_date = r.cohort_date", sql)
        self.assertIn("c.day_n = r.day_n", sql)
        self.assertIn("c.category = r.category", sql)
        self.assertIn("c.region = r.region", sql)
        self.assertIn("c.new_vs_returning_user = r.new_vs_returning_user", sql)


if __name__ == "__main__":
    unittest.main()
