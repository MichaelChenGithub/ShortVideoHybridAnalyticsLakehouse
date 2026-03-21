from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.bt_sessionization_daily_sql import (  # noqa: E402
    create_batch_sessionization_daily_sql,
    delete_batch_sessionization_daily_data_date_sql,
    insert_batch_sessionization_daily_for_data_date_sql,
    required_batch_sessionization_daily_columns,
)


class BtSessionizationDailySqlTests(unittest.TestCase):
    def test_required_columns_match_contract(self) -> None:
        expected = (
            ("data_date", "DATE"),
            ("category", "STRING"),
            ("region", "STRING"),
            ("new_vs_returning_user", "STRING"),
            ("sessions", "BIGINT"),
            ("sessions_per_user", "DOUBLE"),
            ("avg_session_duration_sec", "DOUBLE"),
            ("events_per_session", "DOUBLE"),
            ("watch_time_per_session_ms", "DOUBLE"),
            ("published_at", "TIMESTAMP"),
        )
        self.assertEqual(required_batch_sessionization_daily_columns(), expected)

    def test_create_sql_contains_contract_schema_and_layout(self) -> None:
        sql = create_batch_sessionization_daily_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.gold.batch_sessionization_daily", sql)
        for column_name, data_type in required_batch_sessionization_daily_columns():
            self.assertIn(f"{column_name} {data_type}", sql)
        self.assertIn("PARTITIONED BY (data_date)", sql)

    def test_delete_sql_targets_one_data_date_slice(self) -> None:
        sql = delete_batch_sessionization_daily_data_date_sql(
            target_table="lakehouse.gold.batch_sessionization_daily_canary",
            data_date_sql="DATE '2026-03-20'",
        )
        self.assertIn("DELETE FROM lakehouse.gold.batch_sessionization_daily_canary", sql)
        self.assertIn("WHERE data_date = DATE '2026-03-20'", sql)

    def test_insert_sql_contains_governed_formulas_and_fallbacks(self) -> None:
        sql = insert_batch_sessionization_daily_for_data_date_sql(
            source_table="lakehouse.silver.user_activity_sessions_30m_canary",
            target_table="lakehouse.gold.batch_sessionization_daily_canary",
            data_date_sql="DATE '2026-03-20'",
        )
        self.assertIn("INSERT INTO lakehouse.gold.batch_sessionization_daily_canary", sql)
        self.assertIn("FROM lakehouse.silver.user_activity_sessions_30m_canary", sql)
        self.assertIn("COALESCE(category, 'unknown') AS category", sql)
        self.assertIn("COALESCE(region, 'unknown') AS region", sql)
        self.assertIn("COALESCE(new_vs_returning_user, 'unknown') AS new_vs_returning_user", sql)
        self.assertIn("COUNT(session_id) AS sessions", sql)
        self.assertIn("COUNT(DISTINCT user_id) AS distinct_active_users", sql)
        self.assertIn("AVG(CAST(session_duration_sec AS DOUBLE)) AS avg_session_duration_sec", sql)
        self.assertIn("SUM(event_count) AS total_events", sql)
        self.assertIn("SUM(watch_time_sum_ms) AS total_watch_time_sum_ms", sql)
        self.assertIn(
            "CAST(sessions AS DOUBLE) / CAST(GREATEST(distinct_active_users, 1) AS DOUBLE)",
            sql,
        )
        self.assertIn("CAST(total_events AS DOUBLE) / CAST(GREATEST(sessions, 1) AS DOUBLE)", sql)
        self.assertIn(
            "CAST(total_watch_time_sum_ms AS DOUBLE) / CAST(GREATEST(sessions, 1) AS DOUBLE)",
            sql,
        )
        self.assertIn("current_timestamp() AS published_at", sql)


if __name__ == "__main__":
    unittest.main()
