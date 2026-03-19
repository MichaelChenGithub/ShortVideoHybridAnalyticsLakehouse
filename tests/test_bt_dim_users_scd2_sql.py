from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.bt_dim_users_scd2_sql import (  # noqa: E402
    OPEN_END_VALID_TO,
    create_dim_users_scd2_sql,
    overwrite_dim_users_scd2_from_raw_sql,
    required_dim_users_scd2_columns,
)


class BtDimUsersScd2SqlTests(unittest.TestCase):
    def test_required_columns_match_contract(self) -> None:
        expected = (
            ("user_sk", "STRING"),
            ("user_id", "STRING"),
            ("region", "STRING"),
            ("new_vs_returning_user", "STRING"),
            ("valid_from", "TIMESTAMP"),
            ("valid_to", "TIMESTAMP"),
            ("is_current", "BOOLEAN"),
        )
        self.assertEqual(required_dim_users_scd2_columns(), expected)

    def test_create_sql_contains_contract_schema_and_layout(self) -> None:
        sql = create_dim_users_scd2_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.dims.dim_users_scd2", sql)
        for column_name, data_type in required_dim_users_scd2_columns():
            self.assertIn(f"{column_name} {data_type}", sql)
        self.assertIn("PARTITIONED BY (days(valid_from), bucket(64, user_id))", sql)

    def test_open_end_constant_matches_contract(self) -> None:
        self.assertEqual(OPEN_END_VALID_TO, "9999-12-31 00:00:00")

    def test_overwrite_sql_contains_core_scd2_windows(self) -> None:
        sql = overwrite_dim_users_scd2_from_raw_sql(
            source_table="lakehouse.bronze.raw_cdc_users",
            target_table="lakehouse.dims.dim_users_scd2_canary",
        )
        self.assertIn("INSERT OVERWRITE lakehouse.dims.dim_users_scd2_canary", sql)
        self.assertIn("FROM lakehouse.bronze.raw_cdc_users", sql)
        self.assertIn("ROW_NUMBER() OVER", sql)
        self.assertIn("LAG(region) OVER", sql)
        self.assertIn("LAG(new_vs_returning_user) OVER", sql)
        self.assertIn("LEAD(valid_from) OVER", sql)
        self.assertIn("SHA2(CONCAT_WS('|', user_id, CAST(ts_ms AS STRING)), 256)", sql)

    def test_overwrite_sql_enforces_valid_source_filter(self) -> None:
        sql = overwrite_dim_users_scd2_from_raw_sql(source_table="lakehouse.bronze.raw_cdc_users")
        self.assertIn("WHERE op IN ('c', 'u')", sql)
        self.assertIn("AND user_id IS NOT NULL", sql)
        self.assertIn("AND ts_ms IS NOT NULL", sql)
        self.assertIn("AND region IS NOT NULL", sql)
        self.assertIn("AND new_vs_returning_user IS NOT NULL", sql)
        self.assertIn(
            "AND LOWER(TRIM(new_vs_returning_user)) IN ('new', 'returning', 'unknown')",
            sql,
        )
        self.assertIn("LOWER(TRIM(new_vs_returning_user)) AS new_vs_returning_user", sql)

    def test_overwrite_sql_enforces_tie_break_and_noop_collapse(self) -> None:
        sql = overwrite_dim_users_scd2_from_raw_sql(source_table="lakehouse.bronze.raw_cdc_users")
        self.assertIn("PARTITION BY user_id, ts_ms", sql)
        self.assertIn("ORDER BY source_partition DESC NULLS LAST, source_offset DESC NULLS LAST", sql)
        self.assertIn("prev_region IS NULL", sql)
        self.assertIn("prev_new_vs_returning_user IS NULL", sql)
        self.assertIn("region <=> prev_region", sql)
        self.assertIn("new_vs_returning_user <=> prev_new_vs_returning_user", sql)

    def test_overwrite_sql_enforces_interval_and_current_row_logic(self) -> None:
        sql = overwrite_dim_users_scd2_from_raw_sql(source_table="lakehouse.bronze.raw_cdc_users")
        self.assertIn("CAST((ts_ms / 1000.0) AS TIMESTAMP) AS valid_from", sql)
        self.assertIn("LEAD(valid_from) OVER", sql)
        self.assertIn("COALESCE(next_valid_from, TO_TIMESTAMP('9999-12-31 00:00:00')) AS valid_to", sql)
        self.assertIn("CASE WHEN next_valid_from IS NULL THEN true ELSE false END AS is_current", sql)


if __name__ == "__main__":
    unittest.main()
