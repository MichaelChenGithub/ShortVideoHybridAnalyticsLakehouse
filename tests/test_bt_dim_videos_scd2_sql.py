from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.bt_dim_videos_scd2_sql import (  # noqa: E402
    OPEN_END_VALID_TO,
    create_dim_videos_scd2_sql,
    overwrite_dim_videos_scd2_from_raw_sql,
    required_dim_videos_scd2_columns,
)


class BtDimVideosScd2SqlTests(unittest.TestCase):
    def test_required_columns_match_contract(self) -> None:
        expected = (
            ("video_sk", "STRING"),
            ("video_id", "STRING"),
            ("category", "STRING"),
            ("region", "STRING"),
            ("status", "STRING"),
            ("valid_from", "TIMESTAMP"),
            ("valid_to", "TIMESTAMP"),
            ("is_current", "BOOLEAN"),
        )
        self.assertEqual(required_dim_videos_scd2_columns(), expected)

    def test_create_sql_contains_contract_schema_and_layout(self) -> None:
        sql = create_dim_videos_scd2_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.dims.dim_videos_scd2", sql)
        for column_name, data_type in required_dim_videos_scd2_columns():
            self.assertIn(f"{column_name} {data_type}", sql)
        self.assertIn("PARTITIONED BY (days(valid_from), bucket(64, video_id))", sql)

    def test_open_end_constant_matches_contract(self) -> None:
        self.assertEqual(OPEN_END_VALID_TO, "9999-12-31 00:00:00")

    def test_overwrite_sql_contains_core_scd2_windows(self) -> None:
        sql = overwrite_dim_videos_scd2_from_raw_sql(
            source_table="lakehouse.bronze.raw_cdc_videos",
            target_table="lakehouse.dims.dim_videos_scd2_canary",
        )
        self.assertIn("INSERT OVERWRITE lakehouse.dims.dim_videos_scd2_canary", sql)
        self.assertIn("FROM lakehouse.bronze.raw_cdc_videos", sql)
        self.assertIn("ROW_NUMBER() OVER", sql)
        self.assertIn("LAG(category) OVER", sql)
        self.assertIn("LEAD(valid_from) OVER", sql)
        self.assertIn("SHA2(CONCAT_WS('|', video_id, CAST(ts_ms AS STRING)), 256)", sql)

    def test_overwrite_sql_enforces_valid_source_filter(self) -> None:
        sql = overwrite_dim_videos_scd2_from_raw_sql(
            source_table="lakehouse.bronze.raw_cdc_videos",
        )
        self.assertIn("WHERE op IN ('c', 'u')", sql)
        self.assertIn("AND video_id IS NOT NULL", sql)
        self.assertIn("AND ts_ms IS NOT NULL", sql)

    def test_overwrite_sql_enforces_tie_break_and_noop_collapse(self) -> None:
        sql = overwrite_dim_videos_scd2_from_raw_sql(
            source_table="lakehouse.bronze.raw_cdc_videos",
        )
        self.assertIn("PARTITION BY video_id, ts_ms", sql)
        self.assertIn("ORDER BY source_partition DESC NULLS LAST, source_offset DESC NULLS LAST", sql)
        self.assertIn("prev_category IS NULL", sql)
        self.assertIn("category <=> prev_category", sql)
        self.assertIn("region <=> prev_region", sql)
        self.assertIn("status <=> prev_status", sql)

    def test_overwrite_sql_enforces_interval_and_current_row_logic(self) -> None:
        sql = overwrite_dim_videos_scd2_from_raw_sql(
            source_table="lakehouse.bronze.raw_cdc_videos",
        )
        self.assertIn("CAST((ts_ms / 1000.0) AS TIMESTAMP) AS valid_from", sql)
        self.assertIn("LEAD(valid_from) OVER", sql)
        self.assertIn("COALESCE(next_valid_from, TO_TIMESTAMP('9999-12-31 00:00:00')) AS valid_to", sql)
        self.assertIn("CASE WHEN next_valid_from IS NULL THEN true ELSE false END AS is_current", sql)


if __name__ == "__main__":
    unittest.main()
