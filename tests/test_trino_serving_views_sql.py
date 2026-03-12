from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SERVING_SQL_FILE = REPO_ROOT / "src" / "trino" / "rt_video_metrics_serving.sql"


class TrinoServingViewsSqlTests(unittest.TestCase):
    def test_serving_sql_file_exists(self) -> None:
        self.assertTrue(SERVING_SQL_FILE.exists())

    def test_metrics_view_name_and_source_table(self) -> None:
        sql = SERVING_SQL_FILE.read_text(encoding="utf-8")
        self.assertIn("CREATE OR REPLACE VIEW lakehouse.serving.v_rt_video_metrics_30m_1m AS", sql)
        self.assertIn("FROM lakehouse.gold.rt_video_stats_1min", sql)

    def test_required_fields_are_projected(self) -> None:
        sql = SERVING_SQL_FILE.read_text(encoding="utf-8")
        required_projection_fragments = (
            "video_id,",
            "metric_minute,",
            "impressions_30m,",
            "play_start_30m,",
            "play_finish_30m,",
            "likes_30m,",
            "shares_30m,",
            "skips_30m,",
            "AS velocity_30m,",
            "AS completion_rate_30m,",
            "AS skip_rate_30m,",
            "processed_at_max",
        )
        for fragment in required_projection_fragments:
            self.assertIn(fragment, sql)

    def test_formulas_match_contract_with_denominator_guards(self) -> None:
        sql = SERVING_SQL_FILE.read_text(encoding="utf-8")
        self.assertIn(
            "CAST((likes_30m + 5 * shares_30m) AS DOUBLE) / CAST(GREATEST(impressions_30m, 100) AS DOUBLE) AS velocity_30m",
            sql,
        )
        self.assertIn(
            "CAST(play_finish_30m AS DOUBLE) / CAST(GREATEST(play_start_30m, 1) AS DOUBLE) AS completion_rate_30m",
            sql,
        )
        self.assertIn(
            "CAST(skips_30m AS DOUBLE) / CAST(GREATEST(play_start_30m, 1) AS DOUBLE) AS skip_rate_30m",
            sql,
        )

    def test_window_and_processed_at_max_definition(self) -> None:
        sql = SERVING_SQL_FILE.read_text(encoding="utf-8")
        self.assertIn("PARTITION BY video_id", sql)
        self.assertIn("ORDER BY window_start", sql)
        self.assertIn("RANGE BETWEEN INTERVAL '29' MINUTE PRECEDING AND CURRENT ROW", sql)
        self.assertIn("MAX(processed_at) OVER w AS processed_at_max", sql)

    def test_freshness_check_query_is_present(self) -> None:
        sql = SERVING_SQL_FILE.read_text(encoding="utf-8")
        self.assertIn("MAX(metric_minute) AS latest_metric_minute", sql)
        self.assertIn("date_diff('second', MAX(metric_minute), current_timestamp) AS lag_seconds", sql)
        self.assertIn("FROM lakehouse.serving.v_rt_video_metrics_30m_1m;", sql)


if __name__ == "__main__":
    unittest.main()
