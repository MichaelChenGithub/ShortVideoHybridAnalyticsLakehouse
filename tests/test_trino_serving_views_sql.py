from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SERVING_SQL_FILE = REPO_ROOT / "src" / "trino" / "rt_video_metrics_serving.sql"


class TrinoServingViewsSqlTests(unittest.TestCase):
    def _read_sql(self) -> str:
        return SERVING_SQL_FILE.read_text(encoding="utf-8")

    def test_serving_sql_file_exists(self) -> None:
        self.assertTrue(SERVING_SQL_FILE.exists())

    def test_metrics_view_name_and_source_table(self) -> None:
        sql = self._read_sql()
        self.assertIn("CREATE OR REPLACE VIEW lakehouse.serving.v_rt_video_metrics_30m_1m AS", sql)
        self.assertIn("FROM lakehouse.gold.rt_video_stats_1min", sql)

    def test_required_fields_are_projected(self) -> None:
        sql = self._read_sql()
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
        sql = self._read_sql()
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
        sql = self._read_sql()
        self.assertIn("PARTITION BY video_id", sql)
        self.assertIn("ORDER BY window_start", sql)
        self.assertIn("RANGE BETWEEN INTERVAL '29' MINUTE PRECEDING AND CURRENT ROW", sql)
        self.assertIn("MAX(processed_at) OVER w AS processed_at_max", sql)

    def test_freshness_check_query_is_present(self) -> None:
        sql = self._read_sql()
        self.assertIn("MAX(metric_minute) AS latest_metric_minute", sql)
        self.assertIn("date_diff('second', MAX(metric_minute), current_timestamp) AS lag_seconds", sql)
        self.assertIn("FROM lakehouse.serving.v_rt_video_metrics_30m_1m;", sql)

    def test_decision_context_view_and_join_paths(self) -> None:
        sql = self._read_sql()
        self.assertIn("CREATE OR REPLACE VIEW lakehouse.serving.v_rt_video_decision_context_30m_1m AS", sql)
        self.assertIn("FROM lakehouse.serving.v_rt_video_metrics_30m_1m m", sql)
        self.assertIn("LEFT JOIN lakehouse.dims.dim_videos d", sql)
        self.assertIn("ON m.video_id = d.video_id", sql)
        self.assertIn("CROSS JOIN locked_thresholds t", sql)

    def test_decision_context_uses_locked_global_baselines(self) -> None:
        sql = self._read_sql()
        self.assertIn("FROM lakehouse.dims.rt_rule_quantile_baselines", sql)
        self.assertIn("WHERE rule_version = 'rt_rules_v1'", sql)
        self.assertIn("AND cohort_category IS NULL", sql)
        self.assertIn("AND cohort_region IS NULL", sql)
        self.assertIn("'rt_rules_v1' AS rule_version", sql)
        self.assertIn("WHEN metric_name = 'velocity_30m' AND percentile = 90 THEN threshold_value", sql)
        self.assertIn("WHEN metric_name = 'impressions_30m' AND percentile = 40 THEN threshold_value", sql)

    def test_decision_context_required_preview_fields_and_fallback(self) -> None:
        sql = self._read_sql()
        required_fragments = [
            "AS candidate_flag,",
            "AS quality_gate_pass,",
            "AS under_exposed_flag",
            "END AS decision_type_preview,",
            "p90_velocity_threshold,",
            "p40_impressions_threshold,",
            "WHEN category IS NULL OR region IS NULL OR status IS NULL OR upload_time IS NULL THEN 'NO_ACTION'",
        ]
        for fragment in required_fragments:
            self.assertIn(fragment, sql)

    def test_decision_context_preview_flag_formulas_match_contract(self) -> None:
        normalized_sql = re.sub(r"\s+", " ", self._read_sql())
        self.assertRegex(
            normalized_sql,
            r"COALESCE\(\s*\(\s*m\.velocity_30m >= t\.p90_velocity_threshold\s+AND m\.impressions_30m >= 100\s*\)\s*,\s*FALSE\s*\)\s+AS candidate_flag",
        )
        self.assertRegex(
            normalized_sql,
            r"\(\s*m\.completion_rate_30m >= 0\.55\s+AND m\.skip_rate_30m <= 0\.35\s+AND m\.play_start_30m >= 30\s*\)\s+AS quality_gate_pass",
        )
        self.assertRegex(
            normalized_sql,
            r"COALESCE\(\s*\(\s*m\.impressions_30m <= t\.p40_impressions_threshold\s*\)\s*,\s*FALSE\s*\)\s+AS under_exposed_flag",
        )

    def test_decision_type_preview_domain_is_exact(self) -> None:
        sql = self._read_sql()
        match = re.search(r"CASE(?P<body>.*?)END AS decision_type_preview", sql, flags=re.DOTALL)
        self.assertIsNotNone(match)
        assert match is not None
        literals = set(re.findall(r"'([A-Z_]+)'", match.group("body")))
        self.assertSetEqual(literals, {"BOOST", "REVIEW", "RESCUE", "NO_ACTION"})

    def test_decision_context_grain_acceptance_query_is_present(self) -> None:
        sql = self._read_sql()
        self.assertIn("WITH metrics_grain AS (", sql)
        self.assertIn("FROM lakehouse.serving.v_rt_video_metrics_30m_1m", sql)
        self.assertIn("FROM lakehouse.serving.v_rt_video_decision_context_30m_1m", sql)
        self.assertIn("GROUP BY video_id, metric_minute", sql)
        self.assertIn("HAVING COUNT(*) > 1", sql)
        self.assertIn("AS metrics_duplicate_keys", sql)
        self.assertIn("AS context_duplicate_keys", sql)
        self.assertIn("AS row_delta", sql)


if __name__ == "__main__":
    unittest.main()
