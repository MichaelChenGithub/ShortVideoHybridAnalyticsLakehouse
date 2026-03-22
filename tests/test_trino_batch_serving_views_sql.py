from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SERVING_SQL_FILE = REPO_ROOT / "src" / "trino" / "bt_semantic_serving.sql"


class TrinoBatchServingViewsSqlTests(unittest.TestCase):
    def _read_sql(self) -> str:
        return SERVING_SQL_FILE.read_text(encoding="utf-8")

    def _extract_view_sql(self, view_name: str) -> str:
        sql = self._read_sql()
        match = re.search(
            rf"CREATE OR REPLACE VIEW lakehouse\.serving\.{re.escape(view_name)} AS(?P<body>.*?);",
            sql,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match)
        assert match is not None
        return match.group("body")

    def test_serving_sql_file_exists(self) -> None:
        self.assertTrue(SERVING_SQL_FILE.exists())

    def test_view_names_and_source_tables(self) -> None:
        sql = self._read_sql()
        self.assertIn("CREATE OR REPLACE VIEW lakehouse.serving.v_bt_retention_daily AS", sql)
        self.assertIn("FROM lakehouse.gold.batch_retention_daily", sql)
        self.assertIn("CREATE OR REPLACE VIEW lakehouse.serving.v_bt_engagement_daily AS", sql)
        self.assertIn("FROM lakehouse.gold.batch_engagement_daily;", sql)
        self.assertIn("CREATE OR REPLACE VIEW lakehouse.serving.v_bt_sessionization_daily AS", sql)
        self.assertIn("FROM lakehouse.gold.batch_sessionization_daily;", sql)

    def test_retention_view_restricts_supported_day_n_horizons(self) -> None:
        sql = self._read_sql()
        self.assertIn("WHERE day_n IN (1, 7);", sql)

    def test_retention_view_projects_contract_fields(self) -> None:
        sql = self._read_sql()
        required_fragments = (
            "cohort_date,",
            "day_n,",
            "category,",
            "region,",
            "new_vs_returning_user,",
            "cohort_users,",
            "retained_users,",
            "retention_rate,",
            "data_date,",
            "published_at",
        )
        for fragment in required_fragments:
            self.assertIn(fragment, sql)

    def test_engagement_view_projects_contract_fields(self) -> None:
        sql = self._read_sql()
        required_fragments = (
            "impressions,",
            "play_start,",
            "play_finish,",
            "likes,",
            "shares,",
            "skips,",
            "play_start_rate,",
            "completion_rate,",
            "interaction_rate,",
            "skip_rate,",
            "published_at",
        )
        for fragment in required_fragments:
            self.assertIn(fragment, sql)

    def test_sessionization_view_projects_contract_fields(self) -> None:
        sql = self._read_sql()
        required_fragments = (
            "sessions,",
            "sessions_per_user,",
            "avg_session_duration_sec,",
            "events_per_session,",
            "watch_time_per_session_ms,",
            "published_at",
        )
        for fragment in required_fragments:
            self.assertIn(fragment, sql)

    def test_serving_sql_does_not_recompute_batch_formulas(self) -> None:
        for view_name in (
            "v_bt_retention_daily",
            "v_bt_engagement_daily",
            "v_bt_sessionization_daily",
        ):
            view_sql = self._extract_view_sql(view_name)
            self.assertNotIn("GREATEST(", view_sql)
            self.assertNotIn("CAST(", view_sql)
            self.assertNotIn("current_timestamp()", view_sql)
            self.assertNotIn(" / ", view_sql)

    def test_retention_acceptance_query_is_present(self) -> None:
        sql = self._read_sql()
        self.assertIn("WITH expected_publish_date AS (", sql)
        self.assertIn("retention_target_rows AS (", sql)
        self.assertIn("retention_dupes AS (", sql)
        self.assertIn("WHERE r.data_date = e.expected_data_date", sql)
        self.assertIn("FROM retention_target_rows", sql)
        self.assertIn("GROUP BY 1, 2, 3, 4, 5", sql)
        self.assertIn("AS null_retention_rate", sql)
        self.assertIn("AS null_published_at", sql)
        self.assertIn("AS target_row_count", sql)

    def test_engagement_acceptance_query_is_present(self) -> None:
        sql = self._read_sql()
        self.assertIn("engagement_target_rows AS (", sql)
        self.assertIn("engagement_dupes AS (", sql)
        self.assertIn("WHERE e.data_date = p.expected_data_date", sql)
        self.assertIn("FROM engagement_target_rows", sql)
        self.assertIn("GROUP BY 1, 2, 3, 4", sql)
        self.assertIn("AS null_interaction_rate", sql)
        self.assertIn("AS null_skip_rate", sql)
        self.assertIn("CROSS JOIN engagement_target_summary", sql)

    def test_sessionization_acceptance_query_is_present(self) -> None:
        sql = self._read_sql()
        self.assertIn("sessionization_target_rows AS (", sql)
        self.assertIn("sessionization_dupes AS (", sql)
        self.assertIn("WHERE s.data_date = e.expected_data_date", sql)
        self.assertIn("FROM sessionization_target_rows", sql)
        self.assertIn("GROUP BY 1, 2, 3, 4", sql)
        self.assertIn("AS null_events_per_session", sql)
        self.assertIn("AS null_watch_time_per_session_ms", sql)
        self.assertIn("CROSS JOIN sessionization_target_summary", sql)

    def test_duplicate_key_counts_come_from_dedicated_duplicate_ctes(self) -> None:
        normalized_sql = re.sub(r"\s+", " ", self._read_sql())
        self.assertRegex(
            normalized_sql,
            r"FROM retention_target_rows CROSS JOIN \(SELECT COUNT\(\*\) AS duplicate_keys FROM retention_dupes\)",
        )
        self.assertRegex(
            normalized_sql,
            r"FROM engagement_target_rows CROSS JOIN \(SELECT COUNT\(\*\) AS duplicate_keys FROM engagement_dupes\)",
        )
        self.assertRegex(
            normalized_sql,
            r"FROM sessionization_target_rows CROSS JOIN \(SELECT COUNT\(\*\) AS duplicate_keys FROM sessionization_dupes\)",
        )

    def test_acceptance_queries_assert_d_minus_1_target_slice(self) -> None:
        normalized_sql = re.sub(r"\s+", " ", self._read_sql())
        self.assertRegex(
            normalized_sql,
            r"date_add\('day', -1, CAST\(current_timestamp AT TIME ZONE 'America/New_York' AS date\)\) AS expected_data_date",
        )
        self.assertNotIn("from_utc_timestamp", normalized_sql)
        self.assertNotIn("to_date(", normalized_sql)
        self.assertNotIn("date_sub(", normalized_sql)
        self.assertNotIn("latest_data_date", normalized_sql)
        self.assertNotIn("latest_data_date_matches_d_minus_1", normalized_sql)

    def test_acceptance_queries_do_not_claim_manifest_gating(self) -> None:
        normalized_sql = re.sub(r"\s+", " ", self._read_sql())
        self.assertIn("Publish-manifest readiness gating is deferred", normalized_sql)
        self.assertNotIn("FROM lakehouse.gold.batch_publish_manifest", normalized_sql)
        self.assertNotIn("JOIN lakehouse.gold.batch_publish_manifest", normalized_sql)


if __name__ == "__main__":
    unittest.main()
