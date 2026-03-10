from __future__ import annotations

import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.rt_rule_quantile_baselines_sql import (  # noqa: E402
    M1_EFFECTIVE_FROM,
    M1_EFFECTIVE_TO,
    M1_RULE_VERSION,
    create_rt_rule_quantile_baselines_sql,
    manual_alter_rt_rule_quantile_baselines_statements,
    missing_rt_rule_quantile_baselines_columns,
    publish_rt_rules_v1_seed_sql,
    required_rt_rule_quantile_baselines_columns,
)


class RtRuleQuantileBaselinesSqlTests(unittest.TestCase):
    def test_required_columns_match_contract(self) -> None:
        expected = (
            ("rule_version", "STRING"),
            ("effective_from", "DATE"),
            ("effective_to", "DATE"),
            ("metric_name", "STRING"),
            ("percentile", "INT"),
            ("cohort_category", "STRING"),
            ("cohort_region", "STRING"),
            ("threshold_value", "DOUBLE"),
            ("sample_size", "BIGINT"),
            ("is_fallback", "BOOLEAN"),
            ("computed_at", "TIMESTAMP"),
        )
        self.assertEqual(required_rt_rule_quantile_baselines_columns(), expected)

    def test_create_sql_contains_required_fields(self) -> None:
        sql = create_rt_rule_quantile_baselines_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS lakehouse.dims.rt_rule_quantile_baselines", sql)
        for column_name, data_type in required_rt_rule_quantile_baselines_columns():
            self.assertIn(f"{column_name} {data_type}", sql)

    def test_missing_columns_and_manual_alter_sql(self) -> None:
        existing = ["rule_version", "effective_from", "metric_name"]
        missing = missing_rt_rule_quantile_baselines_columns(existing)
        self.assertEqual(
            missing,
            [
                ("effective_to", "DATE"),
                ("percentile", "INT"),
                ("cohort_category", "STRING"),
                ("cohort_region", "STRING"),
                ("threshold_value", "DOUBLE"),
                ("sample_size", "BIGINT"),
                ("is_fallback", "BOOLEAN"),
                ("computed_at", "TIMESTAMP"),
            ],
        )
        statements = manual_alter_rt_rule_quantile_baselines_statements(existing)
        self.assertIn(
            "ALTER TABLE lakehouse.dims.rt_rule_quantile_baselines ADD COLUMNS (effective_to DATE);",
            statements,
        )
        self.assertIn(
            "ALTER TABLE lakehouse.dims.rt_rule_quantile_baselines ADD COLUMNS (is_fallback BOOLEAN);",
            statements,
        )

    def test_publish_sql_is_insert_only_and_guarded(self) -> None:
        sql = publish_rt_rules_v1_seed_sql()
        self.assertIn("INSERT INTO lakehouse.dims.rt_rule_quantile_baselines", sql)
        self.assertIn("WHERE NOT EXISTS", sql)
        self.assertIn(f"existing.rule_version = '{M1_RULE_VERSION}'", sql)
        self.assertIn(f"existing.effective_from = DATE '{M1_EFFECTIVE_FROM}'", sql)

        upper_sql = sql.upper()
        self.assertNotIn("UPDATE ", upper_sql)
        self.assertNotIn("DELETE ", upper_sql)
        self.assertNotIn("MERGE ", upper_sql)

    def test_publish_sql_contains_fixed_window_and_required_seed_rows(self) -> None:
        sql = publish_rt_rules_v1_seed_sql()
        self.assertIn(f"DATE '{M1_EFFECTIVE_FROM}'", sql)
        self.assertIn(f"DATE '{M1_EFFECTIVE_TO}'", sql)
        self.assertEqual(sql.count(f"('{M1_RULE_VERSION}'"), 2)

        self.assertIn("'velocity_30m', 90, NULL, NULL, 0.68, 1800, FALSE", sql)
        self.assertIn("'impressions_30m', 40, NULL, NULL, 160.0, 1800, FALSE", sql)
        self.assertNotIn("'comedy'", sql)
        self.assertNotIn("'gaming'", sql)


if __name__ == "__main__":
    unittest.main()
