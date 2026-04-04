from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_FILE = REPO_ROOT / "dbt_project.yml"
DBT_PACKAGES_FILE = REPO_ROOT / "packages.yml"
SCHEMA_FILE = REPO_ROOT / "models" / "batch" / "semantic" / "schema.yml"
SANITY_TEST_FILE = REPO_ROOT / "tests_dbt" / "test_batch_semantic_sanity.sql"
DBT_PROJECT_NAME = "ShortVideoHybridAnalyticsLakehouse"
MODEL_FILES = {
    "bt_retention_daily": REPO_ROOT / "models" / "batch" / "semantic" / "bt_retention_daily.sql",
    "bt_engagement_daily": REPO_ROOT / "models" / "batch" / "semantic" / "bt_engagement_daily.sql",
    "bt_sessionization_daily": REPO_ROOT / "models" / "batch" / "semantic" / "bt_sessionization_daily.sql",
}


class DbtBatchSemanticQualityProjectTests(unittest.TestCase):
    def test_dbt_project_exists_and_declares_paths(self) -> None:
        self.assertTrue(DBT_PROJECT_FILE.exists())
        contents = DBT_PROJECT_FILE.read_text(encoding="utf-8")
        self.assertIn(f'name: {DBT_PROJECT_NAME}', contents)
        self.assertIn(f'profile: {DBT_PROJECT_NAME}', contents)
        self.assertIn(f'  {DBT_PROJECT_NAME}:', contents)
        self.assertNotIn('realtime_transactional_data_lakehouse', contents)
        self.assertIn('model-paths:', contents)
        self.assertIn('- models', contents)
        self.assertIn('test-paths:', contents)
        self.assertIn('- tests_dbt', contents)
        self.assertIn('+schema: semantic_quality', contents)
        self.assertIn('batch_semantic_quality', contents)

    def test_dbt_packages_include_dbt_utils_for_multi_column_uniqueness(self) -> None:
        self.assertTrue(DBT_PACKAGES_FILE.exists())
        contents = DBT_PACKAGES_FILE.read_text(encoding="utf-8")
        self.assertIn('package: dbt-labs/dbt_utils', contents)

    def test_all_batch_semantic_model_files_exist_and_are_passthroughs(self) -> None:
        for model_name, path in MODEL_FILES.items():
            with self.subTest(model_name=model_name):
                self.assertTrue(path.exists())
                contents = path.read_text(encoding="utf-8")
                self.assertIn("select *", contents.lower())
                self.assertIn("{{ wap_source('gold',", contents)
                self.assertNotIn("lakehouse.serving", contents)

    def test_schema_yaml_declares_expected_models_and_sources(self) -> None:
        self.assertTrue(SCHEMA_FILE.exists())
        contents = SCHEMA_FILE.read_text(encoding="utf-8")
        self.assertIn('name: gold', contents)
        self.assertIn('name: batch_retention_daily', contents)
        self.assertIn('name: batch_engagement_daily', contents)
        self.assertIn('name: batch_sessionization_daily', contents)
        self.assertIn('- name: bt_retention_daily', contents)
        self.assertIn('- name: bt_engagement_daily', contents)
        self.assertIn('- name: bt_sessionization_daily', contents)

    def test_schema_yaml_declares_uniqueness_and_governed_domain_coverage(self) -> None:
        contents = SCHEMA_FILE.read_text(encoding="utf-8")
        self.assertEqual(contents.count('dbt_utils.unique_combination_of_columns'), 3)
        self.assertIn('values: [1, 7]', contents)
        self.assertEqual(contents.count("values: ['new', 'returning', 'unknown']"), 3)

    def test_schema_yaml_declares_required_not_null_coverage(self) -> None:
        contents = SCHEMA_FILE.read_text(encoding="utf-8")
        required_fragments = (
            'name: cohort_date',
            'name: cohort_users',
            'name: retained_users',
            'name: impressions',
            'name: play_start_rate',
            'name: completion_rate',
            'name: interaction_rate',
            'name: skip_rate',
            'name: sessions',
            'name: sessions_per_user',
            'name: avg_session_duration_sec',
            'name: events_per_session',
            'name: watch_time_per_session_ms',
            'name: published_at',
            '- not_null',
        )
        for fragment in required_fragments:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, contents)

    def test_sanity_test_exists_and_covers_all_three_batch_domains(self) -> None:
        self.assertTrue(SANITY_TEST_FILE.exists())
        contents = SANITY_TEST_FILE.read_text(encoding="utf-8")
        self.assertIn("{{ ref('bt_retention_daily') }}", contents)
        self.assertIn("{{ ref('bt_engagement_daily') }}", contents)
        self.assertIn("{{ ref('bt_sessionization_daily') }}", contents)
        self.assertIn('retention_rate_formula_mismatch', contents)
        self.assertIn('play_start_rate_formula_mismatch', contents)
        self.assertIn('interaction_rate_formula_mismatch', contents)
        self.assertIn('negative_metric_field', contents)

    def test_dbt_scaffold_does_not_take_over_serving_or_manifest_logic(self) -> None:
        combined = "\n".join(
            path.read_text(encoding="utf-8")
            for path in [DBT_PROJECT_FILE, DBT_PACKAGES_FILE, SCHEMA_FILE, SANITY_TEST_FILE, *MODEL_FILES.values()]
        )
        self.assertNotIn("lakehouse.serving", combined)
        self.assertNotIn("batch_publish_manifest", combined)
        self.assertNotIn("publish-ready", combined)


if __name__ == "__main__":
    unittest.main()
