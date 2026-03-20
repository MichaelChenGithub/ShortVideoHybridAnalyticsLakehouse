from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.bt_events_conformed import (  # noqa: E402
    ENV_APP_NAME,
    ENV_DATA_DATE,
    ENV_SOURCE_TABLE,
    ENV_TARGET_TABLE,
    JobSettings,
    load_job_settings,
    main,
    run_batch,
)
from spark.bt_events_conformed_sql import (  # noqa: E402
    EVENTS_CONFORMED_TABLE,
    RAW_EVENTS_TABLE,
    create_events_conformed_sql,
    delete_events_conformed_data_date_sql,
    insert_events_conformed_for_data_date_sql,
)


class _FakeSpark:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []

    def sql(self, query: str) -> None:
        self.sql_calls.append(query)


class _FakeBuilder:
    def __init__(self, spark: _FakeSpark):
        self._spark = spark
        self._app_name = ""

    def appName(self, app_name: str) -> "_FakeBuilder":
        self._app_name = app_name
        return self

    def getOrCreate(self) -> _FakeSpark:
        return self._spark


class BtEventsConformedTransformTests(unittest.TestCase):
    def test_load_job_settings_defaults(self) -> None:
        settings = load_job_settings({})
        self.assertEqual(settings.app_name, "spark_bt_events_conformed")
        self.assertEqual(settings.source_table, RAW_EVENTS_TABLE)
        self.assertEqual(settings.target_table, EVENTS_CONFORMED_TABLE)
        self.assertIsNone(settings.data_date_sql)

    def test_load_job_settings_overrides(self) -> None:
        settings = load_job_settings(
            {
                ENV_APP_NAME: "custom_bt_events_conformed",
                ENV_SOURCE_TABLE: "lakehouse.bronze.raw_events_canary",
                ENV_TARGET_TABLE: "lakehouse.silver.events_conformed_canary",
                ENV_DATA_DATE: "2026-03-04",
            }
        )
        self.assertEqual(settings.app_name, "custom_bt_events_conformed")
        self.assertEqual(settings.source_table, "lakehouse.bronze.raw_events_canary")
        self.assertEqual(settings.target_table, "lakehouse.silver.events_conformed_canary")
        self.assertEqual(settings.data_date_sql, "DATE '2026-03-04'")

    @patch("spark.bt_events_conformed.run_batch")
    @patch("spark.bt_events_conformed.load_job_settings")
    def test_main_calls_run_batch_with_loaded_settings(self, mock_load_settings, mock_run_batch) -> None:
        sentinel_settings = object()
        mock_load_settings.return_value = sentinel_settings

        result = main()

        self.assertEqual(result, 0)
        mock_load_settings.assert_called_once_with()
        mock_run_batch.assert_called_once_with(sentinel_settings)

    def test_run_batch_issues_create_delete_and_insert_sql(self) -> None:
        settings = JobSettings(
            app_name="spark_bt_events_conformed_test",
            source_table="lakehouse.bronze.raw_events_canary",
            target_table="lakehouse.silver.events_conformed_canary",
            data_date_sql="DATE '2026-03-04'",
        )
        fake_spark = _FakeSpark()
        fake_builder = _FakeBuilder(fake_spark)
        fake_spark_session = types.SimpleNamespace(builder=fake_builder)

        fake_pyspark_module = types.ModuleType("pyspark")
        fake_pyspark_sql_module = types.ModuleType("pyspark.sql")
        fake_pyspark_sql_module.SparkSession = fake_spark_session

        with patch.dict(
            sys.modules,
            {
                "pyspark": fake_pyspark_module,
                "pyspark.sql": fake_pyspark_sql_module,
            },
        ):
            run_batch(settings)

        self.assertIn("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver", fake_spark.sql_calls[0])
        self.assertEqual(fake_spark.sql_calls[1], create_events_conformed_sql(settings.target_table))
        self.assertEqual(
            fake_spark.sql_calls[2],
            delete_events_conformed_data_date_sql(
                target_table=settings.target_table,
                data_date_sql=settings.data_date_sql,
            ),
        )
        self.assertEqual(
            fake_spark.sql_calls[3],
            insert_events_conformed_for_data_date_sql(
                source_table=settings.source_table,
                target_table=settings.target_table,
                data_date_sql=settings.data_date_sql,
            ),
        )


if __name__ == "__main__":
    unittest.main()
