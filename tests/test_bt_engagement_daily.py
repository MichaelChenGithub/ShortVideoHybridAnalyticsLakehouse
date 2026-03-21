from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.bt_engagement_daily import (  # noqa: E402
    ENV_APP_NAME,
    ENV_DATA_DATE,
    ENV_SOURCE_TABLE,
    ENV_TARGET_TABLE,
    ENV_USERS_DIM_TABLE,
    ENV_VIDEOS_DIM_TABLE,
    JobSettings,
    load_job_settings,
    main,
    run_batch,
)
from spark.bt_engagement_daily_sql import (  # noqa: E402
    BATCH_ENGAGEMENT_DAILY_TABLE,
    DIM_USERS_SCD2_TABLE,
    DIM_VIDEOS_SCD2_TABLE,
    EVENTS_CONFORMED_TABLE,
    create_batch_engagement_daily_sql,
    delete_batch_engagement_daily_data_date_sql,
    insert_batch_engagement_daily_for_data_date_sql,
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


class BtEngagementDailyTransformTests(unittest.TestCase):
    def test_load_job_settings_defaults(self) -> None:
        settings = load_job_settings({})
        self.assertEqual(settings.app_name, "spark_bt_engagement_daily")
        self.assertEqual(settings.source_table, EVENTS_CONFORMED_TABLE)
        self.assertEqual(settings.users_dim_table, DIM_USERS_SCD2_TABLE)
        self.assertEqual(settings.videos_dim_table, DIM_VIDEOS_SCD2_TABLE)
        self.assertEqual(settings.target_table, BATCH_ENGAGEMENT_DAILY_TABLE)
        self.assertIsNone(settings.data_date_sql)

    def test_load_job_settings_overrides(self) -> None:
        settings = load_job_settings(
            {
                ENV_APP_NAME: "custom_bt_engagement_daily",
                ENV_SOURCE_TABLE: "lakehouse.silver.events_conformed_canary",
                ENV_USERS_DIM_TABLE: "lakehouse.dims.dim_users_scd2_canary",
                ENV_VIDEOS_DIM_TABLE: "lakehouse.dims.dim_videos_scd2_canary",
                ENV_TARGET_TABLE: "lakehouse.gold.batch_engagement_daily_canary",
                ENV_DATA_DATE: "2026-03-20",
            }
        )
        self.assertEqual(settings.app_name, "custom_bt_engagement_daily")
        self.assertEqual(settings.source_table, "lakehouse.silver.events_conformed_canary")
        self.assertEqual(settings.users_dim_table, "lakehouse.dims.dim_users_scd2_canary")
        self.assertEqual(settings.videos_dim_table, "lakehouse.dims.dim_videos_scd2_canary")
        self.assertEqual(settings.target_table, "lakehouse.gold.batch_engagement_daily_canary")
        self.assertEqual(settings.data_date_sql, "DATE '2026-03-20'")

    @patch("spark.bt_engagement_daily.run_batch")
    @patch("spark.bt_engagement_daily.load_job_settings")
    def test_main_calls_run_batch_with_loaded_settings(self, mock_load_settings, mock_run_batch) -> None:
        sentinel_settings = object()
        mock_load_settings.return_value = sentinel_settings

        result = main()

        self.assertEqual(result, 0)
        mock_load_settings.assert_called_once_with()
        mock_run_batch.assert_called_once_with(sentinel_settings)

    def test_run_batch_issues_create_delete_and_insert_sql(self) -> None:
        settings = JobSettings(
            app_name="spark_bt_engagement_daily_test",
            source_table="lakehouse.silver.events_conformed_canary",
            users_dim_table="lakehouse.dims.dim_users_scd2_canary",
            videos_dim_table="lakehouse.dims.dim_videos_scd2_canary",
            target_table="lakehouse.gold.batch_engagement_daily_canary",
            data_date_sql="DATE '2026-03-20'",
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

        self.assertIn("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold", fake_spark.sql_calls[0])
        self.assertEqual(fake_spark.sql_calls[1], create_batch_engagement_daily_sql(settings.target_table))
        self.assertEqual(
            fake_spark.sql_calls[2],
            delete_batch_engagement_daily_data_date_sql(
                target_table=settings.target_table,
                data_date_sql=settings.data_date_sql,
            ),
        )
        self.assertEqual(
            fake_spark.sql_calls[3],
            insert_batch_engagement_daily_for_data_date_sql(
                source_table=settings.source_table,
                users_dim_table=settings.users_dim_table,
                videos_dim_table=settings.videos_dim_table,
                target_table=settings.target_table,
                data_date_sql=settings.data_date_sql,
            ),
        )
