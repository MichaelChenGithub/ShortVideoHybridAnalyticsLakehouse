from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from spark.bt_dim_users_scd2 import (  # noqa: E402
    ENV_APP_NAME,
    ENV_SOURCE_TABLE,
    ENV_TARGET_TABLE,
    JobSettings,
    RAW_CDC_TABLE,
    load_job_settings,
    main,
    run_batch,
)
from spark.bt_dim_users_scd2_sql import (  # noqa: E402
    DIM_USERS_SCD2_TABLE,
    overwrite_dim_users_scd2_from_raw_sql,
)


class _FakeLoad:
    def __init__(self, count_result: int):
        self._count_result = count_result

    def count(self) -> int:
        return self._count_result


class _FakeRead:
    def __init__(self, counts: list[int]):
        self._counts = counts

    def format(self, _fmt: str) -> "_FakeRead":
        return self

    def load(self, _table: str) -> _FakeLoad:
        return _FakeLoad(self._counts.pop(0))


class _FakeSpark:
    def __init__(self, counts: list[int]):
        self.read = _FakeRead(counts)
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


class BtDimUsersScd2TransformTests(unittest.TestCase):
    def test_load_job_settings_defaults(self) -> None:
        settings = load_job_settings({})
        self.assertEqual(settings.app_name, "spark_bt_dim_users_scd2")
        self.assertEqual(settings.source_table, RAW_CDC_TABLE)
        self.assertEqual(settings.target_table, DIM_USERS_SCD2_TABLE)

    def test_load_job_settings_overrides(self) -> None:
        settings = load_job_settings(
            {
                ENV_APP_NAME: "custom_bt_dim_users_scd2",
                ENV_SOURCE_TABLE: "lakehouse.bronze.raw_cdc_users_canary",
                ENV_TARGET_TABLE: "lakehouse.dims.dim_users_scd2_canary",
            }
        )
        self.assertEqual(settings.app_name, "custom_bt_dim_users_scd2")
        self.assertEqual(settings.source_table, "lakehouse.bronze.raw_cdc_users_canary")
        self.assertEqual(settings.target_table, "lakehouse.dims.dim_users_scd2_canary")

    @patch("spark.bt_dim_users_scd2.run_batch")
    @patch("spark.bt_dim_users_scd2.load_job_settings")
    def test_main_calls_run_batch_with_loaded_settings(self, mock_load_settings, mock_run_batch) -> None:
        sentinel_settings = object()
        mock_load_settings.return_value = sentinel_settings

        result = main()

        self.assertEqual(result, 0)
        mock_load_settings.assert_called_once_with()
        mock_run_batch.assert_called_once_with(sentinel_settings)

    def test_run_batch_issues_create_and_overwrite_sql(self) -> None:
        settings = JobSettings(
            app_name="spark_bt_dim_users_scd2_test",
            source_table="lakehouse.bronze.raw_cdc_users_canary",
            target_table="lakehouse.dims.dim_users_scd2_canary",
        )
        fake_spark = _FakeSpark(counts=[10, 4])
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

        self.assertIn("CREATE NAMESPACE IF NOT EXISTS lakehouse.dims", fake_spark.sql_calls[0])
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS lakehouse.dims.dim_users_scd2_canary",
            fake_spark.sql_calls[1],
        )
        self.assertEqual(
            fake_spark.sql_calls[2],
            overwrite_dim_users_scd2_from_raw_sql(
                source_table="lakehouse.bronze.raw_cdc_users_canary",
                target_table="lakehouse.dims.dim_users_scd2_canary",
            ),
        )


if __name__ == "__main__":
    unittest.main()
