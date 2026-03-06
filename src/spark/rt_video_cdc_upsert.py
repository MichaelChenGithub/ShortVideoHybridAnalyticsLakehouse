"""MIC-37 Spark streaming job for CDC video upserts into dim_videos."""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StringType, StructField, StructType

try:
    from spark.rt_video_cdc_contract import JobSettings, load_job_settings
    from spark.rt_video_cdc_upsert_sql import (
        create_dim_videos_sql,
        manual_alter_statements,
        merge_dim_videos_sql,
        missing_required_columns,
    )
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from rt_video_cdc_contract import JobSettings, load_job_settings
    from rt_video_cdc_upsert_sql import (
        create_dim_videos_sql,
        manual_alter_statements,
        merge_dim_videos_sql,
        missing_required_columns,
    )


def create_spark_session(settings: JobSettings) -> SparkSession:
    return SparkSession.builder.appName(settings.app_name).getOrCreate()


def _table_columns(spark: SparkSession, table_name: str) -> Iterable[str]:
    return [field.name for field in spark.table(table_name).schema.fields]


def init_dim_videos_table(spark: SparkSession, settings: JobSettings) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.dims")
    spark.sql(create_dim_videos_sql(settings.dim_videos_table))

    existing_columns = list(_table_columns(spark, settings.dim_videos_table))
    missing_columns = missing_required_columns(existing_columns)
    if not missing_columns:
        return

    manual_sql = manual_alter_statements(existing_columns, settings.dim_videos_table)
    missing_names = ", ".join(name for name, _ in missing_columns)
    details = "\n".join(f"  - {statement}" for statement in manual_sql)
    raise RuntimeError(
        "dim_videos schema is missing required columns: "
        f"{missing_names}.\n"
        "Run these manual migration statements, then restart the job:\n"
        f"{details}"
    )


def _cdc_schema() -> StructType:
    return StructType(
        [
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("schema_version", StringType(), True),
            StructField(
                "after",
                StructType(
                    [
                        StructField("video_id", StringType(), True),
                        StructField("category", StringType(), True),
                        StructField("region", StringType(), True),
                        StructField("upload_time", StringType(), True),
                        StructField("status", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )


def read_video_cdc_stream(spark: SparkSession, settings: JobSettings) -> DataFrame:
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("kafka.group.id", settings.consumer_group)
        .load()
    )

    parsed = (
        raw_stream.select(
            col("topic").alias("source_topic"),
            col("partition").alias("source_partition"),
            col("offset").alias("source_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("raw_value"),
        )
        .withColumn("cdc", from_json(col("raw_value"), _cdc_schema()))
        .select(
            "source_topic",
            "source_partition",
            "source_offset",
            "kafka_timestamp",
            "raw_value",
            col("cdc.op").alias("op"),
            col("cdc.ts_ms").alias("ts_ms"),
            col("cdc.schema_version").alias("schema_version"),
            col("cdc.after.video_id").alias("video_id"),
            col("cdc.after.category").alias("category"),
            col("cdc.after.region").alias("region"),
            col("cdc.after.upload_time").alias("upload_time"),
            col("cdc.after.status").alias("status"),
        )
    )

    return parsed


def process_videos_batch(df: DataFrame, batch_id: int, table_name: str) -> None:
    scoped = df.filter(
        col("op").isin("c", "u") & col("video_id").isNotNull() & col("ts_ms").isNotNull()
    ).select(
        "op",
        "ts_ms",
        "source_offset",
        "video_id",
        "category",
        "region",
        "upload_time",
        "status",
    )

    if scoped.limit(1).count() == 0:
        print(f"Batch {batch_id}: no valid c/u records.")
        return

    scoped.createOrReplaceTempView("video_updates")
    df.sparkSession.sql(merge_dim_videos_sql("video_updates", table_name))
    print(f"Batch {batch_id}: merged CDC updates into dim_videos.")


def main() -> None:
    settings = load_job_settings()
    spark = create_spark_session(settings)
    spark.sparkContext.setLogLevel("WARN")

    init_dim_videos_table(spark, settings)
    cdc_stream = read_video_cdc_stream(spark, settings)

    query = (
        cdc_stream.writeStream.foreachBatch(
            lambda df, batch_id: process_videos_batch(df, batch_id, settings.dim_videos_table)
        )
        .option("checkpointLocation", settings.checkpoint_dim_videos)
        .trigger(processingTime=settings.trigger_interval)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
