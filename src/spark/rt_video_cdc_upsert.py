"""Spark streaming job for CDC video upserts and invalid-record quarantine."""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, from_json, lit, when
from pyspark.sql.types import LongType, StringType, StructField, StructType

try:
    from spark.rt_video_cdc_contract import JobSettings, load_job_settings
    from spark.rt_video_cdc_upsert_sql import (
        create_dim_videos_sql,
        create_invalid_events_cdc_videos_sql,
        manual_alter_invalid_events_cdc_videos_statements,
        manual_alter_statements,
        merge_dim_videos_sql,
        missing_invalid_events_cdc_videos_columns,
        missing_required_columns,
    )
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from rt_video_cdc_contract import JobSettings, load_job_settings
    from rt_video_cdc_upsert_sql import (
        create_dim_videos_sql,
        create_invalid_events_cdc_videos_sql,
        manual_alter_invalid_events_cdc_videos_statements,
        manual_alter_statements,
        merge_dim_videos_sql,
        missing_invalid_events_cdc_videos_columns,
        missing_required_columns,
    )


def create_spark_session(settings: JobSettings) -> SparkSession:
    return SparkSession.builder.appName(settings.app_name).getOrCreate()


def _table_columns(spark: SparkSession, table_name: str) -> Iterable[str]:
    return [field.name for field in spark.table(table_name).schema.fields]


def _raise_missing_columns_error(
    table_name: str,
    missing_columns: list[tuple[str, str]],
    statements: list[str],
) -> None:
    missing_names = ", ".join(name for name, _ in missing_columns)
    details = "\n".join(f"  - {statement}" for statement in statements)
    raise RuntimeError(
        f"{table_name} is missing required columns: {missing_names}.\n"
        "Run these manual migration statements, then restart the job:\n"
        f"{details}"
    )


def init_output_tables(spark: SparkSession, settings: JobSettings) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.dims")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql(create_dim_videos_sql(settings.dim_videos_table))
    spark.sql(create_invalid_events_cdc_videos_sql(settings.invalid_cdc_table))

    dim_columns = list(_table_columns(spark, settings.dim_videos_table))
    dim_missing = missing_required_columns(dim_columns)
    if dim_missing:
        _raise_missing_columns_error(
            settings.dim_videos_table,
            dim_missing,
            manual_alter_statements(dim_columns, settings.dim_videos_table),
        )

    invalid_columns = list(_table_columns(spark, settings.invalid_cdc_table))
    invalid_missing = missing_invalid_events_cdc_videos_columns(invalid_columns)
    if invalid_missing:
        _raise_missing_columns_error(
            settings.invalid_cdc_table,
            invalid_missing,
            manual_alter_invalid_events_cdc_videos_statements(
                invalid_columns,
                settings.invalid_cdc_table,
            ),
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
        .option("groupIdPrefix", settings.consumer_group)
        .load()
    )

    parsed = (
        raw_stream.select(
            col("topic").alias("source_topic"),
            col("partition").cast("int").alias("source_partition"),
            col("offset").cast("long").alias("source_offset"),
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
            "cdc",
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


def split_valid_and_invalid_rows(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid_condition = (
        col("cdc").isNotNull()
        & col("op").isin("c", "u")
        & col("ts_ms").isNotNull()
        & col("schema_version").isNotNull()
        & col("video_id").isNotNull()
    )

    error_code = (
        when(col("cdc").isNull(), lit("CDC_PARSE_ERROR"))
        .when(col("op").isNull(), lit("CDC_MISSING_OP"))
        .when(~col("op").isin("c", "u"), lit("CDC_UNSUPPORTED_OP"))
        .when(col("ts_ms").isNull(), lit("CDC_MISSING_TS_MS"))
        .when(col("schema_version").isNull(), lit("CDC_MISSING_SCHEMA_VERSION"))
        .when(col("video_id").isNull(), lit("CDC_MISSING_AFTER_VIDEO_ID"))
        .otherwise(lit("CDC_CONTRACT_VIOLATION"))
    )

    error_reason = (
        when(col("cdc").isNull(), lit("raw payload failed CDC JSON parsing"))
        .when(col("op").isNull(), lit("required field op is missing"))
        .when(
            ~col("op").isin("c", "u"),
            lit("op must be one of c/u for M1 CDC upsert scope"),
        )
        .when(col("ts_ms").isNull(), lit("required field ts_ms is missing"))
        .when(col("schema_version").isNull(), lit("required field schema_version is missing"))
        .when(col("video_id").isNull(), lit("required field after.video_id is missing for c/u"))
        .otherwise(lit("CDC record failed contract validation"))
    )

    valid_rows = df.filter(valid_condition).select(
        "op",
        "ts_ms",
        "source_offset",
        "video_id",
        "category",
        "region",
        "upload_time",
        "status",
    )

    invalid_rows = df.filter(~valid_condition).select(
        concat_ws(
            ":",
            col("source_topic"),
            col("source_partition").cast("string"),
            col("source_offset").cast("string"),
        ).alias("invalid_event_id"),
        "raw_value",
        "source_topic",
        "source_partition",
        "source_offset",
        when(col("schema_version").isNull(), lit("__missing__"))
        .otherwise(col("schema_version"))
        .alias("schema_version"),
        error_code.alias("error_code"),
        error_reason.alias("error_reason"),
        current_timestamp().alias("ingested_at"),
    )

    return valid_rows, invalid_rows


def align_to_table_columns(df: DataFrame, table_columns: Iterable[str]) -> DataFrame:
    projected = []
    input_columns = set(df.columns)
    for column_name in table_columns:
        if column_name in input_columns:
            projected.append(col(column_name))
        else:
            projected.append(lit(None).alias(column_name))
    return df.select(*projected)


def process_videos_batch(df: DataFrame, batch_id: int, table_name: str) -> None:
    if df.limit(1).count() == 0:
        print(f"Batch {batch_id}: no valid c/u records.")
        return

    df.createOrReplaceTempView("video_updates")
    df.sparkSession.sql(merge_dim_videos_sql("video_updates", table_name))
    print(f"Batch {batch_id}: merged CDC updates into dim_videos.")


def process_invalid_batch(df: DataFrame, batch_id: int, table_name: str) -> None:
    count_rows = df.count()
    if count_rows == 0:
        print(f"Batch {batch_id}: no CDC invalid records.")
        return

    print(f"Batch {batch_id}: writing {count_rows} CDC invalid records to {table_name}.")
    df.write.format("iceberg").mode("append").save(table_name)


def main() -> None:
    settings = load_job_settings()
    spark = create_spark_session(settings)
    spark.sparkContext.setLogLevel("WARN")

    init_output_tables(spark, settings)
    cdc_stream = read_video_cdc_stream(spark, settings)
    valid_rows, invalid_rows = split_valid_and_invalid_rows(cdc_stream)

    invalid_table_columns = list(_table_columns(spark, settings.invalid_cdc_table))
    invalid_rows = align_to_table_columns(invalid_rows, invalid_table_columns)

    query_dim = (
        valid_rows.writeStream.foreachBatch(
            lambda df, batch_id: process_videos_batch(df, batch_id, settings.dim_videos_table)
        )
        .option("checkpointLocation", settings.checkpoint_dim_videos)
        .trigger(processingTime=settings.trigger_interval)
        .start()
    )

    query_invalid = (
        invalid_rows.writeStream.foreachBatch(
            lambda df, batch_id: process_invalid_batch(df, batch_id, settings.invalid_cdc_table)
        )
        .option("checkpointLocation", settings.checkpoint_invalid_cdc)
        .trigger(processingTime=settings.trigger_interval)
        .start()
    )

    # Keep local references so both queries remain active until process exit.
    _ = (query_dim, query_invalid)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
