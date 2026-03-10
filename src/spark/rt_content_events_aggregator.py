"""MIC-39 Spark streaming job for content event contract enforcement and aggregation."""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    concat_ws,
    count,
    current_timestamp,
    from_json,
    lit,
    sum as spark_sum,
    to_json,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType

try:
    from spark.rt_content_events_aggregator_sql import (
        create_invalid_events_content_sql,
        create_raw_events_sql,
        create_rt_video_stats_sql,
        manual_alter_invalid_events_content_statements,
        manual_alter_raw_events_statements,
        manual_alter_rt_video_stats_statements,
        merge_rt_video_stats_sql,
        missing_invalid_events_content_columns,
        missing_raw_events_columns,
        missing_rt_video_stats_columns,
    )
    from spark.rt_content_events_contract import JobSettings, load_job_settings
    from spark.rt_content_events_validation import (
        ALLOWED_EVENT_TYPES,
        INVALID_EVENT_TIMESTAMP,
        INVALID_EVENT_TYPE,
        INVALID_PAYLOAD_JSON,
        MISSING_REQUIRED_FIELD,
        PARSE_ERROR,
        UNKNOWN_SCHEMA_VERSION,
        error_reason_for_code,
    )
except ModuleNotFoundError:  # pragma: no cover - direct spark-submit fallback
    from rt_content_events_aggregator_sql import (
        create_invalid_events_content_sql,
        create_raw_events_sql,
        create_rt_video_stats_sql,
        manual_alter_invalid_events_content_statements,
        manual_alter_raw_events_statements,
        manual_alter_rt_video_stats_statements,
        merge_rt_video_stats_sql,
        missing_invalid_events_content_columns,
        missing_raw_events_columns,
        missing_rt_video_stats_columns,
    )
    from rt_content_events_contract import JobSettings, load_job_settings
    from rt_content_events_validation import (
        ALLOWED_EVENT_TYPES,
        INVALID_EVENT_TIMESTAMP,
        INVALID_EVENT_TYPE,
        INVALID_PAYLOAD_JSON,
        MISSING_REQUIRED_FIELD,
        PARSE_ERROR,
        UNKNOWN_SCHEMA_VERSION,
        error_reason_for_code,
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
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    spark.sql(create_raw_events_sql(settings.raw_table))
    spark.sql(create_rt_video_stats_sql(settings.gold_table))
    spark.sql(create_invalid_events_content_sql(settings.invalid_table))

    raw_existing = list(_table_columns(spark, settings.raw_table))
    raw_missing = missing_raw_events_columns(raw_existing)
    if raw_missing:
        _raise_missing_columns_error(
            settings.raw_table,
            raw_missing,
            manual_alter_raw_events_statements(raw_existing, settings.raw_table),
        )

    gold_existing = list(_table_columns(spark, settings.gold_table))
    gold_missing = missing_rt_video_stats_columns(gold_existing)
    if gold_missing:
        _raise_missing_columns_error(
            settings.gold_table,
            gold_missing,
            manual_alter_rt_video_stats_statements(gold_existing, settings.gold_table),
        )

    invalid_existing = list(_table_columns(spark, settings.invalid_table))
    invalid_missing = missing_invalid_events_content_columns(invalid_existing)
    if invalid_missing:
        _raise_missing_columns_error(
            settings.invalid_table,
            invalid_missing,
            manual_alter_invalid_events_content_statements(invalid_existing, settings.invalid_table),
        )


def _content_event_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("video_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("schema_version", StringType(), True),
            StructField("payload_json", StringType(), True),
            StructField(
                "payload",
                StructType(
                    [
                        StructField("watch_time_ms", LongType(), True),
                        StructField("device_os", StringType(), True),
                        StructField("app_version", StringType(), True),
                        StructField("network_type", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )


def _payload_json_schema() -> StructType:
    return StructType([StructField("watch_time_ms", LongType(), True)])


def read_content_events_stream(spark: SparkSession, settings: JobSettings) -> DataFrame:
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.topic)
        .option("startingOffsets", settings.starting_offsets)
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
        .withColumn("event", from_json(col("raw_value"), _content_event_schema()))
        .select(
            "source_topic",
            "source_partition",
            "source_offset",
            "kafka_timestamp",
            "raw_value",
            col("event").isNull().alias("is_parse_error"),
            col("event.event_id").alias("event_id"),
            to_timestamp(col("event.event_timestamp")).alias("event_timestamp"),
            col("event.video_id").alias("video_id"),
            col("event.user_id").alias("user_id"),
            col("event.event_type").alias("event_type"),
            col("event.schema_version").alias("schema_version"),
            col("event.payload_json").alias("payload_json"),
            col("event.payload").alias("payload"),
        )
        .withColumn(
            "payload_json",
            when(col("payload_json").isNotNull(), col("payload_json")).otherwise(to_json(col("payload"))),
        )
        .withColumn("payload_json_parsed", from_json(col("payload_json"), _payload_json_schema()))
        .withColumn(
            "watch_time_ms",
            when(col("payload.watch_time_ms").isNotNull(), col("payload.watch_time_ms")).otherwise(
                col("payload_json_parsed").getField("watch_time_ms")
            ),
        )
        .withColumn("watch_time_ms", when(col("watch_time_ms").isNull(), lit(0)).otherwise(col("watch_time_ms")))
    )

    return parsed


def split_valid_and_invalid_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    missing_fields_csv = concat_ws(
        ",",
        when(col("event_id").isNull(), lit("event_id")),
        when(col("video_id").isNull(), lit("video_id")),
        when(col("user_id").isNull(), lit("user_id")),
        when(col("schema_version").isNull(), lit("schema_version")),
        when(col("payload_json").isNull(), lit("payload_json")),
    )

    is_missing_required_field = (
        col("event_id").isNull()
        | col("video_id").isNull()
        | col("user_id").isNull()
        | col("schema_version").isNull()
        | col("payload_json").isNull()
    )

    is_invalid_event_type = col("event_type").isNull() | (~col("event_type").isin(*ALLOWED_EVENT_TYPES))
    is_invalid_payload_json = col("payload_json_parsed").isNull()

    annotated = (
        df.withColumn("missing_fields_csv", missing_fields_csv)
        .withColumn(
            "error_code",
            when(col("is_parse_error"), lit(PARSE_ERROR))
            .when(is_missing_required_field, lit(MISSING_REQUIRED_FIELD))
            .when(col("event_timestamp").isNull(), lit(INVALID_EVENT_TIMESTAMP))
            .when(is_invalid_event_type, lit(INVALID_EVENT_TYPE))
            .when(is_invalid_payload_json, lit(INVALID_PAYLOAD_JSON))
            .otherwise(lit(None).cast("string")),
        )
        .withColumn(
            "error_reason",
            when(col("error_code") == lit(PARSE_ERROR), lit(error_reason_for_code(PARSE_ERROR)))
            .when(
                col("error_code") == lit(MISSING_REQUIRED_FIELD),
                concat(lit("missing required field(s): "), col("missing_fields_csv")),
            )
            .when(
                col("error_code") == lit(INVALID_EVENT_TIMESTAMP),
                lit(error_reason_for_code(INVALID_EVENT_TIMESTAMP)),
            )
            .when(
                col("error_code") == lit(INVALID_EVENT_TYPE),
                lit(error_reason_for_code(INVALID_EVENT_TYPE)),
            )
            .when(
                col("error_code") == lit(INVALID_PAYLOAD_JSON),
                lit(error_reason_for_code(INVALID_PAYLOAD_JSON)),
            )
            .otherwise(lit(None).cast("string")),
        )
    )

    valid_events = annotated.filter(col("error_code").isNull())
    invalid_events = annotated.filter(col("error_code").isNotNull())
    return valid_events, invalid_events


def process_bronze_batch(df: DataFrame, batch_id: int, table_name: str) -> None:
    count_rows = df.count()
    if count_rows == 0:
        print(f"Batch {batch_id}: no valid bronze rows.")
        return

    print(f"Batch {batch_id}: writing {count_rows} rows to {table_name}.")
    (
        df.sort("event_timestamp", "video_id")
        .write.format("iceberg")
        .mode("append")
        .save(table_name)
    )


def process_gold_batch(df: DataFrame, batch_id: int, table_name: str) -> None:
    count_rows = df.count()
    if count_rows == 0:
        print(f"Batch {batch_id}: no valid gold rows.")
        return

    print(f"Batch {batch_id}: merging {count_rows} rows into {table_name}.")
    df.createOrReplaceTempView("gold_updates")
    df.sparkSession.sql(merge_rt_video_stats_sql("gold_updates", table_name))


def process_invalid_batch(df: DataFrame, batch_id: int, table_name: str) -> None:
    count_rows = df.count()
    if count_rows == 0:
        print(f"Batch {batch_id}: no invalid rows.")
        return

    print(f"Batch {batch_id}: writing {count_rows} invalid rows to {table_name}.")
    (
        df.sort("source_topic", "source_partition", "source_offset")
        .write.format("iceberg")
        .mode("append")
        .save(table_name)
    )


def build_gold_aggregates(valid_events: DataFrame, watermark_delay: str) -> DataFrame:
    deduped = valid_events.withWatermark("event_timestamp", watermark_delay).dropDuplicates(["event_id"])

    return (
        deduped.groupBy(window(col("event_timestamp"), "1 minute"), col("video_id"))
        .agg(
            count(when(col("event_type") == "impression", 1)).alias("impressions"),
            count(when(col("event_type") == "play_start", 1)).alias("play_start"),
            count(when(col("event_type") == "play_finish", 1)).alias("play_finish"),
            count(when(col("event_type") == "like", 1)).alias("likes"),
            count(when(col("event_type") == "share", 1)).alias("shares"),
            count(when(col("event_type") == "skip", 1)).alias("skips"),
            spark_sum(col("watch_time_ms").cast("long")).alias("watch_time_sum_ms"),
        )
        .select(
            col("video_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("impressions").cast("long"),
            col("play_start").cast("long"),
            col("play_finish").cast("long"),
            col("likes").cast("long"),
            col("shares").cast("long"),
            col("skips").cast("long"),
            col("watch_time_sum_ms").cast("long"),
            current_timestamp().alias("processed_at"),
        )
    )


def align_to_table_columns(df: DataFrame, table_columns: Iterable[str]) -> DataFrame:
    projected = []
    input_columns = set(df.columns)
    for column_name in table_columns:
        if column_name in input_columns:
            projected.append(col(column_name))
        elif column_name == "payload" and "payload_json" in input_columns:
            projected.append(col("payload_json").alias("payload"))
        else:
            projected.append(lit(None).alias(column_name))
    return df.select(*projected)


def main() -> None:
    settings = load_job_settings()
    spark = create_spark_session(settings)
    spark.sparkContext.setLogLevel("WARN")

    print(
        "MIC-39 scope: content contract validation enabled with invalid_events_content quarantine sink; "
        "CDC enforcement remains out of scope."
    )

    init_output_tables(spark, settings)
    parsed_stream = read_content_events_stream(spark, settings)
    valid_events, invalid_events = split_valid_and_invalid_events(parsed_stream)

    bronze_rows = valid_events.select(
        "event_id",
        "event_timestamp",
        "video_id",
        "user_id",
        "event_type",
        "payload_json",
        "schema_version",
        "source_topic",
        "source_partition",
        "source_offset",
        current_timestamp().alias("ingested_at"),
    )

    invalid_rows = invalid_events.select(
        concat_ws(
            ":",
            coalesce(col("source_topic"), lit("unknown")),
            coalesce(col("source_partition").cast("string"), lit("-1")),
            coalesce(col("source_offset").cast("string"), lit("-1")),
        ).alias("invalid_event_id"),
        "raw_value",
        "source_topic",
        "source_partition",
        "source_offset",
        coalesce(col("schema_version"), lit(UNKNOWN_SCHEMA_VERSION)).alias("schema_version"),
        "error_code",
        "error_reason",
        current_timestamp().alias("ingested_at"),
    )

    raw_table_columns = list(_table_columns(spark, settings.raw_table))
    gold_table_columns = list(_table_columns(spark, settings.gold_table))
    invalid_table_columns = list(_table_columns(spark, settings.invalid_table))

    bronze_rows = align_to_table_columns(bronze_rows, raw_table_columns)
    gold_rows = align_to_table_columns(
        build_gold_aggregates(valid_events, settings.watermark_gold),
        gold_table_columns,
    )
    invalid_rows = align_to_table_columns(invalid_rows, invalid_table_columns)

    query_bronze = (
        bronze_rows.writeStream.foreachBatch(
            lambda df, batch_id: process_bronze_batch(df, batch_id, settings.raw_table)
        )
        .option("checkpointLocation", settings.checkpoint_raw)
        .trigger(processingTime=settings.trigger_raw)
        .start()
    )

    query_gold = (
        gold_rows.writeStream.foreachBatch(
            lambda df, batch_id: process_gold_batch(df, batch_id, settings.gold_table)
        )
        .outputMode("update")
        .option("checkpointLocation", settings.checkpoint_gold)
        .trigger(processingTime=settings.trigger_gold)
        .start()
    )

    query_invalid = (
        invalid_rows.writeStream.foreachBatch(
            lambda df, batch_id: process_invalid_batch(df, batch_id, settings.invalid_table)
        )
        .option("checkpointLocation", settings.checkpoint_invalid)
        .trigger(processingTime=settings.trigger_raw)
        .start()
    )

    # Keep query refs alive for debugging logs/readability.
    del query_bronze, query_gold, query_invalid
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
