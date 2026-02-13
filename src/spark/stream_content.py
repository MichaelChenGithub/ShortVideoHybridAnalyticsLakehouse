from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("IcebergContentStream") \
        .getOrCreate()

def init_tables(spark):
    print("Initializing Content Tables...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    # 1. Bronze: Raw Events (Partitioned by Time)
    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.raw_events (
        event_id STRING,
        event_timestamp TIMESTAMP,
        video_id STRING,
        user_id STRING,
        event_type STRING,
        payload STRING,
        ingested_at TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (hours(event_timestamp))
    """)

    # 2. Gold: Video Metrics Log (Append-Only Tumbling Window)
    # Aggregates metrics per video per minute.
    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.video_stats_1min (
        video_id STRING,
        window_start TIMESTAMP,
        impressions LONG,
        likes LONG,
        shares LONG,
        play_start LONG,
        play_finish LONG
    ) USING iceberg
    PARTITIONED BY (days(window_start), bucket(16, video_id))
    """)

def process_bronze_batch(df, batch_id):
    count = df.count()
    if df.count() == 0:
        return
    print(f"--- Processing Bronze Batch ID: {batch_id} - {count} records---")

    # Sort by partition key to ensure efficient writing
    df.sort("event_timestamp") \
        .write \
        .format("iceberg") \
        .mode("append") \
        .save("lakehouse.bronze.raw_events")

def process_gold_batch(df, batch_id):
    count = df.count()
    if count == 0:
        return
    print(f"--- Processing Gold Batch ID: {batch_id} - {count} records---")

    # Sort by partition keys
    df.sort("window_start", "video_id") \
        .write \
        .format("iceberg") \
        .mode("append") \
        .save("lakehouse.gold.video_stats_1min")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    init_tables(spark)

    # Schema for Header + Body pattern
    # Payload is kept as Struct for easy access, converted to JSON string for Bronze
    event_schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_timestamp", TimestampType()),
        StructField("video_id", StringType()),
        StructField("user_id", StringType()),
        StructField("event_type", StringType()),
        StructField("payload", StructType([
            StructField("watch_time_ms", LongType()),
            StructField("device_os", StringType()),
            StructField("app_version", StringType()),
            StructField("network_type", StringType())
        ]))
    ])

    print("Starting Content Stream Processing...")
    
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "content_events") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream = raw_stream \
        .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
        .select("data.*")

    # --- Stream A: Bronze (Raw Append) ---
    query_bronze = parsed_stream \
        .select(
            col("event_id"),
            col("event_timestamp"),
            col("video_id"),
            col("user_id"),
            col("event_type"),
            to_json(col("payload")).alias("payload"),
            current_timestamp().alias("ingested_at")
        ) \
        .writeStream \
        .foreachBatch(process_bronze_batch) \
        .option("checkpointLocation", "s3a://checkpoints/content_bronze_v1") \
        .trigger(processingTime="10 seconds") \
        .start()

    # --- Stream B: Gold (Tumbling Window Aggregation) ---
    # Window: 1 minute, Watermark: 10 seconds
    query_gold = parsed_stream \
        .withWatermark("event_timestamp", "10 seconds") \
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("video_id")
        ) \
        .agg(
            count(when(col("event_type") == "impression", 1)).alias("impressions"),
            count(when(col("event_type") == "like", 1)).alias("likes"),
            count(when(col("event_type") == "share", 1)).alias("shares"),
            count(when(col("event_type") == "play_start", 1)).alias("play_start"),
            count(when(col("event_type") == "play_finish", 1)).alias("play_finish")
        ) \
        .select(
            col("video_id"),
            col("window.start").alias("window_start"),
            col("impressions"),
            col("likes"),
            col("shares"),
            col("play_start"),
            col("play_finish")
        ) \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(process_gold_batch) \
        .option("checkpointLocation", "s3a://checkpoints/content_gold_v1") \
        .trigger(processingTime="1 minute") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
