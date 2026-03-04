from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("IcebergDimsLoader") \
        .getOrCreate()

def init_tables(spark):
    print("Initializing Dimension Tables...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.dims")

    # 1. Users Dimension
    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.dims.dim_users (
        user_id STRING,
        register_country STRING,
        device_os STRING,
        is_creator BOOLEAN,
        ltv_segment STRING,
        join_at TIMESTAMP,
        last_updated_ts TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (bucket(16, user_id))
    TBLPROPERTIES (
        'write.merge.mode'='merge-on-read',
        'format-version'='2'
    )
    """)

    # 2. Videos Dimension
    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.dims.dim_videos (
        video_id STRING,
        creator_id STRING,
        category STRING,
        hashtags ARRAY<STRING>,
        duration_ms LONG,
        status STRING,
        upload_time TIMESTAMP,
        last_updated_ts TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (bucket(16, video_id))
    TBLPROPERTIES (
        'write.merge.mode'='merge-on-read',
        'format-version'='2'
    )
    """)

def process_users_batch(df, batch_id):
    print(f"--- Processing Users Batch ID: {batch_id} ---")
    count = df.count()
    if count == 0:
        print(f"Batch {batch_id}: No new data.")
        return

    print(f"Batch {batch_id}: Found {count} records. Starting Merge...")
    df.cache()
    try:
        df.createOrReplaceTempView("user_updates")
        
        # Deduplicate: Keep latest op per user_id in this batch
        spark = df.sparkSession
        spark.sql("""
        MERGE INTO lakehouse.dims.dim_users AS target
        USING (
            SELECT * FROM (
                SELECT 
                    after.user_id,
                    after.register_country,
                    after.device_os,
                    after.is_creator,
                    after.ltv_segment,
                    cast(after.join_at as timestamp) as join_at,
                    ts_ms,
                    ROW_NUMBER() OVER (PARTITION BY after.user_id ORDER BY ts_ms DESC) as rn
                FROM user_updates
                WHERE op IN ('c', 'u')
            ) WHERE rn = 1
        ) AS source
        ON target.user_id = source.user_id
        WHEN MATCHED THEN UPDATE SET
            target.ltv_segment = source.ltv_segment,
            target.is_creator = source.is_creator,
            target.last_updated_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            user_id, register_country, device_os, is_creator, ltv_segment, join_at, last_updated_ts
        ) VALUES (
            source.user_id, source.register_country, source.device_os, source.is_creator, source.ltv_segment, source.join_at, current_timestamp()
        )
        """)
        print(f"Batch {batch_id}: Merge completed successfully.")
    except Exception as e:
        print(f"Error in Users Batch {batch_id}: {str(e)}")
        raise e
    finally:
        df.unpersist()

def process_videos_batch(df, batch_id):
    print(f"--- Processing Videos Batch ID: {batch_id} ---")
    count = df.count()
    if count == 0:
        print(f"Batch {batch_id}: No new data.")
        return

    print(f"Batch {batch_id}: Found {count} records. Starting Merge...")
    df.cache()
    try:
        df.createOrReplaceTempView("video_updates")
        
        spark = df.sparkSession
        spark.sql("""
        MERGE INTO lakehouse.dims.dim_videos AS target
        USING (
            SELECT * FROM (
                SELECT 
                    after.video_id,
                    after.creator_id,
                    after.category,
                    after.hashtags,
                    after.duration_ms,
                    after.status,
                    cast(after.upload_time as timestamp) as upload_time,
                    ts_ms,
                    ROW_NUMBER() OVER (PARTITION BY after.video_id ORDER BY ts_ms DESC) as rn
                FROM video_updates
                WHERE op IN ('c', 'u')
            ) WHERE rn = 1
        ) AS source
        ON target.video_id = source.video_id
        WHEN MATCHED THEN UPDATE SET
            target.status = source.status,
            target.category = source.category,
            target.last_updated_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            video_id, creator_id, category, hashtags, duration_ms, status, upload_time, last_updated_ts
        ) VALUES (
            source.video_id, source.creator_id, source.category, source.hashtags, source.duration_ms, source.status, source.upload_time, current_timestamp()
        )
        """)
        print(f"Batch {batch_id}: Merge completed successfully.")
    except Exception as e:
        print(f"Error in Videos Batch {batch_id}: {str(e)}")
        raise e
    finally:
        df.unpersist()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    init_tables(spark)

    # Common CDC Schema
    cdc_schema = StructType([
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField("after", StringType()) # Parse JSON later or use Struct
    ])

    # --- Stream 1: Users ---
    user_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "cdc.users.profiles") \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), "op STRING, ts_ms LONG, after STRUCT<user_id STRING, register_country STRING, device_os STRING, is_creator BOOLEAN, ltv_segment STRING, join_at STRING>").alias("data")) \
        .select("data.*")

    query_users = user_stream.writeStream \
        .foreachBatch(process_users_batch) \
        .option("checkpointLocation", "s3a://checkpoints/dims_users_v1") \
        .trigger(processingTime="1 minute") \
        .start()

    # --- Stream 2: Videos ---
    video_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "cdc.content.videos") \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), "op STRING, ts_ms LONG, after STRUCT<video_id STRING, creator_id STRING, category STRING, hashtags ARRAY<STRING>, duration_ms LONG, status STRING, upload_time STRING>").alias("data")) \
        .select("data.*")

    query_videos = video_stream.writeStream \
        .foreachBatch(process_videos_batch) \
        .option("checkpointLocation", "s3a://checkpoints/dims_videos_v1") \
        .trigger(processingTime="1 minute") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()