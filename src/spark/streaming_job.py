import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("IcebergLakehouseMedallion") \
        .getOrCreate()

def init_tables(spark):
    """
    Initialize Bronze (ODS) 和 Gold (State) tables
    """
    print("Initializing Iceberg Tables (Bronze & Gold)...")
    
    # 0. make sure namespaces exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    # 1. Create Bronze Layer: orders_events (ODS / Raw Log)
    # Append-Only, keep Nested Array, full history
    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders_events (
        event_id STRING,
        event_type STRING,
        event_timestamp TIMESTAMP,
        order_id STRING,
        user_id STRING,
        total_amount DOUBLE,
        currency STRING,
        payment_method STRING,
        items ARRAY<STRUCT<sku: STRING, quantity: INT, unit_price: DOUBLE, category: STRING>>,
        current_status STRING,
        kafka_ts TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (hours(event_timestamp))
    """)

    # 2. Create Gold Layer: orders_current (Business State)
    # Merge-on-Read, keep newest status, use for order fast lookups
    spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.orders_current (
        order_id STRING,
        user_id STRING,
        total_amount DOUBLE,
        currency STRING,
        payment_method STRING,
        items ARRAY<STRUCT<sku: STRING, quantity: INT, unit_price: DOUBLE, category: STRING>>,
        current_status STRING,
        last_updated_ts TIMESTAMP,
        kafka_ts TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (bucket(16, order_id))
    """)

def process_batch(df, batch_id):
    """
    Dual-Stream Logic: 
    1. Stream A -> Bronze (Append)
    2. Stream B -> Gold (Merge)
    """
    record_count = df.count()
    print(f"Processing Batch ID: {batch_id} with {record_count} records")
    
    if record_count == 0:
        return

    df.cache()
    
    try:
        # ==========================================
        # Stream A: Write to Bronze (ODS / Append-Only) -> Source of Truth
        # ==========================================
        df.write \
            .format("iceberg") \
            .mode("append") \
            .save("lakehouse.bronze.orders_events")
        
        # ==========================================
        # Stream B: Write to Gold (State / Upsert)
        # ==========================================
        # Create a Temp View for Merge usage
        df.createOrReplaceTempView("batch_updates")

        df.sparkSession.sql("""
        MERGE INTO lakehouse.gold.orders_current AS target
        USING (
            SELECT * FROM (
                SELECT 
                    order_id,
                    user_id, -- 假設 user_id 不會變，取最新即可
                    

                    first_value(current_status) OVER (
                        PARTITION BY order_id 
                        ORDER BY event_timestamp DESC
                    ) as current_status,
                    
                    first_value(total_amount, true) OVER (
                        PARTITION BY order_id 
                        ORDER BY event_timestamp DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as total_amount,
                    
                    first_value(currency, true) OVER (
                        PARTITION BY order_id 
                        ORDER BY event_timestamp DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as currency,
                    
                    first_value(payment_method, true) OVER (
                        PARTITION BY order_id 
                        ORDER BY event_timestamp DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as payment_method,

                    first_value(items, true) OVER (
                        PARTITION BY order_id 
                        ORDER BY event_timestamp DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as items,

                    max(event_timestamp) OVER (PARTITION BY order_id) as event_timestamp,
                    max(kafka_ts) OVER (PARTITION BY order_id) as kafka_ts,
                    
                    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_timestamp DESC) as rn
                FROM batch_updates
            ) WHERE rn = 1
        ) AS source
        ON target.order_id = source.order_id
        
        WHEN MATCHED THEN UPDATE SET
            target.current_status = source.current_status,
            target.total_amount = coalesce(source.total_amount, target.total_amount),
            target.items = coalesce(source.items, target.items),
            target.currency = coalesce(source.currency, target.currency),
            target.payment_method = coalesce(source.payment_method, target.payment_method),
            target.last_updated_ts = source.event_timestamp,
            target.kafka_ts = source.kafka_ts
            
        WHEN NOT MATCHED THEN INSERT (
            order_id, user_id, total_amount, currency, payment_method, 
            items, current_status, last_updated_ts, kafka_ts
        ) VALUES (
            source.order_id, source.user_id, source.total_amount, source.currency, source.payment_method, 
            source.items, source.current_status, source.event_timestamp, source.kafka_ts
        )
        """)
        
    except Exception as e:
        print(f"Error in batch {batch_id}: {e}")
        raise e
    finally:
        df.unpersist()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    init_tables(spark)

    # 1. 定義 Schema
    json_schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_timestamp", LongType()),
        StructField("order_id", StringType()),
        StructField("user_id", StringType()),
        StructField("total_amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("payment_method", StringType()),
        StructField("items", ArrayType(StructType([
            StructField("sku", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("category", StringType())
        ]))),
        StructField("current_status", StringType())
    ])

    # 2. Read from Kafka
    print("Connecting to Kafka...")
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "orders") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "5000") \
        .load()

    # 3. Parse JSON
    parsed_stream = kafka_stream \
        .select(from_json(col("value").cast("string"), json_schema).alias("data"), col("timestamp").alias("kafka_ts")) \
        .select("data.*", "kafka_ts") \
        .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))

    # 4. Start Streaming
    print("Starting Medallion Architecture (Bronze: Append, Gold: Merge)...")
    
    query = parsed_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "s3a://checkpoints/orders_bronze_gold_v1") \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()