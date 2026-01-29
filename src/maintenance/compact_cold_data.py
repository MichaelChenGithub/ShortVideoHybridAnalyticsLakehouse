import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("IcebergColdCompaction") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/") \
        .getOrCreate()

def compact_bronze_cold(spark):
    """
    Bronze is partitioned by hours(event_timestamp), most suitable for cold-hot separation.
    Stategy: ONLY compact data older than 1 hour
    """
    table_name = "lakehouse.bronze.orders_events"
    print(f"\n Compacting Cold Data for: {table_name}")
    print("Strategy: Ignore data from the last 1 hour")
    
    one_hour_ago = datetime.now() - timedelta(hours=1)
    cutoff_ts = one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')

    spark.sql(f"""
    CALL lakehouse.system.rewrite_data_files(
        table => '{table_name}',
        where => "event_timestamp < TIMESTAMP'{cutoff_ts}'",
        options => map(
            'min-input-files', '2',
            'target-file-size-bytes', '134217728' 
        )
    )
    """).show(truncate=False)

def compact_gold_cold(spark):
    """
    Gold is bucket partitioned, new and old data are physically mixed.
    But we can use kafka_ts (data arrival time) to try filtering.
    Strategy: Try merging those records where "kafka_ts" is older than 1 hour.
    """
    table_name = "lakehouse.gold.orders_current"
    print(f"\n Compacting Cold Data for: {table_name}")

    one_hour_ago = datetime.now() - timedelta(hours=1)
    cutoff_ts = one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')

    spark.sql(f"""
    CALL lakehouse.system.rewrite_data_files(
        table => '{table_name}',
        where => "kafka_ts < TIMESTAMP '{cutoff_ts}'",
        options => map(
            'min-input-files', '2',
            'target-file-size-bytes', '134217728' 
        )
    )
    """).show(truncate=False)

def expire_snapshots(spark):
    """
    Clean up old snapshots to reduce metadata size. This step is generally safe to do globally.
    """
    print(f"\n Expiring Snapshots (Global)...")
    tables = ["lakehouse.bronze.orders_events", "lakehouse.gold.orders_current"]
    one_hour_ago = datetime.now() - timedelta(hours=1)
    cutoff_ts = one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')
    for table in tables:
        print(f"   Target: {table}")
        spark.sql(f"""
        CALL lakehouse.system.expire_snapshots(
            table => '{table}',
            retain_last => 5,
            older_than => TIMESTAMP '{cutoff_ts}'
        )
        """).show(truncate=False)

def remove_orphan_files(spark):
    """
    Clean up orphan files left on MinIO due to failed writes (Crashes). This step is generally safe to do globally.
    """
    print(f"\n Removing Orphan Files (Global)...")
    
    one_day_ago = datetime.now() - timedelta(days=1)
    cutoff_ts = one_day_ago.strftime('%Y-%m-%d %H:%M:%S')

    tables = ["lakehouse.bronze.orders_events", "lakehouse.gold.orders_current"]
    
    for table in tables:
        print(f"   Target: {table}")
        spark.sql(f"""
        CALL lakehouse.system.remove_orphan_files(
            table => '{table}',
            older_than => TIMESTAMP '{cutoff_ts}'
        )
        """).show(truncate=False)

def main():
    spark = create_spark_session()
    
    try:
        compact_bronze_cold(spark)
        
        compact_gold_cold(spark)

        expire_snapshots(spark)

        remove_orphan_files(spark)
        
    except Exception as e:
        print(f"Error during maintenance: {e}")

if __name__ == "__main__":
    main()