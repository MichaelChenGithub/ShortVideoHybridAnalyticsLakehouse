from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def main():
    spark = SparkSession.builder \
        .appName("VerifyMedallion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/") \
        .getOrCreate()

    print("========================================")
    print("🥉 Bronze Layer: orders_events (History)")
    print("========================================")
    # 這裡應該要看到「很多」筆資料，包含同一個 order_id 的多個狀態 (CREATED, PAID...)
    bronze_df = spark.read.format("iceberg").load("lakehouse.bronze.orders_events")
    bronze_count = bronze_df.count()
    print(f"Total Events: {bronze_count}")
    
    print("\nSample History for a specific order (if any):")
    
    orders_with_history = bronze_df.groupBy("order_id") \
        .count() \
        .filter(col("count") > 1) \
        .select("order_id") \
        .limit(1) \
        .collect()
    
    if orders_with_history:
        ord_id = orders_with_history[0][0]
        print(f"✅ Found interesting order: {ord_id}")
        
        print("\n📜 Bronze History (Should see multiple rows):")
        bronze_df.filter(col("order_id") == ord_id) \
            .select("*") \
            .orderBy("event_timestamp") \
            .show(truncate=False)

        print("\n========================================")
        print("🥇 Gold Layer: orders_current (State)")
        print("========================================")
        
        gold_df = spark.read.format("iceberg").load("lakehouse.gold.orders_current")
        
        print(f"💎 Gold State (Should see ONLY 1 row with latest status):")
        gold_df.filter(col("order_id") == ord_id) \
            .select("*") \
            .show(truncate=False)
            
    else:
        print("⚠️ No orders with multiple events found yet. Wait for the generator to create PAID/SHIPPED events.")

if __name__ == "__main__":
    main()