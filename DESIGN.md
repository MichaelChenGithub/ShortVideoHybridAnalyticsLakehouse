# Design Doc: Real-time Transactional Data Lakehouse
## 1. Overview
### 1.1 Objective
Build a high-throughput streaming lakehouse capable of handling **50,000+ events/second** with **<1 minute data latency** and **ACID compliance**. The system simulates a real-world E-commerce scenario (Create, Paid, Shipped, Cancel, Return) to demonstrate modern Data Engineering capabilities using **Apache Spark Structured Streaming** and **Apache Iceberg**.

### 1.2 Core Metrics Goals
* **Throughput:** Peak support for 50k events/sec.
* **Latency:** Data visibility in the serving layer (`orders_current`) within 60 seconds.
* **Consistency:** Exactly-once semantics via idempotent writes; consistent reads via Snapshot isolation.
* **Efficiency:** Query latency < 5s achieved via an automated "Sawtooth" compaction strategy.

## 2. Architecture
### 2.1 High-Level Data Flow

```mermaid
graph LR
    Generator[Mock Generator (Python)] -->|JSON Key: order_id| Kafka[Apache Kafka]
    Kafka -->|Topic: orders| Spark[Spark Structured Streaming]
    
    subgraph "Processing Layer (Spark)"
        Spark -->|Stream 1: Append| DWD[Table: orders_events]
        Spark -->|Stream 2: Upsert| DWS[Table: orders_current]
    end
    
    subgraph "Storage Layer (MinIO/S3)"
        DWD -->|Parquet Files| S3_Bucket
        DWS -->|Parquet + Delete Files| S3_Bucket
    end
    
    subgraph "Maintenance & Serving"
        Airflow[Airflow DAG] -->|Trigger: Compaction| Spark_Batch[Spark Batch Job]
        Trino[Trino Engine] -->|SQL Query| S3_Bucket
    end 
```

## 3. Component Design
### 3.1 Ingestion Layer: Mock Generator
* **Source:** Python-based state machine generator.
* **Logic:** Simulates realistic order lifecycles (`CREATED` -> `PAID` -> `SHIPPED` -> `CANCELLED` / `RETURNED`).
* **Kafka Partitioning Strategy:**
    * **Topic:** `orders` (4 partitions)
    * **Key:** `order_id` (CRITICAL: Ensures strict ordering for sequential state updates of a single order).
    * **Value:** JSON Payload.

**Event Schema (JSON):**
```json
{
  "event_id": "uuid-v4",
  "event_type": "ORDER_CREATED", 
  "event_timestamp": 1706600000,
  "order_id": "ord-20260127-001",
  "user_id": "u-12345",
  "total_amount": 299.00,
  "currency": "USD",
  "payment_method": "CREDIT_CARD",
  "items": [
    {
      "sku": "SKU-999", 
      "quantity": 1, 
      "unit_price": 299.00, 
      "category": "Electronics"
    }
  ],
  "current_status": "CREATED"
}
```
### 3.2 Storage Layer: Medallion Architecture (Dual-Stream)

Instead of a linear dependency, the system employs a **Parallel Processing** pattern where Spark splits the Kafka stream into two independent storage targets to serve different analytical needs.

#### **Stream A: Silver Layer - `orders_events` (Event Log)**
* **Pattern:** Append-Only Log.
* **Role:** The "Source of Truth" for history.
* **Use Case:**
    * Audit trails (e.g., "When exactly did the user cancel?").
    * Time-series analysis (e.g., "Conversion rate from Created to Paid").
* **Partitioning:** `hour(event_timestamp)`

#### **Stream B: Gold Layer - `orders_current` (State Snapshot)**
* **Pattern:** Merge-on-Read (MoR) / Upsert.
* **Role:** The "Materialized View" of the latest business state.
* **Use Case:**
    * Real-time Operational Dashboard (e.g., "Current active orders", "Inventory levels").
    * Strictly requires ACID consistency to handle `CANCEL` events correctly.
* **Write Logic:** `MERGE INTO` target USING source ON target.id = source.id
* **Partitioning:** `bucket(16, order_id)`

### 3.3 Processing Layer: Spark Structured Streaming
* **Engine:** Apache Spark 3.x
* **Trigger Interval:** 1 minute (Micro-batch).
* **Consistency:** Enabled by Iceberg's Optimistic Concurrency Control (OCC).
* **Parallelism:** Aligned with Kafka partitions (4 cores minimum) to maximize throughput without shuffle overhead.

### 3.4 Maintenance Layer: Compaction Strategy
* **Problem:** Streaming ingestion creates thousands of small files (Sawtooth pattern accumulation).
* **Solution:** Automated Hourly Compaction.
    * **Schedule:** Every 60 minutes.
    * **Action:**
        1.  **Bin-packing:** Rewrite small data files into larger (128MB) files.
        2.  **Expire Snapshots:** Remove old metadata to keep the manifest list lightweight.
        3.  **Rewrite Position Delete Files:** (For MoR table) Merge delete files into data files to improve read performance.

## 4. Infrastructure (Docker)

The environment simulates a cloud-native stack locally:

| Service | Role | Port |
| :--- | :--- | :--- |
| **MinIO** | Object Storage (AWS S3 Compatible) | 9000 |
| **Kafka** | Message Queue (w/ Zookeeper) | 9092 |
| **Spark** | Distributed Compute (Iceberg Runtime) | - |
| **Trino** | Ad-hoc Query Engine | 8080 |

## 5. Potential Bottlenecks & Solutions

1.  **Small File Problem:** Mitigated by `spark.sql.shuffle.partitions` tuning and hourly compaction jobs.
2.  **Backpressure:** Handled via `spark.streaming.kafka.maxOffsetsPerTrigger` to prevent OOM during traffic spikes.
3.  **Data Ordering:** Enforced by Kafka Partition Key (`order_id`) and Spark's 1:1 partition mapping.