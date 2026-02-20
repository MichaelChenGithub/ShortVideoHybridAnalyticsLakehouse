
---

# Design Doc : Data Model & Schema Design

## 1. Overview

This document defines the **Apache Iceberg** table schemas for the Lakehouse. We follow the **Medallion Architecture** with a **Hybrid Write Strategy**:
* **Bronze:** Raw ingestion (Append-only).
* **Silver:** Cleaned, enriched, and sessionized (Batch T+1).
* **Gold (RT):** Pre-aggregated Metrics Log (**Real-time Append-Only**).
* **Gold (Batch):** Strategic Metric Mart (**Daily Overwrite/Upsert**).
* **Dims:** Reference metadata (CDC Streaming / **Real-time Upsert**).
---

## 2. Bronze Layer: Ingestion Log

**Table:** `lakehouse.bronze.raw_events`

* **Pattern:** Immutable Append-Only.
* **Format:** Parquet (Iceberg Copy-on-Write).
* **Partition Strategy:** `hours(event_timestamp)` — Optimized for time-based replay and retention.

| Column Name | Type | Description |
| --- | --- | --- |
| `event_id` | `STRING` | UUID. Primary Key for Deduplication. |
| `event_timestamp` | `TIMESTAMP` | Event generation time (Client time). |
| `video_id` | `STRING` | **Header Column.** Extracted for partitioning/filtering. |
| `user_id` | `STRING` | **Header Column.** Extracted for user-level analysis. |
| `event_type` | `STRING` | `impression`, `play_start`, `play_finish`, `like`, `share`. |
| `raw_payload` | `STRING` | **Full JSON Blob.** Contains `device_model`, `network`, `app_ver`. |
| `ingested_at` | `TIMESTAMP` | Processing time (Audit). |

---

## 3. Silver Layer: The "Diagnosis" (Batch)

### 3.1 Fact Table: User Engagement

**Table:** `lakehouse.silver.fact_user_engagement`

* **Pattern:** Enriched Wide Table.
* **Update Frequency:** Daily (T+1) via Spark Batch Job.
* **Partition Strategy:** `days(event_timestamp)` — Optimized for analytical queries (DS/PM).
* **Transformation Logic:**
* Parses `raw_payload` from Bronze.
* Performs **Session Stitching** using Window Functions (`LAG` time diff > 30m).
* Performs **Point-in-Time Join** with Dimensions (e.g., getting user's segment *at that moment*).



| Column Name | Type | Source / Logic | Analytics Value |
| --- | --- | --- | --- |
| `event_id` | `STRING` | Bronze | Unique Identifier. |
| `event_timestamp` | `TIMESTAMP` | Bronze | Time series analysis. |
| `date` | `DATE` | Derived | Partition Key. |
| `user_id` | `STRING` | Bronze | User aggregation. |
| `video_id` | `STRING` | Bronze | Content aggregation. |
| `session_id` | `STRING` | **Derived (Batch)** | Calculated via SQL Windowing (30m timeout). Critical for "Retention" & "Depth" analysis. |
| `event_type` | `STRING` | Bronze | Funnel analysis. |
| `watch_time_ms` | `LONG` | Payload | Consumption depth. |
| `video_duration_ms` | `LONG` | **Enriched (Dim)** | Join `dim_videos`. Used to calc completion rate. |
| `completion_rate` | `FLOAT` | **Derived** | `watch_time / video_duration`. Pre-calculated for fast DS queries. |

### 3.2 Dimension Tables

**Update Frequency:** Near Real-time (CDC Stream -> Upsert).

#### **Table:** `lakehouse.dims.dim_videos`

* **Partition Strategy:** `bucket(16, video_id)` — Optimized for Join performance.

| Column Name | Type | Description |
| --- | --- | --- |
| `video_id` | `STRING` | PK. |
| `creator_id` | `STRING` | FK to `dim_users`. |
| `category` | `STRING` | e.g., "Gaming", "Beauty". |
| `hashtags` | `LIST<STRING>` | Array of tags for search/filtering. |
| `duration_ms` | `LONG` | Video length. |
| `upload_time` | `TIMESTAMP` | Used for "Freshness" calculation. |
| `status` | `STRING` | `active`, `banned`. |

#### **Table:** `lakehouse.dims.dim_users`

* **Partition Strategy:** `bucket(16, user_id)`.

| Column Name | Type | Description |
| --- | --- | --- |
| `user_id` | `STRING` | PK. |
| `register_country` | `STRING` | Geographic segmentation. |
| `device_os` | `STRING` | `iOS`, `Android`. |
| `is_creator` | `BOOLEAN` | Flag to filter Supply-side vs Demand-side users. |
| `ltv_segment` | `STRING` | `VIP`, `High`, `Low`. (Mutable field). |
| `join_at` | `TIMESTAMP` | Registration time.|

---

## 4. Gold Layer: The "Pulse" (Real-time)

### 4.1 Real-time Mart: Video Metrics Log

**Table:** `lakehouse.gold.rt_video_stats_1min`
* **Pattern:** **Time-Series Log (Append-Only)**.
* **Format:** Parquet (Iceberg Copy-on-Write).
* **Update Frequency:** Streaming (Micro-batch 1 min Trigger).
* **Partition Strategy:** `days(window_start)`, `bucket(16, video_id)` — Dual partitioning allows efficient pruning by both Time (Recency) and Video Entity.


| Column Name | Type | Metric Type | Logic / Definition |
| --- | --- | --- | --- |
| `video_id` | `STRING` | **Dimension** | Partition Key. |
| `window_start` | `TIMESTAMP` | **Dimension** | The start time of the 1-min tumbling window. **Critical for Trino Predicate Pushdown.** |
| `upload_ts` | `TIMESTAMP` | **Dimension** | Carried over to calculate "Freshness" downstream. |
| `impressions` | `LONG` | **Denominator** | Count of `impression` events in this 1m bucket. |
| `likes` | `LONG` | **Interaction** | Count of `like` events in this 1m bucket. |
| `shares` | `LONG` | **Interaction** | Count of `share` events in this 1m bucket. |
| `play_start` | `LONG` | **Volume** | Count of `play_start` events (Raw Views). |
| `play_finish` | `LONG` | **Quality** | Count of `play_finish` events. Used to calc **Completion Rate**. |


*Note on Derived Metrics:*
Fields like `velocity_score` and `acceleration` are **removed** from the physical schema. They are now calculated at **Read-Time** via Trino Views by aggregating the last  minutes of log data.
---

## 5. Gold Layer: The "Diagnosis" (Batch)

### 5.1 Strategic Mart: Daily Content Performance

**Table:** `lakehouse.gold.batch_video_daily`
* **Pattern:** **Aggregated State (Overwrite/Upsert)**.
* **Source:** `Silver Fact` + `Dims`.
* **Update Frequency:** Daily (09:00 AM).
* **Purpose:** High-precision analysis with correct dimension attribution (SCD Type 2 resolved).

| Column Name | Type | Logic / Definition |
| --- | --- | --- |
| `date` | `DATE` | Partition Key. |
| `video_id` | `STRING` | Entity Key. |
| `category_at_date` | `STRING` | **Frozen Dimension:** The category of the video *on that specific day*. |
| `total_watch_time_hrs` | `DOUBLE` | Sum of `watch_time_ms` / 3.6e6. |
| `completion_rate` | `FLOAT` | Accurate completion rate based on sessionized data. |
| `unique_viewers` | `LONG` | Count Distinct User ID (HyperLogLog). |
| `retention_d1` | `FLOAT` | % of viewers who returned the next day. |

---

## 6. Schema Evolution & Compatibility

### 5.1 JSON Handling (Bronze to Silver)

* **Strategy:** The `raw_payload` in Bronze allows upstream producers to add fields (e.g., `vr_headset_model`) without breaking the Bronze ingestion pipeline.
* **Evolution:** The Silver Batch Job schema must be updated to extract new fields using `get_json_object` or Iceberg's schema evolution features (e.g., `ALTER TABLE silver ADD COLUMN`).

### 5.2 Time-Travel & Snapshots

* **Gold Table Configuration:**
* **Log Retention:** Since the Gold table is now an Append-Only log, "history" is preserved physically as data rows.
* **Optimization:** `write.metadata.delete-after-commit.enabled = true` is enabled to keep metadata lightweight, as we don't need Iceberg-level time travel to see past states (we can simply query `WHERE window_start = ...`).
* **TTL:** A separate maintenance job runs `DELETE FROM gold WHERE window_start < now() - interval '7' days` to manage storage costs, as Ops only monitors the last 24 hours.

---