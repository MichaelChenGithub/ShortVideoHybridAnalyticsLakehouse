
---

# Design Doc : Data Model & Schema Design

## 1. Overview

This document defines the **Apache Iceberg** table schemas for the Lakehouse. We follow the **Medallion Architecture**:

* **Bronze:** Raw ingestion (Append-only).
* **Silver:** Cleaned, enriched, and sessionized (Batch T+1).
* **Gold:** Aggregated business metrics (Real-time Upsert).
* **Dims:** Reference metadata (CDC Streaming / Fast Batch).

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

### 4.1 Fact Table: Virality State

**Table:** `lakehouse.gold.virality_state`

* **Pattern:** Accumulating Snapshot (State Machine).
* **Format:** Parquet (Iceberg **Merge-on-Read**).
* **Update Frequency:** Streaming (Micro-batch 10-30s).
* **Partition Strategy:** `bucket(16, video_id)` — Critical for high-throughput Upserts (avoids full table scans).

| Column Name | Type | Metric Type | Logic / Definition |
| --- | --- | --- | --- |
| `video_id` | `STRING` | **Dimension** | Primary Key. |
| `upload_ts` | `TIMESTAMP` | **Dimension** | Video age (Used for Fresh Supply Ratio). |
| `window_end_ts` | `TIMESTAMP` | **System** | Last updated timestamp (Watermark). |
| `impressions_10m` | `LONG` | **Velocity** | Count of views in rolling window. |
| `likes_10m` | `LONG` | **Velocity** | Count of likes in rolling window. |
| `shares_10m` | `LONG` | **Velocity** | Count of shares in rolling window. |
| `velocity_score` | `DOUBLE` | **Derived** | `(likes*5 + shares*10) / impressions`. The core "Pulse" metric. |
| `acceleration_score` | `DOUBLE` | **Derived** | `velocity_current - velocity_5m_ago`. |
| `total_views` | `LONG` | **Cumulative** | Lifetime views (Size of bubble in Scatter Plot). |
| `is_bot_flagged` | `BOOLEAN` | **Risk** | Flagged by upstream fraud detection logic. |

---

## 5. Schema Evolution & Compatibility

### 5.1 JSON Handling (Bronze to Silver)

* **Strategy:** The `raw_payload` in Bronze allows upstream producers to add fields (e.g., `vr_headset_model`) without breaking the Bronze ingestion pipeline.
* **Evolution:** The Silver Batch Job schema must be updated to extract new fields using `get_json_object` or Iceberg's schema evolution features (e.g., `ALTER TABLE silver ADD COLUMN`).

### 5.2 Time-Travel & Snapshots

* **Gold Table:** Configured with `write.metadata.delete-after-commit.enabled = false` and `history.expire.max-snapshot-age-ms = 86400000` (24h) to allow short-term time travel for debugging "Ghost Velocity" incidents.

---