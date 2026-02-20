
---

# Design Doc : Dashboard Design & Metrics

## 1. Overview

This document defines the visualization layer served by **Trino**. I implement a "Dual-Dashboard" strategy:

1. **Metabase:** Business-facing dashboards for Operations (Real-time) and Strategy (Batch).
2. **Grafana:** Technical dashboard for Data Engineering to monitor pipeline health.

---

## 2. Metabase: Real-time Ops Dashboard ("The Pulse")

* **Primary Source:** `lakehouse.gold.rt_video_stats_1min` (**Iceberg Append-Only Log**)
* **Secondary Source:** `lakehouse.dims.dim_videos` (Iceberg MoR)
* **Update Frequency:** 1 minute (Auto-refresh). *Note: Data latency is ~3 mins.*
* **Target Audience:** Content Operations, Trust & Safety

### 2.1 Layout Strategy (Command Center)

Designed for large-screen monitoring. Focuses on "Anomaly Detection" and "Immediate Action".
```text
+---------------------------------------------------------------+
|  [ Alert: Global Doomscroll Rate Spiking (>45%) - Check RecSys ] |
+-----------------------+-----------------------+---------------+
| 1. Viral Velocity     | 2. Doomscroll Rate    | 3. Cold Start |
| (Scatter Plot)        | (Line Chart)          | (Gauge)       |
| "Star Finder"         | User Boredom Index    | Quality Check |
+-----------------------+-----------------------+---------------+
| 4. Top Trending Videos Table (Aggregated View)                |
| Video ID | Category | Velocity | Completion % | Action        |
| v_1023   | Beauty   | 9.8      | 65%          | [Boost]       |
+---------------------------------------------------------------+

```

### 2.2 Metric Specifications & Query Logic
## 📊 Product Growth Intelligence Metrics

| Metric Section | Visual | Definition & Logic | Business Action |
|---------------|--------|-------------------|----------------|
| **1. Viral Velocity (Explosion Monitor)** | Scatter Plot <br>• X: Velocity <br>• Y: Completion % <br>• Size: Total Views | **View Logic (See `src/trino/create_views.sql`):** <br>Calculates weighted velocity over a sliding 30-minute window using Read-Time Joins with `dim_videos`. <br><br>**Why?** Correlates *Hype (Velocity)* with *Quality (Completion).* | **Discovery:** <br>• High Vel + High Completion → **Supernova** (Boost immediately) <br>• High Vel + Low Completion → **Clickbait** (Suppress / Review) |
| **2. Global Doomscroll Rate (System Health)** | Line Chart <br>• X: Time (1m bins) <br>• Y: Skip Rate | **Formula:** `Sum(Skips) / Sum(Impressions)` (Global Aggregation) <br><br>**Logic:** Proxy for *User Boredom.* High skip rate suggests recommendation quality issues. | **Incident Response:** <br>Spike > 40% → Alert SRE. Possible RecSys model degradation or irrelevant content serving. |
| **3. High-Quality Cold Start (Supply Health)** | Gauge Chart <br>• Target: > 20% | **Formula:** % of new videos (age < 1h) with `Velocity > 0.05` <br><br>**Logic:** Focus on engagement quality rather than raw impressions. Measures whether new creators get traction. | **Supply Control:** <br>If Red (<10%) → Increase exploration traffic allocation to support new creators. |
| **4. Trending Table (Enriched Details)** | Table | **Query Logic (Read-Time Join):** <br>`WITH metrics AS ( SELECT video_id, SUM(likes) AS likes FROM gold WHERE window_start >= now() - interval '1h' minute GROUP BY video_id ) SELECT m.*, d.category, d.region FROM metrics m LEFT JOIN dims.dim_videos d ON m.video_id = d.video_id ORDER BY m.likes DESC LIMIT 50;` | **Ops Execution:** <br>Provides metadata context (Category, Region) for boosting decisions and operational insight. |




### 2.3 Architectural Note: Read-Time Joins
Instead of denormalizing dimension data (e.g., `category`, `author_region`) into the streaming event log, we utilize a **Star Schema** approach with **Read-Time Joins** in Trino. This allows for:

* **Flexibility:** Metadata updates (e.g., a video re-categorization) are reflected immediately without re-processing the stream.
* **Efficiency:** We leverage **Broadcast Joins** in Trino, where the smaller dimension tables or filtered fact results are efficiently distributed across workers to minimize network shuffle.
---

## 3. Metabase: Strategic Analysis Dashboard ("The Diagnosis")

* **Source:** `lakehouse.gold.batch_video_daily` (Iceberg Batch Aggregation)
* **Update Frequency:** Daily (T+1)
* **Target Audience:** Product Managers, Data Scientists

### 3.1 Layout & Metrics

| Metric Section | Visual | Definition & Logic | Business Action |
| --- | --- | --- | --- |
| **Quality Attribution**<br><br>(Content/User Fit) | **Heatmap**<br><br>• X: Duration Bin<br><br>• Y: Category<br><br>• Color: Completion Rate | **Formula:** `Avg(watch_time / video_duration)`<br><br>Bins: 0-15s, 15-60s, >60s<br><br><br>*Logic:* Correlates content length and type with user patience. | **Algo Tuning:**<br><br>Example: If long "Humor" videos have low completion, downrank them in the algorithm. |
| **Creator Retention**<br><br>(Ecosystem Health) | **Cohort Table** | **Formula:** Creators active in Month M who returned to upload in M+1.<br><br><br>*Logic:* Measures the sustainability of the supply side. | **Incentive Strategy:**<br><br>Drop in retention triggers "Creator Fund" bonuses or gamification campaigns. |

---

## 4. Grafana: Unified System Health Dashboard ("The Engine Room")

* **Source:** Prometheus (Kafka Exporter, Spark Metrics), Trino (Iceberg Metadata)
* **Update Frequency:** Real-time (10s - 1min)
* **Target Audience:** Data Engineers, SRE

### 4.1 Pipeline Performance (Ingestion & Compute)

| Metric | Visualization | Query / Source | Operational Significance |
| --- | --- | --- | --- |
| **E2E Data Latency** | **Stat Panel** (Big Number) | `SELECT now() - max(window_start) FROM gold.rt_video_stats_1min` | **SLA Check:** Target < 3 mins. Reflects the physical limitation of Micro-batch (1m) + Watermark (10s) + Commit Time. |
| **Kafka Consumer Lag** | **Time Series** | `kafka_consumergroup_lag > 0` (Prometheus) | **Backpressure:** Rising lag indicates Spark Executors cannot keep up with the 1-minute trigger cycle. |
| **Iceberg Commit Duration** | **Time Series** | `commit_duration_ms` (Spark Metrics) | **Metadata Bloat:** Spikes > 10s indicate that the Append-Only pattern is generating too many small files, requiring more frequent bin-packing. |

### 4.2 Storage Health (Compaction Monitoring)

* **Goal:** Monitor the "Small File Problem" and ensure Airflow Compaction Jobs are effective.
* **Source:** Trino Query on Iceberg Metadata Tables (`lakehouse.gold$files`).

| Metric | Visualization | Query Logic | Operational Significance |
| --- | --- | --- | --- |
| **Avg File Size** | **Gauge** | `SELECT avg(file_size_in_bytes) FROM "gold$files"` | **Target:** ~128MB.<br><br>**Critical:** If < 1MB, Compaction is failing. Read performance will degrade significantly. |
| **Active Data Files** | **Time Series** | `SELECT count(*) FROM "gold$files"` | **Trend:** Should show a "Sawtooth" pattern (rising during streaming, dropping after compaction). Linear growth indicates failure to expire snapshots. |

---