
---

# Design Doc : Data Generator & Simulation Strategy

**Project:** TikTok Content Ecosystem Health Platform
**Role:** Analytics Data Engineer
**Status:** Draft v1.2

## 1. Overview

The Data Generator is a high-fidelity **stochastic simulation engine**. It serves as the "Source of Truth" for the pipeline, modeling realistic user behavior funnels, content lifecycles, and operational incidents.

It feeds three distinct Kafka topics to support the "Level 2: Fast Batch Dims" architecture:

1. **`content_events`:** High-velocity user interactions (The "Firehose").
2. **`cdc.content.videos`:** Video metadata changes (Uploads, Status changes).
3. **`cdc.users.profiles`:** User profile changes (The "Slowly Changing Dimension").

---

## 2. Topic 1: Raw Events Stream (`content_events`)

* **Purpose:** The primary source for Real-time Dashboards (Gold) and Historical Analysis (Silver).
* **Traffic Pattern:** High Throughput (Simulating 1k - 10k events/sec).
* **Partition Key:** `video_id` (Ensures events for the same video land in the same Spark partition).

### 2.1 Event Schema (Header + Body Pattern)

To support Schema Evolution, we enforce a strict schema on routing fields ("Header") while encapsulating business attributes in a JSON blob ("Body").

```json
{
  // --- Header (Strict Schema) ---
  "event_id": "uuid-v4",
  "event_timestamp": "2026-02-07T10:00:00.123Z",
  "video_id": "v_1023",
  "user_id": "u_8812",
  "event_type": "like", // Enum

  // --- Body (Flexible JSON) ---
  "payload": {
    "watch_time_ms": 5400,          // The progress time when event occurred
    "video_duration_ms": 60000,     // Total length of the video
    "device_os": "iOS",
    "app_version": "14.2.0",
    "network_type": "5G",
    "referral_source": "foryou_page"
  }
}

```

### 2.2 Event Type Logic & Definitions

This logic defines how the `payload` fields are populated for each event type.

| Event Type | Definition | `watch_time_ms` Logic | Analytics Use Case |
| --- | --- | --- | --- |
| **`impression`** | Video appears on screen. | `0` | **Denominator:** For CTR & Viral Velocity. |
| **`play_start`** | Video starts auto-playing. | `0` | **Metric:** "Raw View Count". |
| **`play_finish`** | Video plays to the end (Loop). | `= video_duration_ms` | **Metric:** "Completion Rate" (Quality). |
| **`skip`** | User swipes away before finish. | `actual_duration` (e.g., 3200) | **Metric:** "Doomscroll Rate" (Boredom Signal). |
| **`like`** | User taps the heart button. | `time_at_action` (e.g., 5400) | **Numerator:** Viral Velocity. **Analysis:** "Hook Analysis" (When do users like?). |
| **`share`** | User opens share menu. | `time_at_action` | **Numerator:** Viral Velocity. |

---

## 3. Metadata Streams (CDC)

We split metadata into two topics to respect domain boundaries and allow independent scaling.

### 3.1 Video Metadata (`cdc.content.videos`)

* **Purpose:** Enriches events with `Category`, `Region`, `Status`.
* **Trigger:** New Uploads or Trust & Safety (T&S) Actions.
* **Partition Key:** `video_id`.

**Payload (Debezium Style):**

```json
{
  "op": "c", // c=create, u=update
  "ts_ms": 1707123456789,
  "source": "content_db",
  "after": {
    "video_id": "v_1023",
    "author_id": "u_555",
    "category": "Gaming", // Critical for Dashboard Aggregation
    "region": "US",
    "duration_sec": 60,
    "status": "active",   // active -> banned
    "upload_time": "2026-02-07T09:55:00Z"
  }
}

```

### 3.2 User Profile Metadata (`cdc.users.profiles`)

* **Purpose:** Enriches events with `User Segment`, `Creator Status`.
* **Trigger:** Registration or Profile Update.
* **Partition Key:** `user_id`.

**Payload:**

```json
{
  "op": "u", // Update
  "ts_ms": 1707123499999,
  "after": {
    "user_id": "u_8812",
    "register_country": "US",
    "is_creator": true,       // Changed from false -> true
    "ltv_segment": "VIP"      // Marketing segment update
  }
}

```

---

## 4. Simulation Logic: The "Probabilistic Funnel"

To support valid analysis, events are generated via a state-machine simulation, not random sampling.

### 4.1 The Interaction Funnel

Events follow a strict logical sequence per user/video session:

1. **Impression:** Generated based on a **Zipfian Distribution** (Power Law).
* *Logic:* 20% of "Head" videos receive 80% of impressions. This enables "Distribution Fairness" analysis in Silver Layer.


2. **Play Start:** 85% probability after Impression.
3. **Consumption:**
* Determines if `play_finish` or `skip` occurs based on `video_quality_score` vs. `user_patience`.


4. **Engagement (Like/Share):**
* **Like Rate:** ~8% global average.
* **Share Rate:** ~1.5% global average.
* *Constraint:* Can only occur *after* `play_start`.
* *Timing:* The `watch_time_ms` for likes is randomly sampled from a Log-Normal distribution skewed towards the beginning of the video (The "Hook").



---

## 5. Engineering Chaos: Scenario Injection

The generator includes a **Chaos Controller** to validate Dashboard Alerting logic.

### Scenario A: The "Supernova" (Viral Trend)

* **Trigger:** Manually boost `viral_coefficient` for `video_id=v_999` by 500x.
* **Effect:** Rapid flood of `impression` -> `play` -> `like` events.
* **Dashboard Validation:**
* **Viral Velocity Scatter Plot:** Dot moves rapidly to top-right quadrant.
* **Trending Table:** `v_999` appears at Rank #1.



### Scenario B: The "Broken Client" (App Bug)

* **Trigger:** Simulate a bad app update (v15.0).
* **Effect:** For users with `app_version='15.0'`, enforce `Like Probability = 0%`.
* **Dashboard Validation:**
* **Global Interaction Rate:** Line chart drops proportionally to v15.0 adoption rate.



### Scenario C: The "Inventory Drought" (Supply Chain)

* **Trigger:** Stop emitting `op='c'` events to `cdc.content.videos`.
* **Effect:** No new videos uploaded.
* **Dashboard Validation:**
* **Fresh Supply Ratio:** Trends downward as the numerator (Videos < 1h old) decays to zero.



---

## 6. Technical Implementation Stack

* **Language:** Python 3.10+
* **Producer:** `confluent-kafka` (High performance librdkafka wrapper).
* **Data Faker:** `Faker` library for PII (names, IPs).
* **Math:** `numpy` for weighted probability distributions (Zipf, Gamma).
* **Concurrency:** Python `threading` to run Event Stream and CDC Streams in parallel loops.