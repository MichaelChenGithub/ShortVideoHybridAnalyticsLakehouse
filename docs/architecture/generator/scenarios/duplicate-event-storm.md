# Scenario: Duplicate Event Storm

## 1. The Real-World Problem

Kafka's default delivery guarantee is **at-least-once**. This is a deliberate design
choice, not a limitation — it trades the complexity of distributed coordination for
throughput and availability. The consequence is that in any realistic production
deployment, duplicate messages are not an edge case: they are a guaranteed, periodic
occurrence.

Duplicates arise from two distinct sources in a short-video event pipeline:

**Source 1 — Producer retry on timeout.**
The mobile SDK or server-side ingestion service sends an event to Kafka. The broker
receives and writes it, but the acknowledgment is lost in transit due to a brief network
hiccup. The producer's timeout fires. It retries. The event is written a second time
with a new Kafka offset. Both copies now exist in the partition with identical
`event_id` payloads.

**Source 2 — Consumer restart after partial write.**
A Spark Structured Streaming job crashes mid-microbatch after writing some rows to
Iceberg but before committing the Kafka offset checkpoint. When the job restarts, it
re-reads from the last committed offset and reprocesses the entire microbatch. Any
event that was already written to Iceberg before the crash is written again from the
same Kafka message.

Both sources produce the same symptom: the same `event_id` appears multiple times in
the ingested data. If the pipeline does not deduplicate, downstream aggregates
double-count engagement metrics — inflating impression counts, like counts, and velocity
scores. A video could trigger a spurious BOOST decision purely because its events were
delivered twice.

At a platform processing tens of millions of events per hour, a 1% duplicate rate
translates to hundreds of thousands of phantom events per hour affecting metric
accuracy across the entire content catalog.

---

## 2. Why Deduplication Is Not Trivial in Streaming

The straightforward solution — check whether `event_id` already exists before writing —
does not work at streaming scale for two reasons:

**State explosion.** A streaming job would need to maintain a set of every `event_id`
seen since the beginning of time to guarantee global deduplication. At a platform
emitting 100k events per second, that set grows by 8.6 billion entries per day. No
practical state backend can hold this indefinitely.

**Window-bounded deduplication.** The practical solution is to deduplicate within a
bounded time window (e.g., the last 10 minutes of event time). This covers the vast
majority of duplicate patterns — producer retries and consumer restarts typically
produce the duplicate within seconds to minutes of the original. Duplicates that arrive
outside the dedup window are accepted as known imprecision and are bounded by the retry
timeout configuration.

The implication is that the deduplication guarantee is scoped:

- Duplicates arriving within the dedup window: eliminated.
- Duplicates arriving outside the dedup window (extremely rare in practice): may appear
  in gold, bounded by retry timeout SLA.

---

## 3. What the Scenario Simulates

The `duplicate_event_storm` scenario replays 15% of already-emitted `content_events`
with identical `event_id` values but new Kafka offsets. The replayed events arrive
within the same processing window as the originals — simulating the producer-retry
pattern where the duplicate arrives within seconds of the original.

Key properties of the simulated duplicate storm:

1. Duplicates are drawn from the replay pool of events already emitted in the current
   run. The `event_id` is identical; the Kafka message is new.
2. Duplicates are distributed uniformly across `video_id` values to avoid concentrating
   duplication on a single video.
3. The duplicate ratio is fixed at 15% of total content events, injected continuously
   throughout the emission window rather than in a single burst.
4. The scenario targets the `normal_baseline` and `viral_high_quality` video pools
   specifically, so the correctness of `NO_ACTION` and `BOOST` decisions can be verified
   against known-good expected actions even in the presence of duplicates.

---

## 4. How the Pipeline Handles It

### 4.1 Bronze is append-only — duplicates land here

`lakehouse.bronze.raw_events` accepts every event that passes schema validation,
regardless of whether the `event_id` has been seen before. Bronze is an immutable audit
log of everything the pipeline received from Kafka. Duplicates are not errors at this
layer — they are a truthful record of what arrived.

This design is intentional. Bronze serves two purposes:

1. Audit trail for compliance and debugging.
2. Source of truth for any future replay or backfill into the realtime path.

If deduplication were applied at the bronze layer, the original and the duplicate would
both be silently collapsed, making it impossible to distinguish a genuine data quality
problem (e.g., a buggy producer emitting wrong data) from a normal delivery retry.

### 4.2 Deduplication happens at the aggregation layer

The realtime aggregator (`rt_content_events_aggregator`) applies `event_id`-level
deduplication within the Spark Structured Streaming dedup window before computing
window aggregates. Spark's `dropDuplicates(["event_id"])` with a watermark-bounded
state ensures that within the dedup window, each `event_id` contributes to the
aggregate exactly once.

```
bronze (append-only, has duplicates)
    ↓
rt_content_events_aggregator
    dropDuplicates("event_id") within watermark window
    ↓
gold.rt_video_stats_1min (deduplicated counts)
```

### 4.3 Gold reflects unique engagement, not raw delivery volume

After deduplication, the 1-minute tumbling window aggregates in `gold.rt_video_stats_1min`
represent the count of unique engagement actions per video per minute. A video that
received 100 genuine impressions and 15 retried duplicates will show `impression_count = 100`
in gold — not 115.

The BOOST/REVIEW/RESCUE decisioning downstream operates on gold metrics, so decisions
are made on deduplicated engagement signals.

---

## 5. Acceptance Assertions

The following conditions must all hold after a `duplicate_event_storm` scenario run:

| # | Assertion | Rationale |
|---|-----------|-----------|
| 1 | `bronze.raw_events` row count = total emitted events including duplicates | Bronze is append-only |
| 2 | `bronze.raw_events` distinct `event_id` count < total row count | Proves duplicates landed in bronze |
| 3 | `gold.rt_video_stats_1min` aggregated counts match counts derived from unique `event_id` set only | Dedup is applied before aggregation |
| 4 | `viral_high_quality` videos still receive `BOOST` decision (not inflated past threshold incorrectly) | Decision accuracy is not affected by duplicates |
| 5 | `normal_baseline` videos still receive `NO_ACTION` (duplicates do not push them over BOOST threshold) | No false positive decisions from duplicate inflation |

### 5.1 Verification query

```sql
-- Bronze has duplicates
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT event_id) AS unique_events,
    COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_count
FROM lakehouse.bronze.raw_events
WHERE run_id = '<run_id>';

-- Gold reflects only unique events
SELECT
    video_id,
    SUM(impression_count) AS gold_impressions
FROM lakehouse.gold.rt_video_stats_1min
WHERE run_id = '<run_id>'
GROUP BY video_id;
```

---

## 6. Engineering Decision Record

**Why deduplicate at the aggregation layer instead of at bronze write time?**

Bronze is the audit log. It must reflect exactly what the pipeline received. If a
producer has a bug that causes it to send the same valid event 10,000 times, that
anomaly should be visible in bronze — it is a signal that something upstream went wrong.
Silently deduplicating at bronze would hide the problem.

The aggregation layer is where deduplication belongs because that is where the business
semantics are defined: "how many times did a user genuinely engage with this video?"
That is a question about unique interactions, not about Kafka delivery counts.

**Why use `event_id` as the dedup key rather than `(video_id, user_id, event_type, event_timestamp)`?**

A composite key based on content attributes has two problems. First, legitimate events
can share the same `(video_id, user_id, event_type, event_timestamp)` tuple if a user
rapidly double-taps or if two events land in the same millisecond. Second, timestamp
precision from mobile clients is often unreliable — devices may truncate to second
granularity. Using a generator-assigned `event_id` that is unique per interaction avoids
both problems and is a standard pattern: the producer is responsible for assigning a
stable, unique ID before sending.

**What happens to duplicates that arrive outside the dedup window?**

Duplicates arriving more than `watermark_lag + dedup_window` seconds after the original
may pass through deduplication and contribute to gold aggregates. In practice, producer
retry timeouts are configured to seconds and consumer restart recovery completes within
minutes — so cross-window duplicates are negligible. They represent known, bounded
imprecision and do not warrant the operational cost of a global dedup store.

---

## 7. Linked Docs

1. `docs/architecture/generator/mock-event-generator-contract-and-scenario-matrix.md`
2. `docs/architecture/streaming/spark-realtime-jobs-contract.md`
3. `docs/architecture/data-model/data-model-contract.md`
4. `docs/architecture/messaging/kafka-topic-schema-retention-contract.md`
