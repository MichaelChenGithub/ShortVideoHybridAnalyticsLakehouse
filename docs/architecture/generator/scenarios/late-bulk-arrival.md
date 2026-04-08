# Scenario: Late Bulk Arrival

## 1. The Real-World Problem

Mobile apps on short-video platforms do not flush events to the server in real time.
They batch and buffer locally on the device and upload in bulk when conditions allow.

A user opens TikTok on the subway. The app records every impression, play start, play
finish, like, and skip locally. When the train exits the tunnel and connectivity resumes,
the SDK flushes several minutes of buffered events in a single HTTP burst to the
ingestion endpoint, which then produces them to Kafka. The events carry the original
`event_timestamp` from when the user actually interacted — not the time they arrived
at the server.

From the pipeline's perspective, a single Kafka partition receives a burst of several
hundred events that are all timestamped 15–20 minutes in the past. They arrive at
roughly the same processing time but span a wide event-time range.

This is not a rare failure mode. It is the default behavior of any mobile SDK that
prioritizes battery life and network efficiency over real-time upload. At platform
scale, a meaningful fraction of events in any given minute are arriving late in this
pattern.

---

## 2. Why This Strains Streaming Pipelines

Spark Structured Streaming and Flink both use **watermarks** to track how far event
time has progressed. The watermark is derived from the maximum observed `event_timestamp`
minus a configured lag tolerance. Once the watermark advances past a window's end time,
that window is considered closed and its aggregate is committed to the sink.

The problem with bulk late arrival is timing asymmetry:

1. On-time events advance the watermark normally.
2. The bulk of late events arrives after the watermark has already moved past their
   window boundaries.
3. The streaming runtime must decide: include them in a re-opened window, or drop them.

If the pipeline silently drops late events with no record, data is lost without any
audit trail. Operators have no way to know whether a low metric reading reflects genuine
low engagement or a late-data drop event.

If the pipeline holds windows open indefinitely to accommodate any possible late arrival,
state grows unbounded and the pipeline accumulates memory pressure that eventually
causes degradation or failure.

Neither silent drop nor unbounded hold is acceptable in a production analytics system.

---

## 3. What the Scenario Simulates

The `late_bulk_arrival` scenario introduces two-phase emission to force deterministic
watermark evaluation:

**Phase A — on-time events:**
The generator emits the majority of content events with `event_timestamp` close to
`clock.now()`. These events are processed normally and advance the Spark watermark.

**Phase B — bulk late events:**
After a `phase_gap_seconds` delay (configured at `>= 75 seconds` to exceed the gold
trigger interval plus safety buffer), the generator emits a batch of events whose
`event_timestamp` is backdated by `[300, 1200]` seconds — 5 to 20 minutes in the past.
This places them outside the watermark tolerance of 2 minutes (or 5 minutes for the
lag-prone stream) and guarantees they arrive after the watermark has closed their
target window.

Key properties of the simulated late batch:

1. All late events carry valid `event_id` values (not duplicates of phase A).
2. All late events target the same video IDs as phase A, so their impact on aggregates
   is measurable.
3. The late batch volume is fixed at 20% of total scenario event count.
4. Late event `event_timestamp` offsets are deterministic from `seed` to ensure
   reproducible acceptance results.

---

## 4. How the Pipeline Handles It

### 4.1 Watermark policy

The realtime aggregator (`rt_content_events_aggregator`) configures a watermark on
`event_timestamp` with a lag tolerance of `2 minutes` for the content events stream.
Any event with `event_timestamp < (max_observed_event_timestamp - 2 minutes)` is
considered late at the time of processing.

```
watermark = max(event_timestamp seen so far) - 2 minutes
```

Phase B events arrive with `event_timestamp` that is 5–20 minutes behind the current
watermark. They are outside the allowed lateness window. Spark drops them from the
windowed aggregation — they do not contribute to `gold.rt_video_stats_1min`.

### 4.2 Bronze as the full audit log

Bronze (`lakehouse.bronze.raw_events`) is an append-only table with no deduplication
and no watermark filtering. Every event emitted by the generator — phase A and phase B
alike — is written to bronze at ingestion time, before any windowed aggregation occurs.

This means:

- Bronze row count = total events emitted (phase A + phase B)
- Gold row count reflects only on-time events that fell within open windows

The delta between bronze and gold is a precise, queryable measure of data dropped due
to late arrival.

### 4.3 No silent loss

Late events are not silently discarded at the Kafka consumer layer. They pass contract
validation (valid schema, valid `event_id`, known `video_id`) and land in bronze
normally. The drop happens only at the windowed aggregation step, and it is auditable
by comparing bronze vs. gold counts for the same `video_id` and time range.

Operators can query:

```sql
SELECT
    b.video_id,
    COUNT(*) AS bronze_count,
    SUM(g.impression_count) AS gold_impressions,
    COUNT(*) - SUM(g.impression_count) AS estimated_late_drop
FROM lakehouse.bronze.raw_events b
LEFT JOIN lakehouse.gold.rt_video_stats_1min g
    ON b.video_id = g.video_id
WHERE b.event_type = 'impression'
GROUP BY b.video_id
```

---

## 5. Acceptance Assertions

The following conditions must all hold after a `late_bulk_arrival` scenario run:

| # | Assertion | Rationale |
|---|-----------|-----------|
| 1 | `bronze.raw_events` row count = total emitted events (phase A + phase B) | Bronze is append-only, no drop |
| 2 | `gold.rt_video_stats_1min` aggregates contain only phase A events | Watermark correctly drops phase B |
| 3 | `(bronze count - gold aggregated count) / total emitted` is within 2% of configured late ratio | Drop volume matches expectation |
| 4 | No pipeline errors or task failures during phase B ingestion | Late events do not crash the job |
| 5 | Bronze `event_id` for phase B events is distinct from phase A (no duplicate IDs) | Late events are genuinely new events, not replays |

---

## 6. Engineering Decision Record

**Why 2 minutes for the watermark tolerance?**

The 1-minute gold trigger interval requires the watermark to close windows within a
bounded delay. A 2-minute tolerance allows for normal network jitter and minor Kafka
lag without holding state open indefinitely. Events arriving more than 2 minutes late
represent the mobile offline-buffer pattern and are accepted as data loss by design —
they are retained in bronze for potential backfill workflows.

**Why not extend the watermark to 20 minutes to capture all late events?**

A 20-minute watermark means every 1-minute window stays open for 20 additional minutes
before it is committed. This multiplies in-memory state by 20x and introduces a 20-minute
latency before any decision can be made. For a realtime decisioning system targeting
sub-3-minute metric freshness, that tradeoff is unacceptable.

**Why not replay late events through the batch path instead?**

The batch pipeline (D-1 processing) does pick up all bronze events including the late
arrivals. T+1 reconciliation between batch and realtime outputs would surface the
discrepancy. However, T+1 reconciliation is currently out of scope (see
`docs/architecture/realtime-decisioning/reconciliation-and-slo.md`). The design
intentionally accepts bounded data loss in the realtime path and relies on the batch
path as the authoritative historical record.

---

## 7. Linked Docs

1. `docs/architecture/generator/mock-event-generator-contract-and-scenario-matrix.md`
2. `docs/architecture/streaming/spark-realtime-jobs-contract.md`
3. `docs/architecture/realtime-decisioning/reconciliation-and-slo.md`
4. `docs/architecture/data-model/data-model-contract.md`
