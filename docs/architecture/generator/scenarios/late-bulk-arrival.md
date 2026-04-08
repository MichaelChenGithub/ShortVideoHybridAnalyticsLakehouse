# Scenario: Peak Load (Bulk Arrival)

## 1. The Real-World Problem

Short-video platforms are not uniformly loaded. Traffic spikes sharply and predictably
around events: a trending meme, a sports final, a celebrity post going viral, a live
stream starting. Within seconds, impression and play_start events can jump 10–20x above
the baseline rate.

From the pipeline's perspective this looks like a burst of valid events all arriving at
roughly the same time on the same Kafka partitions. The questions this raises:

- Can Kafka absorb the throughput without dropping messages or stalling producers?
- Does the Spark streaming job keep up, or does consumer lag grow unbounded?
- Can bronze absorb a burst of concurrent Iceberg micro-batch commits without conflicts?

This is not a data correctness problem. All events are valid and on-time. It is purely
a **throughput and stability** test.

---

## 2. What the Scenario Simulates

The generator emits three phases in a single run:

**Phase 1 — Baseline:**
Normal traffic at configured `events_per_sec`. Establishes a steady-state baseline.
Duration: `baseline_duration_seconds` (default: 300s).

**Phase 2 — Burst:**
Traffic spikes to `burst_multiplier × events_per_sec` for `burst_duration_seconds`.
All events are valid, on-time, with normal `event_timestamp`.
Duration: `burst_duration_seconds` (default: 60s).

**Phase 3 — Recovery:**
Traffic returns to baseline `events_per_sec`. Tests that the pipeline recovers
and consumer lag drains within `recovery_timeout_seconds`.
Duration: `recovery_duration_seconds` (default: 300s).

```
events/sec
    │
    │              ┌──────────┐
    │              │  burst   │
    │              │  phase   │
    ├──────────────┤          ├────────────
    │   baseline   │          │  recovery
    │              │          │
    └──────────────┴──────────┴────────────▶ time
         Phase 1      Phase 2     Phase 3
```

---

## 3. Config

```json
{
  "run_id": "adv-peak-load-seed42",
  "seed": 42,
  "started_at": "2026-04-07T12:00:00Z",
  "duration_minutes": 30,
  "events_per_sec": 120,
  "adversarial_scenario": "peak_load",
  "scenario_params": {
    "burst_multiplier": 10,
    "burst_duration_seconds": 60,
    "baseline_duration_seconds": 300,
    "recovery_duration_seconds": 300,
    "recovery_timeout_seconds": 120
  }
}
```

`duration_minutes` governs the overall run envelope. The three phase durations must
sum to less than `duration_minutes × 60`.

---

## 4. How the Pipeline Handles It

### 4.1 Kafka

Kafka is the first stress point. The burst emits
`burst_multiplier × events_per_sec × burst_duration_seconds` messages in a short window.
Acceptance requires that **no messages are dropped** — verified by comparing producer-side
send count against consumer-side offset advancement for `content_events`.

### 4.2 Spark Streaming (Bronze write)

The Spark micro-batch job reads from Kafka and writes to bronze Iceberg. Under burst load
the micro-batch size grows. Acceptance requires that:
- No micro-batch failures or task retries
- Consumer lag recovers to near-zero within `recovery_timeout_seconds` after burst ends

### 4.3 Iceberg Commits

Concurrent micro-batch Iceberg commits can cause snapshot conflicts under high write
pressure. Acceptance requires zero commit-conflict errors in the Spark driver log during
the burst phase.

---

## 5. Acceptance Assertions

| # | Assertion | How to Verify |
|---|-----------|---------------|
| 1 | Total bronze row count = total emitted events (all three phases) | Compare generator summary vs `SELECT COUNT(*) FROM bronze` |
| 2 | No Kafka producer send failures | Generator summary `send_errors = 0` |
| 3 | Consumer lag returns to < 1000 messages within `recovery_timeout_seconds` | Query Kafka consumer group offsets |
| 4 | No Spark task failures during burst phase | Spark driver log scan |
| 5 | No Iceberg commit conflicts during burst phase | Spark driver log scan |

---

## 6. What This Does Not Test

- **Timestamp correctness.** All events are on-time. Late arrival behavior is covered by
  `late-arrival-reprocess.md`.
- **Deduplication.** Duplicate event handling is covered by `duplicate-event-storm.md`.
- **Schema enforcement.** Schema violations are covered by `schema-version-mismatch.md`.
- **Decisioning accuracy.** Events are all `normal_baseline` — no decisioning assertions.

---

## 7. Linked Docs

1. `docs/architecture/generator/mock-event-generator-contract-and-scenario-matrix.md`
2. `docs/architecture/messaging/kafka-topic-schema-retention-contract.md`
3. `docs/architecture/streaming/spark-realtime-jobs-contract.md`
