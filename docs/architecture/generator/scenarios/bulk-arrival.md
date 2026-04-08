# Scenario: Bulk Arrival (Peak Load)

## 1. The Real-World Problem

Short-video platforms are not uniformly loaded. Traffic spikes sharply around events: a
trending meme, a sports final, a celebrity post going viral, a live stream starting.
Within seconds, impression and play_start events can jump 10–20x above the baseline rate.

The naive answer is to scale up — more Kafka partitions, more Spark executors. But
dynamic scaling has latency (minutes, not seconds) and cost. The engineering question
is not "can we scale?" but **"can the pipeline stay stable and self-recover during the
spike without infrastructure changes?"**

This scenario validates that answer by demonstrating two concrete optimization strategies.

---

## 2. Optimization Strategies Under Test

### 2.1 Spark Backpressure via `maxOffsetsPerTrigger`

Without a per-trigger read limit, a 10x spike causes Spark to attempt a massive
micro-batch — long GC pause, potential OOM, task failures. With `maxOffsetsPerTrigger`
configured, Spark reads at most N messages per trigger regardless of how many are
available in Kafka.

The trade-off is deliberate: **bounded lag over unbounded resource consumption**.
Consumer lag builds intentionally during the burst, then drains gradually as the spike
subsides. The pipeline stays stable without scaling.

```
Without maxOffsetsPerTrigger:
  spike → Spark reads full backlog → OOM / task failure → pipeline stall

With maxOffsetsPerTrigger:
  spike → Spark reads N msg/trigger → lag builds → spike subsides → lag drains
```

### 2.2 Iceberg Small File Compaction

During a burst, each micro-batch creates a new Iceberg snapshot with new small data
files. A 10x spike at a 5-second trigger interval produces ~12 extra snapshots in 60
seconds. Post-burst, write amplification in bronze degrades batch read performance.

The optimization is scheduled Iceberg **compaction** after the burst. Without it, batch
job scan time grows proportionally to small file count. With it, file counts stay bounded
and read performance is stable.

---

## 3. What the Scenario Simulates

Three phases in a single generator run. All events are valid and on-time.

**Phase 1 — Baseline:**
Normal traffic at `events_per_sec`. Establishes steady-state consumer lag near zero.
Duration: `baseline_duration_seconds` (default: 300s).

**Phase 2 — Burst:**
Traffic spikes to `burst_multiplier × events_per_sec` for `burst_duration_seconds`.
Duration: `burst_duration_seconds` (default: 60s).

**Phase 3 — Recovery:**
Traffic returns to baseline. Lag drains. Compaction runs.
Duration: `recovery_duration_seconds` (default: 300s).

```
events/sec
    │
    │              ┌──────────┐
    │              │  burst   │
    │              │  10x     │
    ├──────────────┤          ├────────────
    │   baseline   │          │  recovery
    │                              + compaction
    └──────────────┴──────────┴────────────▶ time
         Phase 1      Phase 2     Phase 3
```

---

## 4. Config

```json
{
  "run_id": "adv-bulk-arrival-seed42",
  "seed": 42,
  "started_at": "2026-04-07T12:00:00Z",
  "duration_minutes": 30,
  "events_per_sec": 120,
  "adversarial_scenario": "bulk_arrival",
  "scenario_params": {
    "burst_multiplier": 10,
    "burst_duration_seconds": 60,
    "baseline_duration_seconds": 300,
    "recovery_duration_seconds": 300,
    "max_lag_threshold": 50000,
    "recovery_timeout_seconds": 120
  }
}
```

`duration_minutes` governs the overall run envelope. The three phase durations must sum
to less than `duration_minutes × 60`.

---

## 5. Acceptance Assertions

| # | Assertion | What It Proves |
|---|-----------|----------------|
| 1 | Consumer lag stays below `max_lag_threshold` during burst | `maxOffsetsPerTrigger` is tuned correctly |
| 2 | Lag fully drains within `recovery_timeout_seconds` after burst | Backpressure strategy self-recovers without intervention |
| 3 | No Spark task failures during burst phase | Bounded batch size prevents OOM |
| 4 | Total bronze row count = total emitted events | No messages dropped at any layer |
| 5 | Bronze Iceberg file count post-compaction within bounds | Compaction strategy prevents read degradation |

---

## 6. What This Does Not Test

- **Timestamp correctness.** All events are on-time. Late arrival behavior is in `late-arrival-reprocess.md`.
- **Deduplication.** Covered by `duplicate-event-storm.md`.
- **Schema enforcement.** Covered by `schema-version-mismatch.md`.
- **Decisioning accuracy.** All events are `normal_baseline` — no action assertions.

---

## 7. Linked Docs

1. `docs/architecture/generator/mock-event-generator-contract-and-scenario-matrix.md`
2. `docs/architecture/messaging/kafka-topic-schema-retention-contract.md`
3. `docs/architecture/streaming/spark-realtime-jobs-contract.md`
4. `spark-defaults.conf` — `maxOffsetsPerTrigger` setting
