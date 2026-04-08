# Adversarial Scenario: Late Arrival Reprocess

## 1. Purpose

Validate that the batch pipeline correctly detects and reprocesses D-1 events that arrive in bronze **after** the D-1 batch job has already completed.

This scenario tests **batch pipeline resilience**, not realtime watermark behavior.

---

## 2. The Real-World Story

A region's mobile clients go offline (poor signal, subway, device sleep) during a window of D-1. When connectivity is restored, the clients flush their buffered events. These events have `event_timestamp` values from D-1 but physically arrive in Kafka hours later — potentially after the 8 AM batch job has already processed D-1 and published metrics.

Without a detection and reprocess mechanism, these events are silently orphaned in bronze. The published D-1 metrics are permanently understated with no alert raised.

---

## 3. What This Is Not

- **Not a watermark scenario.** Spark watermarks govern realtime windowing. This scenario is about the batch path: daily Airflow DAG reading from bronze Iceberg.
- **Not a volume/throughput test.** The concern is correctness of D-1 metrics, not Kafka throughput.
- **Not a single generator run.** The scenario requires two generator runs split around the batch job execution.

---

## 4. Scenario Structure

### Two-Phase Execution

The phase separation lives in the **acceptance script**, not the generator. Each phase is a normal generator run with the same `started_at` (D-1).

```
Phase A (generator run 1)         Batch job runs          Phase B (generator run 2)
────────────────────────    ────────────────────────    ────────────────────────────
started_at = D-1            reads bronze D-1            started_at = D-1 (same)
normal volume               publishes metrics           smaller burst, late arrivals
event_timestamp = D-1       writes bronze snapshot      event_timestamp = D-1
        │                           │                           │
        ▼                           ▼                           ▼
  lands in bronze             manifest written            lands in bronze
  (D-1 partition)             row_count = N               (D-1 partition, delta)
                                                                │
                                                                ▼
                                                      detection sensor fires
                                                      backfill DAG triggered
                                                      D-1 reprocessed
                                                      final count = N + delta
```

### Phase A Config

```json
{
  "run_id": "late-arrival-phase-a-seed42",
  "seed": 42,
  "started_at": "2026-04-07T12:00:00Z",
  "duration_minutes": 30,
  "events_per_sec": 120,
  "adversarial_scenario": "late_arrival_reprocess",
  "scenario_params": {
    "phase": "A",
    "paired_run_id": "late-arrival-phase-b-seed42"
  }
}
```

### Phase B Config

```json
{
  "run_id": "late-arrival-phase-b-seed42",
  "seed": 42,
  "started_at": "2026-04-07T12:00:00Z",
  "duration_minutes": 10,
  "events_per_sec": 120,
  "adversarial_scenario": "late_arrival_reprocess",
  "scenario_params": {
    "phase": "B",
    "paired_run_id": "late-arrival-phase-a-seed42"
  }
}
```

Key: both runs share the same `started_at` (D-1). Phase B is smaller — it represents the late flush, not a full re-run.

---

## 5. New Components Required

### 5.1 Bronze Snapshot Manifest

After each successful batch run, write a snapshot entry to a manifest table recording the bronze row count for the processed date partition.

Proposed table: `lakehouse.qa.bronze_partition_manifest`

| Column | Type | Description |
|---|---|---|
| `event_date` | DATE | The processed partition date |
| `dag_run_id` | STRING | Airflow DAG run ID |
| `completed_at` | TIMESTAMP | When the batch job finished |
| `bronze_row_count` | BIGINT | Row count in bronze at completion time |

Written by a new task in `batch_publish_daily.py` at the end of the `publish-and-evidence` task group.

### 5.2 Late Arrival Detection Sensor

A new Airflow DAG (or sensor task) that runs periodically (e.g. every 30 minutes) and compares current bronze row counts per `event_date` against the manifest.

```
for each completed event_date in manifest:
    current_count = SELECT COUNT(*) FROM bronze WHERE event_date = ?
    manifest_count = SELECT bronze_row_count FROM manifest WHERE event_date = ?
    delta = current_count - manifest_count
    if delta > LATE_ARRIVAL_THRESHOLD:
        trigger backfill DAG for event_date
```

`LATE_ARRIVAL_THRESHOLD` — minimum row delta to trigger a reprocess (avoids false positives from micro-batches). Recommended default: 1000 rows.

### 5.3 Backfill DAG (already exists)

The existing Airflow backfill mechanism handles reprocessing once triggered. No changes required. The detection sensor passes `event_date` as a DAG run conf parameter.

---

## 6. Acceptance Script

`src/scripts/run_late_arrival_reprocess_acceptance.sh`

Steps:
1. Run Phase A generator (`late-arrival-phase-a-seed42`)
2. Drain Kafka and verify Phase A events land in bronze
3. Run D-1 batch job — produces metrics, writes bronze snapshot to manifest
4. Verify manifest entry written with correct row count
5. Run Phase B generator (`late-arrival-phase-b-seed42`)
6. Drain Kafka and verify Phase B events land in bronze (D-1 partition grows)
7. Wait for detection sensor to fire (or trigger manually in test)
8. Verify backfill DAG triggered for the correct `event_date`
9. Wait for backfill to complete
10. Verify final D-1 metrics = Phase A + Phase B combined counts

---

## 7. Acceptance Criteria

1. Phase A events land in bronze under correct D-1 partition: 100%
2. Bronze snapshot written to manifest after batch completes: row count matches bronze at that moment
3. Phase B events land in bronze under same D-1 partition: 100%
4. Detection sensor fires within one sensor interval after Phase B lands
5. Backfill DAG triggered for the correct `event_date`
6. Final D-1 metric row counts match Phase A + Phase B combined (within ±0.1%)
7. No Phase A metrics are corrupted or double-counted after backfill

---

## 8. Linked Docs

1. `docs/architecture/generator/mock-event-generator-contract-and-scenario-matrix.md`
2. `src/generator/bounded_run/adversarial.py`
3. `dags/batch_publish_daily.py`
