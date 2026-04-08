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

Table: `lakehouse.qa.bronze_partition_manifest`

```sql
CREATE TABLE IF NOT EXISTS lakehouse.qa.bronze_partition_manifest (
    event_date       DATE,
    dag_run_id       VARCHAR,
    completed_at     TIMESTAMP,
    bronze_row_count BIGINT
)
WITH (
    partitioning = ARRAY['days(event_date)']
)
```

Design decisions:

- **Append-only (`INSERT INTO`)** — one row per DAG run, never upserted. In Iceberg/Trino, `INSERT INTO` always appends; there is no overwrite. `batch_publish_daily` can run multiple times for the same date (backfills), so multiple rows per `event_date` are expected. The detection query uses `MAX(bronze_row_count)` over the latest completed run per `event_date` as the baseline. Full reprocessing history is preserved.
- **`bronze_row_count` sourced from Iceberg partition metadata** — the row count is read from `"lakehouse"."bronze"."raw_events$partitions"` (Iceberg metadata), not from `COUNT(*)` on the table. This is a metadata-only read — O(partitions), not O(rows) — consistent with the detection query source of truth.
- **Written via Trino** — the manifest is a single-row metadata insert, not a distributed data processing job. A Trino client call from the Airflow task is sufficient; no Spark job needed. Table is created with `CREATE TABLE IF NOT EXISTS` inline on first write — idempotent, no separate migration.
- **Written after merge_coordinator succeeds** — placed as a new task at the end of the `publish-and-evidence` task group in `batch_publish_daily.py`, after `emit_publish_ready`. The manifest row marks "D-1 is published and this was the bronze partition record count at that moment."
- **Partitioned by `event_date`** — every query against the manifest filters by date.

Manifest write (Trino):

```sql
-- read bronze record count for the target date from Iceberg partition metadata
SELECT SUM(record_count) AS bronze_row_count
FROM "lakehouse"."bronze"."raw_events$partitions"
WHERE partition.event_date = DATE '{event_date}'

-- append one row to the manifest
INSERT INTO lakehouse.qa.bronze_partition_manifest
    (event_date, dag_run_id, completed_at, bronze_row_count)
VALUES (DATE '{event_date}', '{dag_run_id}', TIMESTAMP '{completed_at}', {bronze_row_count})
```

### 5.2 Late Arrival Detection Sensor

A new Airflow DAG (`dags/late_arrival_sensor.py`) on a 30-minute schedule that compares current bronze partition record counts against the latest manifest baseline per `event_date` and triggers `batch_backfill` for any date exceeding the threshold.

Both sides of the comparison read from **Iceberg partition metadata** (`$partitions`), not from the data files. This makes the detection query a metadata-only operation — O(partitions), not O(rows) — safe to run frequently at any data scale.

Detection query (runs via Trino):

```sql
SELECT m.event_date
FROM (
    SELECT event_date, MAX(bronze_row_count) AS baseline
    FROM lakehouse.qa.bronze_partition_manifest
    GROUP BY event_date
) m
JOIN (
    SELECT partition.event_date AS event_date, SUM(record_count) AS current_count
    FROM "lakehouse"."bronze"."raw_events$partitions"
    GROUP BY partition.event_date
) b ON b.event_date = m.event_date
WHERE b.current_count - m.baseline > :late_arrival_threshold
```

`LATE_ARRIVAL_THRESHOLD` — minimum row delta to trigger a reprocess (avoids false positives from micro-batches). Default: 1000 rows, configurable via Airflow Variable `LATE_ARRIVAL_THRESHOLD`.

For each `event_date` returned, the sensor writes a row to `lakehouse.qa.late_arrival_trigger_log` then triggers `batch_backfill` with `conf={"start_date": event_date, "end_date": event_date}`.

**Idempotency — trigger log suppression:**

The sensor can fire multiple times while a backfill is still running. Without a guard, the same `event_date` would be re-triggered every 30 minutes until the backfill completes. The trigger log prevents this.

Table: `lakehouse.qa.late_arrival_trigger_log`

```sql
CREATE TABLE IF NOT EXISTS lakehouse.qa.late_arrival_trigger_log (
    event_date   DATE,
    triggered_at TIMESTAMP(6)
)
WITH (
    partitioning = ARRAY['days(event_date)']
)
```

The detection query includes a suppression clause:

```sql
SELECT m.event_date
FROM (
    SELECT event_date, MAX(bronze_row_count) AS baseline, MAX(completed_at) AS last_completed_at
    FROM lakehouse.qa.bronze_partition_manifest
    GROUP BY event_date
) m
JOIN (
    SELECT partition.event_date AS event_date, SUM(record_count) AS current_count
    FROM "lakehouse"."bronze"."raw_events$partitions"
    GROUP BY partition.event_date
) b ON b.event_date = m.event_date
LEFT JOIN (
    SELECT event_date, MAX(triggered_at) AS last_triggered_at
    FROM lakehouse.qa.late_arrival_trigger_log
    GROUP BY event_date
) t ON t.event_date = m.event_date
WHERE b.current_count - m.baseline > :late_arrival_threshold
  AND (t.last_triggered_at IS NULL OR t.last_triggered_at < m.last_completed_at)
```

Suppression lifecycle:
- **Suppressed** once a trigger row is written (`last_triggered_at > last_completed_at`) — no re-trigger while backfill runs
- **Re-enabled** automatically when `batch_publish_daily` writes a new manifest row after backfill completes (`last_completed_at` advances past `last_triggered_at`)
- **Handles backfill failure** safely — if the backfill never completes, the suppression stays active indefinitely rather than hammering a broken pipeline

**Acceptance testing note:** the sensor fires on a 30-minute schedule. In acceptance testing, trigger the sensor DAG manually after Phase B drains rather than waiting for the schedule interval.

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
