# Scenario: Schema Version Mismatch

> **Status: Draft — Not Finalized**
> Design and acceptance criteria are under discussion. Do not implement against this doc yet.

## 1. The Real-World Problem

No mobile app update is instantaneous. When an engineering team ships a new version of
the short-video client — one that introduces a new event type, renames a field, adds
required payload attributes, or changes the schema version identifier — that update
rolls out to users progressively over days or weeks through the App Store and Google
Play.

During the rollout window, two populations of clients coexist:

- **Old clients** on the previous app version, producing events with the current schema
  (e.g., `schema_version: "m1_v1"`).
- **New clients** on the new app version, producing events with the updated schema
  (e.g., `schema_version: "m1_v2"`), which may include new fields, new event types,
  or modified payload structure.

Both populations produce to the same Kafka topic (`content_events`). The pipeline
receives an interleaved stream of `m1_v1` and `m1_v2` events in the same partition,
indistinguishable by their Kafka metadata — only the payload reveals which schema
version an event uses.

This is not a hypothetical. Every major feature launch at a platform of any scale
produces this pattern. If the pipeline does not handle unknown schema versions
gracefully, it has two bad failure modes:

**Failure mode 1 — hard crash.**
The job encounters a field it does not expect, throws a parsing exception, and the
entire Spark streaming job fails. Processing halts for all videos, not just those
sending the new schema. Every video's decision pipeline goes dark until the job
is restarted or redeployed.

**Failure mode 2 — silent corruption.**
The job attempts to parse `m1_v2` events using the `m1_v1` schema, misreads fields,
and writes incorrect data to the aggregation table. A new event type gets mapped to
a wrong category, or a renamed field produces null values, or an engagement metric is
double-counted. The pipeline appears healthy while silently producing wrong numbers.

Both failure modes are worse than the problem they stem from. A pipeline that handles
schema evolution correctly must tolerate unknown versions without crashing and without
corrupting known-good data.

---

## 2. Why Schema Evolution Is an Operational Reality, Not an Exception

Teams often assume that schema changes will be coordinated: the pipeline team will
deploy support for `m1_v2` before the mobile team ships `m1_v2` clients. In practice,
this coordination breaks down for several reasons:

**Staged rollout overlap.** Even if the pipeline is updated on day zero of the app
release, the old app version lingers on devices that have auto-update disabled. A
pipeline that only supports `m1_v2` would then fail on `m1_v1` events from legacy
devices — the mirror image of the same problem.

**Hotfixes skip coordination.** An urgent client-side bug fix ships with a schema
change because it is bundled with a feature that required new fields. The pipeline
team learns about the new schema version when events start arriving.

**A/B testing the schema itself.** Some platforms experiment with event schemas — a
new richer payload for treatment users, the old schema for control users. In this
case, both schema versions are intentionally coexisting by design.

In all of these cases, the pipeline must be robust to schema versions it has not seen
before. The engineering contract is: **unknown schema versions are quarantined, not
crashed on, and not silently accepted.**

---

## 3. What the Scenario Simulates

The `schema_version_mismatch` scenario emits a stream of events where 25% carry an
unknown schema version (`m1_v2`) interleaved with 75% normal `m1_v1` events.

The `m1_v2` events are structurally designed to represent a realistic forward-incompatible
change. They include a new required field (`content_format`) that does not exist in the
`m1_v1` schema, and they use a new event type (`repost`) that is not in the current
`ALLOWED_EVENT_TYPES` set.

Key properties of the simulated mismatch:

1. `m1_v2` events have valid Kafka structure (valid JSON, valid key) — they are not
   malformed at the transport layer. The incompatibility is semantic, not syntactic.
2. `m1_v2` events are distributed across the full emission window, not concentrated
   in a burst. This simulates gradual rollout overlap rather than a sudden cutover.
3. The affected video IDs overlap with `normal_baseline` videos so the impact on
   expected decisions can be verified: videos that lose 25% of their events to
   quarantine should still produce `NO_ACTION` decisions (not accidentally cross
   a BOOST threshold due to corrupted counts).
4. The `m1_v1` events in the same run are otherwise normal — they should be processed
   without any degradation.

---

## 4. How the Pipeline Handles It

### 4.1 Contract validation at the aggregator boundary

The realtime aggregator (`rt_content_events_aggregator`) validates every incoming event
against the schema contract before any processing occurs. The validation layer
(`rt_content_events_validation.py`) checks:

1. `schema_version` is present and is a known, supported version.
2. `event_type` is present and is in `ALLOWED_EVENT_TYPES`.
3. All required fields (`event_id`, `video_id`, `user_id`, `event_timestamp`) are
   non-null and correctly typed.
4. `payload_json` is valid JSON.

An event that fails any of these checks is routed to `lakehouse.bronze.invalid_events_content`
with a structured error code. It never reaches the window aggregation or the gold layer.

For `m1_v2` events in this scenario, the validation fails at check 1
(`UNKNOWN_SCHEMA_VERSION`) and at check 2 (`UNKNOWN_EVENT_TYPE` for `repost` events).
Both error codes are recorded in the quarantine table alongside the original raw payload.

### 4.2 Pipeline continues on valid events

Quarantine routing is per-event, not per-batch. A microbatch that contains 1000 events
— 750 valid `m1_v1` and 250 invalid `m1_v2` — processes the 750 valid events normally
and routes the 250 to quarantine. The job does not stop, retry the batch, or enter a
degraded state.

```
Kafka microbatch (1000 events)
    ↓
contract validation
    ├── 750 pass → window aggregation → gold.rt_video_stats_1min
    └── 250 fail → bronze.invalid_events_content (with UNKNOWN_SCHEMA_VERSION)
```

### 4.3 Quarantine table is queryable for ops diagnosis

`lakehouse.bronze.invalid_events_content` stores the full original event payload
alongside the rejection reason code. When the pipeline team is ready to add `m1_v2`
support, they can query the quarantine table to inspect real `m1_v2` events, understand
the actual schema structure that arrived in production, and design the contract extension
before deploying it.

This makes the quarantine table an operational tool, not just a discard bin.

### 4.4 No partial schema adoption

The pipeline does not attempt best-effort parsing of unknown schema versions. It does
not try to extract `video_id` and `event_type` from `m1_v2` events on the assumption
that those fields might still be present. This policy is intentional:

A future schema version may change field semantics, not just structure. An `impression`
event in `m1_v2` might be defined differently than in `m1_v1` — for example, counting
only 3-second views instead of any render. Silently accepting `m1_v2` events using
`m1_v1` semantics would mix incompatible definitions in the same aggregate, producing
metrics that are wrong in a way that is very difficult to detect.

---

## 5. Acceptance Assertions

The following conditions must all hold after a `schema_version_mismatch` scenario run:

| # | Assertion | Rationale |
|---|-----------|-----------|
| 1 | `bronze.invalid_events_content` contains rows with `error_code = 'UNKNOWN_SCHEMA_VERSION'` equal to the emitted `m1_v2` count | All unknown-version events are quarantined |
| 2 | `bronze.raw_events` contains only events that passed schema validation | Quarantined events do not appear in the valid raw log |
| 3 | `gold.rt_video_stats_1min` aggregates for affected `video_id` values reflect only `m1_v1` event counts | No `m1_v2` data leaks into aggregates |
| 4 | Expected `NO_ACTION` decisions for `normal_baseline` videos are preserved | Loss of 25% of events to quarantine does not cause false positives |
| 5 | No Spark job failures, task retries exceeding normal threshold, or checkpoint errors during `m1_v2` ingestion | Pipeline is operationally stable throughout |

### 5.1 Quarantine inspection query

```sql
-- Verify quarantine captured all unknown-version events
SELECT
    error_code,
    COUNT(*) AS quarantine_count,
    MIN(event_timestamp) AS earliest,
    MAX(event_timestamp) AS latest
FROM lakehouse.bronze.invalid_events_content
WHERE run_id = '<run_id>'
GROUP BY error_code;

-- Confirm no m1_v2 events leaked into valid bronze
SELECT schema_version, COUNT(*) AS count
FROM lakehouse.bronze.raw_events
WHERE run_id = '<run_id>'
GROUP BY schema_version;
```

---

## 6. Engineering Decision Record

**Why quarantine instead of a dead-letter queue (DLQ) topic?**

A Kafka DLQ topic requires the consuming application to route rejected events back to
a second Kafka producer, adding network I/O and retry complexity inside the streaming
job. An Iceberg quarantine table has two advantages: it is queryable with SQL directly
by the pipeline team and data analysts without requiring access to a Kafka consumer,
and it is co-located with the rest of the pipeline's data artifacts so standard data
access controls apply uniformly.

**Why not use a Confluent Schema Registry to enforce schema compatibility at the
producer?**

A Schema Registry with backward-compatibility enforcement would reject `m1_v2` events
at the producer before they reach Kafka — making this pipeline-level handling
unnecessary. This is the correct long-term architecture at scale. However, the Schema
Registry is a shared infrastructure component that requires coordination across mobile
app teams, backend teams, and data teams. In the current scope, the pipeline takes a
defensive posture: it assumes the upstream cannot guarantee schema enforcement and
handles the mismatch at the consumer boundary. Adding a Schema Registry is called out
as a future infrastructure improvement in
`docs/architecture/messaging/kafka-topic-schema-retention-contract.md`.

**Why not support multiple schema versions simultaneously?**

Supporting multiple active schema versions requires maintaining multiple parsing code
paths and multiple sets of field semantics in the aggregation logic. This increases
complexity, makes the code harder to reason about, and creates risk of metric
definition drift across versions. The simpler contract — one active schema version,
unknown versions go to quarantine — keeps the aggregation logic clean and makes schema
migration a conscious, gated operation rather than an emergent accumulation of
compatibility shims.

---

## 7. Linked Docs

1. `docs/architecture/generator/mock-event-generator-contract-and-scenario-matrix.md`
2. `docs/architecture/streaming/spark-realtime-jobs-contract.md`
3. `docs/architecture/messaging/kafka-topic-schema-retention-contract.md`
4. `docs/architecture/streaming/reference/content-contract-acceptance.md`
