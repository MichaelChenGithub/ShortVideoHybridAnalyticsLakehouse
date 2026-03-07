# Streaming Contracts

This folder contains Spark Structured Streaming execution contracts.

## Current Specs

1. `spark-realtime-jobs-contract-m1.md`

## MIC-37 Bring-up Commands

Scope here is limited to MIC-37: stable `rt_video_cdc_upsert` for `cdc.content.videos`, checkpoint path validation, and `dim_videos` insert/update health checks.

Runtime defaults locked for MIC-37:

1. `startingOffsets=latest`
2. trigger interval `1 minute`
3. checkpoint `s3a://checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1`

### 1. Start local services

```bash
docker compose up -d minio minio-mc iceberg-rest zookeeper kafka spark
```

### 2. Ensure CDC topic exists

```bash
docker exec lakehouse-kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic cdc.content.videos --partitions 3 --replication-factor 1
```

### 3. Start MIC-37 Spark CDC upsert job

```bash
docker exec lakehouse-spark bash -lc "/opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_video_cdc_upsert.py"
```

Wait about 30 seconds after startup before producing validation fixtures.

Fail-fast schema note:

1. If `dim_videos` is missing required columns, startup fails and prints exact manual `ALTER TABLE` commands to run before restart.

### 4. Emit bounded-run traffic and deterministic CDC fixture

```bash
python3 src/generator/m1_bounded_run.py --config docs/architecture/generator/examples/m1_run_config.example.json --sink kafka --bootstrap-servers localhost:9092
BASE_TS_MS=$(( $(date +%s) * 1000 ))
EXPECTED_SOURCE_TS_MS=$((BASE_TS_MS + 2000))
python3 src/scripts/emit_cdc_videos_fixture.py --bootstrap-servers localhost:9092 --video-id mic37_vid_001 --scenario full --base-ts-ms "$BASE_TS_MS"
```

Because trigger cadence is `1 minute`, wait at least 75 seconds after fixture emission before running verifier.

### 5. Verify upsert health and deterministic final state

`full` scenario expected final state:

1. `status=copyright_strike`
2. `source_ts_ms=$EXPECTED_SOURCE_TS_MS`

```bash
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_video_cdc_upsert.py --video-id mic37_vid_001 --max-freshness-minutes 10 --expect-status copyright_strike --expect-source-ts-ms "$EXPECTED_SOURCE_TS_MS"
```

### 6. Validate checkpoint path

```bash
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_video_cdc_upsert/dim_videos/v1 | head -n 40"
```

## MIC-40 Bring-up Commands

Scope here is limited to MIC-40: bring up `rt_content_events_aggregator` for `content_events` with writes to `bronze.raw_events` and `gold.rt_video_stats_1min` plus checkpoint validation for both sinks.

Runtime defaults locked for MIC-40:

1. `startingOffsets=latest`
2. trigger intervals: `raw_events=10 seconds`, `rt_video_stats_1min=1 minute`
3. checkpoints:
   - `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1`
   - `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1`

Scope note:

1. Historical MIC-40 note: `invalid_events_content` sink/checkpoint were deferred in that ticket and are implemented by MIC-39.
2. For MIC-39 content contract enforcement and quarantine validation, use the section below.

### 1. Run one-command MIC-40 acceptance flow

```bash
bash src/scripts/run_mic40_acceptance.sh
```

### 2. Equivalent manual flow

```bash
docker compose up -d minio minio-mc iceberg-rest zookeeper kafka spark
docker exec lakehouse-kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic content_events --partitions 6 --replication-factor 1
docker exec lakehouse-spark bash -lc "/opt/spark/bin/spark-submit /home/iceberg/local/src/spark/rt_content_events_aggregator.py"
python3 src/generator/m1_bounded_run.py --config docs/architecture/generator/examples/m1_run_config.example.json --sink kafka --bootstrap-servers localhost:9092
```

Wait at least 75 seconds after bounded-run emission so the 1-minute Gold trigger can commit output.

### 3. Verify Bronze/Gold health and checkpoint evidence

```bash
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_aggregator.py --min-raw-rows 1 --min-gold-rows 1 --max-freshness-minutes 10
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1 | head -n 40"
```

## MIC-39 Contract Enforcement Commands

Scope here is limited to MIC-39: enforce `content_events` schema contract, route invalid records to `lakehouse.bronze.invalid_events_content`, and keep valid `bronze.raw_events` + `gold.rt_video_stats_1min` flow healthy.

Runtime defaults locked for MIC-39:

1. `startingOffsets=latest`
2. trigger intervals: `raw_events=10 seconds`, `rt_video_stats_1min=1 minute`, `invalid_events_content=10 seconds`
3. checkpoints:
   - `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1`
   - `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1`
   - `s3a://checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1`
4. acceptance gates:
   - `min_invalid_rows=1`
   - `max_invalid_rate=0.20`
   - `max_freshness_minutes=10`

Out-of-scope reminder:

1. CDC contract enforcement for `cdc.content.videos` is tracked separately (MIC-43).

### 1. Run one-command MIC-39 acceptance flow

```bash
bash src/scripts/run_mic39_acceptance.sh
```

### 2. Equivalent manual verification command

Capture a run boundary timestamp before emission:

```bash
MIN_INGESTED_AT_MS=$(( $(date +%s) * 1000 ))
```

Then run verifier:

```bash
docker exec lakehouse-spark python /home/iceberg/local/src/scripts/verify_rt_content_events_contract_enforcement.py \
  --min-raw-rows 1 \
  --min-gold-rows 1 \
  --min-invalid-rows 1 \
  --max-invalid-rate 0.20 \
  --max-freshness-minutes 10 \
  --min-ingested-at-ms "$MIN_INGESTED_AT_MS"
```

### 3. Validate checkpoint evidence for all three sinks

```bash
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/raw_events/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/rt_video_stats_1min/v1 | head -n 40"
docker exec lakehouse-minio sh -lc "ls -R /data/checkpoints/jobs/spark_rt_content_events_aggregator/invalid_events_content/v1 | head -n 40"
```
