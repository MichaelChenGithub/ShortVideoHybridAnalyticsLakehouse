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
