# AWS Deployment and Scale Benchmark

Status: Final

## 1. Purpose

Define the cloud deployment baseline and benchmark evidence contract for analytics-grade scale claims.

## 2. Baseline Architecture

Cloud baseline stack:

1. ingestion bus: Amazon MSK
2. processing: Spark (stream + batch)
3. orchestration: Airflow on Amazon ECS
4. storage: S3 + Iceberg catalog via Glue
5. serving/query: Trino/Athena semantic layer
6. quality layer: dbt Core

## 3. Benchmark Targets

1. sustained ingest rate target: `>= 5,000 events/sec`
2. peak ingest rate target: `>= 10,000 events/sec`
3. equivalent daily processed volume target: `>= 432M rows/day`
4. realtime freshness target: `P95 < 3 minutes`
5. batch publish readiness target: `D-1` outputs ready by `08:00` (`America/New_York`)

## 4. Benchmark Method

1. drive event generation at target throughput profile
2. run realtime + batch flows on cloud baseline stack
3. execute daily batch orchestration through Airflow on ECS
4. capture throughput, lag, freshness, and publish evidence
5. validate semantic/dbt quality gates for batch publish

## 5. Required Evidence Artifacts

1. benchmark run metadata (`run_id`, config, timeframe)
2. ingest throughput timeline and peak snapshots
3. daily processed volume counts
4. freshness/publish-timeliness measurements
5. quality-gate outcomes for publish windows
6. concise run summary suitable for resume/interview proof points

## 6. Boundary and Future Plan

1. current scope demonstrates benchmark evidence and architecture readiness
2. long-running cost optimization and autoscaling policies are future plan items
3. canonical deferred-scope reference: `../../milestone/future-plan.md`
