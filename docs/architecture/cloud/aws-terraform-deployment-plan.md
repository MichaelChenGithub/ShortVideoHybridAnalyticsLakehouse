# AWS Terraform Deployment Plan

Status: Draft

## 1. Purpose

Define the Terraform-based provisioning plan for the M2 AWS cloud deployment. This document captures the agreed service mapping from local docker-compose to AWS-managed services, the Terraform file structure, and the change surface in application code.

This document is the implementation reference for MIC-159 and its sub-issues.

## 2. Service Mapping

| Local (docker-compose) | AWS Service | Notes |
|---|---|---|
| MinIO | S3 | Direct replacement; endpoint config change only |
| catalog-postgres + iceberg-rest | Glue Data Catalog | Replaces REST catalog + RDS entirely |
| Zookeeper + Kafka | MSK | Managed Kafka; Zookeeper dropped |
| Spark (realtime `rt_*.py`) | ECS Service (EC2-backed) | `tabulario/spark-iceberg` image; scripts unchanged |
| Spark (batch `bt_*.py`) | Glue ETL Jobs | Thin GlueContext wrapper required |
| Airflow | ECS Task | GlueJobOperator replaces `docker exec` pattern |
| Trino | Athena | Serverless; minor SQL syntax adjustments |
| Metabase | ECS Task (Fargate) | Connects to Athena via JDBC driver |
| Grafana | — | Skipped |
| dbt Core | runs inside Airflow ECS task | No separate service; `dbt-athena-community` adapter connects to Athena; install in Airflow Docker image |

## 3. Terraform File Structure

```
terraform/
  main.tf        — provider (aws), S3+DynamoDB state backend, shared variables
  network.tf     — VPC, public+private subnets, IGW, NAT gateway, route tables, security groups
  storage.tf     — S3 buckets (warehouse, checkpoints), Glue Data Catalog database
  iam.tf         — IAM roles: ECS task execution, Glue job, MSK client, Airflow task
  messaging.tf   — MSK cluster (1 broker, kafka.m5.large)
  compute.tf     — ECS cluster; task definitions + services: Spark streaming, Airflow, Metabase
  batch.tf       — Glue ETL job definitions (one per bt_*.py script)
  outputs.tf     — MSK broker endpoints, S3 bucket names, ECS cluster ARN, Athena workgroup
```

## 4. Application Code Changes

The following files require changes when moving from local to AWS. `rt_*.py` streaming scripts are unchanged.

| File | Change Required |
|---|---|
| `spark-defaults.conf` | Switch catalog impl to `org.apache.iceberg.aws.glue.GlueCatalog`; update S3 endpoint to real AWS |
| `src/orchestration/airflow_batch_tasks.py` | Replace `docker exec spark ...` with `GlueJobOperator` calls |
| `bt_*.py` batch scripts | Add `GlueContext` init wrapper per Glue ETL job requirements |
| `src/trino/*.sql` (serving views) | Adjust Presto/Athena syntax differences from Trino |
| `Dockerfile-airflow` | Add `dbt-core` + `dbt-athena-community` to image |
| `profiles.yml` (new) | Add Athena profile: S3 results bucket, Glue catalog DB, region — consumed by `dbt test` inside Airflow task |

## 5. Architecture Diagram

```
MSK (Kafka)
  → ECS Service (Spark Streaming, tabulario/spark-iceberg)  ─┐
                                                              ├→ S3 (warehouse/)
Airflow (ECS) → Glue ETL Jobs (bt_*.py)  ────────────────────┘
                                                              ↓
                                               Glue Data Catalog
                                                              ↓
                                                          Athena
                                                              ↓
                                               Metabase (ECS Fargate)
```

## 6. Terraform Issue Breakdown

Due to scope exceeding the original `<= 5 files / <= 500 LOC` guardrail, work is split across a parent and three sub-issues:

| Issue | Files | Scope |
|---|---|---|
| MIC-159 (baseline) | `main.tf`, `network.tf`, `storage.tf`, `iam.tf` | VPC, S3, Glue Catalog, IAM — foundation for all downstream |
| MIC-159-A (messaging) | `messaging.tf` | MSK cluster and security group rules |
| MIC-159-B (compute) | `compute.tf` | ECS cluster; Spark streaming service, Airflow task, Metabase task |
| MIC-159-C (batch) | `batch.tf`, `outputs.tf` | Glue ETL job definitions and stack outputs |

## 7. Benchmark Targets (from aws-deployment-and-scale-benchmark.md)

These targets are unchanged and drive sizing decisions:

- Sustained ingest: `>= 5,000 events/sec`
- Peak ingest: `>= 10,000 events/sec`
- Equivalent daily volume: `>= 432M rows/day`
- Realtime freshness: `P95 < 3 minutes`
- Batch publish readiness: `D-1` outputs by `08:00 America/New_York`
