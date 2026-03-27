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
| Zookeeper + Kafka | MSK Serverless | Managed Kafka; Zookeeper dropped; IAM auth only (port 9098) |
| Spark (realtime `rt_*.py`) | EMR Serverless | One application; scripts submitted as long-running streaming job runs; no code changes required |
| Spark (batch `bt_*.py`) | EMR Serverless | Same application as realtime; submitted as batch job runs by Airflow; no GlueContext wrapper required |
| Airflow | ECS Task | boto3 EMR Serverless client replaces `docker exec` pattern; no GlueJobOperator needed |
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
  iam.tf         — IAM roles: ECS task execution, EMR Serverless execution, MSK client, Airflow task
  messaging.tf   — MSK Serverless cluster and security group rules
  compute.tf     — EMR Serverless application (all Spark work: rt_*.py + bt_*.py); ECS cluster for Airflow and Metabase only; Airflow task definition; Metabase Fargate service
  outputs.tf     — MSK broker endpoint, S3 bucket names, ECS cluster ARN, EMR Serverless app ID, Athena workgroup
```

Note: `batch.tf` is eliminated. EMR Serverless replaces both EC2-backed ECS (Spark streaming) and Glue ETL (Spark batch) — all Spark work runs through the single EMR Serverless application in `compute.tf`. `bt_*.py` scripts require no GlueContext wrapper.

## 4. Application Code Changes

The following files require changes when moving from local to AWS. `rt_*.py` streaming scripts are unchanged.

| File | Change Required |
|---|---|
| `spark-defaults.conf` | Switch catalog impl to `org.apache.iceberg.aws.glue.GlueCatalog`; remove MinIO S3A settings (IAM-based auth on AWS) |
| `src/orchestration/airflow_batch_tasks.py` | Replace `docker exec spark ...` with boto3 EMR Serverless `start_job_run` + polling; `SPARK_BATCH_SPECS` maps job keys to S3 script paths |
| `bt_*.py` batch scripts | No changes required — EMR Serverless runs native Spark; no GlueContext wrapper needed |
| `src/trino/*.sql` (serving views) | Adjust Presto/Athena syntax differences from Trino |
| `Dockerfile-airflow` | Add `dbt-core` + `dbt-athena-community` + `boto3` to image |
| `profiles.yml` (new) | Add Athena profile: S3 results bucket, Glue catalog DB, region — consumed by `dbt test` inside Airflow task |

## 5. Architecture Diagram

```
MSK Serverless (Kafka, SASL/IAM)
  → EMR Serverless (rt_*.py streaming job runs)  ─┐
                                                   ├→ S3 (warehouse/)
Airflow (ECS) → EMR Serverless (bt_*.py batch)  ──┘
                                                   ↓
                                      Glue Data Catalog (Iceberg metastore)
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
| MIC-196 (messaging) | `messaging.tf` | MSK Serverless cluster and security group rules |
| MIC-197 (compute) | `compute.tf` | EMR Serverless application; ECS cluster for Airflow + Metabase; Airflow task def; Metabase Fargate service |
| MIC-159-C (outputs) | `outputs.tf` | Stack outputs: MSK endpoint, S3 buckets, ECS cluster ARN, EMR app ID, Athena workgroup |

## 7. Benchmark Targets (from aws-deployment-and-scale-benchmark.md)

These targets are unchanged and drive sizing decisions:

- Sustained ingest: `>= 5,000 events/sec`
- Peak ingest: `>= 10,000 events/sec`
- Equivalent daily volume: `>= 432M rows/day`
- Realtime freshness: `P95 < 3 minutes`
- Batch publish readiness: `D-1` outputs by `08:00 America/New_York`
