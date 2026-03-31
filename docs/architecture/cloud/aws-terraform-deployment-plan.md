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
| MIC-198 (outputs) | `outputs.tf` | Athena workgroup + all cross-stack outputs: MSK endpoint, S3 buckets, Glue DB, ECS cluster ARN, EMR app ID, Athena workgroup |

## 7. Operational Runbook

### One-time bootstrap (do once per account)

```bash
# Create Terraform state backend resources
aws s3 mb s3://short-video-lakehouse-tf-state --region us-east-1
aws dynamodb create-table \
  --table-name short-video-lakehouse-tf-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST --region us-east-1

# Create ECR repository
aws ecr create-repository --repository-name short-video-lakehouse-airflow

# Enable EMR Serverless on the account (one-time console action)
# AWS Console → EMR → EMR Serverless → Get started
```

### Build and push Airflow image

```bash
# From repo root
docker build -f short-video-lakehouse-airflow/Dockerfile \
  -t short-video-lakehouse-airflow .

aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin \
    026177432704.dkr.ecr.us-east-1.amazonaws.com
docker tag short-video-lakehouse-airflow:latest \
  026177432704.dkr.ecr.us-east-1.amazonaws.com/short-video-lakehouse-airflow:latest
docker push \
  026177432704.dkr.ecr.us-east-1.amazonaws.com/short-video-lakehouse-airflow:latest
```

### Deploy stack (demo start)

```bash
cd terraform
terraform init   # only needed on first run or after provider changes
terraform apply \
  -var 'airflow_image=026177432704.dkr.ecr.us-east-1.amazonaws.com/short-video-lakehouse-airflow:latest'
terraform output  # verify all endpoints
```

### Shut down between demos (avoid idle cost)

```bash
cd terraform
terraform destroy \
  -var 'airflow_image=026177432704.dkr.ecr.us-east-1.amazonaws.com/short-video-lakehouse-airflow:latest'
```

NAT gateway and MSK Serverless connection hours are the primary idle costs.
EMR Serverless and Athena are pay-per-use — no charge when not running jobs.

### Stack outputs reference

| Output | Value |
|---|---|
| `msk_bootstrap_brokers_sasl_iam` | MSK Serverless broker endpoint (port 9098) |
| `emr_application_id` | EMR Serverless app ID — set as `EMR_APPLICATION_ID` in Airflow task |
| `warehouse_bucket` | `lakehouse-warehouse-026177432704` |
| `checkpoints_bucket` | `lakehouse-checkpoints-026177432704` |
| `athena_workgroup_name` | `lakehouse` |
| `glue_database_name` | `lakehouse` |
| `ecs_cluster_arn` | ECS cluster for Airflow + Metabase |

## 8. Benchmark Targets (from aws-deployment-and-scale-benchmark.md)

These targets are unchanged and drive sizing decisions:

- Sustained ingest: `>= 5,000 events/sec`
- Peak ingest: `>= 10,000 events/sec`
- Equivalent daily volume: `>= 432M rows/day`
- Realtime freshness: `P95 < 3 minutes`
- Batch publish readiness: `D-1` outputs by `08:00 America/New_York`
