# AWS Benchmark Runbook

End-to-end guide for deploying the lakehouse stack on AWS and running the benchmark.

## Prerequisites

- AWS CLI configured for `us-east-1`
- Terraform installed
- Docker running (for building images)
- `.venv` built: `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`

---

## 1. Deploy Infrastructure

```bash
cd terraform
terraform init
terraform apply
```

Key outputs used by Makefile targets:
| Output | Used by |
|--------|---------|
| `warehouse_bucket` | S3 path for Iceberg + scripts |
| `checkpoints_bucket` | Spark checkpoint paths |
| `emr_application_id` | EMR Serverless job submission |
| `msk_bootstrap_brokers_sasl_iam` | Kafka bootstrap for all jobs |
| `ecs_cluster_arn` | Generator ECS task launch |
| `generator_task_definition` | Generator task ARN |

---

## 2. Build and Push Images

```bash
# Benchmark generator (Fargate, linux/amd64)
make push-generator

# Airflow (if changed)
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

---

## 3. Upload Spark Scripts to S3

Packages all `src/spark/*.py` into `spark_libs.zip` and syncs to S3. Run once after
`terraform apply` and again after any Spark code change.

```bash
make upload-aws-scripts
```

---

## 4. Start Spark Structured Streaming Jobs

Submits all 3 jobs with a 10-second stagger so each claims executor capacity before
the next starts. Required due to the 16 vCPU account quota — each job is capped at
1 vCPU driver + 1–4 vCPU executors.

```bash
make submit-all-streaming
```

Verify all three reach **Running** status in EMR Studio → Streaming job runs tab
(takes ~2 min). If any job fails within 5 minutes, check the driver stderr:

```bash
# Replace APP_ID and JOB_ID with values from EMR Studio
aws s3 cp \
  s3://lakehouse-warehouse-026177432704/emr-logs/<APP_ID>/jobs/<JOB_ID>/SPARK_DRIVER/stderr.gz \
  - | gunzip | tail -50
```

---

## 5. Smoke Test — Verify End-to-End Connectivity

Run **after** all 3 streaming jobs are in Running state.

Launches 1 generator task: seed 99, 10 min, 500 events/sec, started_at = yesterday noon.

```bash
make run-generator-smoke
```

Monitor generator logs:

```bash
aws logs tail /ecs/lakehouse/generator --region us-east-1 --follow
```

Verify data landed in S3 (~12 min after launch):

```bash
aws s3 ls s3://lakehouse-warehouse-026177432704/warehouse/bronze/ \
  --region us-east-1 --recursive --human-readable | head -20
```

---

## 6. Full Benchmark Run ⚠️ NOT YET VALIDATED

> This section has not been run end-to-end. Treat as a draft until validated.

Launches 4 generator tasks in parallel: seeds 1–4, 25K events/sec each = 100K
events/sec total, 3 hours = ~1B events. `started_at` is automatically set to
yesterday at 12:00 UTC so the batch DAG can process D-1.

```bash
make run-generator-benchmark
```

Monitor task ARNs printed by the command:

```bash
aws ecs describe-tasks --region us-east-1 \
  --cluster arn:aws:ecs:us-east-1:026177432704:cluster/lakehouse \
  --tasks <TASK_ARN_1> <TASK_ARN_2> <TASK_ARN_3> <TASK_ARN_4> \
  --query 'tasks[].{id:taskArn,status:lastStatus}' --output table
```

All 4 tasks complete in ~3 hours. After completion, verify row counts in Trino:

```sql
SELECT COUNT(*) FROM lakehouse.bronze.raw_content_events;
SELECT COUNT(*) FROM lakehouse.bronze.raw_cdc_videos;
SELECT COUNT(*) FROM lakehouse.bronze.raw_cdc_users;
```

---

## 7. Trigger Airflow Batch DAG ⚠️ NOT YET VALIDATED

> This section has not been run end-to-end. Treat as a draft until validated.

The `batch_publish_daily` DAG runs daily at 8 AM ET, processing D-1 data. To
trigger manually after the benchmark:

```bash
# Get the Airflow ECS task ARN from the running service, then exec in:
aws ecs run-task --region us-east-1 \
  --cluster arn:aws:ecs:us-east-1:026177432704:cluster/lakehouse \
  --task-definition lakehouse-airflow \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[<PRIVATE_SUBNET>],securityGroups=[<ECS_SG>],assignPublicIp=DISABLED}"
```

Monitor the DAG in the Airflow UI or CloudWatch:

```bash
aws logs tail /ecs/lakehouse/airflow --region us-east-1 --follow
```

---

## 8. Tear Down ⚠️ NOT YET VALIDATED

> Run between demos to avoid idle cost. Validate destroy completes cleanly before relying on this.

**Only the NAT Gateway has meaningful idle cost (~$32/month).** MSK Serverless, EMR
Serverless, and ECS tasks are pay-per-use and cost ~$0 when not running. S3 and Glue
are free to keep. Use targeted destroy to kill only the NAT Gateway — S3 data and
Glue metadata survive intact.

### Between demos (targeted — preserves all data)

Cancel running EMR jobs and stop the application first, then destroy only the NAT Gateway:

```bash
# 1. Cancel all running streaming jobs
aws emr-serverless list-job-runs --region us-east-1 \
  --application-id $(cd terraform && terraform output -raw emr_application_id) \
  --states RUNNING \
  --query 'jobRuns[].jobRunId' --output text | \
  tr '\t' '\n' | \
  xargs -I{} aws emr-serverless cancel-job-run --region us-east-1 \
    --application-id $(cd terraform && terraform output -raw emr_application_id) \
    --job-run-id {}

# 2. Stop the EMR Serverless application
aws emr-serverless stop-application --region us-east-1 \
  --application-id $(cd terraform && terraform output -raw emr_application_id)

# 3. Destroy only the NAT Gateway (the only resource with idle cost)
cd terraform && terraform destroy \
  -target=aws_nat_gateway.main \
  -target=aws_eip.nat
```

To resume: `terraform apply` restores the NAT Gateway and all other resources reconnect
automatically. Then `make submit-all-streaming` to restart the streaming jobs.

### Full destroy (wipes everything except S3 data)

S3 buckets have `force_destroy = false` — Terraform will refuse to delete non-empty
buckets, so your Iceberg data and checkpoints are safe. Everything else (VPC, MSK,
EMR application, ECS, Glue, IAM) will be destroyed.

```bash
cd terraform && terraform destroy
```

> If you want to wipe S3 data too, manually empty the buckets first:
> `aws s3 rm s3://lakehouse-warehouse-026177432704 --recursive`
> `aws s3 rm s3://lakehouse-checkpoints-026177432704 --recursive`

---

## Capacity Notes

| Constraint | Value |
|------------|-------|
| EMR Serverless max vCPU (account quota) | 16 vCPU |
| Per-job: driver | 1 vCPU / 2 GB |
| Per-job: executor | 1 vCPU / 2 GB, max 4 |
| 3 jobs at minimum (3 drivers + 3 executors) | 6 vCPU |
| 3 jobs at maximum (3 drivers + 12 executors) | 15 vCPU |

To request a quota increase for the full benchmark:

```bash
aws service-quotas request-service-quota-increase \
  --region us-east-1 \
  --service-code emr-serverless \
  --quota-code L-4A1E4B3D \
  --desired-value 64
```
