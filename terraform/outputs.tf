# outputs.tf — Athena workgroup + cross-stack outputs for MIC-158 benchmark runs
# MSK, ECS, and EMR outputs live in their respective files (messaging.tf, compute.tf).

# ── Athena Workgroup ──────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "main" {
  name  = var.project_name
  state = "ENABLED"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.checkpoints.bucket}/athena-results/"
    }
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
  }

  tags = { Project = var.project_name }
}

# ── Storage outputs (from storage.tf) ────────────────────────────────────────

output "warehouse_bucket" {
  description = "S3 bucket name for Iceberg warehouse data"
  value       = aws_s3_bucket.warehouse.bucket
}

output "checkpoints_bucket" {
  description = "S3 bucket name for Spark checkpoints and Athena query results"
  value       = aws_s3_bucket.checkpoints.bucket
}

output "glue_database_name" {
  description = "Glue Data Catalog database name (Iceberg metastore)"
  value       = aws_glue_catalog_database.main.name
}

# ── Athena output ─────────────────────────────────────────────────────────────

output "athena_workgroup_name" {
  description = "Athena workgroup name — pass to Metabase JDBC config and dbt profiles.yml"
  value       = aws_athena_workgroup.main.name
}

# ── Network output (from network.tf) ─────────────────────────────────────────

output "vpc_id" {
  description = "VPC ID — useful for validating security group scope during benchmark runs"
  value       = aws_vpc.main.id
}
