# storage.tf — S3 buckets (warehouse, checkpoints) and Glue Data Catalog database

# ── S3 Buckets ────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "warehouse" {
  bucket        = "${var.project_name}-warehouse"
  force_destroy = true
  tags          = { Project = var.project_name }
}

resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  versioning_configuration { status = "Enabled" }
}

# Checkpoints bucket also stores Athena query results (outputs.tf routes results here)
resource "aws_s3_bucket" "checkpoints" {
  bucket        = "${var.project_name}-checkpoints"
  force_destroy = true
  tags          = { Project = var.project_name }
}

# ── Glue Data Catalog ─────────────────────────────────────────────────────────
# Iceberg metastore — replaces local iceberg-rest + catalog-postgres.
# EMR Serverless and Athena both read/write via org.apache.iceberg.aws.glue.GlueCatalog.

resource "aws_glue_catalog_database" "main" {
  name        = replace(var.project_name, "-", "_")
  description = "Iceberg metastore for ${var.project_name}"
}
