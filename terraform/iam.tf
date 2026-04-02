# iam.tf — IAM roles: ECS task execution, ECS task, Airflow task, EMR Serverless execution

locals {
  ecs_trust_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  emr_trust_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# ── ECS Task Execution Role ───────────────────────────────────────────────────
# Allows ECS to pull images from ECR and write logs to CloudWatch.

resource "aws_iam_role" "ecs_task_execution" {
  name               = "${var.project_name}-ecs-task-execution"
  assume_role_policy = local.ecs_trust_policy
  tags               = { Project = var.project_name }
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "ecs_task_execution_secrets" {
  name = "${var.project_name}-ecs-task-execution-secrets"
  role = aws_iam_role.ecs_task_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "SecretsManagerRead"
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = "arn:aws:secretsmanager:${var.aws_region}:*:secret:${var.project_name}/airflow/*"
    }]
  })
}

# ── ECS Task Role (Metabase) ──────────────────────────────────────────────────
# Allows Metabase to query Athena and read results from S3.

resource "aws_iam_role" "ecs_task" {
  name               = "${var.project_name}-ecs-task"
  assume_role_policy = local.ecs_trust_policy
  tags               = { Project = var.project_name }
}

resource "aws_iam_role_policy" "ecs_task" {
  name = "${var.project_name}-ecs-task-policy"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "AthenaQuery"
        Effect   = "Allow"
        Action   = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution", "athena:ListWorkGroups"]
        Resource = "*"
      },
      {
        Sid      = "S3ResultsAccess"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.checkpoints.arn, "${aws_s3_bucket.checkpoints.arn}/*",
                    aws_s3_bucket.warehouse.arn,    "${aws_s3_bucket.warehouse.arn}/*"]
      },
      {
        Sid      = "GlueCatalogRead"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions"]
        Resource = "*"
      }
    ]
  })
}

# ── Airflow Task Role ─────────────────────────────────────────────────────────
# Allows Airflow to submit EMR Serverless job runs and access S3 + Glue.

resource "aws_iam_role" "airflow_task" {
  name               = "${var.project_name}-airflow-task"
  assume_role_policy = local.ecs_trust_policy
  tags               = { Project = var.project_name }
}

resource "aws_iam_role_policy" "airflow_task" {
  name = "${var.project_name}-airflow-task-policy"
  role = aws_iam_role.airflow_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "EMRServerlessJobControl"
        Effect   = "Allow"
        Action   = ["emr-serverless:StartJobRun", "emr-serverless:GetJobRun", "emr-serverless:ListJobRuns", "emr-serverless:CancelJobRun", "emr-serverless:GetApplication"]
        Resource = "*"
      },
      {
        Sid      = "PassEMRExecutionRole"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = aws_iam_role.emr_execution.arn
      },
      {
        Sid      = "S3Access"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.warehouse.arn,    "${aws_s3_bucket.warehouse.arn}/*",
                    aws_s3_bucket.checkpoints.arn,  "${aws_s3_bucket.checkpoints.arn}/*"]
      },
      {
        Sid      = "GlueAccess"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions"]
        Resource = "*"
      },
      {
        Sid      = "SESSendEmail"
        Effect   = "Allow"
        Action   = "ses:SendRawEmail"
        Resource = "*"
      }
    ]
  })
}

# ── EMR Serverless Execution Role ─────────────────────────────────────────────
# Assumed by EMR Serverless workers; needs S3 + Glue + MSK Serverless access.

resource "aws_iam_role" "emr_execution" {
  name               = "${var.project_name}-emr-execution"
  assume_role_policy = local.emr_trust_policy
  tags               = { Project = var.project_name }
}

resource "aws_iam_role_policy" "emr_execution" {
  name = "${var.project_name}-emr-execution-policy"
  role = aws_iam_role.emr_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "S3Access"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.warehouse.arn,    "${aws_s3_bucket.warehouse.arn}/*",
                    aws_s3_bucket.checkpoints.arn,  "${aws_s3_bucket.checkpoints.arn}/*"]
      },
      {
        Sid      = "GlueCatalog"
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
                    "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
                    "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition",
                    "glue:UpdatePartition", "glue:DeletePartition", "glue:BatchCreatePartition"]
        Resource = "*"
      },
      {
        Sid      = "MSKServerlessAccess"
        Effect   = "Allow"
        Action   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster",
                    "kafka-cluster:DescribeTopic", "kafka-cluster:ReadData",
                    "kafka-cluster:WriteData", "kafka-cluster:AlterGroup",
                    "kafka-cluster:DescribeGroup"]
        Resource = "*"
      }
    ]
  })
}
