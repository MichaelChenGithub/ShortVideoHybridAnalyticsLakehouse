# compute.tf — EMR Serverless (all Spark); ECS for Airflow and Metabase only

# ── EMR Serverless Application (rt_*.py streaming + bt_*.py batch) ────────────
# Single application handles both workloads; job runs are submitted per script.
# Pay-per-use: no idle cost between demo runs.

resource "aws_emrserverless_application" "spark" {
  name          = "${var.project_name}-spark"
  release_label = "emr-7.1.0"
  type          = "SPARK"

  # Start with zero capacity; scale on job submission
  initial_capacity {}

  maximum_capacity {
    cpu    = "20 vCPU"
    memory = "40 GB"
  }

  network_configuration {
    subnet_ids         = [aws_subnet.private_a.id]
    security_group_ids = [aws_security_group.ecs_tasks.id]
  }

  tags = { Project = var.project_name }
}

# ── ECS Cluster (Airflow + Metabase only) ────────────────────────────────────

resource "aws_ecs_cluster" "main" {
  name = var.project_name
  tags = { Project = var.project_name }
}

# ── Airflow Task Definition (triggered on-demand by the DAG scheduler) ────────

resource "aws_cloudwatch_log_group" "airflow" {
  name              = "/ecs/${var.project_name}/airflow"
  retention_in_days = 7
}

resource "aws_ecs_task_definition" "airflow" {
  family                   = "${var.project_name}-airflow"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  task_role_arn            = aws_iam_role.airflow_task.arn
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  cpu                      = "1024"
  memory                   = "2048"

  container_definitions = jsonencode([{
    name      = "airflow"
    image     = var.airflow_image
    essential = true
    environment = [
      { name = "AWS_DEFAULT_REGION",       value = var.aws_region },
      { name = "EMR_APPLICATION_ID",       value = aws_emrserverless_application.spark.id },
      { name = "EMR_EXECUTION_ROLE_ARN",   value = aws_iam_role.emr_execution.arn },
      { name = "GLUE_DATABASE",            value = aws_glue_catalog_database.main.name },
      { name = "WAREHOUSE_BUCKET",         value = aws_s3_bucket.warehouse.bucket },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.airflow.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "airflow"
      }
    }
  }])
}

# ── Metabase (Fargate, public-facing on port 3000) ────────────────────────────

resource "aws_security_group" "metabase" {
  name        = "${var.project_name}-metabase-sg"
  description = "Metabase: inbound HTTP on 3000 from anywhere"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project_name}-metabase-sg", Project = var.project_name }
}

resource "aws_cloudwatch_log_group" "metabase" {
  name              = "/ecs/${var.project_name}/metabase"
  retention_in_days = 7
}

resource "aws_ecs_task_definition" "metabase" {
  family                   = "${var.project_name}-metabase"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  task_role_arn            = aws_iam_role.ecs_task.arn
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  cpu                      = "1024"
  memory                   = "2048"

  container_definitions = jsonencode([{
    name      = "metabase"
    image     = "metabase/metabase:latest"
    essential = true
    portMappings = [{ containerPort = 3000, protocol = "tcp" }]
    environment = [
      { name = "MB_DB_TYPE",         value = "h2" },
      { name = "AWS_DEFAULT_REGION", value = var.aws_region },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.metabase.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "metabase"
      }
    }
  }])
}

resource "aws_ecs_service" "metabase" {
  name            = "${var.project_name}-metabase"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.metabase.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = [aws_subnet.public_a.id]
    security_groups  = [aws_security_group.metabase.id]
    assign_public_ip = true
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "emr_application_id" {
  description = "EMR Serverless application ID — pass to airflow_batch_tasks.py via ECS env"
  value       = aws_emrserverless_application.spark.id
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN (Airflow + Metabase)"
  value       = aws_ecs_cluster.main.arn
}

output "metabase_service_name" {
  description = "ECS service name for Metabase; resolve public IP via: aws ecs describe-tasks"
  value       = aws_ecs_service.metabase.name
}
