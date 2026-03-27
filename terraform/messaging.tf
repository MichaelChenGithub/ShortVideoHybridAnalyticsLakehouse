# messaging.tf — MSK Serverless cluster and security group rules
# Serverless: pay-per-use, no broker sizing. IAM auth required (no plaintext).

resource "aws_msk_serverless_cluster" "main" {
  cluster_name = "${var.project_name}-msk"

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id]
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = {
    Name    = "${var.project_name}-msk"
    Project = var.project_name
  }
}

# Security group for the MSK Serverless cluster
resource "aws_security_group" "msk" {
  name        = "${var.project_name}-msk-sg"
  description = "MSK Serverless: allow Kafka/IAM access from ECS tasks only"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name    = "${var.project_name}-msk-sg"
    Project = var.project_name
  }
}

# Inbound: Kafka SASL/IAM (9098) from ECS task security group
# Serverless does not support plaintext (9092) or TLS-only (9094).
resource "aws_security_group_rule" "msk_ingress_from_ecs" {
  type                     = "ingress"
  from_port                = 9098
  to_port                  = 9098
  protocol                 = "tcp"
  security_group_id        = aws_security_group.msk.id
  source_security_group_id = aws_security_group.ecs_tasks.id
  description              = "Kafka SASL/IAM from ECS tasks"
}

# Outbound: unrestricted (control-plane and broker connectivity)
resource "aws_security_group_rule" "msk_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.msk.id
  description       = "Allow all outbound"
}

# Output — broker endpoint consumed by ECS task definitions in compute.tf
# Note: ECS task IAM role (iam.tf) must have kafka-cluster:* permissions on this cluster ARN.
output "msk_bootstrap_brokers_sasl_iam" {
  description = "MSK Serverless SASL/IAM broker endpoint (port 9098)"
  value       = aws_msk_serverless_cluster.main.bootstrap_brokers_sasl_iam
}
