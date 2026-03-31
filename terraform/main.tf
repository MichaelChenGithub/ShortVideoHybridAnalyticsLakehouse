# main.tf — AWS provider, S3+DynamoDB state backend, shared variables

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Bootstrap: create this S3 bucket and DynamoDB table before `terraform init`
  backend "s3" {
    bucket         = "short-video-lakehouse-tf-state"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "short-video-lakehouse-tf-locks"
  }
}

provider "aws" {
  region = var.aws_region
}

variable "project_name" {
  description = "Short name used as prefix for all resources (must be globally unique for S3)"
  type        = string
  default     = "lakehouse"
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "airflow_image" {
  description = "ECR image URI for the Airflow ECS task, e.g. 123456789.dkr.ecr.us-east-1.amazonaws.com/lakehouse-airflow:latest"
  type        = string
}
