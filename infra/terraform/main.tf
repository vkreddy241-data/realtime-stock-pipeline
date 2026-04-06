terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "vkreddy-tf-state"
    key    = "stock-pipeline/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------
# S3 — Delta Lake storage
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "stock_lake" {
  bucket        = "vkreddy-stock-lake"
  force_destroy = false
  tags          = local.common_tags
}

resource "aws_s3_bucket_versioning" "stock_lake" {
  bucket = aws_s3_bucket.stock_lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "stock_lake" {
  bucket = aws_s3_bucket.stock_lake.id
  rule {
    id     = "archive-old-ticks"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# ---------------------------------------------------------------------------
# MSK (Managed Kafka) cluster
# ---------------------------------------------------------------------------
resource "aws_msk_cluster" "stock_kafka" {
  cluster_name           = "stock-pipeline-kafka"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.kafka_sg.id]
    storage_info {
      ebs_storage_info { volume_size = 500 }
    }
  }

  encryption_info {
    encryption_in_transit { client_broker = "TLS" }
  }

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# EMR cluster for Spark Streaming
# ---------------------------------------------------------------------------
resource "aws_emr_cluster" "spark_streaming" {
  name          = "stock-spark-streaming"
  release_label = "emr-6.13.0"
  applications  = ["Spark"]

  ec2_attributes {
    subnet_id                         = var.private_subnet_ids[0]
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
  }

  master_instance_group {
    instance_type = "m5.xlarge"
  }

  core_instance_group {
    instance_type  = "m5.2xlarge"
    instance_count = 2
    ebs_config {
      size                 = 100
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  log_uri    = "s3://${aws_s3_bucket.stock_lake.bucket}/emr-logs/"
  service_role = aws_iam_role.emr_service_role.arn
  tags       = local.common_tags
}

# ---------------------------------------------------------------------------
# Security Group for Kafka
# ---------------------------------------------------------------------------
resource "aws_security_group" "kafka_sg" {
  name   = "kafka-sg"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

locals {
  common_tags = {
    Project   = "realtime-stock-pipeline"
    Owner     = "vikas-reddy"
    ManagedBy = "terraform"
  }
}
