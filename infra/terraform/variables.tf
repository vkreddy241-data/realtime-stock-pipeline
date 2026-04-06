variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "VPC ID for all resources"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs (min 3 for MSK)"
  type        = list(string)
}
