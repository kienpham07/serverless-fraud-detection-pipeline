variable "aws_region" {
  type        = string
  description = "AWS region to deploy all infrastructure resources into."
  default     = "us-east-1"
}

variable "environment" {
  type        = string
  description = "Deployment environment name (e.g., dev, staging, prod)."
  default     = "dev"
}

variable "project_name" {
  type        = string
  description = "Project name used for tagging and prefixing resources."
  default     = "serverless-fraud-detection"
}

variable "alert_email" {
  type        = string
  description = "Target email address to receive immediate SNS fraud alerts."
  validation {
    condition     = can(regex("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", var.alert_email))
    error_message = "The alert_email variable must be a valid email address."
  }
}
