# S3 Outputs
output "raw_transactions_bucket_id" {
  description = "The name of the S3 bucket created for raw transaction streaming."
  value       = aws_s3_bucket.raw_transactions.id
}

output "raw_transactions_bucket_arn" {
  description = "The ARN of the S3 bucket created for raw transaction streaming."
  value       = aws_s3_bucket.raw_transactions.arn
}

# DynamoDB Outputs
output "dynamodb_table_name" {
  description = "The name of the DynamoDB table storing flagged fraud transactions."
  value       = aws_dynamodb_table.flagged_transactions.name
}

output "dynamodb_table_arn" {
  description = "The ARN of the DynamoDB table storing flagged fraud transactions."
  value       = aws_dynamodb_table.flagged_transactions.arn
}

output "dynamodb_stream_arn" {
  description = "The ARN of the DynamoDB table stream for real-time change data capture."
  value       = aws_dynamodb_table.flagged_transactions.stream_arn
}

# SNS Outputs
output "sns_topic_arn" {
  description = "The ARN of the SNS topic for fraud detection alerts."
  value       = aws_sns_topic.fraud_alerts.arn
}

# IAM Role Outputs
output "ingestion_lambda_role_arn" {
  description = "The ARN of the IAM role for the ingestion & ML inference Lambda function."
  value       = aws_iam_role.ingestion_lambda_role.arn
}

output "alerting_lambda_role_arn" {
  description = "The ARN of the IAM role for the DynamoDB stream alerting Lambda function."
  value       = aws_iam_role.alerting_lambda_role.arn
}

# Lambda Outputs
output "ingestion_lambda_function_name" {
  description = "The name of the Ingestion Lambda function."
  value       = aws_lambda_function.ingestion_lambda.function_name
}

output "ingestion_lambda_arn" {
  description = "The ARN of the Ingestion Lambda function."
  value       = aws_lambda_function.ingestion_lambda.arn
}
