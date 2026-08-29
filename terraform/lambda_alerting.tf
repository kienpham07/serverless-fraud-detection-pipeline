# ==============================================================================
# Archive Packaging for Alerting Lambda
# ==============================================================================
data "archive_file" "alerting_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/alerting"
  output_path = "${path.module}/build/alerting_lambda.zip"
}

# ==============================================================================
# Alerting Lambda Function (DynamoDB Streams -> SNS)
# ==============================================================================
resource "aws_lambda_function" "alerting_lambda" {
  function_name = "${var.project_name}-alerting"
  description   = "Consumes DynamoDB Streams from FlaggedTransactions and broadcasts real-time alerts via SNS"

  filename         = data.archive_file.alerting_lambda_zip.output_path
  source_code_hash = data.archive_file.alerting_lambda_zip.output_base64sha256

  runtime     = "python3.11"
  handler     = "handler.lambda_handler"
  memory_size = 128
  timeout     = 30

  role = aws_iam_role.alerting_lambda_role.arn

  environment {
    variables = {
      SNS_TOPIC_ARN   = aws_sns_topic.fraud_alerts.arn
      AWS_REGION_NAME = var.aws_region
    }
  }

  depends_on = [
    aws_iam_role_policy.alerting_lambda_policy,
  ]

  tags = {
    Name        = "${var.project_name}-alerting-lambda"
    Description = "Dispatches high-severity fraud notifications via SNS"
  }
}

# ==============================================================================
# DynamoDB Stream Event Source Mapping
# ==============================================================================
resource "aws_lambda_event_source_mapping" "dynamodb_stream_trigger" {
  event_source_arn                   = aws_dynamodb_table.flagged_transactions.stream_arn
  function_name                      = aws_lambda_function.alerting_lambda.arn
  starting_position                  = "LATEST"
  batch_size                         = 10
  maximum_batching_window_in_seconds = 5
  bisect_batch_on_function_error     = true
  enabled                            = true
}
