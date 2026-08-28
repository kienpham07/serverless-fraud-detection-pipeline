# ==============================================================================
# Archive Packaging for Ingestion Lambda
# ==============================================================================
data "archive_file" "ingestion_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda/ingestion"
  output_path = "${path.module}/build/ingestion_lambda.zip"
}

# ==============================================================================
# Ingestion & Inference Lambda Function
# ==============================================================================
resource "aws_lambda_function" "ingestion_lambda" {
  function_name = "${var.project_name}-ingestion"
  description   = "Processes raw S3 transaction CSVs, executes hybrid rule/ML scoring, and stores flagged fraud in DynamoDB"

  filename         = data.archive_file.ingestion_lambda_zip.output_path
  source_code_hash = data.archive_file.ingestion_lambda_zip.output_base64sha256

  runtime     = "python3.11"
  handler     = "handler.lambda_handler"
  memory_size = 512
  timeout     = 60

  role = aws_iam_role.ingestion_lambda_role.arn

  environment {
    variables = {
      DYNAMODB_TABLE_NAME = aws_dynamodb_table.flagged_transactions.name
      MODEL_PATH          = "model.joblib"
      AWS_REGION_NAME     = var.aws_region
    }
  }

  depends_on = [
    aws_iam_role_policy.ingestion_lambda_policy,
  ]

  tags = {
    Name        = "${var.project_name}-ingestion-lambda"
    Description = "Ingests S3 CSV streaming events and scores transactions"
  }
}

# ==============================================================================
# Lambda Permission for S3 Invocations
# ==============================================================================
resource "aws_lambda_permission" "allow_s3_ingestion" {
  statement_id  = "AllowS3RawBucketInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw_transactions.arn
  source_account = data.aws_caller_identity.current.account_id
}

# ==============================================================================
# S3 Bucket Notification Trigger
# ==============================================================================
resource "aws_s3_bucket_notification" "raw_transactions_notification" {
  bucket = aws_s3_bucket.raw_transactions.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingestion_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".csv"
  }

  depends_on = [
    aws_lambda_permission.allow_s3_ingestion,
  ]
}
