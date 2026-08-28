# Current AWS account and region data sources for dynamic ARN formulation
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Standard Lambda Assume Role Policy Document
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# ==============================================================================
# Ingestion Lambda IAM Role & Policies (S3 raw/ -> ML inference -> DynamoDB)
# ==============================================================================
resource "aws_iam_role" "ingestion_lambda_role" {
  name               = "${var.project_name}-ingestion-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = {
    Name        = "${var.project_name}-ingestion-lambda-role"
    Description = "Least-privilege IAM execution role for ingestion and inference Lambda"
  }
}

data "aws_iam_policy_document" "ingestion_lambda_policy_doc" {
  # 1. Read access strictly to raw/ objects in the dedicated S3 bucket
  statement {
    sid    = "AllowS3RawGetObject"
    effect = "Allow"
    actions = [
      "s3:GetObject"
    ]
    resources = [
      "${aws_s3_bucket.raw_transactions.arn}/raw/*"
    ]
  }

  # 2. Write access strictly to the FlaggedTransactions DynamoDB table
  statement {
    sid    = "AllowDynamoDBPutItem"
    effect = "Allow"
    actions = [
      "dynamodb:PutItem"
    ]
    resources = [
      aws_dynamodb_table.flagged_transactions.arn
    ]
  }

  # 3. CloudWatch Logs permissions
  statement {
    sid    = "AllowCloudWatchLogging"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*"
    ]
  }
}

resource "aws_iam_role_policy" "ingestion_lambda_policy" {
  name   = "${var.project_name}-ingestion-policy"
  role   = aws_iam_role.ingestion_lambda_role.id
  policy = data.aws_iam_policy_document.ingestion_lambda_policy_doc.json
}

# ==============================================================================
# Alerting Lambda IAM Role & Policies (DynamoDB Stream -> SNS Alert)
# ==============================================================================
resource "aws_iam_role" "alerting_lambda_role" {
  name               = "${var.project_name}-alerting-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = {
    Name        = "${var.project_name}-alerting-lambda-role"
    Description = "Least-privilege IAM execution role for DynamoDB stream alerting Lambda"
  }
}

data "aws_iam_policy_document" "alerting_lambda_policy_doc" {
  # 1. Read access to DynamoDB Streams for the FlaggedTransactions table
  statement {
    sid    = "AllowDynamoDBStreamProcessing"
    effect = "Allow"
    actions = [
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:DescribeStream",
      "dynamodb:ListStreams"
    ]
    resources = [
      aws_dynamodb_table.flagged_transactions.stream_arn,
      aws_dynamodb_table.flagged_transactions.arn
    ]
  }

  # 2. Publish access strictly to the fraud-detection-alerts SNS topic
  statement {
    sid    = "AllowSNSPublishAlerts"
    effect = "Allow"
    actions = [
      "sns:Publish"
    ]
    resources = [
      aws_sns_topic.fraud_alerts.arn
    ]
  }

  # 3. CloudWatch Logs permissions
  statement {
    sid    = "AllowCloudWatchLogging"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*"
    ]
  }
}

resource "aws_iam_role_policy" "alerting_lambda_policy" {
  name   = "${var.project_name}-alerting-policy"
  role   = aws_iam_role.alerting_lambda_role.id
  policy = data.aws_iam_policy_document.alerting_lambda_policy_doc.json
}
