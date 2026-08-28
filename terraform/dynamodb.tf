# DynamoDB Table for Flagged Fraudulent Transactions
resource "aws_dynamodb_table" "flagged_transactions" {
  name             = "FlaggedTransactions"
  billing_mode     = "PAY_PER_REQUEST"
  hash_key         = "transaction_id"
  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"

  attribute {
    name = "transaction_id"
    type = "S"
  }

  attribute {
    name = "account_id"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "S"
  }

  global_secondary_index {
    name            = "AccountTimeIndex"
    hash_key        = "account_id"
    range_key       = "timestamp"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled = true
  }

  tags = {
    Name        = "FlaggedTransactions"
    Description = "Storage for transactions flagged as fraudulent with real-time stream"
  }
}
