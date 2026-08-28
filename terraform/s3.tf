# S3 Bucket for Raw Transaction Streaming
resource "aws_s3_bucket" "raw_transactions" {
  bucket_prefix = "fraud-pipeline-"
  force_destroy = true

  tags = {
    Name        = "${var.project_name}-raw-transactions"
    Description = "Landing bucket for raw credit card streaming transactions"
  }
}

# S3 Server-Side Encryption (SSE-S3 / AES256)
resource "aws_s3_bucket_server_side_encryption_configuration" "raw_transactions_sse" {
  bucket = aws_s3_bucket.raw_transactions.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "raw_transactions_pab" {
  bucket = aws_s3_bucket.raw_transactions.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Lifecycle Configuration: Transition raw/ objects to STANDARD_IA after 30 days
resource "aws_s3_bucket_lifecycle_configuration" "raw_transactions_lifecycle" {
  bucket = aws_s3_bucket.raw_transactions.id

  rule {
    id     = "transition-raw-to-standard-ia"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}
