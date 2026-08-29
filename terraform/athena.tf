# ==============================================================================
# AWS Glue Data Catalog Database for Fraud Analytics
# ==============================================================================
resource "aws_glue_catalog_database" "fraud_analytics_db" {
  name        = "fraud_analytics_db"
  description = "Glue Data Catalog database for Serverless Fraud Detection analytics"
}

# ==============================================================================
# AWS Glue Catalog Table: raw_transactions (S3 CSV Mapping)
# ==============================================================================
resource "aws_glue_catalog_table" "raw_transactions" {
  name          = "raw_transactions"
  database_name = aws_glue_catalog_database.fraud_analytics_db.name
  table_type    = "EXTERNAL_TABLE"
  description   = "External Glue table mapping to raw transaction CSV logs in S3"

  parameters = {
    "classification"         = "csv"
    "typeOfData"             = "file"
    "delimiter"              = ","
    "skip.header.line.count" = "1"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.raw_transactions.id}/raw/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "csv-serde"
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"

      parameters = {
        "field.delim"          = ","
        "serialization.format" = ","
      }
    }

    columns {
      name    = "transaction_id"
      type    = "string"
      comment = "Unique transaction identifier"
    }

    columns {
      name    = "account_id"
      type    = "string"
      comment = "Customer account identifier"
    }

    columns {
      name    = "timestamp"
      type    = "string"
      comment = "ISO 8601 transaction timestamp"
    }

    columns {
      name    = "amount"
      type    = "double"
      comment = "Transaction monetary amount in USD"
    }

    columns {
      name    = "transaction_frequency"
      type    = "int"
      comment = "Transaction velocity frequency"
    }

    columns {
      name    = "distance_from_last_tx"
      type    = "double"
      comment = "Distance in miles from previous transaction"
    }

    columns {
      name    = "is_international"
      type    = "boolean"
      comment = "Flag indicating cross-border transaction"
    }

    columns {
      name    = "risk_score"
      type    = "double"
      comment = "Continuous risk score between 0.0 and 1.0"
    }

    columns {
      name    = "merchant"
      type    = "string"
      comment = "Merchant business name"
    }

    columns {
      name    = "merchant_category"
      type    = "string"
      comment = "Merchant category code/name"
    }

    columns {
      name    = "card_type"
      type    = "string"
      comment = "Payment card brand/type"
    }

    columns {
      name    = "device_type"
      type    = "string"
      comment = "Originating device type"
    }

    columns {
      name    = "is_fraud"
      type    = "int"
      comment = "Ground truth/simulation fraud label"
    }
  }
}

# ==============================================================================
# AWS Athena Workgroup for Fraud Analysis
# ==============================================================================
resource "aws_athena_workgroup" "fraud_analysis_workgroup" {
  name        = "fraud_analysis_workgroup"
  description = "Dedicated Athena workgroup for ad-hoc fraud investigation and retrospective SQL analytics"
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.raw_transactions.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = {
    Name        = "${var.project_name}-athena-workgroup"
    Description = "Workgroup for querying fraud detection data lake"
  }
}
