# ==============================================================================
# Scikit-Learn & ML Dependencies Lambda Layer (Python 3.11)
# ==============================================================================
# Upload layer zip archive to S3 bucket to bypass Lambda 50MB direct API upload limit
resource "aws_s3_object" "sklearn_layer_zip" {
  bucket = aws_s3_bucket.raw_transactions.id
  key    = "layers/sklearn-layer.zip"
  source = "${path.module}/layers/sklearn-layer.zip"
  etag   = fileexists("${path.module}/layers/sklearn-layer.zip") ? filemd5("${path.module}/layers/sklearn-layer.zip") : null
}

resource "aws_lambda_layer_version" "sklearn_layer" {
  layer_name               = "${var.project_name}-sklearn-layer"
  description              = "Scikit-Learn, NumPy, Joblib, and Pandas runtime dependencies for Python 3.11"
  s3_bucket                = aws_s3_bucket.raw_transactions.id
  s3_key                   = aws_s3_object.sklearn_layer_zip.key
  source_code_hash         = fileexists("${path.module}/layers/sklearn-layer.zip") ? filebase64sha256("${path.module}/layers/sklearn-layer.zip") : null
  compatible_runtimes      = ["python3.11"]
  compatible_architectures = ["x86_64"]

  depends_on = [
    aws_s3_object.sklearn_layer_zip,
  ]
}
