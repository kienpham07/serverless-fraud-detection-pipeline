# ==============================================================================
# Scikit-Learn & ML Dependencies Lambda Layer (Python 3.11)
# ==============================================================================
resource "aws_lambda_layer_version" "sklearn_layer" {
  layer_name          = "${var.project_name}-sklearn-layer"
  description         = "Scikit-Learn, NumPy, Joblib, and Pandas runtime dependencies for Python 3.11"
  filename            = "${path.module}/layers/sklearn-layer.zip"
  source_code_hash    = fileexists("${path.module}/layers/sklearn-layer.zip") ? filebase64sha256("${path.module}/layers/sklearn-layer.zip") : null
  compatible_runtimes = ["python3.11"]
  compatible_architectures = ["x86_64"]
}
