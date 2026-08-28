# SNS Topic for Instant Fraud Alerts
resource "aws_sns_topic" "fraud_alerts" {
  name              = "fraud-detection-alerts"
  kms_master_key_id = "alias/aws/sns"

  tags = {
    Name        = "fraud-detection-alerts"
    Description = "SNS topic for broadcasting real-time high-risk fraud alerts"
  }
}

# Email Subscription for Fraud Alerts
resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.fraud_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
