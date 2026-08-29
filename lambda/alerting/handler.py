"""AWS Lambda handler for DynamoDB Streams fraud alerting.

Processes change data capture events from DynamoDB FlaggedTransactions table,
evaluates severity (CRITICAL vs WARNING), formats structured and human-readable
notifications, and publishes real-time alerts via Amazon SNS.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import BotoCoreError, ClientError

# Configure structured logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")

sns_client = boto3.client("sns", region_name=AWS_REGION)
deserializer = TypeDeserializer()


def deserialize_dynamodb_image(image_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Deserialize raw DynamoDB attribute maps into standard Python primitives.

    Args:
        image_dict: Raw DynamoDB attribute map (e.g. {"Amount": {"N": "120.50"}}).

    Returns:
        Dictionary with deserialized Python primitives (str, Decimal, int, etc.).
    """
    if not image_dict:
        return {}
    deserialized: Dict[str, Any] = {}
    for key, val in image_dict.items():
        try:
            deserialized[key] = deserializer.deserialize(val)
        except Exception as exc:
            logger.warning("Failed to deserialize field '%s': %s", key, str(exc))
            deserialized[key] = val
    return deserialized


def to_serializable(val: Any) -> Any:
    """Convert Decimal or non-serializable objects to JSON serializable formats."""
    if isinstance(val, Decimal):
        return float(val) if (val % 1 > 0) else int(val)
    if isinstance(val, (datetime,)):
        return val.isoformat()
    return val


def determine_alert_level(
    amount: float,
    detection_type: str,
) -> Tuple[str, str]:
    """Determine the severity level and indicator for the fraud alert.

    Alert Level is CRITICAL if amount > $5,000 or if triggered by HYBRID detection.
    Otherwise, Alert Level is WARNING.

    Args:
        amount: Transaction monetary value.
        detection_type: Detection category ("RULE", "ML", "HYBRID").

    Returns:
        Tuple of (alert_level_string, severity_badge).
    """
    is_critical = (amount > 5000.00) or (detection_type.upper() == "HYBRID")
    if is_critical:
        return ("CRITICAL", "🚨 CRITICAL")
    return ("WARNING", "⚠️ WARNING")


def build_alert_payloads(
    record: Dict[str, Any],
) -> Tuple[str, str, str]:
    """Build human-readable message, structured JSON payload, and subject line.

    Args:
        record: Deserialized DynamoDB transaction item.

    Returns:
        Tuple of (subject_line, human_readable_message, json_payload_string).
    """
    tx_id = str(record.get("transaction_id", "UNKNOWN"))
    acc_id = str(record.get("account_id", "UNKNOWN"))
    raw_amount = record.get("amount", 0.0)
    amount = float(raw_amount) if isinstance(raw_amount, (Decimal, int, float)) else 0.0
    timestamp = str(record.get("timestamp", datetime.now(timezone.utc).isoformat()))
    raw_risk = record.get("risk_score", 0.0)
    risk_score = float(raw_risk) if isinstance(raw_risk, (Decimal, int, float)) else 0.0
    flag_reason = str(record.get("flag_reason", "Anomalous transaction pattern detected"))
    detection_type = str(record.get("detection_type", "RULE")).upper()
    status = str(record.get("status", "PENDING_REVIEW"))
    merchant = str(record.get("merchant", "N/A"))
    merchant_category = str(record.get("merchant_category", "N/A"))
    card_type = str(record.get("card_type", "N/A"))
    device_type = str(record.get("device_type", "N/A"))

    alert_level, badge = determine_alert_level(amount=amount, detection_type=detection_type)

    alert_id = f"alt_{uuid.uuid4().hex[:12]}"
    now_iso = datetime.now(timezone.utc).isoformat()

    # Subject line formatted for email / SMS (enforcing SNS 100 character maximum limit)
    subject = f"[{alert_level} FRAUD ALERT] Acc: {acc_id} | ${amount:,.2f} ({detection_type})"
    if len(subject) > 100:
        subject = subject[:97] + "..."

    # Human-readable notification body
    human_message = f"""============================================================
{badge} FRAUD DETECTION ALERT - IMMEDIATE ACTION REQUIRED
============================================================
Alert ID          : {alert_id}
Alert Level       : {alert_level}
Detection Type    : {detection_type}
Status            : {status}
------------------------------------------------------------
TRANSACTION DETAILS:
  Transaction ID  : {tx_id}
  Account ID      : {acc_id}
  Amount          : ${amount:,.2f}
  Timestamp (UTC) : {timestamp}
  Risk Score      : {risk_score:.4f}
  Merchant        : {merchant} ({merchant_category})
  Payment Method  : {card_type}
  Device / Origin : {device_type}
------------------------------------------------------------
TRIGGERED RULES & REASONS:
  {flag_reason}
------------------------------------------------------------
Generated At (UTC): {now_iso}
Action: Please investigate this flagged transaction in the Security Operations Console.
============================================================"""

    # Structured JSON payload for automated downstream ingestion/SIEM
    json_data = {
        "alert_id": alert_id,
        "alert_level": alert_level,
        "detection_type": detection_type,
        "status": status,
        "transaction_id": tx_id,
        "account_id": acc_id,
        "amount": amount,
        "timestamp": timestamp,
        "risk_score": risk_score,
        "flag_reason": flag_reason,
        "merchant": merchant,
        "merchant_category": merchant_category,
        "card_type": card_type,
        "device_type": device_type,
        "published_at": now_iso,
    }
    json_payload = json.dumps(json_data, indent=2)

    return (subject, human_message, json_payload)


def publish_alert_to_sns(
    subject: str,
    message: str,
    alert_level: str,
    detection_type: str,
    account_id: str,
    topic_arn: str,
) -> str:
    """Publish the formatted alert to Amazon SNS with message attributes.

    Args:
        subject: Subject line for email subscribers.
        message: Main alert message body.
        alert_level: "CRITICAL" or "WARNING".
        detection_type: "RULE", "ML", or "HYBRID".
        account_id: Customer account identifier.
        topic_arn: Destination SNS topic ARN.

    Returns:
        SNS Publish Message ID.
    """
    if not topic_arn:
        logger.warning(
            json.dumps(
                {
                    "event": "sns_publish_skipped",
                    "reason": "SNS_TOPIC_ARN environment variable is not configured.",
                }
            )
        )
        return "MOCK_MESSAGE_ID"

    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message,
        MessageAttributes={
            "AlertLevel": {
                "DataType": "String",
                "StringValue": alert_level,
            },
            "DetectionType": {
                "DataType": "String",
                "StringValue": detection_type,
            },
            "AccountID": {
                "DataType": "String",
                "StringValue": account_id,
            },
        },
    )
    return response.get("MessageId", "UNKNOWN")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main DynamoDB Stream consumer entry point.

    Args:
        event: DynamoDB Stream event payload containing batched records.
        context: Lambda execution context.

    Returns:
        Summary response dictionary.
    """
    start_time = time.perf_counter()
    logger.info(json.dumps({"event": "alerting_invocation_received", "raw_record_count": len(event.get("Records", []))}))

    records = event.get("Records", [])
    if not records:
        logger.info(json.dumps({"event": "empty_stream_batch", "message": "No stream records to process."}))
        return {"statusCode": 200, "body": json.dumps({"processed_count": 0})}

    processed_alerts: List[Dict[str, Any]] = []
    skipped_count = 0

    for stream_record in records:
        event_name = stream_record.get("eventName")

        # Process strictly INSERT events representing newly flagged transactions
        if event_name != "INSERT":
            skipped_count += 1
            logger.info(
                json.dumps(
                    {
                        "event": "skipped_non_insert_event",
                        "eventName": event_name,
                        "eventID": stream_record.get("eventID"),
                    }
                )
            )
            continue

        dynamodb_data = stream_record.get("dynamodb", {})
        raw_new_image = dynamodb_data.get("NewImage")

        if not raw_new_image:
            skipped_count += 1
            logger.warning(
                json.dumps(
                    {
                        "event": "missing_new_image",
                        "eventID": stream_record.get("eventID"),
                    }
                )
            )
            continue

        try:
            # 1. Deserialize DynamoDB attribute map
            item = deserialize_dynamodb_image(raw_new_image)

            # 2. Build alert representations
            subject, human_msg, json_payload = build_alert_payloads(item)

            raw_amount = item.get("amount", 0.0)
            amount_val = float(raw_amount) if isinstance(raw_amount, (Decimal, int, float)) else 0.0
            det_type = str(item.get("detection_type", "RULE")).upper()
            alert_level, _ = determine_alert_level(amount=amount_val, detection_type=det_type)
            acc_id = str(item.get("account_id", "UNKNOWN"))

            # 3. Publish to SNS
            msg_id = publish_alert_to_sns(
                subject=subject,
                message=human_msg,
                alert_level=alert_level,
                detection_type=det_type,
                account_id=acc_id,
                topic_arn=SNS_TOPIC_ARN,
            )

            alert_summary = {
                "transaction_id": item.get("transaction_id"),
                "account_id": acc_id,
                "amount": amount_val,
                "alert_level": alert_level,
                "detection_type": det_type,
                "sns_message_id": msg_id,
            }
            processed_alerts.append(alert_summary)

            logger.info(
                json.dumps(
                    {
                        "event": "fraud_alert_published",
                        "alert_summary": alert_summary,
                        "sns_topic_arn": SNS_TOPIC_ARN,
                    }
                )
            )

        except Exception as exc:
            logger.exception(
                json.dumps(
                    {
                        "event": "alert_processing_exception",
                        "eventID": stream_record.get("eventID"),
                        "error": str(exc),
                    }
                )
            )
            # Raising exception triggers DynamoDB stream batch retry / bisect
            raise exc

    duration_ms = (time.perf_counter() - start_time) * 1000

    summary = {
        "event": "alerting_batch_summary",
        "total_records": len(records),
        "alerts_dispatched": len(processed_alerts),
        "skipped_records": skipped_count,
        "duration_ms": round(duration_ms, 2),
    }
    logger.info(json.dumps(summary))

    return {
        "statusCode": 200,
        "body": json.dumps(summary),
    }
