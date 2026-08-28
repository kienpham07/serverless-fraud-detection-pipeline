"""AWS Lambda handler for S3 raw CSV ingestion and fraud detection.

Streams transaction CSV records from S3, applies hybrid scoring (rule-based
heuristics + Scikit-Learn ML inference), and writes flagged fraud records to
the DynamoDB FlaggedTransactions table.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
import urllib.parse
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3
import joblib
import numpy as np
import pandas as pd

# Configure structured logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global clients and model caching across warm execution contexts
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME", "FlaggedTransactions")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

s3_client = boto3.client("s3")
dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION)
dynamodb_table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)

# Load Scikit-Learn model outside the handler to optimize warm invocations
MODEL_PATH = os.environ.get(
    "MODEL_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "model.joblib"),
)

MODEL: Optional[Any] = None
try:
    if os.path.exists(MODEL_PATH):
        MODEL = joblib.load(MODEL_PATH)
        logger.info(
            json.dumps(
                {
                    "event": "model_initialization",
                    "status": "success",
                    "model_path": MODEL_PATH,
                    "model_type": str(type(MODEL)),
                }
            )
        )
    else:
        logger.warning(
            json.dumps(
                {
                    "event": "model_initialization",
                    "status": "file_not_found",
                    "model_path": MODEL_PATH,
                }
            )
        )
except Exception as err:
    logger.exception(
        json.dumps(
            {
                "event": "model_initialization",
                "status": "failed",
                "error": str(err),
            }
        )
    )
    MODEL = None

# Feature order strictly matching training pipeline
FEATURE_NAMES = [
    "Amount",
    "TransactionFrequency",
    "DistanceFromLastTx",
    "IsInternational",
    "RiskScore",
]


def parse_numeric(val: Any, default: float = 0.0) -> float:
    """Safely parse float values from string inputs."""
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def parse_int(val: Any, default: int = 0) -> int:
    """Safely parse int values from string inputs."""
    if val is None or val == "":
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def evaluate_rules(
    amount: float,
    risk_score: float,
    is_international: int,
) -> Tuple[bool, List[str]]:
    """Evaluate deterministic rule-based fraud heuristics.

    Rule 1: Single transaction amount > $5,000.
    Rule 2: High risk score > 0.85 with international flag set.

    Args:
        amount: Transaction monetary amount.
        risk_score: Baseline/external heuristic risk score (0.0 to 1.0).
        is_international: 1 if international, 0 if domestic.

    Returns:
        Tuple of (is_flagged_by_rules, list_of_triggered_reasons).
    """
    reasons: List[str] = []

    # Rule 1: High transaction amount threshold
    if amount > 5000.00:
        reasons.append(f"Rule 1 Triggered: Transaction amount (${amount:,.2f}) exceeds $5,000 threshold")

    # Rule 2: High risk score with international transaction
    if risk_score > 0.85 and is_international == 1:
        reasons.append(
            f"Rule 2 Triggered: Elevated risk score ({risk_score:.4f} > 0.85) with international flag"
        )

    return (len(reasons) > 0, reasons)


def evaluate_ml(
    features: List[float],
) -> Tuple[bool, float, Optional[str]]:
    """Evaluate machine learning model inference.

    Args:
        features: Feature array [Amount, TransactionFrequency, DistanceFromLastTx, IsInternational, RiskScore].

    Returns:
        Tuple of (is_flagged_by_ml, fraud_probability, reason_string).
    """
    if MODEL is None:
        return (False, 0.0, None)

    try:
        # Wrap in DataFrame to maintain feature name consistency with fitted model
        df_input = pd.DataFrame([features], columns=FEATURE_NAMES)

        if hasattr(MODEL, "predict_proba"):
            probabilities = MODEL.predict_proba(df_input)[0]
            fraud_prob = float(probabilities[1]) if len(probabilities) > 1 else float(probabilities[0])
            is_ml_fraud = fraud_prob >= 0.50
        else:
            prediction = int(MODEL.predict(df_input)[0])
            is_ml_fraud = prediction == 1
            fraud_prob = 1.0 if is_ml_fraud else 0.0

        reason = None
        if is_ml_fraud:
            reason = f"ML Model Triggered: Classified as fraud with probability {fraud_prob:.4f}"

        return (is_ml_fraud, fraud_prob, reason)

    except Exception as exc:
        logger.error(
            json.dumps(
                {
                    "event": "ml_inference_error",
                    "error": str(exc),
                    "features": features,
                }
            )
        )
        return (False, 0.0, None)


def evaluate_transaction(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Evaluate a single transaction record against hybrid fraud detection logic.

    Args:
        row: Dictionary of CSV transaction fields.

    Returns:
        Flagged record dict ready for DynamoDB, or None if transaction is normal.
    """
    tx_id = row.get("TransactionID") or row.get("transaction_id", "")
    acc_id = row.get("AccountID") or row.get("account_id", "")
    ts = row.get("Timestamp") or row.get("timestamp", "")

    if not tx_id or not acc_id:
        return None

    # Parse numerical features
    amount = parse_numeric(row.get("Amount") or row.get("amount", 0.0))
    tx_freq = parse_int(row.get("TransactionFrequency") or row.get("transaction_frequency", 1))
    distance = parse_numeric(row.get("DistanceFromLastTx") or row.get("distance_from_last_tx", 0.0))
    is_intl = parse_int(row.get("IsInternational") or row.get("is_international", 0))
    risk_score = parse_numeric(row.get("RiskScore") or row.get("risk_score", 0.0))

    # 1. Rule-based evaluation
    rule_flagged, rule_reasons = evaluate_rules(
        amount=amount,
        risk_score=risk_score,
        is_international=is_intl,
    )

    # 2. Machine Learning evaluation
    feature_vector = [amount, float(tx_freq), distance, float(is_intl), risk_score]
    ml_flagged, ml_prob, ml_reason = evaluate_ml(feature_vector)

    # 3. Determine detection type and combined reason
    if rule_flagged and ml_flagged:
        detection_type = "HYBRID"
        combined_reasons = rule_reasons + ([ml_reason] if ml_reason else [])
        flag_reason = " | ".join(combined_reasons)
    elif rule_flagged:
        detection_type = "RULE"
        flag_reason = " | ".join(rule_reasons)
    elif ml_flagged:
        detection_type = "ML"
        flag_reason = ml_reason or "ML Model detected anomalous fraud pattern"
    else:
        # Legitimate transaction - not flagged
        return None

    # Prepare DynamoDB item with exact required schema
    flagged_item: Dict[str, Any] = {
        "transaction_id": str(tx_id),
        "account_id": str(acc_id),
        "amount": Decimal(str(round(amount, 2))),
        "timestamp": str(ts),
        "risk_score": Decimal(str(round(risk_score, 4))),
        "flag_reason": flag_reason,
        "detection_type": detection_type,
        "status": "PENDING_REVIEW",
        # Contextual metadata
        "ml_fraud_probability": Decimal(str(round(ml_prob, 4))),
        "transaction_frequency": int(tx_freq),
        "distance_from_last_tx": Decimal(str(round(distance, 2))),
        "is_international": int(is_intl),
        "merchant": str(row.get("Merchant") or row.get("merchant", "UNKNOWN")),
        "merchant_category": str(row.get("MerchantCategory") or row.get("merchant_category", "UNKNOWN")),
        "card_type": str(row.get("CardType") or row.get("card_type", "UNKNOWN")),
        "device_type": str(row.get("DeviceType") or row.get("device_type", "UNKNOWN")),
    }

    return flagged_item


def process_s3_file(bucket_name: str, object_key: str) -> Dict[str, Any]:
    """Stream and parse S3 CSV row-by-row and write flagged items to DynamoDB.

    Args:
        bucket_name: S3 bucket name.
        object_key: S3 object key.

    Returns:
        Summary metrics dictionary.
    """
    start_time = time.perf_counter()
    records_scanned = 0
    records_flagged = 0
    counts_by_type = {"RULE": 0, "ML": 0, "HYBRID": 0}

    logger.info(
        json.dumps(
            {
                "event": "s3_ingestion_start",
                "bucket": bucket_name,
                "key": object_key,
            }
        )
    )

    # Stream S3 object body line-by-line using streaming response
    s3_response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    body_stream = s3_response["Body"]

    # Wrap raw bytes stream into a generator yielding decoded text lines
    def line_generator():
        for line in body_stream.iter_lines():
            if line:
                yield line.decode("utf-8")

    csv_reader = csv.DictReader(line_generator())

    flagged_batch: List[Dict[str, Any]] = []

    for row in csv_reader:
        records_scanned += 1
        flagged_record = evaluate_transaction(row)

        if flagged_record is not None:
            records_flagged += 1
            det_type = flagged_record["detection_type"]
            counts_by_type[det_type] = counts_by_type.get(det_type, 0) + 1
            flagged_batch.append(flagged_record)

            # Write to DynamoDB in batches of 25 to optimize network requests
            if len(flagged_batch) >= 25:
                write_flagged_batch(flagged_batch)
                flagged_batch = []

    # Flush any remaining flagged records
    if flagged_batch:
        write_flagged_batch(flagged_batch)

    duration_ms = (time.perf_counter() - start_time) * 1000

    summary = {
        "event": "s3_ingestion_summary",
        "bucket": bucket_name,
        "key": object_key,
        "records_scanned": records_scanned,
        "records_flagged": records_flagged,
        "flagged_by_type": counts_by_type,
        "duration_ms": round(duration_ms, 2),
        "dynamodb_table": DYNAMODB_TABLE_NAME,
    }

    logger.info(json.dumps(summary))
    return summary


def write_flagged_batch(items: List[Dict[str, Any]]) -> None:
    """Write a batch of flagged transaction records into DynamoDB using batch_writer."""
    if not items:
        return
    try:
        with dynamodb_table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        logger.info(
            json.dumps(
                {
                    "event": "dynamodb_batch_write_success",
                    "item_count": len(items),
                }
            )
        )
    except Exception as exc:
        logger.exception(
            json.dumps(
                {
                    "event": "dynamodb_batch_write_error",
                    "error": str(exc),
                    "item_count": len(items),
                }
            )
        )
        # Fallback to individual put_item for granular error isolation
        for item in items:
            try:
                dynamodb_table.put_item(Item=item)
            except Exception as single_exc:
                logger.error(
                    json.dumps(
                        {
                            "event": "dynamodb_single_put_error",
                            "transaction_id": item.get("transaction_id"),
                            "error": str(single_exc),
                        }
                    )
                )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda entry point invoked on S3 ObjectCreated events.

    Args:
        event: S3 event payload.
        context: Lambda execution context.

    Returns:
        API response dictionary.
    """
    logger.info(json.dumps({"event": "lambda_invocation_received", "raw_event": event}))

    records = event.get("Records", [])
    if not records:
        logger.warning(json.dumps({"event": "empty_records_warning", "message": "No Records found in event."}))
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "No S3 records to process."}),
        }

    results: List[Dict[str, Any]] = []

    for record in records:
        s3_data = record.get("s3", {})
        bucket_name = s3_data.get("bucket", {}).get("name")
        raw_key = s3_data.get("object", {}).get("key")

        if not bucket_name or not raw_key:
            logger.error(
                json.dumps(
                    {
                        "event": "invalid_record_error",
                        "record": record,
                    }
                )
            )
            continue

        # Decode URL-encoded characters (e.g. spaces as '+' or '%20')
        object_key = urllib.parse.unquote_plus(raw_key)

        # Ignore non-CSV files or keys outside raw/
        if not object_key.endswith(".csv"):
            logger.info(
                json.dumps(
                    {
                        "event": "skipped_non_csv_object",
                        "bucket": bucket_name,
                        "key": object_key,
                    }
                )
            )
            continue

        try:
            summary = process_s3_file(bucket_name=bucket_name, object_key=object_key)
            results.append(summary)
        except Exception as exc:
            logger.exception(
                json.dumps(
                    {
                        "event": "file_processing_exception",
                        "bucket": bucket_name,
                        "key": object_key,
                        "error": str(exc),
                    }
                )
            )
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {
                        "message": "Error processing S3 file",
                        "bucket": bucket_name,
                        "key": object_key,
                        "error": str(exc),
                    }
                ),
            }

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Successfully processed S3 ingestion",
                "processed_files_count": len(results),
                "summaries": results,
            }
        ),
    }
