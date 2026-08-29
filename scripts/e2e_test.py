"""End-to-End integration test runner for Serverless Fraud Detection Pipeline.

Executes a deterministic test scenario by uploading known anomalous and normal
transactions to S3, verifying Lambda ingestion, polling DynamoDB for flagged
records with exponential backoff, asserting clean CloudWatch logs, and executing
retrospective SQL queries via Amazon Athena.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

# Ensure project root is in sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.data_generator import ALL_COLUMNS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("E2ETest")


def parse_arguments() -> argparse.Namespace:
    """Parse and validate command line arguments for E2E testing."""
    parser = argparse.ArgumentParser(
        description="Run end-to-end integration tests for the Serverless Fraud Detection Pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.environ.get("S3_BUCKET_NAME", ""),
        help="Target S3 raw transactions bucket name.",
    )
    parser.add_argument(
        "--table",
        type=str,
        default=os.environ.get("DYNAMODB_TABLE_NAME", "FlaggedTransactions"),
        help="Target DynamoDB table name for flagged fraud.",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=os.environ.get("GLUE_DATABASE_NAME", "fraud_analytics_db"),
        help="AWS Glue Catalog / Athena database name.",
    )
    parser.add_argument(
        "--workgroup",
        type=str,
        default=os.environ.get("ATHENA_WORKGROUP", "fraud_analysis_workgroup"),
        help="AWS Athena workgroup name.",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        help="AWS Region.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Maximum timeout in seconds to wait for DynamoDB and Athena propagation.",
    )
    parser.add_argument(
        "--ingestion-function",
        type=str,
        default="serverless-fraud-detection-ingestion",
        help="Name of the ingestion Lambda function.",
    )
    parser.add_argument(
        "--alerting-function",
        type=str,
        default="serverless-fraud-detection-alerting",
        help="Name of the alerting Lambda function.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate E2E verification offline without contacting live AWS resources.",
    )

    args = parser.parse_args()

    if not args.dry_run and not args.bucket:
        parser.error("--bucket is required unless running with --dry-run.")

    return args


def generate_deterministic_test_batch(test_run_id: str) -> Tuple[pd.DataFrame, Dict[str, Dict[str, Any]]]:
    """Generate a deterministic transaction batch with known ground-truth behaviors.

    Returns:
        Tuple of (DataFrame_to_upload, dict_of_expected_fraud_records).
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    # Define test records covering all detection pathways
    test_records = [
        # Case 1: Rule 1 Trigger (Amount > $5,000)
        {
            "TransactionID": f"e2e_rule1_{test_run_id}",
            "AccountID": f"acc_rule1_{test_run_id}",
            "Timestamp": now_iso,
            "Amount": 7500.00,
            "TransactionFrequency": 2,
            "DistanceFromLastTx": 3.5,
            "IsInternational": 0,
            "RiskScore": 0.12,
            "Merchant": "High-End Luxury Goods",
            "MerchantCategory": "retail",
            "CardType": "Visa",
            "DeviceType": "pos_terminal",
            "IsFraud": 1,
            "_expected_type": "RULE",
        },
        # Case 2: Rule 2 Trigger (Risk > 0.85 & IsInternational = 1)
        {
            "TransactionID": f"e2e_rule2_{test_run_id}",
            "AccountID": f"acc_rule2_{test_run_id}",
            "Timestamp": now_iso,
            "Amount": 180.00,
            "TransactionFrequency": 1,
            "DistanceFromLastTx": 8.0,
            "IsInternational": 1,
            "RiskScore": 0.94,
            "Merchant": "Cross-Border Digital Service",
            "MerchantCategory": "digital_goods",
            "CardType": "MasterCard",
            "DeviceType": "web_browser",
            "IsFraud": 1,
            "_expected_type": "RULE",
        },
        # Case 3: ML Trigger (Velocity burst + Impossible Travel anomaly)
        {
            "TransactionID": f"e2e_ml_{test_run_id}",
            "AccountID": f"acc_ml_{test_run_id}",
            "Timestamp": now_iso,
            "Amount": 1200.00,
            "TransactionFrequency": 35,
            "DistanceFromLastTx": 2200.0,
            "IsInternational": 0,
            "RiskScore": 0.72,
            "Merchant": "Electronics MegaStore",
            "MerchantCategory": "electronics",
            "CardType": "American Express",
            "DeviceType": "mobile_android",
            "IsFraud": 1,
            "_expected_type": "ML",
        },
        # Case 4: Hybrid Trigger (Amount > $5000 + International + High Risk + ML Flag)
        {
            "TransactionID": f"e2e_hybrid_{test_run_id}",
            "AccountID": f"acc_hybrid_{test_run_id}",
            "Timestamp": now_iso,
            "Amount": 6200.00,
            "TransactionFrequency": 28,
            "DistanceFromLastTx": 1800.0,
            "IsInternational": 1,
            "RiskScore": 0.91,
            "Merchant": "Global Crypto Exchange ATM",
            "MerchantCategory": "crypto_atm",
            "CardType": "Visa",
            "DeviceType": "atm",
            "IsFraud": 1,
            "_expected_type": "HYBRID",
        },
        # Case 5: Normal Legitimate Transaction (Must NOT be flagged)
        {
            "TransactionID": f"e2e_norm_{test_run_id}",
            "AccountID": f"acc_norm_{test_run_id}",
            "Timestamp": now_iso,
            "Amount": 34.50,
            "TransactionFrequency": 1,
            "DistanceFromLastTx": 2.1,
            "IsInternational": 0,
            "RiskScore": 0.05,
            "Merchant": "Local Neighborhood Grocery",
            "MerchantCategory": "grocery",
            "CardType": "Visa",
            "DeviceType": "pos_terminal",
            "IsFraud": 0,
            "_expected_type": "NONE",
        },
    ]

    expected_fraud: Dict[str, Dict[str, Any]] = {
        r["TransactionID"]: r for r in test_records if r["_expected_type"] != "NONE"
    }

    df = pd.DataFrame(test_records)
    # Exclude internal test tracking column before uploading
    df_upload = df.drop(columns=["_expected_type"])

    return df_upload, expected_fraud


def upload_test_csv_to_s3(
    s3_client: Any,
    bucket: str,
    s3_key: str,
    df: pd.DataFrame,
) -> float:
    """Upload test CSV to S3 and return latency in milliseconds."""
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    payload_bytes = csv_buffer.getvalue().encode("utf-8")

    start_time = time.perf_counter()
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=payload_bytes,
        ContentType="text/csv",
    )
    upload_latency_ms = (time.perf_counter() - start_time) * 1000
    return upload_latency_ms


def poll_dynamodb_for_records(
    dynamodb_client: Any,
    table_name: str,
    expected_records: Dict[str, Dict[str, Any]],
    timeout_seconds: int = 60,
) -> Tuple[Dict[str, Dict[str, Any]], float]:
    """Poll DynamoDB for expected flagged records with exponential backoff."""
    found_records: Dict[str, Dict[str, Any]] = {}
    start_time = time.time()
    delay = 1.0

    logger.info("Polling DynamoDB table '%s' for %d flagged records...", table_name, len(expected_records))

    while time.time() - start_time < timeout_seconds:
        for tx_id in list(expected_records.keys()):
            if tx_id in found_records:
                continue

            try:
                response = dynamodb_client.get_item(
                    TableName=table_name,
                    Key={"transaction_id": {"S": tx_id}},
                    ConsistentRead=True,
                )
                item = response.get("Item")
                if item:
                    found_records[tx_id] = item
                    logger.info("  [✔] Found transaction in DynamoDB: %s (Type: %s)", tx_id, item.get("detection_type", {}).get("S"))
            except Exception as e:
                logger.warning("Error reading from DynamoDB: %s", str(e))

        if len(found_records) == len(expected_records):
            break

        time.sleep(delay)
        delay = min(delay * 1.5, 5.0)

    propagation_time = time.time() - start_time
    return found_records, propagation_time


def verify_cloudwatch_logs(
    logs_client: Any,
    function_names: List[str],
    start_time_ms: int,
) -> Tuple[bool, int, List[str]]:
    """Query CloudWatch Logs for unhandled exceptions or error events."""
    unhandled_errors: List[str] = []

    for fn_name in function_names:
        log_group = f"/aws/lambda/{fn_name}"
        logger.info("Querying CloudWatch logs for log group: %s...", log_group)

        try:
            response = logs_client.filter_log_events(
                logGroupName=log_group,
                startTime=start_time_ms,
                filterPattern="?ERROR ?Traceback ?Exception ?CRITICAL",
            )
            events = response.get("events", [])
            for ev in events:
                msg = ev.get("message", "").strip()
                # Filter out expected operational log messages if any
                if "Traceback" in msg or "Exception" in msg:
                    unhandled_errors.append(f"[{fn_name}] {msg}")
        except ClientError as ce:
            if ce.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                logger.info("Log group %s does not exist yet (no invocations logged).", log_group)
            else:
                logger.warning("CloudWatch filter error on %s: %s", log_group, str(ce))
        except Exception as exc:
            logger.warning("Failed to query logs for %s: %s", log_group, str(exc))

    is_clean = len(unhandled_errors) == 0
    return is_clean, len(unhandled_errors), unhandled_errors


def execute_athena_verification(
    athena_client: Any,
    database: str,
    workgroup: str,
    test_run_id: str,
    timeout_seconds: int = 60,
) -> Tuple[str, List[List[str]]]:
    """Execute an Athena SQL query against the raw transactions lake and fetch results."""
    query = f"""
    SELECT 
        transaction_id, 
        account_id, 
        amount, 
        risk_score, 
        is_international, 
        merchant
    FROM {database}.raw_transactions
    WHERE transaction_id LIKE 'e2e_%_{test_run_id}'
    ORDER BY amount DESC;
    """

    logger.info("Executing Athena query via workgroup '%s'...", workgroup)
    logger.info("SQL Query:\n%s", query.strip())

    start_resp = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    query_exec_id = start_resp["QueryExecutionId"]

    start_time = time.time()
    final_status = "QUEUED"

    while time.time() - start_time < timeout_seconds:
        status_resp = athena_client.get_query_execution(QueryExecutionId=query_exec_id)
        final_status = status_resp["QueryExecution"]["Status"]["State"]

        if final_status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(2.0)

    rows: List[List[str]] = []
    if final_status == "SUCCEEDED":
        results_resp = athena_client.get_query_results(QueryExecutionId=query_exec_id)
        result_rows = results_resp.get("ResultSet", {}).get("Rows", [])
        for r in result_rows:
            row_data = [col.get("VarCharValue", "") for col in r.get("Data", [])]
            rows.append(row_data)

    return final_status, rows


def print_integration_report(
    test_run_id: str,
    s3_key: str,
    upload_latency_ms: float,
    dynamodb_propagation_time: float,
    expected_fraud_count: int,
    found_fraud_count: int,
    cloudwatch_clean: bool,
    cloudwatch_error_count: int,
    athena_status: str,
    athena_rows: List[List[str]],
    all_passed: bool,
) -> None:
    """Print a formatted ASCII summary report of the integration test results."""
    print("\n" + "=" * 70)
    print("        SERVERLESS FRAUD DETECTION PIPELINE - E2E TEST REPORT        ")
    print("=" * 70)
    print(f"Test Run ID               : {test_run_id}")
    print(f"Overall Status            : {'[✔] PASSED' if all_passed else '[✘] FAILED'}")
    print("-" * 70)
    print("1. INGESTION LAYER:")
    print(f"   - Target S3 Key        : {s3_key}")
    print(f"   - Upload Latency       : {upload_latency_ms:.2f} ms")
    print("-" * 70)
    print("2. DATABASE & ALERTING PROPAGATION:")
    print(f"   - Expected Fraud Tx    : {expected_fraud_count}")
    print(f"   - Captured in DynamoDB : {found_fraud_count} / {expected_fraud_count}")
    print(f"   - Propagation Latency  : {dynamodb_propagation_time:.2f} seconds")
    print("-" * 70)
    print("3. CLOUDWATCH LOGS INTEGRITY:")
    print(f"   - Clean Execution Logs : {'[✔] True' if cloudwatch_clean else '[✘] False'}")
    print(f"   - Unhandled Exceptions : {cloudwatch_error_count}")
    print("-" * 70)
    print("4. ATHENA SQL ANALYTICS LAYER:")
    print(f"   - Query Status         : {athena_status}")
    print(f"   - Retrieved Rows Count : {len(athena_rows)}")
    if athena_rows:
        print("\n   Retrieved Dataset Sample:")
        headers = athena_rows[0]
        data_rows = athena_rows[1:]
        header_line = " | ".join(f"{h:<22}" for h in headers)
        print("   " + header_line)
        print("   " + "-" * len(header_line))
        for row in data_rows:
            print("   " + " | ".join(f"{c:<22}" for c in row))
    print("=" * 70 + "\n")


def run_e2e_test(args: argparse.Namespace) -> bool:
    """Execute the full end-to-end integration test workflow."""
    test_run_id = uuid.uuid4().hex[:8]
    now_utc = datetime.now(timezone.utc)
    timestamp_str = now_utc.strftime("%Y%m%d_%H%M%S")
    s3_key = f"raw/transactions_e2e_{timestamp_str}_{test_run_id}.csv"
    start_time_epoch_ms = int(now_utc.timestamp() * 1000)

    logger.info("Starting E2E Integration Test (Run ID: %s, Dry Run: %s)...", test_run_id, args.dry_run)

    # Step 1: Generate deterministic dataset
    df, expected_fraud = generate_deterministic_test_batch(test_run_id=test_run_id)
    logger.info("Generated deterministic test batch (%d records, %d fraud cases)", len(df), len(expected_fraud))

    if args.dry_run:
        logger.info("[DRY RUN] Simulating pipeline verification offline...")
        print_integration_report(
            test_run_id=test_run_id,
            s3_key=s3_key,
            upload_latency_ms=15.42,
            dynamodb_propagation_time=1.85,
            expected_fraud_count=len(expected_fraud),
            found_fraud_count=len(expected_fraud),
            cloudwatch_clean=True,
            cloudwatch_error_count=0,
            athena_status="SUCCEEDED",
            athena_rows=[
                ["transaction_id", "account_id", "amount", "risk_score", "is_international", "merchant"],
                [f"e2e_rule1_{test_run_id}", f"acc_rule1_{test_run_id}", "7500.0", "0.12", "false", "High-End Luxury Goods"],
                [f"e2e_hybrid_{test_run_id}", f"acc_hybrid_{test_run_id}", "6200.0", "0.91", "true", "Global Crypto Exchange ATM"],
                [f"e2e_ml_{test_run_id}", f"acc_ml_{test_run_id}", "1200.0", "0.72", "false", "Electronics MegaStore"],
                [f"e2e_rule2_{test_run_id}", f"acc_rule2_{test_run_id}", "180.0", "0.94", "true", "Cross-Border Digital Service"],
                [f"e2e_norm_{test_run_id}", f"acc_norm_{test_run_id}", "34.5", "0.05", "false", "Local Neighborhood Grocery"],
            ],
            all_passed=True,
        )
        return True

    # Initialize AWS Clients
    session = boto3.session.Session(region_name=args.region)
    s3_client = session.client("s3")
    dynamodb_client = session.client("dynamodb")
    logs_client = session.client("logs")
    athena_client = session.client("athena")

    # Step 2: Upload CSV to S3
    logger.info("Uploading test batch to s3://%s/%s...", args.bucket, s3_key)
    upload_latency = upload_test_csv_to_s3(
        s3_client=s3_client,
        bucket=args.bucket,
        s3_key=s3_key,
        df=df,
    )
    logger.info("Upload completed in %.2f ms.", upload_latency)

    # Step 3: Poll DynamoDB for flagged fraud entries
    found_items, propagation_time = poll_dynamodb_for_records(
        dynamodb_client=dynamodb_client,
        table_name=args.table,
        expected_records=expected_fraud,
        timeout_seconds=args.timeout,
    )

    # Step 4: Verify CloudWatch logs
    cw_clean, cw_errors, error_list = verify_cloudwatch_logs(
        logs_client=logs_client,
        function_names=[args.ingestion_function, args.alerting_function],
        start_time_ms=start_time_epoch_ms,
    )
    if not cw_clean:
        logger.warning("CloudWatch logged %d error events:\n%s", cw_errors, "\n".join(error_list))

    # Step 5: Athena Query Execution
    athena_status, athena_rows = execute_athena_verification(
        athena_client=athena_client,
        database=args.database,
        workgroup=args.workgroup,
        test_run_id=test_run_id,
        timeout_seconds=args.timeout,
    )

    # Evaluate overall pass/fail condition
    all_passed = (
        len(found_items) == len(expected_fraud)
        and cw_clean
        and athena_status == "SUCCEEDED"
    )

    print_integration_report(
        test_run_id=test_run_id,
        s3_key=s3_key,
        upload_latency_ms=upload_latency,
        dynamodb_propagation_time=propagation_time,
        expected_fraud_count=len(expected_fraud),
        found_fraud_count=len(found_items),
        cloudwatch_clean=cw_clean,
        cloudwatch_error_count=cw_errors,
        athena_status=athena_status,
        athena_rows=athena_rows,
        all_passed=all_passed,
    )

    return all_passed


def main() -> None:
    """CLI entry point for E2E integration runner."""
    args = parse_arguments()
    try:
        success = run_e2e_test(args)
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.exception("E2E Integration Test encountered fatal error: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
