"""Synthetic transaction log replay and streaming simulator.

Streams synthetic credit card transactions as CSV files into AWS S3 at regular
intervals to simulate real-time event-driven ingestion for AWS Lambda.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    EndpointConnectionError,
    NoCredentialsError,
)

# Ensure scripts directory is in sys.path when running directly
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.data_generator import generate_synthetic_transactions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ReplayLogs")


class StreamingStats:
    """Track streaming metrics across batches."""

    def __init__(self) -> None:
        self.start_time: float = time.time()
        self.total_batches: int = 0
        self.total_records: int = 0
        self.total_fraud: int = 0
        self.total_bytes: int = 0

    def record_batch(self, records_count: int, fraud_count: int, bytes_count: int) -> None:
        self.total_batches += 1
        self.total_records += records_count
        self.total_fraud += fraud_count
        self.total_bytes += bytes_count

    def print_summary(self) -> None:
        elapsed = time.time() - self.start_time
        print("\n" + "=" * 60)
        print("             STREAMING REPLAY SESSION SUMMARY                ")
        print("=" * 60)
        print(f"Total Duration          : {elapsed:.2f} seconds")
        print(f"Total Batches Generated : {self.total_batches}")
        print(f"Total Transactions Sent : {self.total_records}")
        print(f"Total Fraud Injected    : {self.total_fraud} ({0 if self.total_records == 0 else (self.total_fraud / self.total_records * 100):.2f}%)")
        print(f"Total Data Transferred  : {self.total_bytes / 1024:.2f} KB")
        if elapsed > 0:
            print(f"Average Throughput      : {self.total_records / elapsed:.2f} tx/sec")
        print("=" * 60 + "\n")


def parse_arguments() -> argparse.Namespace:
    """Parse and validate command line arguments for log replaying."""
    parser = argparse.ArgumentParser(
        description="Stream synthetic transaction CSV batches to AWS S3 for serverless fraud detection.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=False,
        default=os.environ.get("S3_BUCKET_NAME", ""),
        help="Target AWS S3 bucket name (or set S3_BUCKET_NAME environment variable).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of transaction records per generated CSV batch.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Interval in seconds between successive batch uploads.",
    )
    parser.add_argument(
        "--inject-fraud-rate",
        type=float,
        default=0.05,
        help="Target proportion of fraudulent transactions per batch (0.0 to 1.0).",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Optional maximum number of batches to upload (runs indefinitely if omitted).",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="raw",
        help="S3 key prefix for storing raw transactions.",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        help="AWS region name.",
    )
    parser.add_argument(
        "--endpoint-url",
        type=str,
        default=os.environ.get("AWS_ENDPOINT_URL", None),
        help="Custom AWS S3 endpoint URL (useful for LocalStack/MinIO).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate generation and CSV formatting locally without uploading to S3.",
    )
    parser.add_argument(
        "--local-dir",
        type=str,
        default=None,
        help="Optional local directory to mirror and save generated CSV batches.",
    )

    args = parser.parse_args()

    if not args.dry_run and not args.bucket:
        parser.error("--bucket is required unless --dry-run is specified.")
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1.")
    if args.interval < 0.0:
        parser.error("--interval must be non-negative.")
    if not 0.0 <= args.inject_fraud_rate <= 1.0:
        parser.error("--inject-fraud-rate must be between 0.0 and 1.0.")
    if args.max_batches is not None and args.max_batches < 1:
        parser.error("--max-batches must be at least 1.")

    return args


def generate_batch_csv(
    batch_size: int,
    fraud_rate: float,
) -> tuple[str, int, int]:
    """Generate a batch of transactions and format as a CSV string.

    Args:
        batch_size: Number of records to generate.
        fraud_rate: Fraction of fraudulent records.

    Returns:
        Tuple containing (csv_content_string, total_records, fraud_records_count).
    """
    df = generate_synthetic_transactions(
        n_samples=batch_size,
        fraud_rate=fraud_rate,
        include_metadata=True,
    )
    fraud_count = int(df["IsFraud"].sum())
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue(), len(df), fraud_count


def upload_batch_to_s3(
    s3_client: Any,
    bucket_name: str,
    s3_key: str,
    csv_content: str,
) -> int:
    """Upload CSV payload to AWS S3.

    Args:
        s3_client: Initialized boto3 S3 client.
        bucket_name: Destination S3 bucket name.
        s3_key: S3 object key.
        csv_content: Raw CSV string.

    Returns:
        Size of uploaded payload in bytes.
    """
    payload_bytes = csv_content.encode("utf-8")
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=payload_bytes,
        ContentType="text/csv",
    )
    return len(payload_bytes)


def run_replay(
    bucket: str,
    batch_size: int,
    interval: float,
    fraud_rate: float,
    max_batches: Optional[int] = None,
    prefix: str = "raw",
    region: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    dry_run: bool = False,
    local_dir: Optional[str] = None,
) -> None:
    """Execute continuous log replay loop."""
    stats = StreamingStats()
    running = True

    def handle_shutdown(signum: int, frame: Any) -> None:
        nonlocal running
        logger.info("\nReceived termination signal. Gracefully finishing current batch...")
        running = False

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Initialize S3 client if not in dry-run mode
    s3_client = None
    if not dry_run:
        try:
            session = boto3.session.Session(region_name=region)
            s3_client = session.client("s3", endpoint_url=endpoint_url)
            logger.info("Initialized S3 client (Region: %s, Bucket: %s)", region, bucket)
        except Exception as e:
            logger.error("Failed to initialize AWS S3 client: %s", str(e))
            raise

    # Prepare local mirror directory if requested
    local_path: Optional[Path] = None
    if local_dir:
        local_path = Path(local_dir).resolve()
        local_path.mkdir(parents=True, exist_ok=True)
        logger.info("Local mirror enabled: %s", local_path)

    clean_prefix = prefix.strip("/")
    logger.info(
        "Starting replay stream | Batch size: %d | Interval: %.1fs | Fraud rate: %.1f%% | Dry run: %s",
        batch_size,
        interval,
        fraud_rate * 100,
        dry_run,
    )
    logger.info("Press Ctrl+C to stop streaming.")

    batch_idx = 0
    last_timestamp_str = ""
    timestamp_counter = 0

    while running:
        batch_idx += 1
        if max_batches is not None and batch_idx > max_batches:
            logger.info("Reached maximum requested batches (%d). Stopping.", max_batches)
            break

        now = datetime.now(timezone.utc)
        base_timestamp = now.strftime("%Y%m%d_%H%M%S")

        # Ensure unique filenames even if generating multiple batches in one second
        if base_timestamp == last_timestamp_str:
            timestamp_counter += 1
            filename = f"transactions_{base_timestamp}_{timestamp_counter:03d}.csv"
        else:
            last_timestamp_str = base_timestamp
            timestamp_counter = 0
            filename = f"transactions_{base_timestamp}.csv"

        s3_key = f"{clean_prefix}/{filename}" if clean_prefix else filename

        try:
            # Generate batch data
            csv_content, total_records, fraud_count = generate_batch_csv(
                batch_size=batch_size,
                fraud_rate=fraud_rate,
            )
            payload_bytes_len = len(csv_content.encode("utf-8"))

            # Save locally if enabled
            if local_path:
                batch_file = local_path / filename
                batch_file.write_text(csv_content, encoding="utf-8")

            # Upload to S3 if not dry run
            if not dry_run and s3_client is not None:
                start_upload = time.perf_counter()
                upload_batch_to_s3(
                    s3_client=s3_client,
                    bucket_name=bucket,
                    s3_key=s3_key,
                    csv_content=csv_content,
                )
                upload_latency = (time.perf_counter() - start_upload) * 1000
                logger.info(
                    "[Batch #%04d] Uploaded s3://%s/%s | %d txs (%d fraud) | %.2f KB | %.1f ms",
                    batch_idx,
                    bucket,
                    s3_key,
                    total_records,
                    fraud_count,
                    payload_bytes_len / 1024,
                    upload_latency,
                )
            else:
                logger.info(
                    "[Batch #%04d] [DRY RUN] Generated %s | %d txs (%d fraud) | %.2f KB",
                    batch_idx,
                    s3_key,
                    total_records,
                    fraud_count,
                    payload_bytes_len / 1024,
                )

            stats.record_batch(
                records_count=total_records,
                fraud_count=fraud_count,
                bytes_count=payload_bytes_len,
            )

        except (NoCredentialsError, EndpointConnectionError, ClientError) as aws_err:
            logger.error("AWS S3 Error during upload of batch #%d: %s", batch_idx, str(aws_err))
            if isinstance(aws_err, NoCredentialsError):
                logger.error("No AWS credentials found. Provide credentials via AWS CLI or environment variables, or use --dry-run.")
                break
        except Exception as e:
            logger.exception("Unexpected error in batch #%d: %s", batch_idx, str(e))

        if running and (max_batches is None or batch_idx < max_batches):
            time.sleep(interval)

    stats.print_summary()


def main() -> None:
    """Entry point for CLI execution."""
    args = parse_arguments()
    try:
        run_replay(
            bucket=args.bucket,
            batch_size=args.batch_size,
            interval=args.interval,
            fraud_rate=args.inject_fraud_rate,
            max_batches=args.max_batches,
            prefix=args.prefix,
            region=args.region,
            endpoint_url=args.endpoint_url,
            dry_run=args.dry_run,
            local_dir=args.local_dir,
        )
    except KeyboardInterrupt:
        logger.info("Exiting replay script.")
    except Exception as e:
        logger.exception("Fatal replay error: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
