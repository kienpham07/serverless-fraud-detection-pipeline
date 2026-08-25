"""Synthetic transaction data generator for fraud detection pipeline.

Generates realistic credit card transactions containing both legitimate
patterns and anomalous (fraudulent) behaviors.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from faker import Faker
import numpy as np
import pandas as pd

# Standard ML feature columns used for training and inference
FEATURE_COLUMNS: List[str] = [
    "Amount",
    "TransactionFrequency",
    "DistanceFromLastTx",
    "IsInternational",
    "RiskScore",
]

# Full metadata schema for streaming and ingestion pipelines
ALL_COLUMNS: List[str] = [
    "TransactionID",
    "AccountID",
    "Timestamp",
    "Amount",
    "TransactionFrequency",
    "DistanceFromLastTx",
    "IsInternational",
    "RiskScore",
    "Merchant",
    "MerchantCategory",
    "CardType",
    "DeviceType",
    "IsFraud",
]

MERCHANT_CATEGORIES: List[str] = [
    "grocery",
    "retail",
    "food_dining",
    "travel",
    "electronics",
    "digital_goods",
    "crypto_atm",
    "cash_advance",
]

CARD_TYPES: List[str] = ["Visa", "MasterCard", "American Express", "Discover"]

DEVICE_TYPES: List[str] = [
    "mobile_ios",
    "mobile_android",
    "web_browser",
    "pos_terminal",
    "atm",
]


def generate_single_transaction(
    is_fraud: bool,
    faker_instance: Optional[Faker] = None,
    account_id: Optional[str] = None,
    tx_timestamp: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate a single transaction record with realistic statistical distributions.

    Args:
        is_fraud: Whether this transaction is an anomalous/fraudulent transaction.
        faker_instance: Optional Faker instance for metadata generation.
        account_id: Optional fixed AccountID to simulate repeated account activity.
        tx_timestamp: Optional ISO 8601 timestamp string.

    Returns:
        Dictionary representing the transaction record.
    """
    fake = faker_instance or Faker()

    tx_id = f"tx_{uuid.uuid4().hex[:12]}"
    acc_id = account_id or f"acc_{np.random.randint(10000000, 99999999)}"
    ts = tx_timestamp or datetime.now(timezone.utc).isoformat()

    if is_fraud:
        # Anomalous behavior patterns:
        # 1. Amount: Either high-value unauthorized transactions ($800 - $4500)
        #    or low-value micro-probing card tests ($0.50 - $4.99)
        if np.random.rand() < 0.75:
            amount = float(np.random.uniform(800.0, 4800.0))
        else:
            amount = float(np.random.uniform(0.50, 4.99))

        # 2. TransactionFrequency: High burst velocity (10 to 45 transactions/hour)
        tx_freq = int(np.random.randint(10, 45))

        # 3. DistanceFromLastTx: Impossible travel anomaly (250 to 5000 miles)
        distance = float(np.random.uniform(250.0, 5000.0))

        # 4. IsInternational: High likelihood of cross-border fraud (60% probability)
        is_international = int(1 if np.random.rand() < 0.60 else 0)

        # 5. RiskScore: Elevated heuristic/IP/device risk score (0.65 to 0.99)
        risk_score = float(np.clip(np.random.beta(a=8.0, b=2.0), 0.60, 0.99))

        # Metadata patterns for fraud
        category = np.random.choice(
            [
                "electronics",
                "digital_goods",
                "crypto_atm",
                "cash_advance",
                "travel",
            ]
        )
        device = np.random.choice(
            ["web_browser", "atm", "mobile_android"], p=[0.5, 0.3, 0.2]
        )
        fraud_label = 1

    else:
        # Legitimate behavior patterns:
        # 1. Amount: Standard log-normal retail distribution ($3 to $280)
        raw_amount = float(np.random.lognormal(mean=3.6, sigma=0.85))
        amount = float(np.clip(raw_amount, 2.50, 450.0))

        # 2. TransactionFrequency: Normal transaction velocity (1 to 5 transactions/hour)
        tx_freq = int(np.random.choice([1, 2, 3, 4, 5], p=[0.50, 0.28, 0.14, 0.06, 0.02]))

        # 3. DistanceFromLastTx: Local / regular commute distance (0.1 to 35.0 miles)
        distance = float(np.random.exponential(scale=6.0))
        distance = float(np.clip(distance, 0.1, 45.0))

        # 4. IsInternational: Rare cross-border activity (~3% probability)
        is_international = int(1 if np.random.rand() < 0.03 else 0)

        # 5. RiskScore: Low baseline risk score (0.01 to 0.35)
        risk_score = float(np.clip(np.random.beta(a=1.5, b=8.5), 0.01, 0.35))

        # Metadata patterns for normal
        category = np.random.choice(
            ["grocery", "retail", "food_dining", "travel", "electronics"],
            p=[0.40, 0.30, 0.20, 0.05, 0.05],
        )
        device = np.random.choice(
            ["pos_terminal", "mobile_ios", "mobile_android", "web_browser"],
            p=[0.45, 0.25, 0.20, 0.10],
        )
        fraud_label = 0

    merchant_name = fake.company()
    card_type = str(np.random.choice(CARD_TYPES))

    return {
        "TransactionID": tx_id,
        "AccountID": acc_id,
        "Timestamp": ts,
        "Amount": round(amount, 2),
        "TransactionFrequency": tx_freq,
        "DistanceFromLastTx": round(distance, 2),
        "IsInternational": is_international,
        "RiskScore": round(risk_score, 4),
        "Merchant": merchant_name,
        "MerchantCategory": category,
        "CardType": card_type,
        "DeviceType": device,
        "IsFraud": fraud_label,
    }


def generate_synthetic_transactions(
    n_samples: int,
    fraud_rate: float = 0.05,
    random_seed: Optional[int] = None,
    include_metadata: bool = True,
) -> pd.DataFrame:
    """Generate a synthetic DataFrame of transactions with legitimate and fraudulent samples.

    Args:
        n_samples: Total number of transaction rows to generate.
        fraud_rate: Fraction of transactions that should be fraudulent (0.0 to 1.0).
        random_seed: Optional random seed for reproducible generation.
        include_metadata: If True, include metadata columns (TransactionID, Merchant, etc.).
                         If False, return only FEATURE_COLUMNS + ['IsFraud'].

    Returns:
        pd.DataFrame containing the synthetic transaction data.

    Raises:
        ValueError: If n_samples < 1 or fraud_rate not in [0.0, 1.0].
    """
    if n_samples < 1:
        raise ValueError(f"n_samples must be at least 1, got {n_samples}")
    if not 0.0 <= fraud_rate <= 1.0:
        raise ValueError(f"fraud_rate must be between 0.0 and 1.0, got {fraud_rate}")

    if random_seed is not None:
        np.random.seed(random_seed)
        Faker.seed(random_seed)

    fake = Faker()

    # Calculate exact counts of fraudulent vs legitimate transactions
    n_fraud = int(round(n_samples * fraud_rate))
    n_normal = n_samples - n_fraud

    records: List[Dict[str, Any]] = []

    # Generate normal records
    for _ in range(n_normal):
        records.append(generate_single_transaction(is_fraud=False, faker_instance=fake))

    # Generate fraud records
    for _ in range(n_fraud):
        records.append(generate_single_transaction(is_fraud=True, faker_instance=fake))

    # Convert to DataFrame and shuffle rows
    df = pd.DataFrame(records)
    df = df.sample(frac=1.0, random_state=random_seed).reset_index(drop=True)

    if not include_metadata:
        cols_to_keep = FEATURE_COLUMNS + ["IsFraud"]
        df = df[cols_to_keep]

    return df
