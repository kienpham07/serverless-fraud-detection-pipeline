"""Model training script for Serverless Fraud Detection.

Generates synthetic transaction training data with regular and anomalous behaviors,
trains a lightweight RandomForestClassifier optimized for AWS Lambda execution,
evaluates precision/recall/F1 metrics, and serializes the model to joblib format.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

# Ensure scripts directory is in sys.path when running directly
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.data_generator import (
    FEATURE_COLUMNS,
    generate_synthetic_transactions,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TrainModel")


def parse_arguments() -> argparse.Namespace:
    """Parse and validate command line arguments for model training."""
    parser = argparse.ArgumentParser(
        description="Train a lightweight RandomForest fraud detection model for AWS Lambda.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=25000,
        help="Number of synthetic transactions to generate for training.",
    )
    parser.add_argument(
        "--fraud-rate",
        type=float,
        default=0.05,
        help="Fraction of transactions that are fraudulent (0.0 to 1.0).",
    )
    parser.add_argument(
        "--n-estimators",
        type=int,
        default=40,
        help="Number of decision trees (optimized for Lambda size & latency).",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=6,
        help="Maximum depth of trees to prevent overfitting and limit model size.",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.20,
        help="Proportion of the dataset to include in the test split.",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--output-model",
        type=str,
        default=str(PROJECT_ROOT / "lambda" / "ingestion" / "model.joblib"),
        help="Target filepath for serializing the trained model.",
    )
    parser.add_argument(
        "--output-dataset",
        type=str,
        default=str(PROJECT_ROOT / "data" / "synthetic_transactions.csv"),
        help="Target filepath for saving the generated dataset CSV.",
    )

    args = parser.parse_args()

    if args.samples < 100:
        parser.error("--samples must be at least 100.")
    if not 0.0 < args.fraud_rate < 1.0:
        parser.error("--fraud-rate must be strictly between 0.0 and 1.0.")
    if not 0.0 < args.test_size < 1.0:
        parser.error("--test-size must be strictly between 0.0 and 1.0.")
    if args.n_estimators < 1:
        parser.error("--n-estimators must be at least 1.")
    if args.max_depth < 1:
        parser.error("--max-depth must be at least 1.")

    return args


def prepare_dataset(
    n_samples: int,
    fraud_rate: float,
    random_state: int,
    output_dataset_path: Path,
) -> pd.DataFrame:
    """Generate synthetic transactions and save local copy."""
    logger.info(
        "Generating %d synthetic transactions (fraud_rate=%.2f%%)...",
        n_samples,
        fraud_rate * 100,
    )
    df = generate_synthetic_transactions(
        n_samples=n_samples,
        fraud_rate=fraud_rate,
        random_seed=random_state,
        include_metadata=True,
    )

    # Save dataset to staging directory
    output_dataset_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dataset_path, index=False)
    logger.info("Saved generated dataset to %s (%d rows)", output_dataset_path, len(df))

    return df


def train_fraud_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    n_estimators: int = 40,
    max_depth: int = 6,
    random_state: int = 42,
) -> RandomForestClassifier:
    """Train a lightweight Random Forest model with class balancing."""
    logger.info(
        "Training RandomForestClassifier (n_estimators=%d, max_depth=%d, features=%s)...",
        n_estimators,
        max_depth,
        list(X_train.columns),
    )
    start_time = time.perf_counter()

    clf = RandomForestClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
        class_weight="balanced",
        n_jobs=-1,
    )
    clf.fit(X_train, y_train)

    train_duration = time.perf_counter() - start_time
    logger.info("Training completed in %.3f seconds.", train_duration)
    return clf


def evaluate_model(
    clf: RandomForestClassifier,
    X_test: pd.DataFrame,
    y_test: pd.Series,
) -> Dict[str, float]:
    """Evaluate model performance on test set and log comprehensive metrics."""
    logger.info("Evaluating model on %d test samples...", len(y_test))

    y_pred = clf.predict(X_test)
    y_prob = clf.predict_proba(X_test)[:, 1]

    precision = float(precision_score(y_test, y_pred, zero_division=0))
    recall = float(recall_score(y_test, y_pred, zero_division=0))
    f1 = float(f1_score(y_test, y_pred, zero_division=0))
    roc_auc = float(roc_auc_score(y_test, y_prob))

    cm = confusion_matrix(y_test, y_pred)
    tn, fp, fn, tp = cm.ravel()

    print("\n" + "=" * 60)
    print("           FRAUD DETECTION MODEL EVALUATION REPORT           ")
    print("=" * 60)
    print(f"Test Set Total Samples : {len(y_test)}")
    print(f"Legitimate (Class 0)   : {(y_test == 0).sum()}")
    print(f"Fraudulent (Class 1)   : {(y_test == 1).sum()}")
    print("-" * 60)
    print(f"Precision (Fraud)      : {precision:.4f} ({precision*100:.2f}%)")
    print(f"Recall (Fraud)         : {recall:.4f} ({recall*100:.2f}%)")
    print(f"F1-Score (Fraud)       : {f1:.4f}")
    print(f"ROC-AUC Score          : {roc_auc:.4f}")
    print("-" * 60)
    print("Confusion Matrix:")
    print(f"  [TN: {tn:5d} | FP: {fp:5d}]")
    print(f"  [FN: {fn:5d} | TP: {tp:5d}]")
    print("-" * 60)
    print("Detailed Classification Report:\n")
    print(classification_report(y_test, y_pred, target_names=["Legitimate", "Fraud"], digits=4))
    print("-" * 60)
    print("Feature Importances:")
    importances = clf.feature_importances_
    sorted_indices = np.argsort(importances)[::-1]
    for idx in sorted_indices:
        feat_name = FEATURE_COLUMNS[idx]
        feat_imp = importances[idx]
        print(f"  - {feat_name:<22}: {feat_imp:.4f} ({feat_imp*100:.1f}%)")
    print("=" * 60 + "\n")

    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "roc_auc": roc_auc,
        "tp": int(tp),
        "fp": int(fp),
        "tn": int(tn),
        "fn": int(fn),
    }


def serialize_model(
    clf: RandomForestClassifier,
    output_path: Path,
) -> int:
    """Serialize the trained model to joblib format and verify file size.

    Args:
        clf: Trained scikit-learn classifier.
        output_path: Destination path for joblib model file.

    Returns:
        Size of the saved model in bytes.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Serializing model to %s (compress=3)...", output_path)

    joblib.dump(clf, output_path, compress=3)

    model_size_bytes = output_path.stat().st_size
    model_size_kb = model_size_bytes / 1024
    logger.info("Model saved successfully. Size: %.2f KB (%d bytes)", model_size_kb, model_size_bytes)

    # Verification: test loading and running a single dummy prediction
    logger.info("Verifying serialized model load and inference test...")
    loaded_clf: RandomForestClassifier = joblib.load(output_path)
    sample_input = pd.DataFrame(
        [
            {
                "Amount": 1250.00,
                "TransactionFrequency": 25,
                "DistanceFromLastTx": 850.0,
                "IsInternational": 1,
                "RiskScore": 0.88,
            }
        ],
        columns=FEATURE_COLUMNS,
    )
    pred = loaded_clf.predict(sample_input)[0]
    prob = loaded_clf.predict_proba(sample_input)[0, 1]
    logger.info(
        "Verification inference result: Prediction=%d (Fraud), Prob=%.4f",
        pred,
        prob,
    )

    return model_size_bytes


def main() -> None:
    """Execute the complete data generation, model training, evaluation, and export pipeline."""
    args = parse_arguments()

    output_model_path = Path(args.output_model).resolve()
    output_dataset_path = Path(args.output_dataset).resolve()

    try:
        # Step 1: Generate dataset
        df = prepare_dataset(
            n_samples=args.samples,
            fraud_rate=args.fraud_rate,
            random_state=args.random_state,
            output_dataset_path=output_dataset_path,
        )

        # Step 2: Split features and labels
        X = df[FEATURE_COLUMNS]
        y = df["IsFraud"]

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=args.test_size,
            random_state=args.random_state,
            stratify=y,
        )

        # Step 3: Train model
        clf = train_fraud_model(
            X_train=X_train,
            y_train=y_train,
            n_estimators=args.n_estimators,
            max_depth=args.max_depth,
            random_state=args.random_state,
        )

        # Step 4: Evaluate model
        metrics = evaluate_model(clf=clf, X_test=X_test, y_test=y_test)

        if metrics["recall"] < 0.80 or metrics["precision"] < 0.80:
            logger.warning(
                "Model metrics below 0.80 threshold (Precision=%.2f, Recall=%.2f). "
                "Consider adjusting dataset distribution or hyperparameters.",
                metrics["precision"],
                metrics["recall"],
            )

        # Step 5: Serialize model artifact
        serialize_model(clf=clf, output_path=output_model_path)
        logger.info("Pipeline completed successfully.")

    except Exception as e:
        logger.exception("Model training failed: %s", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
