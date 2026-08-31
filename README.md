# Serverless Fraud Detection Pipeline

Event-driven real-time financial fraud detection and analytics pipeline built on AWS serverless architecture.

The pipeline ingests streaming credit card transaction batches via Amazon S3, executes hybrid detection logic combining deterministic heuristic rules and Scikit-Learn Machine Learning inference within AWS Lambda, captures flagged anomalies into Amazon DynamoDB, streams mutations via DynamoDB Streams to an Alerting Lambda for instant Amazon SNS fanout, and catalogs transactions into AWS Glue for retrospective analytical SQL queries via Amazon Athena. All infrastructure is provisioned and governed declaratively via Terraform with least-privilege IAM policies, S3 lifecycle transitions, and server-side encryption.

This repository contains:
1. **Infrastructure as Code (Terraform)**: Modular configurations for S3, Lambda, Lambda Layers, DynamoDB (with Streams & GSI), SNS, Glue Data Catalog, Athena Workgroups, and IAM roles.
2. **Ingestion & Scoring Engine (`lambda/ingestion`)**: High-throughput Lambda runtime utilizing Scikit-Learn Random Forest and heuristic thresholds to evaluate risk.
3. **Change Data Capture Alerting Service (`lambda/alerting`)**: DynamoDB Stream consumer delivering prioritized SNS incident alerts to security teams.
4. **Traffic Simulation & Automation Scripts (`scripts/`)**: Docker-based Lambda layer packager, synthetic data generator, log replay simulator, model trainer, and end-to-end integration test runner.

---

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
  - [Step 1: Configure AWS CLI & Local Environment](#step-1-configure-aws-cli--local-environment)
  - [Step 2: Build the Scikit-Learn Lambda Layer](#step-2-build-the-scikit-learn-lambda-layer)
  - [Step 3: Configure Variables & Deploy Infrastructure with Terraform](#step-3-configure-variables--deploy-infrastructure-with-terraform)
  - [Step 4: Confirm SNS Email Subscription](#step-4-confirm-sns-email-subscription)
  - [Step 5: Run Automated End-to-End Test](#step-5-run-automated-end-to-end-test)
  - [Step 6: Stream Real-Time Traffic Simulation](#step-6-stream-real-time-traffic-simulation)
  - [Step 7: Inspect Multi-Tier Components & Query Analytics](#step-7-inspect-multi-tier-components--query-analytics)
  - [Step 8: Clean Up & Teardown](#step-8-clean-up--teardown)
  - [Transaction Schema & Input Format](#transaction-schema--input-format)
  - [Pipeline Integration Interfaces](#pipeline-integration-interfaces)
  - [Local Development](#local-development)
- [Configuration](#configuration)
  - [What Must Be Changed (Required)](#what-must-be-changed-required)
  - [What You Can Change (Optional)](#what-you-can-change-optional)
  - [Automated Lambda Environment Variables](#automated-lambda-environment-variables)
- [Maintainers](#maintainers)
- [Contributing](#contributing)
- [License](#license)

---

## Background

Financial institutions require real-time mitigation against compromised payment cards and coordinated fraudulent attacks without introducing latency into legitimate consumer checkouts. This architecture solves both immediate threat containment (sub-second alerting) and long-term forensic auditing (serverless SQL lake) without managing dedicated server infrastructure.

```
[ Data Producer / Simulator ]
              │
              ▼  (1. CSV Upload)
      [ Amazon S3 (raw/) ]
              │
              ▼  (2. S3 ObjectCreated Event)
  [ Ingestion Lambda (ML + Rules) ]
              │
              ▼  (3. Flagged Fraud Written)
  [ Amazon DynamoDB (FlaggedTransactions) ]
              │
              ▼  (4. DynamoDB Streams CDC)
     [ Alerting Lambda ]
              │
              ▼  (5. Publish Alert)
     [ Amazon SNS Topic ] ──► [ Security Ops Email / SMS ]
              │
              ▼  (6. Metadata Catalog & Analytics)
   [ AWS Glue Catalog & Amazon Athena ]
```

1. **Ingest**: Raw credit card transaction batches are pushed into the landing S3 bucket (`raw/` prefix).
2. **Process & Score**: S3 triggers the Ingestion Lambda, which parses CSV payloads and applies hybrid evaluation:
   - **Rule 1**: Transaction amount $> \$5,000$.
   - **Rule 2**: Risk score $> 0.85$ on international transactions.
   - **ML Model**: Pre-trained Random Forest classifier evaluates multidimensional behavioral features.
3. **Persist**: Flagged fraudulent records are committed directly to DynamoDB with execution metadata, probabilities, and risk levels.
4. **Alert**: DynamoDB Streams captures row inserts and triggers the Alerting Lambda to dispatch structured alerts across Amazon SNS subscribers.
5. **Analyze**: AWS Glue catalogues the raw transaction schemas, enabling analytical SQL queries across historical datasets via Amazon Athena.

### Expected Input Data Pattern (CSV)

```csv
TransactionID,AccountID,Timestamp,Amount,Merchant,Category,Location,TransactionFrequency,DistanceFromLastTx,IsInternational,RiskScore
e2e_rule1_001,acc_91823,2026-08-30T12:00:00Z,7500.00,Luxury Watch Boutique,Retail,New York NY,3,12.4,0,0.12
e2e_rule2_002,acc_44102,2026-08-30T12:00:01Z,180.00,Cross-Border Web Host,Services,London UK,1,4520.8,1,0.94
e2e_ml_003,acc_77192,2026-08-30T12:00:02Z,1200.00,Electronics MegaStore,Retail,Los Angeles CA,18,850.5,0,0.72
e2e_hybrid_004,acc_10293,2026-08-30T12:00:03Z,6200.00,Global Crypto ATM,Finance,Zurich CH,12,3900.2,1,0.91
e2e_norm_005,acc_55102,2026-08-30T12:00:04Z,34.50,Local Grocery,Groceries,Austin TX,2,4.1,0,0.05
```

---

## Install

### Prerequisites

* **AWS CLI v2** installed and configured (`aws configure`).
* **Terraform** ($\ge$ 1.5.0).
* **Docker** running (required to compile Python 3.11 Amazon Linux 2 Lambda layers).
* **Python 3.11** with `pip` and virtual environment support.

### Provisioning Infrastructure

1. **Clone the repository**:
   ```bash
   git clone https://github.com/kienpham07/serverless-fraud-detection-pipeline.git
   cd serverless-fraud-detection-pipeline
   ```

2. **Configure Terraform variables**:
   ```bash
   cd terraform
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars and set your alert_email
   ```

3. **Deploy via Terraform**:
   ```bash
   terraform init
   terraform apply -auto-approve
   ```

4. **Service & Resource Output Mappings**:
   ```bash
   export S3_BUCKET_NAME=$(terraform output -raw raw_transactions_bucket_id)
   export DYNAMODB_TABLE_NAME=$(terraform output -raw dynamodb_table_name)
   export ATHENA_WORKGROUP=$(terraform output -raw athena_workgroup_name)
   export GLUE_DATABASE_NAME=$(terraform output -raw glue_database_name)
   cd ..
   ```

---

## Usage

### Step 1: Configure AWS CLI & Local Environment

> [!IMPORTANT]
> **AWS Credentials Requirement**: Ensure your local terminal has active AWS credentials configured before running deployment or tests.

1. **Configure AWS CLI credentials**:
   ```bash
   aws configure
   # AWS Access Key ID [None]: <YOUR_AWS_ACCESS_KEY_ID>
   # AWS Secret Access Key [None]: <YOUR_AWS_SECRET_ACCESS_KEY>
   # Default region name [None]: us-east-1
   # Default output format [None]: json
   ```

2. **Setup Python virtual environment & install dependencies**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

---

### Step 2: Build the Scikit-Learn Lambda Layer

AWS Lambda Python 3.11 requires `manylinux` pre-compiled shared objects (`.so`) for NumPy, SciPy, and Scikit-Learn. Package the layer inside the official Amazon ECR SAM container:

```bash
bash scripts/build_layer.sh
```

*(Optional: Retrain the Random Forest model and regenerate `lambda/ingestion/model.joblib`)*:
```bash
python3 scripts/train_model.py
```

---

### Step 3: Configure Variables & Deploy Infrastructure with Terraform

> [!IMPORTANT]
> **Terraform Configuration**: You **must** create `terraform/terraform.tfvars` from `terraform/terraform.tfvars.example` and set your real email address in `alert_email` so SNS can deliver alerts.

1. **Navigate to Terraform directory & create `terraform.tfvars`**:
   ```bash
   cd terraform
   cp terraform.tfvars.example terraform.tfvars
   ```

2. **Edit `terraform.tfvars`**:
   ```hcl
   aws_region   = "us-east-1"
   environment  = "dev"
   project_name = "serverless-fraud-detection"
   alert_email  = "your-actual-email@example.com"  # <-- Replace with your email
   ```

3. **Initialize and deploy infrastructure**:
   ```bash
   terraform init
   terraform apply -auto-approve
   ```

4. **Export resource outputs into your terminal session**:
   ```bash
   export S3_BUCKET_NAME=$(terraform output -raw raw_transactions_bucket_id)
   export DYNAMODB_TABLE_NAME=$(terraform output -raw dynamodb_table_name)
   export ATHENA_WORKGROUP=$(terraform output -raw athena_workgroup_name)
   export GLUE_DATABASE_NAME=$(terraform output -raw glue_database_name)
   cd ..
   ```

---

### Step 4: Confirm SNS Email Subscription

Check the inbox of the email address provided in `alert_email`. Open the email from **AWS Notifications** with subject `AWS Notification - Subscription Confirmation` and click **"Confirm subscription"**.

*(Without confirming, Amazon SNS cannot deliver fraud alert emails).*

---

### Step 5: Run Automated End-to-End Test

Execute the automated test harness to inject known test vectors (`RULE_1`, `RULE_2`, `ML`, `HYBRID`, `NORMAL`), poll DynamoDB, verify CloudWatch logs, and execute Athena SQL validation:

```bash
python3 scripts/e2e_test.py --bucket "$S3_BUCKET_NAME"
```

Expected output (Example):

```text
======================================================================
        SERVERLESS FRAUD DETECTION PIPELINE - E2E TEST REPORT        
======================================================================
Test Run ID               : bf6a3a3b
Overall Status            : [✔] PASSED
----------------------------------------------------------------------
1. INGESTION LAYER:
   - Target S3 Key        : raw/transactions_e2e_20260830_125446_bf6a3a3b.csv
   - Upload Latency       : 210.85 ms
2. DATABASE & ALERTING PROPAGATION:
   - Expected Fraud Tx    : 4
   - Captured in DynamoDB : 4 / 4
   - Propagation Latency  : 8.95 seconds
3. CLOUDWATCH LOGS INTEGRITY:
   - Clean Execution Logs : [✔] True
   - Unhandled Exceptions : 0
4. ATHENA SQL ANALYTICS LAYER:
   - Query Status         : SUCCEEDED
   - Retrieved Rows Count : 6
======================================================================
```

---

### Step 6: Stream Real-Time Traffic Simulation

Simulate continuous real-time credit card streaming using the log replay engine:

```bash
python3 scripts/replay_logs.py \
  --bucket "$S3_BUCKET_NAME" \
  --batch-size 50 \
  --interval 3 \
  --inject-fraud-rate 0.08 \
  --max-batches 5
```

---

### Step 7: Inspect Multi-Tier Components & Query Analytics

#### 1. Verify Flagged Items in DynamoDB
```bash
aws dynamodb scan --table-name "$DYNAMODB_TABLE_NAME" --max-items 5
```

#### 2. Tail CloudWatch Logs
```bash
# Ingestion Lambda
aws logs tail /aws/lambda/serverless-fraud-detection-ingestion --since 5m

# Alerting Lambda
aws logs tail /aws/lambda/serverless-fraud-detection-alerting --since 5m
```

#### 3. Run Analytics via Amazon Athena

**Option A: Athena Web Console**
1. Open the [AWS Athena Query Editor](https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor).
2. Switch **Workgroup** (top right) $\rightarrow$ `fraud_analysis_workgroup`.
3. Switch **Database** (left sidebar) $\rightarrow$ `fraud_analytics_db`.
4. Run analytical SQL (Example):
```sql
SELECT 
    account_id,
    COUNT(transaction_id) AS total_flagged_transactions,
    ROUND(SUM(amount), 2) AS total_flagged_volume_usd,
    ROUND(AVG(risk_score), 4) AS avg_risk_score,
    ROUND(MAX(amount), 2) AS max_single_flagged_amount_usd
FROM 
    fraud_analytics_db.raw_transactions
WHERE 
    amount > 5000.00 
    OR risk_score >= 0.65 
    OR is_fraud = 1
GROUP BY 
    account_id
ORDER BY 
    total_flagged_volume_usd DESC
LIMIT 10;
```

**Option B: AWS CLI**
```bash
QUERY_ID=$(aws athena start-query-execution \
  --work-group "$ATHENA_WORKGROUP" \
  --query-execution-context Database="$GLUE_DATABASE_NAME" \
  --query-string "SELECT account_id, COUNT(transaction_id) AS total_flagged_transactions, ROUND(SUM(amount), 2) AS total_flagged_volume_usd, ROUND(AVG(risk_score), 4) AS avg_risk_score, ROUND(MAX(amount), 2) AS max_single_flagged_amount_usd FROM fraud_analytics_db.raw_transactions WHERE amount > 5000.00 OR risk_score >= 0.65 OR is_fraud = 1 GROUP BY account_id ORDER BY total_flagged_volume_usd DESC LIMIT 10;" \
  --output text --query "QueryExecutionId")

sleep 3
aws athena get-query-results --query-execution-id "$QUERY_ID"
```

---

### Step 8: Clean Up & Teardown

```bash
# Empty S3 landing bucket
aws s3 rm "s3://$S3_BUCKET_NAME" --recursive

# Destroy all cloud resources
cd terraform
terraform destroy -auto-approve
```

---

### Transaction Schema & Input Format

| Field | Type | Description |
|---|---|---|
| `TransactionID` | String | Unique UUID/deterministic identifier for the transaction event. |
| `AccountID` | String | Customer account identifier. |
| `Timestamp` | ISO 8601 String | UTC timestamp of transaction (`YYYY-MM-DDTHH:MM:SSZ`). |
| `Amount` | Float | Transaction dollar value in USD. |
| `Merchant` | String | Merchant / vendor entity name. |
| `Category` | String | Retail category (`Retail`, `Groceries`, `Travel`, `Finance`, etc.). |
| `Location` | String | City and country / state code of purchase origin. |
| `TransactionFrequency` | Integer | Rolling count of transactions on the account in the preceding 24 hours. |
| `DistanceFromLastTx` | Float | Distance in miles between current transaction coordinates and previous event. |
| `IsInternational` | Integer | Boolean integer (`1` = International / cross-border, `0` = Domestic). |
| `RiskScore` | Float | Upstream heuristic baseline anomaly probability score ($0.0000$ to $1.0000$). |

---

### Pipeline Integration Interfaces

| Trigger / Event Source | Target Component | Protocol / Interface | Payload Description |
|---|---|---|---|
| S3 Object Creation (`raw/*.csv`) | Ingestion Lambda | S3 Event Notification | S3 bucket and object key ARN. |
| Ingestion Lambda | DynamoDB Table | DynamoDB `PutItem` API | Flagged fraud item with features and scoring reasons. |
| DynamoDB Table Mutation | Alerting Lambda | DynamoDB Streams | `NEW_IMAGE` CDC record batch (JSON attribute maps). |
| Alerting Lambda | SNS Topic | SNS `Publish` API | Human-readable body and structured JSON alert metadata. |
| Amazon Athena | S3 Raw Storage | AWS Glue Data Catalog | Direct CSV SerDe table scanning and aggregation. |

---

### Local Development

Run the entire pipeline components locally in dry-run mode without AWS credentials:

```bash
# 1. Generate local synthetic dataset (500 samples)
python3 scripts/data_generator.py --samples 500 --fraud-rate 0.10 --output data/local_test.csv

# 2. Train baseline Random Forest model locally
python3 scripts/train_model.py --data data/local_test.csv --model-output lambda/ingestion/model.joblib

# 3. Run dry-run log replayer locally
python3 scripts/replay_logs.py --dry-run --batch-size 20 --interval 1 --max-batches 3

# 4. Execute E2E test harness in offline dry-run mode
python3 scripts/e2e_test.py --dry-run
```

---

## Configuration

All customizable configuration values are organized in the **`terraform/terraform.tfvars`** file.

### What Must Be Changed (Required)

| Variable | File Location | Description |
|---|---|---|
| `alert_email` | `terraform/terraform.tfvars` | **Required.** The actual email address where Amazon SNS will send fraud alert emails. *(e.g. `your-email@example.com`)*. |

### What You Can Change (Optional)

| Variable | File Location | Default | Description |
|---|---|---|---|
| `aws_region` | `terraform/terraform.tfvars` | `"us-east-1"` | AWS Region to deploy all resources into (e.g. `us-east-1`, `us-west-2`). |
| `environment` | `terraform/terraform.tfvars` | `"dev"` | Environment tag (`dev`, `staging`, `prod`) used for resource naming and alerts. |
| `project_name` | `terraform/terraform.tfvars` | `"serverless-fraud-detection"` | Prefix used for naming and tagging all AWS resources. |

---

### Automated Lambda Environment Variables

> [!NOTE]
> **No manual changes required.** The environment variables below are **automatically configured and wired by Terraform** during `terraform apply`. You do not need to edit Python files or configure AWS Lambda manually.

#### Ingestion Lambda (`lambda/ingestion/handler.py`)
* **`DYNAMODB_TABLE_NAME`**: Automatically populated with `aws_dynamodb_table.flagged_transactions.name` (`FlaggedTransactions`).
* **`MODEL_PATH`**: Automatically set to `"model.joblib"` (the Scikit-Learn Random Forest model packaged in the Lambda).
* **`AWS_REGION_NAME`**: Automatically passed from `var.aws_region`.

#### Alerting Lambda (`lambda/alerting/handler.py`)
* **`SNS_TOPIC_ARN`**: Automatically populated with `aws_sns_topic.fraud_alerts.arn`.
* **`ENVIRONMENT`**: Automatically passed from `var.environment` to label alert subject lines.

---

## Maintainers

* **Kien Pham** - [@kienpham07](https://github.com/kienpham07)

---

## Contributing

1. Fork the repository.
2. Create a feature branch (`git checkout -b feat/fraud-rule-expansion`).
3. Validate Terraform formatting:
   ```bash
   cd terraform && terraform fmt -check && terraform validate
   ```
4. Run integration tests in dry-run mode:
   ```bash
   python3 scripts/e2e_test.py --dry-run
   ```
5. Commit changes using Conventional Commits (`git commit -m "feat(rules): add velocity threshold check"`).
6. Open a Pull Request for review.

---

## License

This project is licensed under the [MIT License](LICENSE).
