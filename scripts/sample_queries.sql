-- ==============================================================================
-- Serverless Fraud Detection Pipeline - Athena SQL Analytics Queries
-- Database: fraud_analytics_db
-- Table   : raw_transactions
-- ==============================================================================

-- ==============================================================================
-- Query 1: Top 10 Accounts with the Highest Aggregate Flagged Transaction Volume
-- Identifies compromised accounts experiencing the largest financial impact from
-- flagged/anomalous transactions (amount > $5,000, risk_score > 0.65, or anomalous pattern).

-- This query finds the accounts with the highest number of flagged transactions and the biggest suspicious money flow.

-- It tells you: which accounts are most involved in fraud-like activity, total suspicious transaction count, total amount involved, average amount, biggest single suspicious transaction
-- ==============================================================================
SELECT 
    account_id,
    COUNT(transaction_id) AS total_flagged_transactions,
    ROUND(SUM(amount), 2) AS total_flagged_volume_usd,
    ROUND(AVG(amount), 2) AS avg_flagged_amount_usd,
    ROUND(MAX(amount), 2) AS max_single_flagged_amount_usd,
    ROUND(AVG(risk_score), 4) AS avg_risk_score,
    SUM(CASE WHEN is_international = true THEN 1 ELSE 0 END) AS international_flagged_count,
    ROUND(MAX(distance_from_last_tx), 2) AS max_distance_miles
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


-- ==============================================================================
-- Query 2: Rolling Hourly Fraud Rate Calculation (Total Transactions vs. Anomalous Flags)
-- Aggregates transactions into hourly time buckets, calculates the hourly fraud
-- incidence rate, and computes a 3-hour rolling average fraud rate for trend detection.
-- This query groups transactions by hour and calculates:

-- total transactions per hour
-- suspicious transactions per hour
-- fraud rate percentage
-- a rolling 3-hour average to see trends

-- So it helps answer: “Are suspicious activities increasing recently?”
-- ==============================================================================
WITH hourly_metrics AS (
    SELECT 
        date_trunc('hour', from_iso8601_timestamp(timestamp)) AS hour_bucket,
        COUNT(transaction_id) AS total_transactions,
        SUM(
            CASE 
                WHEN amount > 5000.00 OR risk_score >= 0.65 OR is_fraud = 1 
                THEN 1 
                ELSE 0 
            END
        ) AS flagged_fraud_count,
        ROUND(SUM(amount), 2) AS total_hourly_volume_usd,
        ROUND(
            SUM(
                CASE 
                    WHEN amount > 5000.00 OR risk_score >= 0.65 OR is_fraud = 1 
                    THEN amount 
                    ELSE 0 
                END
            ), 2
        ) AS flagged_fraud_volume_usd
    FROM 
        fraud_analytics_db.raw_transactions
    GROUP BY 
        date_trunc('hour', from_iso8601_timestamp(timestamp))
),
rates_and_windows AS (
    SELECT 
        hour_bucket,
        total_transactions,
        flagged_fraud_count,
        total_hourly_volume_usd,
        flagged_fraud_volume_usd,
        ROUND(
            (CAST(flagged_fraud_count AS DOUBLE) / NULLIF(total_transactions, 0)) * 100.0, 
            2
        ) AS hourly_fraud_rate_pct
    FROM 
        hourly_metrics
)
SELECT 
    hour_bucket,
    total_transactions,
    flagged_fraud_count,
    total_hourly_volume_usd,
    flagged_fraud_volume_usd,
    hourly_fraud_rate_pct,
    ROUND(
        AVG(hourly_fraud_rate_pct) OVER (
            ORDER BY hour_bucket 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 
        2
    ) AS rolling_3hr_avg_fraud_rate_pct
FROM 
    rates_and_windows
ORDER BY 
    hour_bucket DESC;


-- ==============================================================================
-- Query 3: International Transaction Anomaly Breakdown by Risk Score Bracket
-- Segments transactions by domestic vs. international status and stratifies them
-- across 4 standardized risk score brackets (low risk, moderate risk, high risk, critical risk) to evaluate cross-border anomaly exposure.

-- This query splits transactions into:
-- International
-- Domestic
-- Then it groups them into risk bands like: low risk, moderate risk, high risk, critical risk. It shows which group has the highest anomaly rate.
-- ==============================================================================
SELECT 
    CASE 
        WHEN is_international = true THEN 'International'
        ELSE 'Domestic'
    END AS transaction_scope,
    CASE 
        WHEN risk_score < 0.20 THEN '1. Low Risk (0.00 - 0.19)'
        WHEN risk_score >= 0.20 AND risk_score < 0.50 THEN '2. Moderate Risk (0.20 - 0.49)'
        WHEN risk_score >= 0.50 AND risk_score < 0.85 THEN '3. High Risk (0.50 - 0.84)'
        ELSE '4. Critical Risk (0.85 - 1.00)'
    END AS risk_bracket,
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_volume_usd,
    ROUND(AVG(amount), 2) AS avg_transaction_amount_usd,
    ROUND(AVG(distance_from_last_tx), 2) AS avg_distance_miles,
    ROUND(MAX(distance_from_last_tx), 2) AS max_distance_miles,
    SUM(CASE WHEN is_fraud = 1 OR amount > 5000.00 OR risk_score >= 0.65 THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(
        (CAST(SUM(CASE WHEN is_fraud = 1 OR amount > 5000.00 OR risk_score >= 0.65 THEN 1 ELSE 0 END) AS DOUBLE) 
         / COUNT(transaction_id)) * 100.0, 
        2
    ) AS anomaly_rate_pct
FROM 
    fraud_analytics_db.raw_transactions
GROUP BY 
    CASE 
        WHEN is_international = true THEN 'International'
        ELSE 'Domestic'
    END,
    CASE 
        WHEN risk_score < 0.20 THEN '1. Low Risk (0.00 - 0.19)'
        WHEN risk_score >= 0.20 AND risk_score < 0.50 THEN '2. Moderate Risk (0.20 - 0.49)'
        WHEN risk_score >= 0.50 AND risk_score < 0.85 THEN '3. High Risk (0.50 - 0.84)'
        ELSE '4. Critical Risk (0.85 - 1.00)'
    END
ORDER BY 
    transaction_scope ASC,
    risk_bracket ASC;
