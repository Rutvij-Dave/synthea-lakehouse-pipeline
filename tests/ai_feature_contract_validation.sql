-- Run these in Databricks as the AI feature contract validation suite.

-- 1. Every feature in the contract must exist in the AI materialized view.
WITH contract AS (
    SELECT feature_name
    FROM claims_lakehouse.governance.ai_feature_contract
),
actual AS (
    DESCRIBE claims_lakehouse.gold.mv_ai_claim_features
)
SELECT feature_name
FROM contract
WHERE feature_name NOT IN (
    SELECT col_name FROM actual
);
