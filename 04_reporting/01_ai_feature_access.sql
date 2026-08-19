CREATE OR REPLACE VIEW claims_lakehouse.reporting.ai_claim_features
COMMENT "Governed AI feature access layer."
AS
SELECT * FROM claims_lakehouse.gold.mv_ai_claim_features;
