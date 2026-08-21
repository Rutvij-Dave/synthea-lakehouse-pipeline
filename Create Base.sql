-- ============================================================
-- SYNTHETIC CLAIMS LAKEHOUSE - BASE SETUP
-- ============================================================

CREATE CATALOG IF NOT EXISTS claims_lakehouse;

CREATE SCHEMA IF NOT EXISTS claims_lakehouse.raw;
CREATE SCHEMA IF NOT EXISTS claims_lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS claims_lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS claims_lakehouse.gold;
CREATE SCHEMA IF NOT EXISTS claims_lakehouse.governance;
CREATE SCHEMA IF NOT EXISTS claims_lakehouse.reporting;

CREATE VOLUME IF NOT EXISTS
claims_lakehouse.raw.synthea_ingress;

-- RUN 00Landing/ 01 fetchdata.py seperately

LIST '/Volumes/claims_lakehouse/raw/synthea_ingress/extracted';

LIST '/Volumes/claims_lakehouse/raw/synthea_ingress/extracted/csv/csv';

LIST '/Volumes/claims_lakehouse/raw/synthea_ingress/extracted/fhir_r4/fhir';

-- AI TeamSELECT
 SELECT
    claim_id,
    patient_id,
    provider_id,
    claim_type,
    claim_status,
    service_start_date,
    service_end_date,
    claim_created_ts,
    claim_total_amount,
    claim_total_currency,
    claim_line_count,
    line_count_from_detail,
    line_net_amount_total,
    avg_line_amount,
    max_line_amount,
    min_line_amount,
    claim_vs_line_amount_difference,
    claim_amount_per_line,
    claim_amount_log,
    service_duration_days,
    patient_claim_count,
    patient_total_claim_amount,
    patient_avg_claim_amount,
    patient_max_claim_amount,
    patient_avg_line_count,
    provider_claim_count,
    provider_unique_patient_count,
    provider_total_claim_amount,
    provider_avg_claim_amount,
    provider_max_claim_amount,
    provider_avg_line_count,
    claim_to_patient_avg_ratio,
    claim_to_provider_avg_ratio,
    high_amount_flag,
    multi_line_flag,
    missing_patient_flag,
    missing_provider_flag,
    _ingest_ts,
    _record_source
FROM claims_lakehouse.reporting.ai_claim_features
ORDER BY claim_created_ts, claim_id limit 100;

show tables in claims_lakehouse.


SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT claim_id) AS unique_claims,
    COUNT(DISTINCT patient_id) AS unique_patients,
    COUNT(DISTINCT provider_id) AS unique_providers
FROM claims_lakehouse.gold.mv_ai_claim_features;