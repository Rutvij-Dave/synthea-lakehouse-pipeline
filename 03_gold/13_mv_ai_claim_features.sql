CREATE OR REFRESH MATERIALIZED VIEW claims_lakehouse.gold.mv_ai_claim_features
COMMENT "AI-ready claim-level feature mart; one row per claim_id; no anomaly score or model decision."
AS
WITH line_agg AS (
    SELECT
        claim_id,
        COUNT(*) AS line_count_from_detail,
        SUM(COALESCE(line_net_amount, 0D)) AS line_net_amount_total,
        AVG(line_net_amount) AS avg_line_amount,
        MAX(line_net_amount) AS max_line_amount,
        MIN(line_net_amount) AS min_line_amount
    FROM claims_lakehouse.silver.silver_claim_line_fhir_r4
    GROUP BY claim_id
),
patient_agg AS (
    SELECT
        patient_id,
        COUNT(*) AS patient_claim_count,
        SUM(claim_total_amount) AS patient_total_claim_amount,
        AVG(claim_total_amount) AS patient_avg_claim_amount,
        MAX(claim_total_amount) AS patient_max_claim_amount,
        AVG(claim_line_count) AS patient_avg_line_count
    FROM claims_lakehouse.gold.fact_claim
    WHERE patient_id IS NOT NULL AND TRIM(patient_id) <> ''
    GROUP BY patient_id
),
provider_agg AS (
    SELECT
        provider_id,
        COUNT(*) AS provider_claim_count,
        COUNT(DISTINCT patient_id) AS provider_unique_patient_count,
        SUM(claim_total_amount) AS provider_total_claim_amount,
        AVG(claim_total_amount) AS provider_avg_claim_amount,
        MAX(claim_total_amount) AS provider_max_claim_amount,
        AVG(claim_line_count) AS provider_avg_line_count
    FROM claims_lakehouse.gold.fact_claim
    WHERE provider_id IS NOT NULL AND TRIM(provider_id) <> ''
    GROUP BY provider_id
)
SELECT
    c.claim_id,
    c.patient_id,
    c.provider_id,
    c.claim_type,
    c.claim_status,
    c.service_start_date,
    c.service_end_date,
    c.claim_created_ts,
    c.claim_total_amount,
    c.claim_total_currency,
    c.claim_line_count,

    COALESCE(l.line_count_from_detail, 0) AS line_count_from_detail,
    CASE WHEN l.line_count_from_detail IS NULL THEN NULL
         ELSE l.line_net_amount_total END AS line_net_amount_total,
    l.avg_line_amount,
    l.max_line_amount,
    l.min_line_amount,

    CASE
        WHEN c.claim_total_amount IS NULL OR l.line_count_from_detail IS NULL THEN NULL
        ELSE c.claim_total_amount - l.line_net_amount_total
    END AS claim_vs_line_amount_difference,

    CASE
        WHEN c.claim_total_amount IS NULL OR c.claim_line_count IS NULL OR c.claim_line_count = 0 THEN NULL
        ELSE c.claim_total_amount / c.claim_line_count
    END AS claim_amount_per_line,

    CASE
        WHEN c.claim_total_amount IS NULL OR c.claim_total_amount < 0 THEN NULL
        ELSE LOG1P(c.claim_total_amount)
    END AS claim_amount_log,

    CASE
        WHEN c.service_start_date IS NOT NULL AND c.service_end_date IS NOT NULL
        THEN DATEDIFF(c.service_end_date, c.service_start_date)
        ELSE NULL
    END AS service_duration_days,

    CASE WHEN c.patient_id IS NULL OR TRIM(c.patient_id) = '' THEN NULL
         ELSE COALESCE(p.patient_claim_count, 0) END AS patient_claim_count,
    CASE WHEN c.patient_id IS NULL OR TRIM(c.patient_id) = '' THEN NULL
         ELSE COALESCE(p.patient_total_claim_amount, 0D) END AS patient_total_claim_amount,
    CASE WHEN c.patient_id IS NULL OR TRIM(c.patient_id) = '' THEN NULL
         ELSE p.patient_avg_claim_amount END AS patient_avg_claim_amount,
    CASE WHEN c.patient_id IS NULL OR TRIM(c.patient_id) = '' THEN NULL
         ELSE p.patient_max_claim_amount END AS patient_max_claim_amount,
    CASE WHEN c.patient_id IS NULL OR TRIM(c.patient_id) = '' THEN NULL
         ELSE p.patient_avg_line_count END AS patient_avg_line_count,

    CASE WHEN c.provider_id IS NULL OR TRIM(c.provider_id) = '' THEN NULL
         ELSE COALESCE(pr.provider_claim_count, 0) END AS provider_claim_count,
    CASE WHEN c.provider_id IS NULL OR TRIM(c.provider_id) = '' THEN NULL
         ELSE COALESCE(pr.provider_unique_patient_count, 0) END AS provider_unique_patient_count,
    CASE WHEN c.provider_id IS NULL OR TRIM(c.provider_id) = '' THEN NULL
         ELSE COALESCE(pr.provider_total_claim_amount, 0D) END AS provider_total_claim_amount,
    CASE WHEN c.provider_id IS NULL OR TRIM(c.provider_id) = '' THEN NULL
         ELSE pr.provider_avg_claim_amount END AS provider_avg_claim_amount,
    CASE WHEN c.provider_id IS NULL OR TRIM(c.provider_id) = '' THEN NULL
         ELSE pr.provider_max_claim_amount END AS provider_max_claim_amount,
    CASE WHEN c.provider_id IS NULL OR TRIM(c.provider_id) = '' THEN NULL
         ELSE pr.provider_avg_line_count END AS provider_avg_line_count,

    CASE
        WHEN c.patient_id IS NULL OR TRIM(c.patient_id) = '' THEN NULL
        WHEN p.patient_avg_claim_amount IS NULL OR p.patient_avg_claim_amount = 0 THEN NULL
        ELSE c.claim_total_amount / p.patient_avg_claim_amount
    END AS claim_to_patient_avg_ratio,

    CASE
        WHEN c.provider_id IS NULL OR TRIM(c.provider_id) = '' THEN NULL
        WHEN pr.provider_avg_claim_amount IS NULL OR pr.provider_avg_claim_amount = 0 THEN NULL
        ELSE c.claim_total_amount / pr.provider_avg_claim_amount
    END AS claim_to_provider_avg_ratio,

    CASE WHEN c.claim_total_amount >= 1000 THEN 1 ELSE 0 END AS high_amount_flag,
    CASE WHEN c.claim_line_count > 1 THEN 1 ELSE 0 END AS multi_line_flag,
    CASE WHEN c.patient_id IS NULL OR TRIM(c.patient_id) = '' THEN 1 ELSE 0 END AS missing_patient_flag,
    CASE WHEN c.provider_id IS NULL OR TRIM(c.provider_id) = '' THEN 1 ELSE 0 END AS missing_provider_flag,

    c._ingest_ts,
    c._record_source

FROM claims_lakehouse.gold.fact_claim c
LEFT JOIN line_agg l ON c.claim_id = l.claim_id
LEFT JOIN patient_agg p ON c.patient_id = p.patient_id
LEFT JOIN provider_agg pr ON c.provider_id = pr.provider_id
