CREATE OR REPLACE TABLE claims_lakehouse.governance.ai_feature_contract
USING DELTA
COMMENT 'Governed contract for the AI claim feature mart.'
AS
SELECT * FROM VALUES
    ('claim_id','STRING','IDENTIFIER','Claim identifier','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Required','NULL not allowed'),
    ('patient_id','STRING','IDENTIFIER','Patient identifier derived from Claim patient reference','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Required','NULL not allowed'),
    ('provider_id','STRING','IDENTIFIER','Provider/organization UUID derived from Claim provider reference','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Required','NULL not allowed'),

    ('claim_type','STRING','CLAIM','FHIR claim type','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','Source dependent'),
    ('claim_status','STRING','CLAIM','Claim lifecycle status','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','Source dependent'),
    ('service_start_date','DATE','TEMPORAL','Claim service start date','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','<= service_end_date when both exist'),
    ('service_end_date','DATE','TEMPORAL','Claim service end date','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','>= service_start_date when both exist'),
    ('claim_created_ts','TIMESTAMP','TEMPORAL','Claim creation timestamp','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','Valid timestamp'),

    ('claim_total_amount','DOUBLE','FINANCIAL','Total claim amount','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Required','>= 0'),
    ('claim_total_currency','STRING','FINANCIAL','Currency of claim total','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','Source dependent'),
    ('claim_line_count','BIGINT','CLAIM','Number of claim item lines','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Required','>= 0'),

    ('line_count_from_detail','BIGINT','LINE','Claim-line row count observed in Silver','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','>= 0'),
    ('line_net_amount_total','DOUBLE','FINANCIAL','Sum of claim-line net amounts when line detail exists','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','Source dependent'),
    ('avg_line_amount','DOUBLE','FINANCIAL','Average claim-line net amount','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','Source dependent'),
    ('max_line_amount','DOUBLE','FINANCIAL','Maximum claim-line net amount','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','Source dependent'),
    ('min_line_amount','DOUBLE','FINANCIAL','Minimum claim-line net amount','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Input','Source dependent'),
    ('claim_vs_line_amount_difference','DOUBLE','FINANCIAL','Claim total minus line-net aggregate when both are available','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Diagnostic','Do not use as hard equality rule'),
    ('claim_amount_per_line','DOUBLE','FINANCIAL','Claim total divided by claim line count','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','NULL when line count is zero or amount missing'),
    ('claim_amount_log','DOUBLE','FINANCIAL','Log-transformed claim amount feature','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','NULL for invalid negative amount'),

    ('service_duration_days','BIGINT','TEMPORAL','Days between claim service start and end','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 0 expected when both dates exist'),

    ('patient_claim_count','BIGINT','PATIENT','Historical claim count for the patient','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 1 when patient_id exists'),
    ('patient_total_claim_amount','DOUBLE','PATIENT','Historical total claim amount for the patient','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 0 expected'),
    ('patient_avg_claim_amount','DOUBLE','PATIENT','Historical average claim amount for the patient','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 0 expected'),
    ('patient_max_claim_amount','DOUBLE','PATIENT','Historical maximum claim amount for the patient','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 0 expected'),
    ('patient_avg_line_count','DOUBLE','PATIENT','Historical average claim line count for the patient','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 0'),

    ('provider_claim_count','BIGINT','PROVIDER','Historical claim count for the provider','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 1 when provider_id exists'),
    ('provider_unique_patient_count','BIGINT','PROVIDER','Distinct patients associated with provider claims','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 1 when provider_id exists'),
    ('provider_total_claim_amount','DOUBLE','PROVIDER','Historical total claim amount for provider','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 0 expected'),
    ('provider_avg_claim_amount','DOUBLE','PROVIDER','Historical average claim amount for provider','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 0 expected'),
    ('provider_max_claim_amount','DOUBLE','PROVIDER','Historical maximum claim amount for provider','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 0 expected'),
    ('provider_avg_line_count','DOUBLE','PROVIDER','Historical average claim line count for provider','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','>= 0'),

    ('claim_to_patient_avg_ratio','DOUBLE','ANOMALY','Claim amount relative to patient historical average','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','NULL when patient history unavailable or average is zero'),
    ('claim_to_provider_avg_ratio','DOUBLE','ANOMALY','Claim amount relative to provider historical average','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','NULL when provider history unavailable or average is zero'),

    ('high_amount_flag','INT','ANOMALY','Rule-based high amount indicator using current threshold','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','0 or 1'),
    ('multi_line_flag','INT','ANOMALY','Indicator that claim has more than one claim line','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','0 or 1'),
    ('missing_patient_flag','INT','DATA_QUALITY','Indicator that patient identifier is missing','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','0 or 1'),
    ('missing_provider_flag','INT','DATA_QUALITY','Indicator that provider identifier is missing','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Derived','0 or 1'),

    ('_ingest_ts','TIMESTAMP','LINEAGE','Source ingestion timestamp','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Lineage','Not a model feature'),
    ('_record_source','STRING','LINEAGE','Source system identifier','claims_lakehouse.gold.mv_ai_claim_features','1 row per claim_id','Lineage','Not a model feature')
AS feature_contract(
    feature_name,
    data_type,
    feature_group,
    definition,
    source_object,
    grain,
    feature_role,
    validation_rule
);
