from pyspark import pipelines as dp


@dp.table(
    name="claims_lakehouse.governance.ai_feature_contract",
    comment="Governed metadata contract for the AI claim feature mart."
)
def ai_feature_contract():

    rows = [
        ("claim_id", "STRING", "IDENTIFIER",
         "Unique claim identifier", "Required"),

        ("patient_id", "STRING", "IDENTIFIER",
         "Patient identifier extracted from the claim reference", "Required"),

        ("provider_id", "STRING", "IDENTIFIER",
         "Provider/organization UUID extracted from the claim reference", "Required"),

        ("claim_type", "STRING", "CLAIM",
         "FHIR claim type", "Source dependent"),

        ("claim_status", "STRING", "CLAIM",
         "Claim lifecycle status", "Source dependent"),

        ("service_start_date", "DATE", "TEMPORAL",
         "Claim service start date", "Source dependent"),

        ("service_end_date", "DATE", "TEMPORAL",
         "Claim service end date", "Source dependent"),

        ("claim_created_ts", "TIMESTAMP", "TEMPORAL",
         "Claim creation timestamp", "Source dependent"),

        ("claim_total_amount", "DOUBLE", "FINANCIAL",
         "Total claim amount", ">= 0"),

        ("claim_total_currency", "STRING", "FINANCIAL",
         "Currency of claim total", "Source dependent"),

        ("claim_line_count", "BIGINT", "CLAIM",
         "Number of claim lines", ">= 0"),

        ("line_count_from_detail", "BIGINT", "LINE",
         "Number of detailed claim lines", ">= 0"),

        ("line_net_amount_total", "DOUBLE", "FINANCIAL",
         "Sum of claim-line net amounts", "Source dependent"),

        ("avg_line_amount", "DOUBLE", "FINANCIAL",
         "Average claim-line amount", "Source dependent"),

        ("max_line_amount", "DOUBLE", "FINANCIAL",
         "Maximum claim-line amount", "Source dependent"),

        ("min_line_amount", "DOUBLE", "FINANCIAL",
         "Minimum claim-line amount", "Source dependent"),

        ("claim_vs_line_amount_difference", "DOUBLE", "FINANCIAL",
         "Claim total minus detailed line amount total", "Diagnostic only"),

        ("claim_amount_per_line", "DOUBLE", "FINANCIAL",
         "Claim amount divided by number of lines", "Derived"),

        ("claim_amount_log", "DOUBLE", "FINANCIAL",
         "Log-transformed claim amount", "Derived"),

        ("service_duration_days", "BIGINT", "TEMPORAL",
         "Number of days between service start and end", "Derived"),

        ("patient_claim_count", "BIGINT", "PATIENT",
         "Historical claim count for patient", ">= 1 when patient exists"),

        ("patient_total_claim_amount", "DOUBLE", "PATIENT",
         "Historical total claim amount for patient", "Derived"),

        ("patient_avg_claim_amount", "DOUBLE", "PATIENT",
         "Historical average claim amount for patient", "Derived"),

        ("patient_max_claim_amount", "DOUBLE", "PATIENT",
         "Historical maximum claim amount for patient", "Derived"),

        ("patient_avg_line_count", "DOUBLE", "PATIENT",
         "Historical average claim-line count", "Derived"),

        ("provider_claim_count", "BIGINT", "PROVIDER",
         "Historical claim count for provider", ">= 1 when provider exists"),

        ("provider_unique_patient_count", "BIGINT", "PROVIDER",
         "Distinct patients associated with provider", ">= 1 when provider exists"),

        ("provider_total_claim_amount", "DOUBLE", "PROVIDER",
         "Historical total claim amount for provider", "Derived"),

        ("provider_avg_claim_amount", "DOUBLE", "PROVIDER",
         "Historical average claim amount for provider", "Derived"),

        ("provider_max_claim_amount", "DOUBLE", "PROVIDER",
         "Historical maximum claim amount for provider", "Derived"),

        ("provider_avg_line_count", "DOUBLE", "PROVIDER",
         "Historical average claim-line count for provider", "Derived"),

        ("claim_to_patient_avg_ratio", "DOUBLE", "ANOMALY",
         "Claim amount relative to patient historical average", "Derived"),

        ("claim_to_provider_avg_ratio", "DOUBLE", "ANOMALY",
         "Claim amount relative to provider historical average", "Derived"),

        ("high_amount_flag", "INT", "ANOMALY",
         "Rule-based high claim amount flag", "0 or 1"),

        ("multi_line_flag", "INT", "ANOMALY",
         "Claim has more than one line", "0 or 1"),

        ("missing_patient_flag", "INT", "DATA_QUALITY",
         "Patient identifier missing", "0 or 1"),

        ("missing_provider_flag", "INT", "DATA_QUALITY",
         "Provider identifier missing", "0 or 1"),

        ("_ingest_ts", "TIMESTAMP", "LINEAGE",
         "Source ingestion timestamp", "Lineage only"),

        ("_record_source", "STRING", "LINEAGE",
         "Source system identifier", "Lineage only"),
    ]

    return spark.createDataFrame(
        rows,
        [
            "feature_name",
            "data_type",
            "feature_group",
            "definition",
            "validation_rule",
        ],
    )