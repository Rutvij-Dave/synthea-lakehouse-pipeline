from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# SOURCE
# ============================================================

SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_fhir_r4"


# ============================================================
# GOLD: FACT CLAIM
# Grain:
#   1 row = 1 FHIR R4 Claim
# ============================================================

@dp.table(
    name="claims_lakehouse.gold.fact_claim",
    comment="Gold fact table containing one row per FHIR R4 claim."
)
def fact_claim():

    silver_df = spark.read.table(SOURCE_TABLE)

    return (
        silver_df

        # ----------------------------------------------------
        # Select claim attributes from Silver
        # ----------------------------------------------------
        .select(
            # Claim identifier
            F.col("claim_id"),

            # Patient / Provider references
            F.col("claim_payload.patient.reference")
                .alias("patient_reference"),

            F.col("claim_payload.provider.reference")
                .alias("provider_reference"),

            # Claim status
            F.col("claim_payload.status")
                .alias("claim_status"),

            # Claim type
            F.col("claim_payload.type.text")
                .alias("claim_type"),

            # Service period
            F.col("claim_payload.billablePeriod.start")
                .alias("service_start"),

            F.col("claim_payload.billablePeriod.end")
                .alias("service_end"),

            # Claim creation timestamp
            F.col("claim_payload.created")
                .alias("claim_created"),

            # Payment information
            F.col("claim_payload.payment.amount.value")
                .alias("payment_amount"),

            F.col("claim_payload.payment.amount.currency")
                .alias("payment_currency"),

            # Number of claim line items
            F.size(
                F.coalesce(
                    F.col("claim_payload.item"),
                    F.array()
                )
            ).alias("claim_line_count"),

            # ------------------------------------------------
            # Lineage / audit columns
            # ------------------------------------------------
            F.col("_ingest_ts"),
            F.col("_record_source"),

            F.current_timestamp()
                .alias("_gold_created_ts")
        )

        # ----------------------------------------------------
        # Extract canonical Patient ID
        # Example:
        # Patient/12345
        #       ↓
        # 12345
        # ----------------------------------------------------
        .withColumn(
            "patient_id",
            F.regexp_extract(
                F.col("patient_reference"),
                r"Patient/([^/]+)",
                1
            )
        )

        # ----------------------------------------------------
        # Extract canonical Provider ID
        # Supports:
        # Practitioner/123
        # PractitionerRole/123
        # ----------------------------------------------------
        .withColumn(
            "provider_id",
            F.regexp_extract(
                F.col("provider_reference"),
                r"(?:Practitioner|PractitionerRole)/([^/]+)",
                1
            )
        )
    )