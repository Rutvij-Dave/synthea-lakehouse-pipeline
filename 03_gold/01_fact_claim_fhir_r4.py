from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = (
    "claims_lakehouse.silver.silver_claim_fhir_r4"
)


@dp.table(
    name="claims_lakehouse.gold.fact_claim",
    comment="Gold fact table containing one row per FHIR R4 claim."
)
def fact_claim():

    silver_df = spark.read.table(SOURCE_TABLE)

    return (
        silver_df
        .select(
            F.col("claim_id"),

            # Patient / provider relationships
            F.col("claim_payload.patient.reference")
                .alias("patient_reference"),

            F.col("claim_payload.provider.reference")
                .alias("provider_reference"),

            # Claim attributes
            F.col("claim_payload.status")
                .alias("claim_status"),

            F.col("claim_payload.type.text")
                .alias("claim_type"),

            # Service period
            F.col("claim_payload.billablePeriod.start")
                .alias("service_start"),

            F.col("claim_payload.billablePeriod.end")
                .alias("service_end"),

            # Claim creation date
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

            # Audit information
            F.col("_ingest_ts"),
            F.col("_record_source"),

            F.current_timestamp()
                .alias("_gold_created_ts")
        )
        .withColumn(
            "patient_id",
            F.regexp_extract(
                F.col("patient_reference"),
                r"Patient/([^/]+)",
                1
            )
        )
        .withColumn(
            "provider_id",
            F.regexp_extract(
                F.col("provider_reference"),
                r"(?:Practitioner|PractitionerRole)/([^/]+)",
                1
            )
    )