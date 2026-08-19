from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_dedup"

UUID_PATTERN = (
    r"(?i)"
    r"([0-9a-f]{8}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{12})"
)


@dp.table(
    name="claims_lakehouse.gold.fact_claim",
    comment="Gold claim fact; grain is one row per unique claim_id."
)
def fact_claim():

    return (
        spark.read.table(SOURCE_TABLE)

        # Preserve the actual source relationship representation
        .withColumn(
            "patient_reference_raw",
            F.col("claim_payload.patient").cast("string")
        )

        .withColumn(
            "provider_reference_raw",
            F.col("claim_payload.provider").cast("string")
        )

        # Extract the UUID embedded in urn:uuid:...
        .withColumn(
            "patient_id",
            F.regexp_extract(
                F.col("patient_reference_raw"),
                UUID_PATTERN,
                1
            )
        )

        .withColumn(
            "provider_id",
            F.regexp_extract(
                F.col("provider_reference_raw"),
                UUID_PATTERN,
                1
            )
        )

        # Convert failed extraction to NULL
        .withColumn(
            "patient_id",
            F.when(
                F.trim(F.col("patient_id")) == "",
                F.lit(None).cast("string")
            ).otherwise(F.trim(F.col("patient_id")))
        )

        .withColumn(
            "provider_id",
            F.when(
                F.trim(F.col("provider_id")) == "",
                F.lit(None).cast("string")
            ).otherwise(F.trim(F.col("provider_id")))
        )

        .withColumn(
            "claim_type",
            F.col("claim_payload.type")
        )

        .withColumn(
            "claim_status",
            F.col("claim_payload.status")
        )

        .withColumn(
            "claim_total_amount",
            F.get_json_object(
                F.col("claim_payload.total"),
                "$.value"
            ).cast("double")
        )

        .withColumn(
            "claim_total_currency",
            F.get_json_object(
                F.col("claim_payload.total"),
                "$.currency"
            )
        )

        .withColumn(
            "service_start_date",
            F.to_date(
                F.col("claim_payload.billablePeriod.start")
            )
        )

        .withColumn(
            "service_end_date",
            F.to_date(
                F.col("claim_payload.billablePeriod.end")
            )
        )

        .withColumn(
            "claim_created_ts",
            F.to_timestamp(
                F.col("claim_payload.created")
            )
        )

        .withColumn(
            "claim_line_count",
            F.size(
                F.coalesce(
                    F.col("claim_payload.item"),
                    F.array()
                )
            )

        .select(
            "claim_id",
            "patient_id",
            "provider_id",
            "claim_type",
            "claim_status",
            "service_start_date",
            "service_end_date",
            "claim_created_ts",
            "claim_total_amount",
            "claim_total_currency",
            "claim_line_count",
            "patient_reference_raw",
            "provider_reference_raw",
            "_ingest_ts",
            "_record_source"
        )
    )