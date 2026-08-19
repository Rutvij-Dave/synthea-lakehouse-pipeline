from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_dedup"


@dp.table(
    name="claims_lakehouse.gold.fact_claim",
    comment="Gold claim fact. Grain: one row per unique claim."
)
def fact_claim():

    df = spark.read.table(SOURCE_TABLE)

    return (
        df
        .withColumn(
            "patient_reference",
            F.col("claim_payload.patient.reference")
        )
        .withColumn(
            "provider_reference",
            F.col("claim_payload.provider.reference")
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
        .withColumn(
            "claim_type",
            F.col("claim_payload.type")
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
        )
        .select(
            "claim_id",
            "patient_id",
            "provider_id",
            "patient_reference",
            "provider_reference",
            "claim_type",
            "claim_status",
            "service_start_date",
            "service_end_date",
            "claim_created_ts",
            "claim_total_amount",
            "claim_total_currency",
            "claim_line_count",
            "_ingest_ts",
            "_record_source",
            F.current_timestamp().alias("_gold_created_ts")
        )
    )