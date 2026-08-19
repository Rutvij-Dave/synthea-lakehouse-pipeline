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
        .select(
            F.col("claim_id"),

            F.col("patient_id"),
            F.col("provider_id"),

            F.col("patient_reference"),
            F.col("provider_reference"),

            F.col("claim_status"),
            F.col("claim_type"),

            F.to_date(
                F.col("service_start")
            ).alias("service_start_date"),

            F.to_date(
                F.col("service_end")
            ).alias("service_end_date"),

            F.to_timestamp(
                F.col("claim_created")
            ).alias("claim_created_ts"),

            F.get_json_object(
                F.col("claim_payload.total"),
                "$.value"
            ).cast("double").alias("claim_total_amount"),

            F.get_json_object(
                F.col("claim_payload.total"),
                "$.currency"
            ).alias("claim_total_currency"),

            F.size(
                F.coalesce(
                    F.col("claim_payload.item"),
                    F.array()
                )
            ).alias("claim_line_count"),

            F.col("_ingest_ts"),
            F.col("_record_source"),

            F.current_timestamp().alias("_gold_created_ts")
        )
    )