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
    comment="Gold claim fact; one row per unique claim_id."
)
def fact_claim():

    df = spark.read.table(SOURCE_TABLE)

    df = df.withColumn(
        "patient_reference_raw",
        F.col("claim_payload.patient").cast("string")
    )

    df = df.withColumn(
        "provider_reference_raw",
        F.col("claim_payload.provider").cast("string")
    )

    df = df.withColumn(
        "patient_id",
        F.regexp_extract(
            F.coalesce(
                F.col("patient_reference_raw"),
                F.lit("")
            ),
            UUID_PATTERN,
            1
        )
    )

    df = df.withColumn(
        "provider_id",
        F.regexp_extract(
            F.coalesce(
                F.col("provider_reference_raw"),
                F.lit("")
            ),
            UUID_PATTERN,
            1
        )
    )

    df = df.withColumn(
        "patient_id",
        F.when(
            F.trim(F.col("patient_id")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("patient_id"))
        )
    )

    df = df.withColumn(
        "provider_id",
        F.when(
            F.trim(F.col("provider_id")) == "",
            F.lit(None).cast("string")
        ).otherwise(
            F.trim(F.col("provider_id"))
        )
    )

    df = df.withColumn(
        "claim_type",
        F.col("claim_payload.type")
    )

    df = df.withColumn(
        "claim_status",
        F.col("claim_payload.status")
    )

    df = df.withColumn(
        "claim_total_amount",
        F.get_json_object(
            F.col("claim_payload.total"),
            "$.value"
        ).cast("double")
    )

    df = df.withColumn(
        "claim_total_currency",
        F.get_json_object(
            F.col("claim_payload.total"),
            "$.currency"
        )
    )

    df = df.withColumn(
        "service_start_date",
        F.to_date(
            F.col("claim_payload.billablePeriod.start")
        )
    )

    df = df.withColumn(
        "service_end_date",
        F.to_date(
            F.col("claim_payload.billablePeriod.end")
        )
    )

    df = df.withColumn(
        "claim_created_ts",
        F.to_timestamp(
            F.col("claim_payload.created")
        )
    )

    df = df.withColumn(
        "claim_line_count",
        F.size(
            F.coalesce(
                F.col("claim_payload.item"),
                F.array()
            )
        )
    )

    return df.select(
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