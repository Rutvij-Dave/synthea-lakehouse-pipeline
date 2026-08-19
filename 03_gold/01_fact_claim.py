from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_dedup"

@dp.table(
    name="claims_lakehouse.gold.fact_claim",
    comment="Gold claim fact; grain is one row per unique claim_id."
)
def fact_claim():
    return (
        spark.read.table(SOURCE_TABLE)
        .withColumn("patient_reference", F.col("claim_payload.patient.reference"))
        .withColumn("provider_reference", F.col("claim_payload.provider.reference"))
        .withColumn(
            "patient_id_raw",
            F.regexp_extract(
                F.coalesce(F.col("patient_reference"), F.lit("")),
                r"Patient/([^/]+)", 1
            )
        )
        .withColumn(
            "provider_id_raw",
            F.regexp_extract(
                F.coalesce(F.col("provider_reference"), F.lit("")),
                r"(?:Practitioner|PractitionerRole)/([^/]+)", 1
            )
        )
        .withColumn(
            "patient_id",
            F.when(
                F.trim(F.col("patient_id_raw")) == "",
                F.lit(None).cast("string")
            ).otherwise(F.trim(F.col("patient_id_raw")))
        )
        .withColumn(
            "provider_id",
            F.when(
                F.trim(F.col("provider_id_raw")) == "",
                F.lit(None).cast("string")
            ).otherwise(F.trim(F.col("provider_id_raw")))
        )
        .withColumn("claim_type", F.col("claim_payload.type"))
        .withColumn("claim_status", F.col("claim_payload.status"))
        .withColumn(
            "claim_total_amount",
            F.get_json_object(F.col("claim_payload.total"), "$.value").cast("double")
        )
        .withColumn(
            "claim_total_currency",
            F.get_json_object(F.col("claim_payload.total"), "$.currency")
        )
        .withColumn(
            "service_start_date",
            F.to_date(F.col("claim_payload.billablePeriod.start"))
        )
        .withColumn(
            "service_end_date",
            F.to_date(F.col("claim_payload.billablePeriod.end"))
        )
        .withColumn(
            "claim_created_ts",
            F.to_timestamp(F.col("claim_payload.created"))
        )
        .withColumn(
            "claim_line_count",
            F.size(F.coalesce(F.col("claim_payload.item"), F.array()))
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
            "_ingest_ts",
            "_record_source"
        )
    )
