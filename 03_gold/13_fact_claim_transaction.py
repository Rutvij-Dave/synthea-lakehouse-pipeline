from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="claims_lakehouse.gold.fact_claim_transaction",
    comment=(
        "Gold claim financial event fact derived from Silver FHIR adjudication. "
        "Grain is one row per transaction_id."
    )
)
def fact_claim_transaction():

    return (
        spark.read.table(
            "claims_lakehouse.silver.silver_claim_adjudication_fhir_r4"
        )
        .select(
            "adjudication_id",
            "claim_id",
            "line_number",
            "adjudication_category_code",
            "adjudication_category",
            "adjudication_amount",
            F.col("currency").alias("transaction_currency"),
            "_ingest_ts",
            "_record_source",
            "_source_file"
        )
        .withColumn(
            "transaction_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(
                        F.col("adjudication_id"),
                        F.lit("")
                    ),
                    F.coalesce(
                        F.col("claim_id"),
                        F.lit("")
                    ),
                    F.coalesce(
                        F.col("line_number").cast("string"),
                        F.lit("")
                    )
                ),
                256
            )
        )
        .withColumn(
            "transaction_type",
            F.lit("CLAIM_ADJUDICATION")
        )
        .withColumn(
            "transaction_amount",
            F.col("adjudication_amount").cast("double")
        )
        .select(
            "transaction_id",
            "adjudication_id",
            "claim_id",
            "line_number",
            "transaction_type",
            "adjudication_category_code",
            "adjudication_category",
            "transaction_amount",
            "transaction_currency",
            "_ingest_ts",
            "_record_source",
            "_source_file"
        )
    )
