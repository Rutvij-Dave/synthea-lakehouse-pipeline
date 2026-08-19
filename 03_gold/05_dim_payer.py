from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_dedup"


@dp.table(
    name="claims_lakehouse.gold.dim_payer",
    comment="Payer dimension derived from claim insurer references."
)
def dim_payer():

    df = (
        spark.read.table(SOURCE_TABLE)
        .select(
            F.col(
                "claim_payload.insurer.reference"
            ).alias("payer_reference"),

            F.col(
                "claim_payload.insurer.display"
            ).alias("payer_name")
        )
    )

    return (
        df
        .filter(
            F.col("payer_reference").isNotNull()
        )
        .withColumn(
            "payer_id",
            F.regexp_extract(
                F.col("payer_reference"),
                r"Organization/([^/]+)",
                1
            )
        )
        .filter(
            F.col("payer_id") != ""
        )
        .select(
            "payer_id",
            "payer_name"
        )
        .dropDuplicates(["payer_id"])
        .withColumn(
            "payer_key",
            F.sha2(
                F.col("payer_id"),
                256
            )
        )
        .withColumn(
            "_created_ts",
            F.current_timestamp()
        )
    )