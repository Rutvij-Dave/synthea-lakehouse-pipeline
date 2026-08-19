from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_dedup"


@dp.table(
    name="claims_lakehouse.gold.dim_claim_type",
    comment="Claim type dimension."
)
def dim_claim_type():

    return (
        spark.read.table(SOURCE_TABLE)
        .select(
            F.col("claim_payload.type").alias("claim_type")
        )
        .filter(
            F.col("claim_type").isNotNull()
        )
        .dropDuplicates(["claim_type"])
        .withColumn(
            "claim_type_key",
            F.sha2(
                F.col("claim_type"),
                256
            )
        )
    )