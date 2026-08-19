from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_fhir_r4"


@dp.table(
    name="claims_lakehouse.silver.silver_quarantine",
    comment="Silver records failing critical data-quality rules."
)
def silver_quarantine():

    df = spark.read.table(SOURCE_TABLE)

    return (
        df
        .withColumn(
            "_dq_reason",
            F.when(
                F.col("claim_id").isNull(),
                F.lit("MISSING_CLAIM_ID")
            )
            .when(
                F.col("claim_payload").isNull(),
                F.lit("MISSING_CLAIM_PAYLOAD")
            )
        )
        .filter(
            F.col("_dq_reason").isNotNull()
        )
        .withColumn(
            "_quarantine_ts",
            F.current_timestamp()
        )
    )