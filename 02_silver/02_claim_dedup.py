from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# SOURCE
# ============================================================

SOURCE_TABLE = (
    "claims_lakehouse.silver.silver_claim_fhir_r4"
)


# ============================================================
# SILVER: DEDUPLICATED CLAIM
#
# Grain:
#   1 row = 1 unique claim_id
# ============================================================

@dp.table(
    name="claims_lakehouse.silver.silver_claim_dedup",
    comment="Deduplicated canonical Synthea FHIR R4 claims."
)
def silver_claim_dedup():

    source_df = spark.read.table(SOURCE_TABLE)

    # --------------------------------------------------------
    # Rank records having the same claim_id.
    #
    # We keep the latest ingested version.
    # --------------------------------------------------------

    window_spec = (
        Window
        .partitionBy("claim_id")
        .orderBy(
            F.col("_ingest_ts").desc_nulls_last()
        )
    )

    return (
        source_df

        .withColumn(
            "_dedup_rank",
            F.row_number().over(window_spec)
        )

        .filter(
            F.col("_dedup_rank") == 1
        )

        .drop("_dedup_rank")

        .withColumn(
            "_dedup_status",
            F.lit("KEEP")
        )

        .withColumn(
            "_dedup_ts",
            F.current_timestamp()
        )
    )