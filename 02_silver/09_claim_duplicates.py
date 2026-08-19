from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_fhir_r4"


@dp.table(
    name="claims_lakehouse.silver.silver_claim_duplicates",
    comment="Duplicate Claim records retained for audit and governance."
)
def silver_claim_duplicates():

    window_spec = (
        Window
        .partitionBy("claim_id")
        .orderBy(
            F.col("_ingest_ts").desc_nulls_last()
        )
    )

    df = (
        spark.read.table(SOURCE_TABLE)
        .withColumn(
            "_duplicate_count",
            F.count("*").over(
                Window.partitionBy("claim_id")
            )
        )
        .withColumn(
            "_duplicate_rank",
            F.row_number().over(window_spec)
        )
    )

    return (
        df
        .filter(
            F.col("_duplicate_count") > 1
        )
        .withColumn(
            "_duplicate_status",
            F.when(
                F.col("_duplicate_rank") == 1,
                "KEPT"
            ).otherwise("DUPLICATE")
        )
        .withColumn(
            "_quarantine_ts",
            F.current_timestamp()
        )
    )