from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.bronze.bronze_fhir_r4_raw"


@dp.table(
    name="claims_lakehouse.gold.dim_provider",
    comment="Provider dimension."
)
def dim_provider():

    df = (
        spark.read.table(SOURCE_TABLE)
        .select(
            F.explode("entry").alias("entry_item")
        )
        .select(
            F.col("entry_item.resource").alias("resource")
        )
        .filter(
            F.col("resource.resourceType") == "Practitioner"
        )
    )

    return (
        df
        .select(
            F.col("resource.id").alias("provider_id"),
            F.col("resource.name").alias("provider_name"),
            F.col("resource.gender").alias("gender"),
            F.current_timestamp().alias("_created_ts")
        )
        .filter(
            F.col("provider_id").isNotNull()
        )
        .dropDuplicates(["provider_id"])
        .withColumn(
            "provider_key",
            F.sha2(
                F.col("provider_id"),
                256
            )
        )
    )