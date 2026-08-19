from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.silver.silver_organization_fhir_r4"


@dp.table(
    name="claims_lakehouse.gold.dim_organization",
    comment="Organization dimension."
)
def dim_organization():

    df = (
        spark.read.table(SOURCE_TABLE)
        .select(
            F.explode("entry").alias("entry_item")
        )
        .select(
            F.col("entry_item.resource").alias("resource")
        )
        .filter(
            F.col("resource.resourceType") == "Organization"
        )
    )

    return (
        df
        .select(
            F.col("resource.id").alias("organization_id"),
            F.col("resource.name").alias("organization_name"),
            F.col("resource.address").alias("address"),
            F.current_timestamp().alias("_created_ts")
        )
        .filter(
            F.col("organization_id").isNotNull()
        )
        .dropDuplicates(["organization_id"])
        .withColumn(
            "organization_key",
            F.sha2(
                F.col("organization_id"),
                256
            )
        )
    )
