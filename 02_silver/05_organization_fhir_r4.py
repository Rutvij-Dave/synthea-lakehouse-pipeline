from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.bronze.bronze_fhir_r4_raw"


@dp.table(
    name="claims_lakehouse.silver.silver_organization_fhir_r4",
    comment="Conformed FHIR R4 Organization resources."
)
def silver_organization_fhir_r4():

    df = (
        spark.read.table(SOURCE_TABLE)
        .select(
            "_ingest_ts",
            "_record_source",
            F.explode("entry").alias("entry_item")
        )
        .select(
            "_ingest_ts",
            "_record_source",
            F.col("entry_item.resource").alias("resource")
        )
        .filter(
            F.col("resource.resourceType") == "Organization"
        )
    )

    return (
        df
        .select(
            F.col("resource.id")
                .alias("organization_id"),

            F.col("resource.name")
                .alias("organization_name"),

            F.to_json(
                F.col("resource.address")
            ).alias("address_json"),

            F.col("_ingest_ts"),
            F.col("_record_source"),

            F.current_timestamp()
                .alias("_silver_created_ts")
        )
        .filter(
            F.col("organization_id").isNotNull()
        )
        .dropDuplicates(["organization_id"])
    )