from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.bronze.bronze_fhir_r4_raw"


@dp.table(
    name="claims_lakehouse.silver.silver_provider_fhir_r4",
    comment="Conformed FHIR R4 Practitioner resources."
)
def silver_provider_fhir_r4():

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
            F.col("resource.resourceType") == "Practitioner"
        )
    )

    return (
        df
        .select(
            F.col("resource.id")
                .alias("provider_id"),

            F.to_json(
                F.col("resource.name")
            ).alias("provider_name_json"),

            F.col("resource.gender")
                .alias("gender"),

            F.col("_ingest_ts"),
            F.col("_record_source"),

            F.current_timestamp()
                .alias("_silver_created_ts")
        )
        .filter(
            F.col("provider_id").isNotNull()
        )
        .dropDuplicates(["provider_id"])
    )