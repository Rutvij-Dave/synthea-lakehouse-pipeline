from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.bronze.bronze_fhir_r4_raw"


@dp.table(
    name="claims_lakehouse.silver.silver_claim_fhir_r4",
    comment="Canonical Silver claim resources extracted from Synthea FHIR R4 Bundles."
)
def silver_claim_fhir_r4():

    bronze_df = spark.read.table(SOURCE_TABLE)

    return (
        bronze_df
        .select(
            "_ingest_ts",
            "_record_source",
            F.explode("entry").alias("entry_item")
        )
        .select(
            "_ingest_ts",
            "_record_source",
            F.col("entry_item.resource.resourceType").alias("resource_type"),
            F.col("entry_item.resource.id").alias("claim_id"),
            F.col("entry_item.resource").alias("claim_payload")
        )
        .filter(
            F.col("resource_type") == "Claim"
        )
    )