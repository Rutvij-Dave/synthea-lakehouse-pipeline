from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="claims_lakehouse.silver.silver_encounter_fhir_r4",
    comment="Conformed FHIR R4 Encounter resources."
)
def silver_encounter_fhir_r4():
    return (
        spark.read.table("claims_lakehouse.bronze.bronze_fhir_r4_raw")
        .select(
            "_ingest_ts",
            "_record_source",
            "_source_file",
            F.explode("entry").alias("entry_item")
        )
        .select(
            "_ingest_ts",
            "_record_source",
            "_source_file",
            F.col("entry_item.resource").alias("r")
        )
        .filter(F.col("r.resourceType") == "Encounter")
        .select(
            F.col("r.id").alias("encounter_id"),
            F.col("r.status").alias("encounter_status"),
            F.col("r.type").alias("encounter_type"),
            F.col("r.subject.reference").alias("patient_reference"),
            F.col("r.serviceProvider.reference").alias("organization_reference"),
            F.to_timestamp(F.col("r.period.start")).alias("encounter_start_ts"),
            F.to_timestamp(F.col("r.period.end")).alias("encounter_end_ts"),
            "_ingest_ts",
            "_record_source",
            "_source_file",
            F.current_timestamp().alias("_silver_created_ts")
        )
        .filter(F.col("encounter_id").isNotNull())
        .dropDuplicates(["encounter_id"])
    )
