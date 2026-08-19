from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.bronze.bronze_fhir_r4_raw"


@dp.table(
    name="claims_lakehouse.silver.silver_encounter_fhir_r4",
    comment="Conformed FHIR R4 Encounter resources."
)
def silver_encounter_fhir_r4():

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
            F.col("resource.resourceType") == "Encounter"
        )
    )

    return (
        df
        .select(
            F.col("resource.id")
                .alias("encounter_id"),

            F.col("resource.status")
                .alias("encounter_status"),

            F.to_json(
                F.col("resource.type")
            ).alias("encounter_type_json"),

            F.col(
                "resource.subject.reference"
            ).alias("patient_reference"),

            F.col(
                "resource.serviceProvider.reference"
            ).alias("organization_reference"),

            F.to_timestamp(
                F.col("resource.period.start")
            ).alias("encounter_start_ts"),

            F.to_timestamp(
                F.col("resource.period.end")
            ).alias("encounter_end_ts"),

            F.col("_ingest_ts"),
            F.col("_record_source"),

            F.current_timestamp()
                .alias("_silver_created_ts")
        )
        .filter(
            F.col("encounter_id").isNotNull()
        )
        .dropDuplicates(["encounter_id"])
    )