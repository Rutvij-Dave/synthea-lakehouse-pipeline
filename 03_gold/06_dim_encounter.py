from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.bronze.bronze_fhir_r4_raw"


@dp.table(
    name="claims_lakehouse.gold.dim_encounter",
    comment="Encounter dimension."
)
def dim_encounter():

    df = (
        spark.read.table(SOURCE_TABLE)
        .select(
            F.explode("entry").alias("entry_item")
        )
        .select(
            F.col("entry_item.resource").alias("resource")
        )
        .filter(
            F.col("resource.resourceType") == "Encounter"
        )
    )

    return (
        df
        .select(
            F.col("resource.id").alias("encounter_id"),
            F.col("resource.status").alias("encounter_status"),
            F.col("resource.type").alias("encounter_type"),
            F.col(
                "resource.subject.reference"
            ).alias("patient_reference"),
            F.col(
                "resource.serviceProvider.reference"
            ).alias("organization_reference"),
            F.col(
                "resource.period.start"
            ).alias("encounter_start"),
            F.col(
                "resource.period.end"
            ).alias("encounter_end"),
            F.current_timestamp().alias("_created_ts")
        )
        .filter(
            F.col("encounter_id").isNotNull()
        )
        .dropDuplicates(["encounter_id"])
        .withColumn(
            "encounter_key",
            F.sha2(
                F.col("encounter_id"),
                256
            )
        )
    )