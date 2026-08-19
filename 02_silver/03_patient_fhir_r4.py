from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.bronze.bronze_fhir_r4_raw"


@dp.table(
    name="claims_lakehouse.silver.silver_patient_fhir_r4",
    comment="Conformed FHIR R4 Patient resources."
)
def silver_patient_fhir_r4():

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
            F.col("resource.resourceType") == "Patient"
        )
    )

    return (
        df
        .select(
            F.col("resource.id")
                .alias("patient_id"),

            F.col("resource.gender")
                .alias("gender"),

            F.to_date(
                F.col("resource.birthDate")
            ).alias("birth_date"),

            # Preserve complex source structures safely
            F.to_json(
                F.col("resource.name")
            ).alias("patient_name_json"),

            F.to_json(
                F.col("resource.address")
            ).alias("address_json"),

            F.col("_ingest_ts"),
            F.col("_record_source"),

            F.current_timestamp()
                .alias("_silver_created_ts")
        )
        .filter(
            F.col("patient_id").isNotNull()
        )
        .dropDuplicates(["patient_id"])
    )