from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="claims_lakehouse.silver.silver_patient_fhir_r4",
    comment="Conformed FHIR R4 Patient resources."
)
def silver_patient_fhir_r4():
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
        .filter(F.col("r.resourceType") == "Patient")
        .select(
            F.col("r.id").alias("patient_id"),
            F.col("r.gender").alias("gender"),
            F.to_date(F.col("r.birthDate")).alias("birth_date"),
            F.col("r.name").alias("patient_name"),
            F.col("r.address").alias("address"),
            "_ingest_ts",
            "_record_source",
            "_source_file",
            F.current_timestamp().alias("_silver_created_ts")
        )
        .filter(F.col("patient_id").isNotNull())
        .dropDuplicates(["patient_id"])
    )
