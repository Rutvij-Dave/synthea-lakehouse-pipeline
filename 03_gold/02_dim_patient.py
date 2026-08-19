from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.silver.silver_patient_fhir_r4"


@dp.table(
    name="claims_lakehouse.gold.dim_patient",
    comment="Patient dimension."
)
def dim_patient():

    df = (
        spark.read.table(SOURCE_TABLE)
        .select(
            F.explode("entry").alias("entry_item")
        )
        .select(
            F.col("entry_item.resource").alias("resource")
        )
        .filter(
            F.col("resource.resourceType") == "Patient"
        )
    )

    return (
        df
        .select(
            F.col("resource.id").alias("patient_id"),
            F.col("resource.name").alias("patient_name"),
            F.col("resource.gender").alias("gender"),
            F.to_date(
                F.col("resource.birthDate")
            ).alias("birth_date"),
            F.col("resource.address").alias("address"),
            F.current_timestamp().alias("_created_ts")
        )
        .filter(
            F.col("patient_id").isNotNull()
        )
        .dropDuplicates(["patient_id"])
        .withColumn(
            "patient_key",
            F.sha2(
                F.col("patient_id"),
                256
            )
        )
    )
