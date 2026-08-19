from pyspark import pipelines as dp
from pyspark.sql import functions as F


FHIR_R4_PATH = (
    "/Volumes/claims_lakehouse/raw/synthea_ingress/"
    "extracted/fhir_r4/fhir/*.json"
)


@dp.table(
    name="claims_lakehouse.bronze.bronze_fhir_r4_raw",
    comment="Raw Synthea FHIR R4 Bundles."
)
def bronze_fhir_r4_raw():

    return (
        spark.read
        .format("json")
        .option("multiLine", "true")
        .load(FHIR_R4_PATH)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn(
            "_record_source",
            F.lit("SYNTHEA_FHIR_R4")
        )
    )