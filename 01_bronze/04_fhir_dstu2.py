from pyspark import pipelines as dp
from pyspark.sql import functions as F

BASE_PATH = "/Volumes/claims_lakehouse/raw/synthea_ingress/extracted/fhir_dstu2"

@dp.table(
    name="claims_lakehouse.bronze.bronze_fhir_dstu2_raw",
    comment="Raw Synthea FHIR DSTU2 bundles."
)
def bronze_fhir_dstu2_raw():
    return (
        spark.read
        .format("json")
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .load(BASE_PATH)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_record_source", F.lit("SYNTHEA_FHIR_DSTU2"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
