from pyspark import pipelines as dp
from pyspark.sql import functions as F

BASE_PATH = "/Volumes/claims_lakehouse/raw/synthea_ingress/extracted/ccda"

@dp.table(
    name="claims_lakehouse.bronze.bronze_ccda_raw",
    comment="Raw Synthea C-CDA XML."
)
def bronze_ccda_raw():
    return (
        spark.read
        .format("text")
        .option("recursiveFileLookup", "true")
        .load(BASE_PATH)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_record_source", F.lit("SYNTHEA_CCDA"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
