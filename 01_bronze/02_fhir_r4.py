from pyspark import pipelines as dp
from pyspark.sql import functions as F
PATH="/Volumes/claims_lakehouse/raw/synthea_ingress/extracted/fhir_r4/fhir/*.json"
@dp.table(name="claims_lakehouse.bronze.bronze_fhir_r4_raw")
def bronze_fhir_r4_raw():
    return (spark.read.format("json").option("multiLine","true").load(PATH)
            .withColumn("_ingest_ts",F.current_timestamp())
            .withColumn("_record_source",F.lit("SYNTHEA_FHIR_R4"))
            .withColumn("_source_file",F.input_file_name()))
