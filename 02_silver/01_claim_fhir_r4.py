from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.silver.silver_claim_fhir_r4")
def silver_claim_fhir_r4():
    return (spark.read.table("claims_lakehouse.bronze.bronze_fhir_r4_raw")
        .select("_ingest_ts","_record_source","_source_file",F.explode("entry").alias("e"))
        .select("_ingest_ts","_record_source","_source_file",
                F.col("e.resource.resourceType").alias("resource_type"),
                F.col("e.resource.id").alias("claim_id"),
                F.col("e.resource").alias("claim_payload"))
        .filter(F.col("resource_type")=="Claim")
        .withColumn("_silver_created_ts",F.current_timestamp()))
