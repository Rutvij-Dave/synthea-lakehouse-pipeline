from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.silver.silver_provider_fhir_r4")
def silver_provider_fhir_r4():
    return (spark.read.table("claims_lakehouse.bronze.bronze_fhir_r4_raw")
      .select("_ingest_ts","_record_source","_source_file",F.explode("entry").alias("e"))
      .select("_ingest_ts","_record_source","_source_file",F.col("e.resource").alias("r"))
      .filter(F.col("r.resourceType")=="Practitioner")
      .select(F.col("r.id").alias("provider_id"),
              F.to_json(F.col("r.name")).alias("provider_name_json"),
              F.col("r.gender").alias("gender"),
              "_ingest_ts","_record_source","_source_file",
              F.current_timestamp().alias("_silver_created_ts"))
      .filter(F.col("provider_id").isNotNull()).dropDuplicates(["provider_id"]))
