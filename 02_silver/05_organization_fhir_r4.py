from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.silver.silver_organization_fhir_r4")
def silver_organization_fhir_r4():
    return (spark.read.table("claims_lakehouse.bronze.bronze_fhir_r4_raw")
      .select("_ingest_ts","_record_source","_source_file",F.explode("entry").alias("e"))
      .select("_ingest_ts","_record_source","_source_file",F.col("e.resource").alias("r"))
      .filter(F.col("r.resourceType")=="Organization")
      .select(F.col("r.id").alias("organization_id"),F.col("r.name").alias("organization_name"),
              F.to_json(F.col("r.address")).alias("address_json"),
              "_ingest_ts","_record_source","_source_file",
              F.current_timestamp().alias("_silver_created_ts"))
      .filter(F.col("organization_id").isNotNull()).dropDuplicates(["organization_id"]))
