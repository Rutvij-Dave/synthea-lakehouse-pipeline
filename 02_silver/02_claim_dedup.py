from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
@dp.table(name="claims_lakehouse.silver.silver_claim_dedup")
def silver_claim_dedup():
    w=Window.partitionBy("claim_id").orderBy(F.col("_ingest_ts").desc_nulls_last())
    return (spark.read.table("claims_lakehouse.silver.silver_claim_fhir_r4")
        .withColumn("_duplicate_count",F.count("*").over(Window.partitionBy("claim_id")))
        .withColumn("_rn",F.row_number().over(w)).filter(F.col("_rn")==1).drop("_rn")
        .withColumn("_dedup_status",F.when(F.col("_duplicate_count")>1,"DUPLICATE_RESOLVED").otherwise("UNIQUE"))
        .withColumn("_dedup_ts",F.current_timestamp()))
@dp.table(name="claims_lakehouse.silver.silver_claim_duplicates")
def silver_claim_duplicates():
    return (spark.read.table("claims_lakehouse.silver.silver_claim_fhir_r4")
        .withColumn("_duplicate_count",F.count("*").over(Window.partitionBy("claim_id")))
        .filter(F.col("_duplicate_count")>1).withColumn("_audit_ts",F.current_timestamp()))
