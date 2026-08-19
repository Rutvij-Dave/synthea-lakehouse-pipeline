from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.reporting.report_data_quality")
def report_data_quality():
    return spark.read.table("claims_lakehouse.gold.fact_claim").agg(
      F.count("*").alias("claim_rows"),F.countDistinct("claim_id").alias("unique_claims"),
      F.sum(F.when(F.col("patient_id").isNull(),1).otherwise(0)).alias("missing_patient"),
      F.sum(F.when(F.col("provider_id").isNull(),1).otherwise(0)).alias("missing_provider"),
      F.sum(F.when(F.col("claim_total_amount").isNull(),1).otherwise(0)).alias("missing_amount"))
