from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.governance.dq_governance")
def dq_governance():
    c=spark.read.table("claims_lakehouse.gold.fact_claim")
    d=spark.read.table("claims_lakehouse.silver.silver_claim_dedup")
    return c.agg(F.count("*").alias("gold_rows"),F.countDistinct("claim_id").alias("gold_unique_claims"),
                 F.sum(F.when(F.col("patient_id").isNull(),1).otherwise(0)).alias("missing_patient"),
                 F.sum(F.when(F.col("provider_id").isNull(),1).otherwise(0)).alias("missing_provider"),
                 F.sum(F.when(F.col("claim_total_amount").isNull(),1).otherwise(0)).alias("missing_amount"))      .crossJoin(d.agg(F.count("*").alias("silver_rows"),F.countDistinct("claim_id").alias("silver_unique_claims")))      .withColumn("gold_grain_ok",F.col("gold_rows")==F.col("gold_unique_claims"))      .withColumn("silver_grain_ok",F.col("silver_rows")==F.col("silver_unique_claims"))      .withColumn("run_ts",F.current_timestamp())
