from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.reporting.report_provider_summary")
def report_provider_summary():
    return (spark.read.table("claims_lakehouse.gold.fact_claim").groupBy("provider_id")
      .agg(F.countDistinct("claim_id").alias("claim_count"),F.countDistinct("patient_id").alias("patient_count"),
           F.sum("claim_total_amount").alias("total_claim_amount"),F.avg("claim_total_amount").alias("avg_claim_amount")))
