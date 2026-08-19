from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.reporting.report_claim_trends")
def report_claim_trends():
    return (spark.read.table("claims_lakehouse.gold.fact_claim").groupBy("service_start_date")
      .agg(F.countDistinct("claim_id").alias("claim_count"),F.sum("claim_total_amount").alias("total_claim_amount"),F.avg("claim_total_amount").alias("avg_claim_amount")))
