from pyspark import pipelines as dp
@dp.table(name="claims_lakehouse.reporting.report_claim_summary")
def report_claim_summary():
    return spark.read.table("claims_lakehouse.gold.fact_claim")
