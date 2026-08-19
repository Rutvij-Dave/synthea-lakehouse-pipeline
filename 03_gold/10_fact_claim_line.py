from pyspark import pipelines as dp
@dp.table(name="claims_lakehouse.gold.fact_claim_line")
def fact_claim_line():
    return spark.read.table("claims_lakehouse.silver.silver_claim_line_fhir_r4")
