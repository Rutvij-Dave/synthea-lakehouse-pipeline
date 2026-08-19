from pyspark import pipelines as dp
@dp.table(name="claims_lakehouse.gold.fact_claim_adjudication")
def fact_claim_adjudication():
    return spark.read.table("claims_lakehouse.silver.silver_claim_adjudication_fhir_r4")
