from pyspark import pipelines as dp
@dp.table(name="claims_lakehouse.gold.fact_encounter")
def fact_encounter():
    return spark.read.table("claims_lakehouse.silver.silver_encounter_fhir_r4")
