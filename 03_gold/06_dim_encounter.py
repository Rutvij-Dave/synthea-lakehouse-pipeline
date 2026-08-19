from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.gold.dim_encounter")
def dim_encounter():
    return spark.read.table("claims_lakehouse.silver.silver_encounter_fhir_r4").select(F.sha2("encounter_id",256).alias("encounter_key"),"*").dropDuplicates(["encounter_id"])
