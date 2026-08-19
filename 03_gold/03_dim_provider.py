from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.gold.dim_provider")
def dim_provider():
    return spark.read.table("claims_lakehouse.silver.silver_provider_fhir_r4").select(F.sha2("provider_id",256).alias("provider_key"),"*").dropDuplicates(["provider_id"])
