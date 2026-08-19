from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.gold.dim_organization")
def dim_organization():
    return spark.read.table("claims_lakehouse.silver.silver_organization_fhir_r4").select(F.sha2("organization_id",256).alias("organization_key"),"*").dropDuplicates(["organization_id"])
