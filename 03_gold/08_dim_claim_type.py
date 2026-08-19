from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.gold.dim_claim_type")
def dim_claim_type():
    return spark.read.table("claims_lakehouse.gold.fact_claim").select("claim_type").filter(F.col("claim_type").isNotNull()).dropDuplicates().withColumn("claim_type_key",F.sha2("claim_type",256))
