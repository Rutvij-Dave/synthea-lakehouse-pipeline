from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.gold.dim_patient")
def dim_patient():
    return spark.read.table("claims_lakehouse.silver.silver_patient_fhir_r4").select(F.sha2("patient_id",256).alias("patient_key"),"*").dropDuplicates(["patient_id"])
