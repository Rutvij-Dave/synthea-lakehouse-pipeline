from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.silver.silver_quarantine")
def silver_quarantine():
    return (spark.read.table("claims_lakehouse.silver.silver_claim_fhir_r4")
      .withColumn("_dq_reason",
          F.when(F.col("claim_id").isNull(),"MISSING_CLAIM_ID")
           .when(F.col("claim_payload").isNull(),"MISSING_CLAIM_PAYLOAD"))
      .filter(F.col("_dq_reason").isNotNull())
      .withColumn("_quarantine_ts",F.current_timestamp()))
