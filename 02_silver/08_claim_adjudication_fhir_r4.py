from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.silver.silver_claim_adjudication_fhir_r4")
def silver_claim_adjudication_fhir_r4():
    return (spark.read.table("claims_lakehouse.silver.silver_claim_dedup")
      .select("claim_id","_ingest_ts","_record_source","_source_file",
              F.explode_outer("claim_payload.item").alias("i"))
      .select("claim_id","_ingest_ts","_record_source","_source_file",
              F.col("i.sequence").alias("line_number"),F.explode_outer("i.adjudication").alias("a"))
      .withColumn("adjudication_category_code",F.expr("element_at(transform(a.category.coding,x -> x.code),1)"))
      .withColumn("adjudication_category",F.expr("element_at(transform(a.category.coding,x -> x.display),1)"))
      .withColumn("adjudication_amount",F.col("a.amount.value").cast("double"))
      .withColumn("currency",F.col("a.amount.currency"))
      .withColumn("adjudication_id",F.sha2(F.concat_ws("||","claim_id",F.col("line_number").cast("string"),
          F.coalesce("adjudication_category_code",F.lit(""))),256))
      .select("adjudication_id","claim_id","line_number","adjudication_category_code",
              "adjudication_category","adjudication_amount","currency",
              "_ingest_ts","_record_source","_source_file"))
