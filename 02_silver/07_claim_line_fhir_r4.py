from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.silver.silver_claim_line_fhir_r4")
def silver_claim_line_fhir_r4():
    return (spark.read.table("claims_lakehouse.silver.silver_claim_dedup")
      .select("claim_id","_ingest_ts","_record_source","_source_file",
              F.explode_outer("claim_payload.item").alias("i"))
      .withColumn("line_number",F.col("i.sequence"))
      .withColumn("service_code",F.expr("element_at(transform(i.productOrService.coding,x -> x.code),1)"))
      .withColumn("service_description",F.expr("element_at(transform(i.productOrService.coding,x -> x.display),1)"))
      .withColumn("service_start",F.to_date("i.servicedPeriod.start"))
      .withColumn("service_end",F.to_date("i.servicedPeriod.end"))
      .withColumn("line_net_amount",F.col("i.net.value").cast("double"))
      .withColumn("currency",F.col("i.net.currency"))
      .withColumn("claim_line_id",F.sha2(F.concat_ws("||","claim_id",F.col("line_number").cast("string")),256))
      .select("claim_line_id","claim_id","line_number","service_code","service_description",
              "service_start","service_end","line_net_amount","currency",
              "_ingest_ts","_record_source","_source_file"))
