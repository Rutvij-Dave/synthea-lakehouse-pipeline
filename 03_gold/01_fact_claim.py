from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="claims_lakehouse.gold.fact_claim")
def fact_claim():
    return (spark.read.table("claims_lakehouse.silver.silver_claim_dedup")
      .withColumn("patient_reference",F.col("claim_payload.patient.reference"))
      .withColumn("provider_reference",F.col("claim_payload.provider.reference"))
      .withColumn("patient_id",F.regexp_extract("patient_reference",r"Patient/([^/]+)",1))
      .withColumn("provider_id",F.regexp_extract("provider_reference",r"(?:Practitioner|PractitionerRole)/([^/]+)",1))
      .withColumn("claim_type",F.col("claim_payload.type"))
      .withColumn("claim_status",F.col("claim_payload.status"))
      .withColumn("claim_total_amount",F.get_json_object("claim_payload.total","$.value").cast("double"))
      .withColumn("claim_total_currency",F.get_json_object("claim_payload.total","$.currency"))
      .withColumn("service_start_date",F.to_date("claim_payload.billablePeriod.start"))
      .withColumn("service_end_date",F.to_date("claim_payload.billablePeriod.end"))
      .withColumn("claim_created_ts",F.to_timestamp("claim_payload.created"))
      .withColumn("claim_line_count",F.size(F.coalesce("claim_payload.item",F.array())))
      .select("claim_id","patient_id","provider_id","claim_type","claim_status",
              "service_start_date","service_end_date","claim_created_ts",
              "claim_total_amount","claim_total_currency","claim_line_count",
              "_ingest_ts","_record_source"))
