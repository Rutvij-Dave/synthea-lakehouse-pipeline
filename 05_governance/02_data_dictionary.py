from pyspark import pipelines as dp
@dp.table(name="claims_lakehouse.governance.data_dictionary")
def data_dictionary():
    rows=[
      ("silver_claim_dedup","SILVER","Claim","1 row per claim_id"),
      ("silver_claim_line_fhir_r4","SILVER","Claim Line","1 row per Claim.item"),
      ("silver_claim_adjudication_fhir_r4","SILVER","Adjudication","1 row per claim-line adjudication"),
      ("dim_patient","GOLD","Patient","1 row per patient_id"),
      ("dim_provider","GOLD","Provider","1 row per provider_id"),
      ("dim_payer","GOLD","Payer","1 row per payer record"),
      ("fact_claim","GOLD","Claim","1 row per claim_id"),
      ("fact_claim_line","GOLD","Claim Line","1 row per claim line"),
      ("fact_claim_adjudication","GOLD","Adjudication","1 row per adjudication")]
    return spark.createDataFrame(rows,["object_name","layer","domain","grain"])
