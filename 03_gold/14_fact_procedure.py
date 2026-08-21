from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="claims_lakehouse.gold.fact_procedure",
    comment="Gold procedure event fact; grain is one row per procedure_id."
)
def fact_procedure():

    return (
        spark.read.table(
            "claims_lakehouse.silver.silver_procedure"
        )
        .select(
            "procedure_id",
            "patient_id",
            "encounter_id",
            "procedure_code",
            "procedure_description",
            "procedure_start_ts",
            "procedure_end_ts",
            "procedure_duration_days",
            "base_cost",
            "reason_code",
            "reason_description",
            "dq_missing_patient",
            "dq_missing_encounter",
            "_ingest_ts",
            "_record_source",
            "_source_file"
        )
    )
