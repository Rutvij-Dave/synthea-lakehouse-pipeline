from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="claims_lakehouse.silver.silver_procedure",
    comment="Conformed Synthea procedure events from CSV source."
)
def silver_procedure():

    df = (
        spark.read.table(
            "claims_lakehouse.bronze.bronze_csv_procedures_raw"
        )
        .select(
            F.col("PATIENT").cast("string").alias("patient_id"),
            F.col("ENCOUNTER").cast("string").alias("encounter_id"),
            F.to_timestamp("START").alias("procedure_start_ts"),
            F.to_timestamp("STOP").alias("procedure_end_ts"),
            F.col("CODE").cast("string").alias("procedure_code"),
            F.col("DESCRIPTION").cast("string").alias("procedure_description"),
            F.col("BASE_COST").cast("double").alias("base_cost"),
            F.col("REASONCODE").cast("string").alias("reason_code"),
            F.col("REASONDESCRIPTION").cast("string").alias("reason_description"),
            F.col("_ingest_ts"),
            F.col("_record_source"),
            F.col("_source_file")
        )
    )

    df = (
        df
        .withColumn(
            "procedure_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("patient_id"), F.lit("")),
                    F.coalesce(F.col("encounter_id"), F.lit("")),
                    F.coalesce(
                        F.col("procedure_start_ts").cast("string"),
                        F.lit("")
                    ),
                    F.coalesce(F.col("procedure_code"), F.lit("")),
                    F.coalesce(
                        F.col("procedure_description"),
                        F.lit("")
                    )
                ),
                256
            )
        )
        .withColumn(
            "procedure_duration_days",
            F.when(
                F.col("procedure_start_ts").isNotNull()
                & F.col("procedure_end_ts").isNotNull(),
                F.datediff(
                    F.to_date("procedure_end_ts"),
                    F.to_date("procedure_start_ts")
                )
            )
        )
        .withColumn(
            "dq_missing_patient",
            F.when(
                F.col("patient_id").isNull()
                | (F.trim(F.col("patient_id")) == ""),
                1
            ).otherwise(0)
        )
        .withColumn(
            "dq_missing_encounter",
            F.when(
                F.col("encounter_id").isNull()
                | (F.trim(F.col("encounter_id")) == ""),
                1
            ).otherwise(0)
        )
        .dropDuplicates(["procedure_id"])
    )

    return df
