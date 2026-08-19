from pyspark import pipelines as dp
from pyspark.sql import functions as F

BASE = "/Volumes/claims_lakehouse/raw/synthea_ingress/extracted/csv/csv"


def csv_table(filename, source_name):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{BASE}/{filename}")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_record_source", F.lit(source_name))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(name="claims_lakehouse.bronze.bronze_csv_patients_raw")
def bronze_csv_patients_raw():
    return csv_table("patients.csv", "SYNTHEA_CSV")


@dp.table(name="claims_lakehouse.bronze.bronze_csv_encounters_raw")
def bronze_csv_encounters_raw():
    return csv_table("encounters.csv", "SYNTHEA_CSV")


@dp.table(name="claims_lakehouse.bronze.bronze_csv_providers_raw")
def bronze_csv_providers_raw():
    return csv_table("providers.csv", "SYNTHEA_CSV")


@dp.table(name="claims_lakehouse.bronze.bronze_csv_organizations_raw")
def bronze_csv_organizations_raw():
    return csv_table("organizations.csv", "SYNTHEA_CSV")


@dp.table(name="claims_lakehouse.bronze.bronze_csv_payers_raw")
def bronze_csv_payers_raw():
    return csv_table("payers.csv", "SYNTHEA_CSV")


@dp.table(name="claims_lakehouse.bronze.bronze_csv_conditions_raw")
def bronze_csv_conditions_raw():
    return csv_table("conditions.csv", "SYNTHEA_CSV")


@dp.table(name="claims_lakehouse.bronze.bronze_csv_procedures_raw")
def bronze_csv_procedures_raw():
    return csv_table("procedures.csv", "SYNTHEA_CSV")