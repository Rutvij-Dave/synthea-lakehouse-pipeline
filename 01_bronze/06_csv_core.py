from pyspark import pipelines as dp
from pyspark.sql import functions as F


BASE_PATH = (
    "/Volumes/claims_lakehouse/raw/"
    "synthea_ingress/extracted/csv/csv"
)


def read_csv_file(filename: str):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{BASE_PATH}/{filename}")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_record_source", F.lit("SYNTHEA_CSV"))
        .withColumn("_source_file", F.input_file_name())
    )


@dp.table(
    name="claims_lakehouse.bronze.bronze_csv_patients_raw"
)
def bronze_csv_patients_raw():
    return read_csv_file("patients.csv")


@dp.table(
    name="claims_lakehouse.bronze.bronze_csv_encounters_raw"
)
def bronze_csv_encounters_raw():
    return read_csv_file("encounters.csv")


@dp.table(
    name="claims_lakehouse.bronze.bronze_csv_providers_raw"
)
def bronze_csv_providers_raw():
    return read_csv_file("providers.csv")


@dp.table(
    name="claims_lakehouse.bronze.bronze_csv_organizations_raw"
)
def bronze_csv_organizations_raw():
    return read_csv_file("organizations.csv")


@dp.table(
    name="claims_lakehouse.bronze.bronze_csv_payers_raw"
)
def bronze_csv_payers_raw():
    return read_csv_file("payers.csv")


@dp.table(
    name="claims_lakehouse.bronze.bronze_csv_conditions_raw"
)
def bronze_csv_conditions_raw():
    return read_csv_file("conditions.csv")


@dp.table(
    name="claims_lakehouse.bronze.bronze_csv_procedures_raw"
)
def bronze_csv_procedures_raw():
    return read_csv_file("procedures.csv")
