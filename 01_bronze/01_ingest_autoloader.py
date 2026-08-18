import dlt

# ==========================================
# CONFIGURATION VARIABLES (Top-Level)
# ==========================================
SOURCE_DIRECTORY = "/Volumes/claims_lakehouse/raw_landing/synthea_ingress/"
SCHEMA_EVOLUTION_PATH = "/Volumes/claims_lakehouse/raw_landing/synthea_ingress/_schema_checkpoints"
FILE_FORMAT = "json"
BRONZE_TABLE_NAME = "bronze_claims_raw"

# ==========================================
# BRONZE STREAMING INGESTION
# ==========================================
@dlt.table(
    name=BRONZE_TABLE_NAME,
    comment="Raw streaming claims ingestion with Auto Loader schema inference."
)
def bronze_claims_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", FILE_FORMAT)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_EVOLUTION_PATH)
        .option("mode", "PERMISSIVE")
        .load(SOURCE_DIRECTORY)
    )