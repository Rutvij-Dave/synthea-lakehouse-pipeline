import dlt

# ==========================================
# CONFIGURATION VARIABLES
# ==========================================
BASE_LANDING_PATH = "/Volumes/claims_lakehouse/raw_landing/synthea_ingress"
SCHEMA_BASE_PATH = "/Volumes/claims_lakehouse/raw_landing/synthea_ingress/_schema"

# ------------------------------------------
# 1. JSON Auto Loader Stream (Fixed for Multi-line)
# ------------------------------------------
@dlt.table(
    name="bronze_json_raw",
    comment="Raw streaming ingestion for JSON/FHIR records."
)
def bronze_json_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE_PATH}/json")
        .option("multiline", "true") # <--- CRITICAL FIX for Synthea FHIR JSON
        .option("mode", "PERMISSIVE")
        .load(f"{BASE_LANDING_PATH}/json/")
    )
