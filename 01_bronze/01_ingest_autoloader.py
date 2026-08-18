import dlt

# ==========================================
# CONFIGURATION VARIABLES
# ==========================================
BASE_LANDING_PATH = "/Volumes/claims_lakehouse/raw_landing/synthea_ingress"

# ------------------------------------------
# 1. JSON Batch Ingestion
# ------------------------------------------
@dlt.table(
    name="bronze_json_raw",
    comment="Raw batch ingestion for multi-line JSON/FHIR bundles."
)
def bronze_json_raw():
    # Using standard Spark read instead of readStream to natively support multiline
    return (
        spark.read.format("json")
        .option("multiline", "true")
        .load(f"{BASE_LANDING_PATH}/json/")
    )
