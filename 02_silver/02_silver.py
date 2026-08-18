import dlt
import pyspark.sql.functions as F

# ==========================================
# CONFIGURATION VARIABLES (Top-Level)
# ==========================================
INPUT_BRONZE_TABLE = "bronze_claims_raw"
SILVER_TABLE_NAME = "silver_claims_cleaned"

# ==========================================
# SILVER CLEANSING & VALIDATION
# ==========================================
@dlt.table(
    name=SILVER_TABLE_NAME,
    comment="Validated and conformed claims records."
)
@dlt.expect_or_drop("valid_json_payload", "_rescued_data IS NULL")
def silver_claims_cleaned():
    raw_stream = dlt.read_stream(INPUT_BRONZE_TABLE)
    
    # Preserves original column sequence and appends audit timestamps to the end
    return raw_stream.select(
        "*",
        F.current_timestamp().alias("_ingested_timestamp")
    )