import dlt
import pyspark.sql.functions as F

# ==========================================
# CONFIGURATION VARIABLES
# ==========================================
INPUT_JSON_TABLE = "bronze_json_raw"

# ------------------------------------------
# 1. Clean & Explode FHIR Bundle
# ------------------------------------------
@dlt.table(
    name="silver_json_cleaned",
    comment="Exploded FHIR bundle extracting individual clinical resources."
)
def silver_json_cleaned():
    # Use dlt.read() instead of read_stream() for batch data
    raw_df = dlt.read(INPUT_JSON_TABLE)
    
    # Explode the array of records inside the FHIR Bundle
    exploded_df = raw_df.select(F.explode("entry").alias("entry_item"))
    
    # Extract the specific resource type and the raw payload
    return exploded_df.select(
        F.col("entry_item.resource.resourceType").alias("resourceType"),
        F.col("entry_item.resource").alias("payload"),
        F.current_timestamp().alias("_ingested_timestamp")
    )
