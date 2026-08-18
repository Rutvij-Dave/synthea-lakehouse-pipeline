import dlt
import pyspark.sql.functions as F

# ==========================================
# CONFIGURATION VARIABLES
# ==========================================
INPUT_JSON_TABLE = "bronze_json_raw"

# ------------------------------------------
# 1. Clean JSON Stream
# ------------------------------------------
@dlt.table(
    name="silver_json_cleaned",
    comment="Validated and conformed JSON records."
)
@dlt.expect_or_drop("valid_json_payload", "_rescued_data IS NULL")
def silver_json_cleaned():
    raw_stream = dlt.read_stream(INPUT_JSON_TABLE)
    return raw_stream.select(
        "*",
        F.current_timestamp().alias("_ingested_timestamp")
    )
