import dlt
import pyspark.sql.functions as F

# ==========================================
# CONFIGURATION VARIABLES (Top-Level)
# ==========================================
INPUT_SILVER_TABLE = "silver_claims_cleaned"
GOLD_TABLE_NAME = "gold_fraud_analytics"

# ==========================================
# GOLD FEATURE AGGREGATION
# ==========================================
@dlt.table(
    name=GOLD_TABLE_NAME,
    comment="Aggregated feature mart for fraud detection and investigative agents."
)
def gold_fraud_analytics():
    silver_df = dlt.read(INPUT_SILVER_TABLE)
    
    return (
        silver_df
        .groupBy("resourceType")
        .agg(
            F.count("*").alias("total_records"),
            F.min("_ingested_timestamp").alias("earliest_record_timestamp"),
            F.max("_ingested_timestamp").alias("latest_record_timestamp")
        )
    )