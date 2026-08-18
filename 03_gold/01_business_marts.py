import dlt
import pyspark.sql.functions as F

# ==========================================
# CONFIGURATION VARIABLES
# ==========================================
INPUT_SILVER_TABLE = "silver_json_cleaned"

@dlt.table(
    name="gold_fraud_analytics",
    comment="Aggregated feature mart for fraud detection."
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