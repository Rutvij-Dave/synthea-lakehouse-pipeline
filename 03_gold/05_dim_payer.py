from pyspark import pipelines as dp
@dp.table(name="claims_lakehouse.gold.dim_payer")
def dim_payer():
    return spark.read.table("claims_lakehouse.bronze.bronze_csv_payers_raw")
