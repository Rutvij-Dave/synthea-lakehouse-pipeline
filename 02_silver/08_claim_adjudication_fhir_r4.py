from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_dedup"


@dp.table(
    name="claims_lakehouse.silver.silver_claim_adjudication_fhir_r4",
    comment="Conformed claim adjudication records."
)
def silver_claim_adjudication_fhir_r4():

    df = (
        spark.read.table(SOURCE_TABLE)
        .select(
            "claim_id",
            "_ingest_ts",
            "_record_source",
            F.explode_outer(
                F.col("claim_payload.item")
            ).alias("claim_item")
        )
        .select(
            "claim_id",
            "_ingest_ts",
            "_record_source",
            F.col(
                "claim_item.sequence"
            ).alias("line_number"),
            F.explode_outer(
                F.col("claim_item.adjudication")
            ).alias("adjudication")
        )
    )

    return (
        df
        .withColumn(
            "adjudication_category_code",
            F.expr(
                """
                element_at(
                    transform(
                        adjudication.category.coding,
                        x -> x.code
                    ),
                    1
                )
                """
            )
        )

        .withColumn(
            "adjudication_category",
            F.expr(
                """
                element_at(
                    transform(
                        adjudication.category.coding,
                        x -> x.display
                    ),
                    1
                )
                """
            )
        )

        .withColumn(
            "adjudication_amount",
            F.col(
                "adjudication.amount.value"
            ).cast("double")
        )

        .withColumn(
            "currency",
            F.col(
                "adjudication.amount.currency"
            )
        )

        .withColumn(
            "adjudication_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("claim_id"),
                    F.col("line_number").cast("string"),
                    F.coalesce(
                        F.col("adjudication_category_code"),
                        F.lit("")
                    ),
                    F.coalesce(
                        F.col("adjudication_amount").cast("string"),
                        F.lit("")
                    )
                ),
                256
            )
        )

        .withColumn(
            "_silver_created_ts",
            F.current_timestamp()
        )

        .select(
            "adjudication_id",
            "claim_id",
            "line_number",
            "adjudication_category_code",
            "adjudication_category",
            "adjudication_amount",
            "currency",
            "_ingest_ts",
            "_record_source",
            "_silver_created_ts"
        )
    )