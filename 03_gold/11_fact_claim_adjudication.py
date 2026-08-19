from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_adjudication_fhir_r4"


@dp.table(
    name="claims_lakehouse.gold.fact_claim_adjudication",
    comment="Claim adjudication fact. Grain: one row per claim-line adjudication."
)
def fact_claim_adjudication():

    df = (
        spark.read.table(SOURCE_TABLE)
        .select(
            F.col("claim_id"),
            F.explode_outer(
                F.col("claim_payload.item")
            ).alias("claim_item")
        )
        .select(
            F.col("claim_id"),
            F.col("claim_item.sequence").alias("line_number"),
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
            )
        )

        .withColumn(
            "currency",
            F.col(
                "adjudication.amount.currency"
            )
        )

        .withColumn(
            "adjudication_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("claim_id"),
                    F.col("line_number").cast("string"),
                    F.col("adjudication_category_code")
                ),
                256
            )
        )

        .select(
            "adjudication_key",
            "claim_id",
            "line_number",
            "adjudication_category_code",
            "adjudication_category",
            "adjudication_amount",
            "currency"
        )
    )
