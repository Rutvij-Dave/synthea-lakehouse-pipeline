from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_dedup"


@dp.table(
    name="claims_lakehouse.gold.fact_claim_line",
    comment="Claim line fact. Grain: one row per claim item."
)
def fact_claim_line():

    df = (
        spark.read.table(SOURCE_TABLE)
        .select(
            F.col("claim_id"),
            F.explode_outer(
                F.col("claim_payload.item")
            ).alias("claim_item")
        )
    )

    return (
        df
        .withColumn(
            "line_number",
            F.col("claim_item.sequence")
        )

        .withColumn(
            "service_code",
            F.expr(
                """
                element_at(
                    transform(
                        claim_item.productOrService.coding,
                        x -> x.code
                    ),
                    1
                )
                """
            )
        )

        .withColumn(
            "service_description",
            F.expr(
                """
                element_at(
                    transform(
                        claim_item.productOrService.coding,
                        x -> x.display
                    ),
                    1
                )
                """
            )
        )

        .withColumn(
            "service_start",
            F.col(
                "claim_item.servicedPeriod.start"
            )
        )

        .withColumn(
            "service_end",
            F.col(
                "claim_item.servicedPeriod.end"
            )
        )

        .withColumn(
            "line_net_amount",
            F.col(
                "claim_item.net.value"
            )
        )

        .withColumn(
            "currency",
            F.col(
                "claim_item.net.currency"
            )
        )

        .withColumn(
            "claim_line_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("claim_id"),
                    F.col("line_number").cast("string")
                ),
                256
            )
        )

        .select(
            "claim_line_key",
            "claim_id",
            "line_number",
            "service_code",
            "service_description",
            "service_start",
            "service_end",
            "line_net_amount",
            "currency"
        )
    )