from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_TABLE = "claims_lakehouse.silver.silver_claim_dedup"


@dp.table(
    name="claims_lakehouse.silver.silver_claim_line_fhir_r4",
    comment="Conformed claim-line records. Grain: one row per claim item."
)
def silver_claim_line_fhir_r4():

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
            F.to_date(
                F.col(
                    "claim_item.servicedPeriod.start"
                )
            )
        )

        .withColumn(
            "service_end",
            F.to_date(
                F.col(
                    "claim_item.servicedPeriod.end"
                )
            )
        )

        .withColumn(
            "line_net_amount",
            F.col(
                "claim_item.net.value"
            ).cast("double")
        )

        .withColumn(
            "currency",
            F.col(
                "claim_item.net.currency"
            )
        )

        .withColumn(
            "claim_line_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("claim_id"),
                    F.col("line_number").cast("string")
                ),
                256
            )
        )

        .withColumn(
            "_silver_created_ts",
            F.current_timestamp()
        )

        .select(
            "claim_line_id",
            "claim_id",
            "line_number",
            "service_code",
            "service_description",
            "service_start",
            "service_end",
            "line_net_amount",
            "currency",
            "_ingest_ts",
            "_record_source",
            "_silver_created_ts"
        )
    )