from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="claims_lakehouse.gold.dim_date",
    comment="Calendar date dimension."
)
def dim_date():

    start_date = "2015-01-01"
    end_date = "2035-12-31"

    dates = (
        spark.sql(
            f"""
            SELECT explode(
                sequence(
                    to_date('{start_date}'),
                    to_date('{end_date}'),
                    interval 1 day
                )
            ) AS calendar_date
            """
        )
    )

    return (
        dates
        .withColumn(
            "date_key",
            F.date_format(
                F.col("calendar_date"),
                "yyyyMMdd"
            ).cast("int")
        )
        .withColumn(
            "year",
            F.year("calendar_date")
        )
        .withColumn(
            "quarter",
            F.quarter("calendar_date")
        )
        .withColumn(
            "month",
            F.month("calendar_date")
        )
        .withColumn(
            "month_name",
            F.date_format(
                "calendar_date",
                "MMMM"
            )
        )
        .withColumn(
            "week_of_year",
            F.weekofyear("calendar_date")
        )
        .withColumn(
            "day_of_month",
            F.dayofmonth("calendar_date")
        )
        .withColumn(
            "day_name",
            F.date_format(
                "calendar_date",
                "EEEE"
            )
        )
        .withColumn(
            "is_weekend",
            F.when(
                F.dayofweek("calendar_date").isin(1, 7),
                True
            ).otherwise(False)
        )
    )