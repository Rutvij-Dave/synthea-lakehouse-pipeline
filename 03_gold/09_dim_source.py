from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="claims_lakehouse.gold.dim_source",
    comment="Data source and format dimension."
)
def dim_source():

    return spark.createDataFrame(
        [
            ("SYNTHEA_FHIR_R4", "FHIR", "R4"),
            ("SYNTHEA_FHIR_STU3", "FHIR", "STU3"),
            ("SYNTHEA_FHIR_DSTU2", "FHIR", "DSTU2"),
            ("SYNTHEA_CSV", "CSV", None),
            ("SYNTHEA_CCDA", "C-CDA", None),
        ],
        [
            "source_system",
            "source_format",
            "source_version"
        ]
    ).withColumn(
        "source_key",
        F.sha2(
            F.col("source_system"),
            256
        )
    )