from pyspark.sql import functions as F


def transform_registered_users(df):
    df = (
        df
        .withColumn(
            "full_name",
            F.initcap(
                F.concat(
                    F.col("first_name"),
                    F.lit(" "),
                    F.col("last_name")
                )
            )
        )
        .withColumnRenamed("bidder_number", "bidder_id")
        .withColumn(
            "start_date",
            F.to_date(
                F.split(F.col("auction_date"), " - ")[0],
                "MM/dd/yyyy"
            )
        )
        .withColumn(
            "end_date",
            F.to_date(
                F.split(F.col("auction_date"), " - ")[1],
                "MM/dd/yyyy"
            )
        )
    )

    return df