from pyspark.sql import functions as F

# Timezone of the source system's exports
LOCAL_TIMEZONE = "UTC"


def transform_timed_auction(df):
    df = (
        df
        # Create full name, remove currency, and rename lot column
        .withColumn(
            "full_name",
            F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
        )
        .drop("currency")
        .withColumnRenamed("lot_#", "lot_number")

        # Remove duplicate bids
        .dropDuplicates([
            "item",
            "lot_number",
            "max_bid",
            "user_email",
            "date_time"
        ])

        # Extract auction start and end dates
        .withColumn(
            "start_date",
            F.to_date(
                F.trim(F.split(F.col("auction_date"), " - ")[0]),
                "MM/dd/yyyy"
            )
        )
        .withColumn(
            "end_date",
            F.to_date(
                F.trim(F.split(F.col("auction_date"), " - ")[1]),
                "MM/dd/yyyy"
            )
        )
        .drop("auction_date")

        # Remove timezone abbreviation from source timestamp
        .withColumn(
            "date_time",
            F.regexp_replace(
                F.col("date_time"),
                r"\s+[A-Z]{2,4}$",
                ""
            )
        )

        # Convert timestamp string to Spark timestamp
        .withColumn(
            "date_time",
            F.to_timestamp(
                F.col("date_time"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )

        # Convert source local time to UTC
        .withColumn(
            "date_time",
            F.to_utc_timestamp(
                F.col("date_time"),
                LOCAL_TIMEZONE
            )
        )

        # Extract date and time components
        .withColumn("month", F.month("date_time"))
        .withColumn("month_day", F.date_format("date_time", "MMMM"))
        .withColumn("hour", F.hour("date_time"))
        .withColumn("minute", F.minute("date_time"))
        .withColumn("seconds", F.second("date_time"))
        .withColumn("week", F.date_format("date_time", "EEEE"))

        # Extract date from timestamp
        .withColumn("bid_date", F.to_date("date_time"))
    )

    return df