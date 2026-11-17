from pyspark.sql import functions as F

# Timezone of the source system's exports
LOCAL_TIMEZONE = "UTC"


def transform_users(df):
    # Create full name
    df = df.withColumn(
        "full_name",
        F.initcap(
            F.concat(
                F.col("first_name"),
                F.lit(" "),
                F.col("last_name")
            )
        )
    )
    # Rename user ID column
    df = df.withColumnRenamed("user_#", "user_id")

    # Convert date_created to timestamp
    df = df.withColumn(
        "date_created",
        F.to_timestamp(
            F.col("date_created"),
            "MM/dd/yyyy HH:mm z"
        )
    )
    # Convert UTC timestamp to the source system's local time
    df = df.withColumn(
        "date_created",
        F.from_utc_timestamp(
            F.col("date_created"),
            LOCAL_TIMEZONE
        )
    )
    return df