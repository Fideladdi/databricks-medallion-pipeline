from pyspark.sql import functions as F
def transform_auction_summary(df):
    df = df.withColumns(
            {
                'start_date': F.to_date(df.start_date),
                'end_date': F.to_date(df.end_date),
                'published_date': F.to_date(df.published_date)
            }
        )
    
    df = df.withColumnRenamed("id", "auction_id")
    
    return df