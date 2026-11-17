from datetime import date
from src.transformations.auction_summary import (
    transform_auction_summary
)

def test_auction_summary_dates(spark):
    data = [
        (33, "2026-11-01", "2026-11-05", "2026-10-20")
    ]

    df = spark.createDataFrame(
        data,
        ["id", "start_date", "end_date", "published_date"]
    )

    result = transform_auction_summary(df)
    row = result.collect()[0]

    assert row.auction_id == 33
    assert row.start_date == date(2026, 11, 1)
    assert row.end_date == date(2026, 11, 5)
    assert row.published_date == date(2026, 10, 20)
    assert "id" not in result.columns