from pyspark.sql import functions as F


def transform_auction_results(df):
    # Remove unnecessary operational columns
    df = (
        df
        .drop(
            "referencenumber",
            "lotnumberextension",
            "category",
            "consignment_manager",
            "catalogername",
            "catalogeremail",
            "cataloged",
            "photographed",
            "approved",
            "inventorystatus",
            "shippingstatus",
            "price",
            "cost",
            "taxjarcategory",
            "note",
            "disabletax",
            "reserveprice"
        )
        .withColumnsRenamed({
            "lotname*": "lot_name",
            "lotnumber": "lot_number",
            "attribute:format": "format",
            "attribute:sub_region": "sub_region",
            "attribute:locality": "locality",
            "attribute:variety": "variety",
            "attribute:type": "type",
            "attribute:year": "item_year",
            "attribute:category": "category",
            "itemnum": "lot_id",
            "auctionname": "auction_name",
            "consignor_#": "consignor_id",
            "consignorname": "consignor_name",
            "consignoremail": "consignor_email",
            "consignment_#": "consignment_id"
        })
    )

    # Date transformations
    df = (
        df
        .withColumn(
            "start_date",
            F.to_date(
                F.split(F.col("auctiondate"), " - ")[0],
                "MM/dd/yyyy"
            )
        )
        .withColumn(
            "end_date",
            F.to_date(
                F.split(F.col("auctiondate"), " - ")[1],
                "MM/dd/yyyy"
            )
        )
    )

    # Lot classification and sale status
    df = (
        df
        .withColumn(
            "lot_category",
            F.when(
                F.col("category").contains("Mixed"),
                "Mixed"
            ).otherwise("Solid")
        )
        .withColumn(
            "status",
            F.when(
                F.col("hammerprice") == "--",
                "Unsold"
            ).otherwise("Sold")
        )
        .withColumn(
            "hammerprice",
            F.when(
                F.col("hammerprice") == "--",
                "0"
            ).otherwise(F.col("hammerprice"))
        )
        .withColumn(
            "pricerealized",
            F.when(
                F.col("pricerealized") == "--",
                "0"
            ).otherwise(F.col("pricerealized"))
        )
    )

    # Numeric conversions
    numeric_columns = [
        "lot_id",
        "lot_number",
        "consignor_id",
        "consignment_id",
        "totalsettlement",
        "startingbid",
        "lowestimate",
        "highestimate",
        "commissionamount",
        "buyerspremium",
        "pricerealized",
        "hammerprice",
        "favoritescount"
    ]

    df = df.withColumns({
        col: F.col(col).cast("double")
        for col in numeric_columns
    })

    # Geography transformations
    df = (
        df
        .withColumn(
            "region",
            F.when(
                ~F.col("attribute:country").contains("France"),
                F.col("attribute:country")
            ).otherwise(F.col("attribute:region"))
        )
        .withColumn(
            "country",
            F.col("attribute:country")
        )
        .withColumn(
            "region",
            F.when(
                F.col("attribute:country").contains("France"),
                F.when(
                    F.col("region").contains("Provence") |
                    F.col("region").contains("Normandy") |
                    F.col("region").contains("Brittany") |
                    F.col("region").contains("Occitanie"),
                    F.col("region")
                ).otherwise("France(Others)")
            ).otherwise(F.col("region"))
        )
    )

    # Fill missing item_year values
    df = df.na.fill("N/A", subset=["item_year"])

    # Calculate midpoint estimate
    df = df.withColumn(
        "midestimate",
        (F.col("highestimate") + F.col("lowestimate")) / 2
    )

    return df