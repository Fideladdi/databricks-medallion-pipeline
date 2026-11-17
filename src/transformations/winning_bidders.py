from pyspark.sql import functions as F


def transform_winning_bidders(df):
    df = (
        df
        # Create full name
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

        # Rename columns
        .withColumnsRenamed({
            "bidder_number": "bidder_id",
            "lot_#": "lot_number",
            "billing_company": "bidder_billing_company",
            "billing_first_name": "bidder_billing_first_name",
            "billing_last_name": "bidder_billing_last_name",
            "billing_phone_number": "bidder_billing_phone_number",
            "billing_address_1": "bidder_billing_address_1",
            "billing_address_2": "bidder_billing_address_2",
            "billing_city": "bidder_billing_city",
            "billing_state_province": "bidder_billing_state_province",
            "billing_zip_postal_code": "bidder_billing_zip_postal_code",
            "billing_country": "bidder_billing_country",
            "shipping_company": "bidder_shipping_company",
            "shipping_first_name": "bidder_shipping_first_name",
            "shipping_last_name": "bidder_shipping_last_name",
            "shipping_phone_number": "bidder_shipping_phone_number",
            "shipping_address_1": "bidder_shipping_address_1",
            "shipping_address_2": "bidder_shipping_address_2",
            "shipping_city": "bidder_shipping_city",
            "shipping_state_province": "bidder_shipping_state_province",
            "shipping_zip_postal_code": "bidder_shipping_zip_postal_code",
            "shipping_country": "bidder_shipping_country"
        })

        # Convert auction date range into DATE columns
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