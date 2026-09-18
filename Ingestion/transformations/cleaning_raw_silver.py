from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, regexp_replace, round, greatest, to_timestamp
from pyspark.sql.types import StringType, DoubleType, DateType, IntegerType, BooleanType
from datetime import datetime, timedelta

today = datetime.now().strftime('%Y-%m-%d')

today_str = datetime.now().strftime('%Y%m%d')

mobile_regex=r"[^0-9+]"

payment_due=r"[^\d]"

def base_cleaning(df):
    return (
        df.dropna(how="all")\
        .drop_duplicates()\
        .select([
            when(trim(col(c))==" ", None).otherwise(col(c)).alias(c) #when(length(trim(col(c))) == 0, None)
            if isinstance(df.schema[c].dataType, StringType)
            else col(c)
            for c in df.columns
        ])\
        .withColumn("load_date", lit(today))
        .withColumn("load_date", col("load_date").cast(DateType()))
        )

def clean_carriers(df):

    cleaned = (
        base_cleaning(df)\
        .withColumn("on_time_rate_pct", regexp_replace(col("on_time_rate_pct"), r"[^\d\.]", "").cast(DoubleType()))\
        .withColumn("damage_rate_pct", regexp_replace(col("damage_rate_pct"), r"[^\d\.]", "").cast(DoubleType()))\
        .withColumn("cost_per_km", regexp_replace(col("cost_per_km"), r"[^\d\.]", "").cast(DoubleType()))\
        .withColumn("max_weight_kg", col("max_weight_kg").cast(DoubleType()))\
        .withColumn("is_active", when(col("is_active").cast(IntegerType())==1, True).otherwise(False))\
    )
    return cleaned


def clean_warehouse(df):
    cleaned=(
        base_cleaning(df)\
        .withColumn("phone", regexp_replace("phone", mobile_regex, ""))\
        .withColumn("is_active", when(col("is_active").cast(IntegerType())==1, True).otherwise(False))\
        .withColumn("created_at", to_timestamp(col("created_at"), "MM/dd/yyyy HH:mm:ss"))
    )
    return cleaned


def clean_supplier(df):

    cleaned= (
        base_cleaning(df)\
        .withColumn("payment_due", regexp_replace("payment_terms", payment_due, "").cast("int"))\
        .withColumn("phone", regexp_replace("phone", mobile_regex, ""))\
        .withColumn("is_active", when(col("is_active").cast(IntegerType())==1, True).otherwise(False))\
        .withColumn("created_at", to_timestamp(col("created_at"), "MM/dd/yyyy HH:mm:ss"))
    )
    return cleaned


def clean_inventory(df):

    cleaned=(
        base_cleaning(df)\
        .withColumn("calculated_quantity_available", col("quantity_on_hand")-col("quantity_reserved"))\
        .withColumn("final_quantity_available",
                when(col("quantity_available").isNotNull(), col("quantity_available"))
                .otherwise(col("quantity_on_hand") - col("quantity_reserved")))
        .withColumn("calculated_inventory_value", round(col("quantity_on_hand")*col("unit_cost"),2))\
        .withColumn("quantity_status",
                    when(col("calculated_quantity_available")<=0, "Out Of Stock")
                    .when(col("calculated_quantity_available")<=col("reorder_point"), "Low Quantity")
                    .otherwise("Healthy")
                    )\
        .withColumn("is_below_reorder", when(col("final_quantity_available")<=col("reorder_point"), lit(True))
                    .otherwise(lit(False)))\
        .withColumn("created_at", to_timestamp(col("created_at"), "MM/dd/yyyy HH:mm:ss"))

    )
    return cleaned


def clean_shipment(df):
    cleaned=(
        base_cleaning(df)\
        .drop("product_id", "status")  # Drop columns not needed in silver
        .withColumn("shipment_date", to_timestamp(col("shipment_date"), "yyyy-MM-dd"))
        .withColumn("expected_delivery", to_timestamp(col("expected_delivery"), "yyyy-MM-dd"))
        .withColumn("actual_delivery", to_timestamp(col("actual_delivery"), "yyyy-MM-dd"))
        .withColumn("is_delayed", when(col("expected_delivery")==col("actual_delivery"), 'N')
            .when(col("actual_delivery")>col("expected_delivery"), 'Y')
            .when(col("actual_delivery")<col("expected_delivery"), 'N').otherwise('Unknown'))
        .withColumn("delivery_status",
        when(col("quantity_delivered").isNull(), "No Delivery")
                    .when(col("quantity_ordered")==col("quantity_delivered"), "Fully Delivered")
        .otherwise("Partial Delivery"))
        .withColumn("unit_price_clean", 
            when(trim(col("unit_price")).isin("", " "), None)
            .otherwise(col("unit_price").cast(DoubleType())))
        .withColumn("quantity_delivered_clean", 
            when(trim(col("quantity_delivered")).isin("", " "), None)
            .otherwise(col("quantity_delivered").cast(DoubleType())))
        .withColumn("effective_delivered_value", round(col("quantity_delivered_clean")*col("unit_price_clean"), 2)
        .cast("double"))\
        .withColumn("lost_in_transit", when(col("quantity_delivered").isNull(), None)
        .otherwise(greatest(lit(0), col("quantity_shipped")-col("quantity_delivered"))))
        .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"))
    )
    
    return cleaned

def clean_customers(df):

    cleaned=(
        base_cleaning(df)\
        .withColumn("credit_term", regexp_replace("credit_term", payment_due, "").cast("int"))
        .withColumn("credit_limit", regexp_replace(col("credit_limit"), r"[^\d\.]", ""))
        .withColumn("credit_limit", when(col("credit_limit").isNotNull(), col("credit_limit").cast(DoubleType()))
        .otherwise(None))
        .withColumn("is_active", when(col("is_active").cast(IntegerType())==1, True).otherwise(False))
        .withColumn("created_at", to_timestamp(col("created_at"), "MM/dd/yyyy HH:mm:ss"))

    )
    return cleaned

def clean_products(df):

    cleaned=(
        base_cleaning(df)\
        .withColumn("is_fragile", when(col("is_fragile").cast(IntegerType())==1, True).otherwise(False))\
        .withColumn("is_temperature_controlled", when(col("is_temperature_controlled").cast(IntegerType())==1, True).otherwise(False))\
        .withColumn("is_active", when(col("is_active").cast(IntegerType())==1, True).otherwise(False))
        .withColumn("created_at", to_timestamp(col("created_at"), "MM/dd/yyyy HH:mm:ss"))
    )
    return cleaned

def clean_orders(df):

    cleaned=(
        base_cleaning(df)
        .drop("product_id", "sales_channel", "discount_pct")  # Drop columns not needed in silver
        .withColumn("order_date", to_timestamp(col("order_date"), "yyyy-MM-dd"))
        .withColumn("required_delivery", to_timestamp(col("required_delivery"), "yyyy-MM-dd"))
        .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"))
    )
    return cleaned