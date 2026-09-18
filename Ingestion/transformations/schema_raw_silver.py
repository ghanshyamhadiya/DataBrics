from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType

Carrier_schema=StructType([
    StructField("carrier_id", StringType(), False),
    StructField("carrier_name", StringType(), True),
    StructField("carrier_type", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("max_weight_kg", DoubleType(), True),
    StructField("coverage_region", StringType(), True),
    StructField("on_time_rate_pct", DoubleType(), True),
    StructField("damage_rate_pct", DoubleType(), True),
    StructField("cost_per_km", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("load_date", DateType(), True),
    StructField("file_date", StringType(), True)
])

Warehouse_schema=StructType([
    StructField("warehouse_id", StringType(), False),
    StructField("warehouse_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("pincode", IntegerType(), True),
    StructField("region", StringType(), True),
    StructField("capacity_sqft", DoubleType(), True),
    StructField("current_utilization_pct", DoubleType(), True),
    StructField("manager_name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("load_date", DateType(), True),
    StructField("file_date", StringType(), True)
])

Inventory_schema=StructType([
    StructField("inventory_id", StringType(), False),
    StructField("warehouse_id", StringType(), False),
    StructField("sku_code", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("quantity_on_hand", IntegerType(), True),
    StructField("quantity_reserved", IntegerType(), True),
    StructField("quantity_available", IntegerType(), True),
    StructField("reorder_point", IntegerType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("total_inventory_value", DoubleType(), True),
    StructField("last_replenishment_date", TimestampType(), True),
    StructField("snapshot_date", TimestampType(), True),
    StructField("is_below_reorder", BooleanType(), True),
    StructField("calculated_quantity_available", IntegerType(), True),
    StructField("final_quantity_available", IntegerType(), True),
    StructField("calculated_inventory_value", DoubleType(), True),
    StructField("quantity_status", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("load_date", DateType(), True),
    StructField("file_date", StringType(), True)
])

Inventory_snapshot_schema=StructType([
    StructField("inventory_id", StringType(), False),
    StructField("warehouse_id", StringType(), False),
    StructField("sku_code", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("quantity_on_hand", IntegerType(), True),
    StructField("quantity_reserved", IntegerType(), True),
    StructField("quantity_available", IntegerType(), True),
    StructField("reorder_point", IntegerType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("total_inventory_value", DoubleType(), True),
    StructField("last_replenishment_date", TimestampType(), True),
    StructField("snapshot_date", TimestampType(), True),
    StructField("is_below_reorder", BooleanType(), True),
    StructField("calculated_quantity_available", IntegerType(), True),
    StructField("final_quantity_available", IntegerType(), True),
    StructField("calculated_inventory_value", DoubleType(), True),
    StructField("quantity_status", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("load_date", DateType(), True),
    StructField("file_date", StringType(), True)
])

Orders_schema=StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("supplier_id", StringType(), False),
    StructField("warehouse_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("required_delivery", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("quantity_ordered", IntegerType(), True),
    StructField("unit_price", DoubleType(), False),
    StructField("order_value", DoubleType(), True),
    StructField("priority", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("sales_channel", StringType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("created_at", StringType(), True),
    StructField("load_date", DateType(), True),
    StructField("file_date", StringType(), True)
])

Shipment_schema=StructType([
    StructField("shipment_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("supplier_id", StringType(), True),
    StructField("warehouse_id", StringType(), False),
    StructField("carrier_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("shipment_date", StringType(), True),
    StructField("expected_delivery", StringType(), True),
    StructField("actual_delivery", StringType(), True),
    StructField("quantity_ordered", IntegerType(), True),
    StructField("quantity_shipped", DoubleType(), True),
    StructField("quantity_delivered", DoubleType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_value", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("origin_city", StringType(), True),
    StructField("destination_city", StringType(), True),
    StructField("route_code", StringType(), True),
    StructField("delay_days", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("load_date", DateType(), True),
    StructField("file_date", StringType(), True)
])

Supplier_schema=StructType([
    StructField("supplier_id", StringType(), False),
    StructField("supplier_name", StringType(), True),
    StructField("contact_person", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lead_time_days", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("contract_start", TimestampType(), True),
    StructField("contract_end", TimestampType(), True),
    StructField("payment_terms", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("payment_due", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("load_date", DateType(), True),
    StructField("file_date", StringType(), True)
])

Customer_schema=StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("customer_tier", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("region", StringType(), True),
    StructField("credit_term", IntegerType(), True),
    StructField("credit_due", IntegerType(), True),
    StructField("credit_limit", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("load_date", DateType(), True),
    StructField("file_date", StringType(), True)
])

Products_schema=StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("primary_supplier_id", StringType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("margin_pct", DoubleType(), True),
    StructField("weight_kg", DoubleType(), True),
    StructField("is_fragile", BooleanType(), True),
    StructField("is_temperature_controlled", BooleanType(), True),
    StructField("shelf_life_days", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", TimestampType(), True)
])
