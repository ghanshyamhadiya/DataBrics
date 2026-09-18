
SILVER_PATH={
    "orders": "/Volumes/workspace/default/supplychain/silver_data/orders/",
    "shipments": "/Volumes/workspace/default/supplychain/silver_data/shipments/",
    "warehouses": "/Volumes/workspace/default/supplychain/silver_data/warehouses/",
    "carriers": "/Volumes/workspace/default/supplychain/silver_data/carriers/",
    "suppliers": "/Volumes/workspace/default/supplychain/silver_data/suppliers/",
    "inventory_snapshot": "/Volumes/workspace/default/supplychain/silver_data/inventory_snapshot",
    "products": "/Volumes/workspace/default/supplychain/silver_data/products",
    "customers": "/Volumes/workspace/default/supplychain/silver_data/customers"
}

REQUIRE_TABLES = {
    "orders",
    "shipments",
    "warehouses",
    "carriers",
    "suppliers",
    "inventory_snapshot",
    "products",
    "customers"
}

OPTIONAL_SOURCE = {
}




GOLD_CONFIG = {

    "fact_order": {
        "source": "orders",
        "key": ["order_id"],
        "required": True,
        "columns": [
            "order_id",
            "customer_id",
            "warehouse_id",
            "supplier_id",
            "order_date",
            "required_delivery",
            "quantity_ordered",
            "unit_price",
            "order_value",
            "payment_status",
            "order_status",
        ],
    },

    "dim_warehouse": {
        "source": "warehouses",
        "key": ["warehouse_id"],
        "required": True,
        "columns": [
            "warehouse_id",
            "warehouse_name",
            "city",
            "state",
            "region",
            "pincode",
            "phone",
            "capacity_sqft",
            "manager_name",
            "is_active",
            "current_utilization_pct",
        ],
    },

    "fact_shipment": {
        "source": "shipments",
        "key": ["shipment_id"],
        "required": True,
        "columns": [
            "shipment_id",
            "order_id",
            "warehouse_id",
            "supplier_id",
            "carrier_id",
            "product_category",
            "shipment_date",
            "expected_delivery",
            "actual_delivery",
            "quantity_ordered",
            "quantity_shipped",
            "total_value",
            "origin_city",
            "destination_city",
            "route_code",
            "delay_days",
            "is_delayed",
            "delivery_status",
            "unit_price_clean",
            "quantity_delivered_clean",
            "effective_delivered_value",
            "lost_in_transit",
        ],
    },

    "dim_supplier": {
        "source": "suppliers",
        "key": ["supplier_id"],
        "required": True,
        "columns": [
            "supplier_id",
            "supplier_name",
            "contact_person",
            "email",
            "phone",
            "region",
            "city",
            "country",
            "lead_time_days",
            "rating",
            "contract_start",
            "contract_end",
            "payment_due",
            "is_active",
        ],
    },

    "fact_inventory_snapshot": {
        "source": "inventory_snapshot",
        "key": ["inventory_id", "snapshot_date"],
        "required": False,
        "columns": [
            "inventory_id",
            "warehouse_id",
            "product_category",
            "sku_code",
            "quantity_on_hand",
            "quantity_reserved",
            "quantity_available",
            "reorder_point",
            "unit_cost",
            "total_inventory_value",
            "last_replenishment_date",
            "is_below_reorder",
            "final_quantity_available",
            "quantity_status",
            "snapshot_date"
        ],
    },

    "dim_customer": {
        "source": "customers",
        "key": ["customer_id"],
        "required": True,
        "columns": [
            "customer_id",
            "customer_name",
            "customer_segment",
            "customer_tier",
            "city",
            "state",
            "region",
            "credit_term",
            "credit_due",
            "credit_limit",
            "is_active"
        ],
    },

    "dim_product": {
        "source": "products",
        "key": ["product_id"],
        "required": True,
        "columns": [
            "product_id",
            "product_name",
            "product_category",
            "primary_supplier_id",
            "unit_cost",
            "unit_price",
            "margin_pct",
            "weight_kg",
            "is_fragile",
            "is_temperature_controlled",
            "shelf_life_days",
            "is_active",
            "created_at"
        ],
    },

    "dim_carrier": {
        "source": "carriers",
        "key": ["carrier_id"],
        "required": True,
        "columns": [
            "carrier_id",
            "carrier_name",
            "carrier_type",
            "vehicle_type",
            "max_weight_kg",
            "coverage_region",
            "on_time_rate_pct",
            "damage_rate_pct",
            "cost_per_km",
            "is_active",
        ],
    },
}