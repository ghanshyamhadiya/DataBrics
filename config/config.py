import sys
sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/Ingestion/transformations')
from cleaning_raw_silver import clean_carriers, clean_inventory, clean_shipment, clean_supplier, clean_warehouse, clean_orders, clean_customers, clean_products
from schema_raw_silver import Carrier_schema, Warehouse_schema, Inventory_schema, Inventory_snapshot_schema, Orders_schema, Shipment_schema, Supplier_schema, Products_schema, Customer_schema
# from cleaning_silver_gold import 

LOAD_MODE='Incremental'

Raw_volume_path="/Volumes/workspace/default/supplychain/Raw_data/"

silver_volume_path="/Volumes/workspace/default/supplychain/silver_data/"

processed_volume_path="/Volumes/workspace/default/supplychain/processed_raw_files/"

TABLE_CONFIG = {
    "carriers": {
        "cleaner": clean_carriers,
        "schema": Carrier_schema,
        "gold_transform": "transform_carriers",
        "key": "carrier_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },
    
    "inventory": {
        "cleaner": clean_inventory,
        "schema": Inventory_schema,
        "gold_transform": "transform_inventory",
        "key": "inventory_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },

    "orders": {
        "cleaner": clean_orders,
        "schema": Orders_schema,
        "gold_transform": "transform_orders",
        "key": "order_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },
    
    "inventory_snapshot": {
        "cleaner": clean_inventory,
        "schema": Inventory_snapshot_schema,
        "gold_transform": "transform_inventory",
        "key": "inventory_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },
    
    "shipments": {
        "cleaner": clean_shipment,
        "schema": Shipment_schema,
        "gold_transform": "transform_shipments",
        "key": "shipment_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },
    
    "suppliers": {
        "cleaner": clean_supplier,
        "schema": Supplier_schema,
        "gold_transform": "transform_suppliers",
        "key": "supplier_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },

    "warehouse": {
        "cleaner": clean_warehouse,
        "schema": Warehouse_schema,
        "gold_transform": "transform_warehouse",
        "key": "warehouse_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },
    
    # Aliases for plural variations
    "warehouses": {
        "cleaner": clean_warehouse,
        "schema": Warehouse_schema,
        "gold_transform": "transform_warehouse",
        "key": "warehouse_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },

    "customers": {
        "cleaner": clean_customers,
        "schema": Customer_schema,
        "gold_transform": "transform_customers",
        "key": "customer_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },

    "products": {
        "cleaner": clean_products,
        "schema": Products_schema,
        "gold_transform": "transform_products",
        "key": "product_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },    
}

