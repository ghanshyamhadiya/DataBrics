import sys
sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/Ingestion/Raw_to_silver')
from cleaning import clean_carriers, clean_inventory, clean_shipment, clean_supplier, clean_warehouse, clean_orders

LOAD_MODE='Full'

Raw_volume_path="/Volumes/workspace/default/supplychain/Raw_data/"

silver_volume_path="/Volumes/workspace/default/supplychain/silver_data/"

processed_volume_path="/Volumes/workspace/default/supplychain/processed_raw_files/"

TABLE_CONFIG = {
    "carriers": {
        "cleaner": clean_carriers,
        "gold_transform": "transform_carriers",
        "key": "carrier_id",
        "partition": "file_date",
        "load_type": "merge"
    },

    "orders": {
        "cleaner": clean_orders,
        "gold_transform": "transform_orders",
        "key": "order_id",
        "partition": "file_date",
        "load_type": "merge"
    },
    
    "inventory_snapshot": {
        "cleaner": clean_inventory,
        "gold_transform": "transform_inventory",
        "key": "inventory_id",
        "partition": "file_date",
        "load_type": "merge"
    },
    
    "shipments": {
        "cleaner": clean_shipment,
        "gold_transform": "transform_shipments",
        "key": "shipment_id",
        "partition": "file_date",
        "load_type": "merge"
    },
    
    "suppliers": {
        "cleaner": clean_supplier,
        "gold_transform": "transform_suppliers",
        "key": "supplier_id",
        "partition": "file_date",
        "load_type": "merge"
    },

    "warehouse": {
        "cleaner": clean_warehouse,
        "gold_transform": "transform_warehouse",
        "key": "warehouse_id",
        "partition": "file_date",
        "load_type": "merge"
    },
    
    # Aliases for plural variations
    "warehouses": {
        "cleaner": clean_warehouse,
        "gold_transform": "transform_warehouse",
        "key": "warehouse_id",
        "partition": "file_date",
        "load_type": "merge"
    }
}