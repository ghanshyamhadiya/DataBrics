"""Supply Chain Pipeline Validation Script
Run this before executing the pipeline to verify all components are correctly configured.
"""

import sys
import re
import inspect

# Add paths for imports
sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/config')
sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/Ingestion/Raw_to_silver')

from config import TABLE_CONFIG, Raw_volume_path, silver_volume_path, processed_volume_path, LOAD_MODE
from cleaning import clean_carriers, clean_warehouse, clean_inventory, clean_orders, clean_supplier, clean_shipment

print("="*70)
print("   SUPPLY CHAIN PIPELINE VALIDATION")
print("="*70)

# Track validation results
issues = []
warnings = []

# ============================================================================
# 1. CHECK TABLE_CONFIG
# ============================================================================
print("\n[1/7] Checking TABLE_CONFIG...")
print(f"   Total tables configured: {len(TABLE_CONFIG)}")
for table_name in sorted(TABLE_CONFIG.keys()):
    config = TABLE_CONFIG[table_name]
    print(f"   ✓ {table_name:20s} | partition: {config['partition']} | key: {config['key']}")

# ============================================================================
# 2. CHECK RAW FILES
# ============================================================================
print("\n[2/7] Checking raw files...")
try:
    raw_files = [f for f in dbutils.fs.ls(Raw_volume_path) if f.name.endswith(".csv")]
    print(f"   Total CSV files in raw folder: {len(raw_files)}")
    
    if len(raw_files) == 0:
        warnings.append("No CSV files found in raw folder - nothing to process")
        print(f"   ⚠ No CSV files found in {Raw_volume_path}")
    else:
        for f in raw_files:
            print(f"   • {f.name}")
except Exception as e:
    issues.append(f"Cannot access raw folder: {Raw_volume_path}")
    print(f"   ✗ ERROR: {e}")
    raw_files = []

# ============================================================================
# 3. CHECK TABLE NAME EXTRACTION & ROUTING
# ============================================================================
print("\n[3/7] Checking table name extraction & routing...")

def extract_table_name(file_name):
    return re.sub(r"(_\d{8})?\.csv$", "", file_name)

def extract_file_date(file_name):
    match = re.search(r"(\d{8})", file_name)
    return match.group(1) if match else None

if raw_files:
    for f in raw_files:
        extracted = extract_table_name(f.name)
        file_date = extract_file_date(f.name)
        has_config = extracted in TABLE_CONFIG
        
        if has_config:
            print(f"   ✓ {f.name:35s} -> table: '{extracted}' | date: {file_date}")
        else:
            issues.append(f"No config for '{extracted}' (file: {f.name})")
            print(f"   ✗ {f.name:35s} -> table: '{extracted}' | NO CONFIG FOUND")
else:
    print("   (Skipped - no raw files to check)")

# ============================================================================
# 4. CHECK CLEANING FUNCTION SIGNATURES
# ============================================================================
print("\n[4/7] Checking cleaning function signatures...")

for table_name, config in TABLE_CONFIG.items():
    if "cleaner" not in config:
        continue
        
    cleaner_fn = config["cleaner"]
    sig = inspect.signature(cleaner_fn)
    param_count = len(sig.parameters)
    
    if param_count == 1:
        print(f"   ✓ {cleaner_fn.__name__:25s} {sig} - CORRECT")
    else:
        issues.append(f"{cleaner_fn.__name__} has {param_count} params (should be 1)")
        print(f"   ✗ {cleaner_fn.__name__:25s} {sig} - WRONG (should have 1 param)")

# ============================================================================
# 5. CHECK VOLUME PATHS
# ============================================================================
print("\n[5/7] Checking volume paths...")

paths_to_check = [
    (Raw_volume_path, "Raw Data", True),
    (silver_volume_path, "Silver Data", False),
    (processed_volume_path, "Processed Files", False),
    ("/Volumes/workspace/default/supplychain/temp_full_load/", "Temp Staging (Full Load)", False)
]

for path, label, required in paths_to_check:
    try:
        dbutils.fs.ls(path)
        print(f"   ✓ {label:30s} : {path}")
    except Exception:
        if required:
            issues.append(f"Required path does not exist: {path}")
            print(f"   ✗ {label:30s} : {path} (MISSING - REQUIRED)")
        else:
            print(f"   ○ {label:30s} : {path} (will be created if needed)")

# ============================================================================
# 6. CHECK CONTROL TABLE
# ============================================================================
print("\n[6/7] Checking control table...")

try:
    control_table = "supply_chain.control.processed_files"
    result = spark.sql(f"SELECT COUNT(*) as cnt FROM {control_table}").collect()[0]['cnt']
    print(f"   ✓ Control table exists: {control_table}")
    print(f"   ✓ Total records in control table: {result}")
    
    # Show recent processing history
    recent = spark.sql(f"""
        SELECT status, COUNT(*) as cnt 
        FROM {control_table} 
        WHERE layer='raw'
        GROUP BY status
    """).collect()
    
    if recent:
        print(f"   ✓ Processing history:")
        for row in recent:
            print(f"      - {row['status']:10s}: {row['cnt']} files")
except Exception as e:
    warnings.append(f"Control table issue: {e}")
    print(f"   ⚠ Control table check failed: {e}")
    print(f"   Note: Table will be created on first run if using Delta Lake")

# ============================================================================
# 7. CHECK CONFIGURATION SETTINGS
# ============================================================================
print("\n[7/7] Checking configuration settings...")

print(f"   Load Mode          : {LOAD_MODE}")
print(f"   Raw Volume Path    : {Raw_volume_path}")
print(f"   Silver Volume Path : {silver_volume_path}")
print(f"   Processed Path     : {processed_volume_path}")

if LOAD_MODE not in ["Incremental", "Full"]:
    issues.append(f"Invalid LOAD_MODE: {LOAD_MODE} (must be 'Incremental' or 'Full')")
    print(f"   ✗ Invalid LOAD_MODE: {LOAD_MODE}")
else:
    print(f"   ✓ LOAD_MODE is valid")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("   VALIDATION SUMMARY")
print("="*70)

if not issues and not warnings:
    print("\n✅ ALL CHECKS PASSED!")
    print("\nYour pipeline is ready to run.")
    print(f"\nCurrent mode: {LOAD_MODE}")
    if LOAD_MODE == "Incremental":
        print("   - Will process new/failed files from raw folder")
        print("   - Successfully processed files will move to processed folder")
    else:
        print("   - Will copy all files to temp staging")
        print("   - Process from staging (safe - no data loss on failure)")
        print("   - Cleans temp only after all files succeed")
else:
    if issues:
        print(f"\n🔴 CRITICAL ISSUES FOUND: {len(issues)}")
        for i, issue in enumerate(issues, 1):
            print(f"   {i}. {issue}")
        print("\n⚠️  Fix these issues before running the pipeline!")
    
    if warnings:
        print(f"\n🟡 WARNINGS: {len(warnings)}")
        for i, warning in enumerate(warnings, 1):
            print(f"   {i}. {warning}")
        print("\n⚠️  Review these warnings - pipeline may still work.")

print("\n" + "="*70)
print("\nTo run the pipeline:")
print("   %run /Workspace/Users/iamhadiya13@gmail.com/Supply Chain/Ingestion/Raw_to_silver/Row_to_silver.py")
print("="*70)
