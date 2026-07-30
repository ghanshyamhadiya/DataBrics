from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import StringType, DateType
from datetime import datetime, timedelta
from delta.tables import DeltaTable
import sys
import os
import re

# Add config path for imports
sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/config')
from config import (
    TABLE_CONFIG, 
    Raw_volume_path, 
    silver_volume_path, 
    processed_volume_path, LOAD_MODE
)

today = datetime.now().strftime('%Y-%m-%d')

today_str = datetime.now().strftime('%Y%m%d')

spark=SparkSession.builder\
        .appName("SupplyChain")\
        .getOrCreate()


print(f"load={LOAD_MODE} and date={today}")

#log table
def log_file(file_name, processed_date, load_mode, status,Layer, raw_count=0, notes=""):
    log_df=spark.createDataFrame([{
        "file_name":file_name,
        "processed_date":processed_date,
        "load_mode":load_mode,
        "status":status,
        "layer":Layer,
        "raw_count":raw_count,
        "notes": notes
    }])

    DeltaTable.forName(spark, "supply_chain.control.processed_files")\
        .alias("t")\
        .merge(log_df.alias("s"), "t.file_name=s.file_name and t.layer=s.layer")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()


def extract_file_date(file_name):
    match = re.search(r"(\d{8})", file_name)
    return match.group(1) if match else None


def move_to_processed(file_name, raw_path):
    dest_path=f"{processed_volume_path}{file_name}"

    try:
        # Safer 3-step move: copy, verify, delete
        dbutils.fs.mkdirs(processed_volume_path)
        dbutils.fs.cp(raw_path, dest_path)
        
        # Verify copy was successful
        try:
            dbutils.fs.ls(dest_path)
            copy_ok = True
        except Exception:
            copy_ok = False
        
        if copy_ok:
            dbutils.fs.rm(raw_path, recurse=False)
            print(f"{file_name} moved to {dest_path}")
        else:
            raise Exception(f"Copy verification failed for {file_name}")
        
        return dest_path
    except Exception as e:
        print(f"Error while moving {file_name} from {raw_path} to {dest_path}")
        raise Exception(f"Move failed for {file_name}: {str(e)}")


def get_unprocessed_file():
    
    if LOAD_MODE=="Incremental":
        
        all_files=[
                f for f in dbutils.fs.ls(Raw_volume_path)
                if f.name.endswith(".csv")
                ]
        print(f"total file to  process {len(all_files)}")
        sucess_name={
            raw["file_name"]
            for raw in spark.sql("""
                SELECT file_name FROM supply_chain.control.processed_files
                WHERE LAYER='raw' AND STATUS='SUCCESS' """
            ).collect()
        }
        
        print(f"total success file {len(sucess_name)}")
        
        failed_name={
            raw["file_name"]
            for raw in spark.sql("""
                SELECT file_name FROM supply_chain.control.processed_files
                WHERE LAYER='raw' AND STATUS='FAILED' """
            ).collect()
        }
        print(f"total failed files {len(failed_name)}")
        
        to_process=[
            f for f in all_files
            if f.name not in sucess_name
            or f.name in failed_name
        ]
        print(f"files queued for processing {len(to_process)}")
    
    elif LOAD_MODE=="Full":
        print("Full load mode - reading from Raw_volume_path")
        
        # Full load should process all files from raw location
        to_process = [
            f for f in dbutils.fs.ls(Raw_volume_path) if f.name.endswith(".csv")
        ]
        print(f"Total files queued for full load: {len(to_process)}")
    else:
        raise Exception(f"Invalid LOAD_MODE: {LOAD_MODE}. Must be 'Incremental' or 'Full'.")
    
    return to_process

def write_silver(df, table_name, file_name):
    file_date=extract_file_date(file_name)

    if file_date is None:
        raise Exception(f"Invalid file name: {file_name}")
    
    path=f"{silver_volume_path}{table_name}/"

    df=df.withColumn("file_date", to_date(lit(file_date), "yyyyMMdd"))

    df.write\
        .format("delta")\
        .mode("append")\
        .partitionBy(TABLE_CONFIG[table_name]["partition"])\
        .save(path)

    print(f"silver-{path}")
    return path

def extract_table_name(file_name):
    return re.sub(r"(_\d{8})?\.csv$", "", file_name)


def run_pipeline():
    print("Finding processing files...")
    
    # For full load, prepare temp staging area
    temp_staging_path = "/Volumes/workspace/default/supplychain/temp_full_load/"
    
    if LOAD_MODE=="Full":
        print("\n=== FULL LOAD MODE ===")
        print("Step 1: Preparing temp staging area...")
        
        # Clear temp staging from any previous failed runs
        try:
            dbutils.fs.rm(temp_staging_path, recurse=True)
            print("  Cleared old temp staging area")
        except:
            pass
        
        # Create temp staging directory
        dbutils.fs.mkdirs(temp_staging_path)
        
        # Copy all raw files to temp staging
        raw_files = [f for f in dbutils.fs.ls(Raw_volume_path) if f.name.endswith(".csv")]
        print(f"Step 2: Copying {len(raw_files)} files to temp staging...")
        
        for f in raw_files:
            dbutils.fs.cp(f.path, f"{temp_staging_path}{f.name}")
        
        print(f"  Copied {len(raw_files)} files to {temp_staging_path}")
        
        # Now get files from temp staging
        files_to_process = [f for f in dbutils.fs.ls(temp_staging_path) if f.name.endswith(".csv")]
        print(f"Step 3: Processing {len(files_to_process)} files from temp staging...")
    else:
        # Incremental mode - use normal flow
        files_to_process = get_unprocessed_file()

    if not files_to_process:
        print("No file to process")
        return
     
    processed_count = skipped_count = failed_count = 0

    print("\nProcessing files...")

    for file in files_to_process:
        file_name=file.name
        file_path=file.path

        print(f"\n---{file_name}---")
        
        table_name=extract_table_name(file_name)

        table_config=TABLE_CONFIG.get(table_name)
        
        if table_config is None:
            log_file(
                file_name, 
                status="SKIPPED",
                processed_date=datetime.now(), 
                load_mode=LOAD_MODE,
                Layer="raw",
                notes="NO ROUTING MATCHED"
                )
            print(f"NO ROUTING MATCHED for table: {table_name}")
            skipped_count+=1
            continue
        
        try:
            df_raw = spark.read.csv(file_path, header=True, inferSchema=True)
            
            # FIX: Call cleaner with only df parameter (removed file_name)
            df_clean=table_config["cleaner"](df_raw)
            
            write_silver(df_clean, table_name, file_name)
            
            if LOAD_MODE=="Incremental":
                move_to_processed(file_name, file_path)  
            
            log_file(
                file_name=file_name,
                processed_date=datetime.now(), 
                load_mode=LOAD_MODE,
                status="SUCCESS",
                Layer="raw",
                notes=f"Processed successfully"
            )

            processed_count+=1
        except Exception as e:
            print(f"Failed to process file: {file_name}, {str(e)[:200]}")
            print(e)
            log_file(
                file_name=file_name,
                processed_date=datetime.now(), 
                load_mode=LOAD_MODE,
                Layer="raw",
                status="FAILED", 
                notes=str(e)[:300]
                )
            failed_count+=1
    
    # Summary
    print(f"\n\n=== SUMMARY ===")
    print(f"Processed: {processed_count}, Skipped: {skipped_count}, Failed: {failed_count}")
    print(f"Total: {processed_count+skipped_count+failed_count}")

    if failed_count>0:
        print(f"\n⚠️  {failed_count} files failed processing")
        if LOAD_MODE=="Full":
            print(f"⚠️  Temp staging preserved at: {temp_staging_path}")
            print(f"   Silver data NOT deleted due to failures")
        raise Exception(f"Failed to process {failed_count} files")
    
    # Only if ALL files succeeded in full load mode
    if LOAD_MODE=="Full" and failed_count==0:
        print("\nStep 4: All files processed successfully!")
        print("Step 5: Cleaning temp staging area...")
        try:
            dbutils.fs.rm(temp_staging_path, recurse=True)
            print("  ✓ Temp staging cleaned")
        except Exception as e:
            print(f"  Warning: Could not clean temp staging: {e}")

if __name__=="__main__":
    run_pipeline()
    #for every file make structure with index and all also make temp loca for full load after it complete remove it