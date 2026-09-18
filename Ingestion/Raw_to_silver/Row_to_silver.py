from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import StringType, DateType
from datetime import datetime, timedelta
import logging
import sys
import os
import re
import uuid
sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/config')
from config import (
    TABLE_CONFIG, 
    Raw_volume_path, 
    silver_volume_path, 
    processed_volume_path, LOAD_MODE
)
from pipeline_logger import PipelineLogger

# Configure logging to show INFO messages with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

today = datetime.now().strftime('%Y-%m-%d')

today_str = datetime.now().strftime('%Y%m%d')

spark=SparkSession.builder\
        .appName("SupplyChain")\
        .getOrCreate()

logger = PipelineLogger(spark)
logger.setup_log_tables()  # Create control tables if they don't exist
logging.info(f"load={LOAD_MODE} and date={today}")

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
            logging.info(f"{file_name} moved to {dest_path}")
        else:
            raise Exception(f"Copy verification failed for {file_name}")
        
        return dest_path
    except Exception as e:
        logging.error(f"Error while moving {file_name} from {raw_path} to {dest_path}")
        raise Exception(f"Move failed for {file_name}: {str(e)}")


def get_unprocessed_file():
    
    if LOAD_MODE=="Incremental":
        
        all_files=[
                f for f in dbutils.fs.ls(Raw_volume_path)
                if f.name.endswith(".csv")
                ]
        logging.info(f"total file to  process {len(all_files)}")
        sucess_name={
            raw["file_name"]
            for raw in spark.sql("""
                SELECT file_name FROM supply_chain.control.files_log
                WHERE LAYER='raw' AND STATUS='SUCCESS' """
            ).collect()
        }
        
        logging.info(f"total success file {len(sucess_name)}")
        
        failed_name={
            raw["file_name"]
            for raw in spark.sql("""
                SELECT file_name FROM supply_chain.control.files_log
                WHERE LAYER='raw' AND STATUS='FAILED' """
            ).collect()
        }
        logging.info(f"total failed files {len(failed_name)}")
        
        to_process=[
            f for f in all_files
            if f.name not in sucess_name
            or f.name in failed_name
        ]
        logging.info(f"files queued for processing {len(to_process)}")
    
    elif LOAD_MODE=="Full":
        logging.info("Full load mode - processing from temp staging (handled in run_pipeline)")

        to_process = []
        logging.info("Full load files will be processed from temp staging area")
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
        .option("mergeSchema", "true")\
        .partitionBy(TABLE_CONFIG[table_name]["partition"])\
        .save(path)

    logging.info(f"silver-{path}")
    return path

def extract_table_name(file_name):
    return re.sub(r"(_\d{8})?\.csv$", "", file_name)

def get_session_id():
    return f"{str(uuid.uuid4())}_{today_str}_{datetime.now().strftime('%H%M%S')}"

def run_pipeline():
    # Generate session ID and initialize tracking at the START
    session_id = get_session_id()
    start_time = datetime.now()
    total_records = 0
    error_message = None
    
    logging.info(f"Starting pipeline run with session_id: {session_id}")
    logging.info("Finding processing files...")
    
    # For full load, prepare temp staging area
    temp_staging_path = "/Volumes/workspace/default/supplychain/temp_full_load/"
    
    if LOAD_MODE=="Full":
        logging.info("\n=== FULL LOAD MODE ===")
        logging.info("Step 1: Preparing temp staging area...")
        
        # Clear temp staging from any previous failed runs
        try:
            dbutils.fs.rm(temp_staging_path, recurse=True)
            logging.info("  Cleared old temp staging area")
        except:
            pass
        
        # Create temp staging directory
        dbutils.fs.mkdirs(temp_staging_path)
        
        # Copy all files from BOTH raw and processed folders to temp staging
        raw_files = [f for f in dbutils.fs.ls(Raw_volume_path) if f.name.endswith(".csv")]
        
        try:
            processed_files = [f for f in dbutils.fs.ls(processed_volume_path) if f.name.endswith(".csv")]
        except Exception:
            processed_files = []
            logging.info("  No processed files found or folder doesn't exist")
        
        all_files = raw_files + processed_files
        logging.info(f"Step 2: Copying {len(all_files)} files to temp staging...")
        logging.info(f"  - Raw files: {len(raw_files)}")
        logging.info(f"  - Processed files: {len(processed_files)}")
        
        for f in all_files:
            dbutils.fs.cp(f.path, f"{temp_staging_path}{f.name}")
        
        logging.info(f"  Copied {len(all_files)} files to {temp_staging_path}")
        
        # Now get files from temp staging
        files_to_process = [f for f in dbutils.fs.ls(temp_staging_path) if f.name.endswith(".csv")]
        logging.info(f"Step 3: Processing {len(files_to_process)} files from temp staging...")
    else:
        # Incremental mode - use normal flow
        files_to_process = get_unprocessed_file()

    if not files_to_process:
        logging.info("No file to process")
        return
     
    processed_count = skipped_count = failed_count = 0

    logging.info("\nProcessing files...")

    for file in files_to_process:
        file_name=file.name
        file_path=file.path

        logging.info(f"\n---{file_name}---")
        
        table_name=extract_table_name(file_name)

        table_config=TABLE_CONFIG.get(table_name)
        
        if table_config is None:
            logger.log_file(
                file_name, 
                status="SKIPPED",
                processed_date=datetime.now(), 
                load_mode=LOAD_MODE,
                layer="raw",
                notes="NO ROUTING MATCHED",
                session_id=session_id
                )
            logging.info(f"NO ROUTING MATCHED for table: {table_name}")
            skipped_count+=1
            continue
        
        try:
            # Use predefined schema if available, otherwise infer
            if "schema" in table_config:
                df_raw = spark.read.csv(file_path, header=True, schema=table_config["schema"])
            else:
                df_raw = spark.read.csv(file_path, header=True, inferSchema=True)
            
            # FIX: Call cleaner with only df parameter (removed file_name)
            df_clean=table_config["cleaner"](df_raw)
            
            write_silver(df_clean, table_name, file_name)
            
            raw_count=df_clean.count()

            if LOAD_MODE=="Incremental":
                move_to_processed(file_name, file_path) 
                
            total_records += raw_count  # Accumulate total records across all files
            
            logger.log_file(
                file_name=file_name,
                processed_date=datetime.now(), 
                load_mode=LOAD_MODE,
                status="SUCCESS",
                raw_count=raw_count,
                layer="raw",
                notes=f"Processed successfully",
                session_id= session_id
            )

            processed_count+=1
        except Exception as e:
            logging.error(f"Failed to process file: {file_name}, {str(e)[:200]}")
            logging.error(e)
            logger.log_file(
                file_name=file_name,
                processed_date=datetime.now(), 
                load_mode=LOAD_MODE,
                layer="raw",
                status="FAILED", 
                raw_count=0,
                notes=str(e)[:300],
                session_id= session_id
                )
            failed_count+=1

            
    # Summary
    logging.info(f"\n\n=== SUMMARY ===")
    logging.info(f"Processed: {processed_count}, Skipped: {skipped_count}, Failed: {failed_count}")
    logging.info(f"Total: {processed_count+skipped_count+failed_count}")

    # Calculate duration and determine final status
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())
    
    if failed_count>0:
        error_message = f"Failed to process {failed_count} files"
        status = "FAILED"
    else:
        status = "SUCCESS"
        error_message = "None"
    
    # Log pipeline execution to control table BEFORE raising exception
    try:
        logger.log_pipeline(
            session_id=session_id,
            pipeline_name="Raw_to_Silver",
            load_type=LOAD_MODE,
            status=status,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration_seconds,
            total_files=len(files_to_process),
            processed_files=processed_count,
            failed_files=failed_count,
            skipped_files=skipped_count,
            total_records=total_records,
            error_message=error_message,
            created_at=datetime.now()
        )
        logging.info(f"✓ Pipeline execution logged with session_id: {session_id}")
    except Exception as e:
        logging.error(f"Failed to log pipeline execution: {e}")
    
    if LOAD_MODE=="Full" and failed_count==0:
        logging.info("\nStep 4: All files processed successfully!")
        logging.info("Step 5: Cleaning temp staging area...")
        try:
            dbutils.fs.rm(temp_staging_path, recurse=True)
            logging.info("  ✓ Temp staging cleaned")
        except Exception as e:
            logging.warning(f"  Warning: Could not clean temp staging: {e}")
    
    # Raise exception AFTER logging if there were failures
    if failed_count>0:
        logging.warning(f"\n⚠️  {failed_count} files failed processing")
        if LOAD_MODE=="Full":
            logging.warning(f"⚠️  Temp staging preserved at: {temp_staging_path}")
            logging.warning(f"   Silver data NOT deleted due to failures")
        raise Exception(error_message)

if __name__=="__main__":
    run_pipeline()
    #for every file make structure with index and all also make temp loca for full load after it complete remove it