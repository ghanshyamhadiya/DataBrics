import sys
sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/Ingestion/transformations')
sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/config')
from pyspark.sql import SparkSession
from cleaning_silver_gold import transform_to_gold
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def main():    
    logging.info("Starting Silver to Gold transformation...")
    
    try:
        gold_tables = transform_to_gold(spark)
        
        # Write gold tables (example - customize based on your needs)
        gold_volume_path = "/Volumes/workspace/default/supplychain/gold_data/"
        
        for table_name, df in gold_tables.items():
            output_path = f"{gold_volume_path}{table_name}/"
            df.write\
                .format("delta")\
                .mode("append")\
                .option("mergeSchema", "true")\
                .save(output_path)
            
            logging.info(f"Written {table_name} to {output_path}")
        
        logging.info("Silver to Gold transformation completed successfully")
        
    except Exception as e:
        logging.error(f"Error during Silver to Gold transformation: {str(e)}")
        raise

if __name__ == "__main__":
    main()