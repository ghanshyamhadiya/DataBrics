from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/config')
from config import (
    GOLD_CONFIG, 
    SILVER_PATH,
    REQUIRE_TABLES,
    OPTIONAL_SOURCE
)

def load_silver_tables(spark, path):

    tables = {}
    validation = []

    requirement_missing = []
    
        for table_name, path in SILVER_PATH.items():
            try:
                if not DeltaTable.isDeltaTable(path):
                    is_required = table_name in REQUIRE_TABLES
                    
                    status="ERROR" if is_required else "WARNING"

                    tables[table_name]=None
                    validation.append({
                        "table_name": table_name,
                        "path": path,
                        "status": status,
                        "message": f"Table {table_name} not found at {path}"
                    })

                    if is_required:
                        requirement_missing.append(table_name)
                    
                    logger.warning(f"Table {table_name} not found at {path}")
                
                df=spark.read.format("delta").load(path)
                
                tables[table_name]=df

                validation.append({
                    "table_name": table_name,
                    "path": path,
                    "status": "SUCCESS",
                    "message": f"Table {table_name} found at {path}"
                })

                logger.info(
                    "Loaded Silver table: %s", 
                    table_name
                )
            except Exception as e:
                tables[table_name]=None
                validation.append({
                    "table_name": table_name,
                    "path": path,
                    "status": "ERROR",
                    "message": f"Error loading table {table_name} at {path}: {e}"
                })
                logger.exception(f"Error loading table {table_name} at {path}: {e}")
        
        if requirement_missing:
            raise RuntimeError(f"Missing required tables: {', '.join(requirement_missing)}")
        
        return tables, validation


def window_order(df, *column):
    if "created_at" in df.columns:
        window = Window.partitionBy(*column).orderBy(F.col("load_date").desc(), F.col("created_at").desc())
    else:
        window = Window.partitionBy(*column).orderBy(F.col("load_date").desc())
    
    return window


def window_load_df(df, *column):
    return (
        df.withColumn("rn", F.row_number().over(window_order(df, *column)))
        .filter(col("rn") == 1)
        .drop("rn")
    )

def validates_column(df: DataFrame, required_columns, table_name):
    missing_column = [
        column_name for column_name in required_columns
        if column_name not in df.columns
    ]

    if missing_column:
        raise ValueError(
            f"Missing column: {missing_column} in table: {table_name}"
    )

def audit_columns(df: DataFrame):

    current_ts=F.current_timestamp()

    return df
            .withColumn("Insert_dt", current_ts)
            .withColumn("Update_dt", current_ts)


