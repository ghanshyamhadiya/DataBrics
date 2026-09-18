from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType
import uuid

class PipelineLogger:
    
    def __init__(self, spark, catalog="supply_chain", schema="control"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.pipeline_log_table = f"{catalog}.{schema}.pipeline_log"
        self.files_log_table = f"{catalog}.{schema}.files_log"

        self.pipeline_log_schema = StructType([
            StructField("session_id", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("load_type", StringType(), False),
            StructField("status", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("duration_seconds", IntegerType(), True),
            StructField("total_files", IntegerType(), True),
            StructField("processed_files", IntegerType(), True),
            StructField("failed_files", IntegerType(), True),
            StructField("skipped_files", IntegerType(), True),
            StructField("total_records", LongType(), True),
            StructField("error_message", StringType(), True),
            StructField("created_at", TimestampType(), False)
        ])
        self.files_logSchema=StructType([
            StructField("file_name", StringType(), False),
            StructField("processed_date", TimestampType(), False),
            StructField("load_mode", StringType(), False),
            StructField("status", StringType(), False),
            StructField("layer", StringType(), False),
            StructField("raw_count", LongType(), True),
            StructField("notes", StringType(), True),
            StructField("session_id", StringType(), False)
        ])
    
    def setup_log_tables(self):
        
        empty_pipeline_log = self.spark.createDataFrame([], schema=self.pipeline_log_schema)
        empty_pipeline_log.write.format("delta").mode("append").saveAsTable(self.pipeline_log_table)
        
        empty_files_log = self.spark.createDataFrame([], schema=self.files_logSchema)
        empty_files_log.write.format("delta").mode("append").saveAsTable(self.files_log_table)
        return True
    
    def get_session_id(self):
        return f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    
    def log_pipeline(self, session_id, pipeline_name, load_type, status, start_time, end_time, 
                     duration_seconds, total_files, processed_files, failed_files, skipped_files, 
                     total_records, error_message=None, created_at=None):
        if created_at is None:
            created_at = datetime.now()
        
        log_df = self.spark.createDataFrame([{
            "session_id": session_id,
            "pipeline_name": pipeline_name,
            "load_type": load_type,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration_seconds,
            "total_files": total_files,
            "processed_files": processed_files,
            "failed_files": failed_files,
            "skipped_files": skipped_files,
            "total_records": total_records,
            "error_message": error_message,
            "created_at": created_at
        }], schema=self.pipeline_log_schema)
        
        DeltaTable.forName(self.spark, self.pipeline_log_table)\
            .alias("t")\
            .merge(log_df.alias("s"), "t.session_id = s.session_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    
    def log_file(self, session_id, file_name, processed_date, load_mode, status, layer, raw_count=0, notes=""):
        log_df = self.spark.createDataFrame([{
            "session_id": session_id,
            "file_name": file_name,
            "processed_date": processed_date,
            "load_mode": load_mode,
            "status": status,
            "layer": layer,
            "raw_count": raw_count,
            "notes": notes
        }], schema=self.files_logSchema)
        
        DeltaTable.forName(self.spark, self.files_log_table)\
            .alias("t")\
            .merge(log_df.alias("s"), "t.file_name = s.file_name AND t.layer = s.layer")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
