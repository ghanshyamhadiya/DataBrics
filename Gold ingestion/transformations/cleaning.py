from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, regexp_replace, round, concat, lower, upper, coalesce
from pyspark.sql.types import StringType, DoubleType, DateType, IntegerType, TimestampType, BooleanType
from datetime import datetime, timedelta

today_str=datetime.now().strftime("%Y-%m-%d")
today=datetime.now().strftime("%Y%m%d")

