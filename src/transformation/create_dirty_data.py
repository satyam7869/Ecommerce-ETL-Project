from pyspark.sql.functions import *
from src.common.spark_session import create_spark_session
from src.common.file_reader import read_csv
from src.schemas.customer_schema import customer_schema
from config.app_config import RAW_DATA_PATH, DIRTY_DATA_PATH, CUSTOMER_FILE
from pathlib import Path

spark = create_spark_session()

customer_path = Path(RAW_DATA_PATH)/ CUSTOMER_FILE

customer_df = read_csv(
spark, str(customer_path), schema = customer_schema
)

# Introduce Null Values
dirty_df = customer_df.withColumn("customer_city", \
            when(col("customer_zip_code_prefix") < 1010, None) \
           .otherwise(col("customer_city"))
)

# Introduce Invalid Value
dirty_df = customer_df.withColumn("customer_state", \
            when(col("customer_zip_code_prefix") < 1010, lit("XYZ")) \
            .otherwise(col("customer_state"))
)

# Introduce Duplicates
duplicate_df = dirty_df.limit(10)
dirty_df = dirty_df.union(duplicate_df)

# Write Dirty Dataset
dirty_output = Path(DIRTY_DATA_PATH) / CUSTOMER_FILE
#dirty_df.write.mode("overwrite").option("header", True).csv(str(dirty_output))

dirty_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(str(dirty_output))
