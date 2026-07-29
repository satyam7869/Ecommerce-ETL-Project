from pathlib import Path
from pyspark.sql.functions import col
from src.common.spark_session import create_spark_session
from src.common.file_reader import read_csv
from src.schemas.customer_schema import customer_schema
from src.transformation.dirty_data_utils import create_dirty_dataset
from config.app_config import RAW_DATA_PATH, DIRTY_DATA_PATH, CUSTOMER_FILE


# Create Spark Session
spark = create_spark_session()

# Read Customer Dataset
customer_path = Path(RAW_DATA_PATH) / CUSTOMER_FILE

customer_df = read_csv(
    spark,
    str(customer_path),
    schema=customer_schema
)

dirty_config = [
        {
            "type": "null",
            "column": "customer_city",
            "condition": col("customer_zip_code_prefix") < 10005
        },
        {
            "type": "duplicate",
            "number_of_rows": 10
        },
        {
            "type": "invalid",
            "column": "customer_state",
            "condition": col("customer_zip_code_prefix") > 99990,
            "invalid_value": "XYZ"
        }]

dirty_df = create_dirty_dataset(customer_df, dirty_config)

dirty_output = Path(DIRTY_DATA_PATH) / CUSTOMER_FILE

dirty_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(str(dirty_output))

spark.stop()