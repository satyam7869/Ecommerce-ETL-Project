from pyspark.sql.functions import *
from src.common.spark_session import create_spark_session
from src.common.logger import get_logger
from src.common.file_reader import read_csv
from src.schemas.customer_schema import customer_schema
from pathlib import Path
from config.app_config import (RAW_DATA_PATH, CUSTOMER_FILE)

customer_path = Path(RAW_DATA_PATH)/ CUSTOMER_FILE

logger = get_logger(__name__)

spark = create_spark_session()

logger.info("Reading Customer Data from CSV file...")

customer_df = read_csv(spark, str(customer_path), schema = customer_schema)

logger.info("Customer Data read sucessfully.")

customer_df.printSchema()
customer_df.show()
#customer_df.orderBy(col("customer_zip_code_prefix").asc()).show(50)
print(customer_df.count())

spark.stop()
logger.info("Spark Session Stopped")