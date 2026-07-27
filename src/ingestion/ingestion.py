from src.common.spark_session import create_spark_session
from src.common.logger import get_logger
from src.common.file_reader import read_csv

logger = get_logger(__name__)

spark = create_spark_session()

logger.info("Reading Customer Data from CSV file...")

customer_df = read_csv(spark, "data/raw/olist_customers_dataset.csv")

logger.info("Customer Data read sucessfully.")

customer_df.show(5)

spark.stop()

logger.info("Spark Session Stopped")