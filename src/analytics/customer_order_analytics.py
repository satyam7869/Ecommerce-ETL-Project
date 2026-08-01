from pathlib import Path
from pyspark.sql.functions import *
from config.app_config import TRANSFORMED_DATA_PATH
from src.common.spark_session import create_spark_session
from src.common.file_reader import read_csv

spark = create_spark_session()

input_path = Path(TRANSFORMED_DATA_PATH) / "customer_orders"

customer_orders_df = read_csv(spark, str(input_path))

# State-wise Total Orders
state_orders_df = customer_orders_df.groupBy("customer_state").agg(count("order_id").alias("total_orders"))
#state_orders_df.show(truncate=False)

# State-wise Revenue
state_revenue_df = customer_orders_df.groupbby("customer_state"),agg(sum(""))