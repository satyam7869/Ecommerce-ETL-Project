from pathlib import Path
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.common.file_reader import read_csv
from src.common.spark_session import create_spark_session
from config.app_config import JOINED_DATA_PATH, TRANSFORMED_DATA_PATH

spark = create_spark_session()

customer_orders_df = read_csv(spark, str(Path(JOINED_DATA_PATH) / "customer_orders_payments.csv"))

customer_orders_df = customer_orders_df.withColumn("order_year", year(col("order_purchase_timestamp")))

customer_order_df = customer_orders_df.withColumn("order_month", month(col("order_purchase_timestamp")))

customer_orders_df = customer_orders_df.withColumn("orders_day", dayofmonth(col("order_purchase_timestamp")))

customer_orders_df = customer_order_df.withColumn("is_delivered", when(col("order_status") == "delivered", True).otherwise(False))

customer_orders_df.select("order_status", "is_delivered").show(truncate=False)

customer_orders_df = customer_orders_df.withColumn("payment_value",col("payment_value"))

output_path = Path(TRANSFORMED_DATA_PATH) / "customer_orders_payments.csv"

customer_orders_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(str(output_path))
