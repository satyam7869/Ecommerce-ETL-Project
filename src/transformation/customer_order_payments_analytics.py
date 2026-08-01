from pathlib import Path
from pyspark.sql.functions import *
from config.app_config import TRANSFORMED_DATA_PATH
from src.common.file_reader import read_csv
from src.common.spark_session import create_spark_session

spark = create_spark_session()
input_path = Path(TRANSFORMED_DATA_PATH) / "customer_orders_payments.csv"

customer_order_payments_df = read_csv(spark, str(input_path))

# State-wise Total Orders
state_orders_df = customer_order_payments_df.groupBy("customer_state").agg(count("order_id").alias("total_orders"))
#state_orders_df.show(truncate=False)

# State-wise Revenue
state_revenue_df = customer_order_payments_df.groupBy("customer_state").agg(sum("payment_value").alias("total_revenue"))
#state_revenue_df.show(truncate=False)

# Total Revenue
total_revenue_df = customer_order_payments_df.agg(format_number(sum("payment_value"),2).alias("total_revenue"))
total_revenue_df.show(truncate = False)

# Total Orders
total_orders_df = customer_order_payments_df.agg(count_distinct("order_id").alias("total_orders"))
total_orders_df.show()

# Average Order Value
avg_order_value_df = customer_order_payments_df.agg(format_number(avg("payment_value"),2).alias("avg_order_value"))
avg_order_value_df.show()

# Windows Functions
from pyspark.sql.window import Window

# Define a window specification
window_spec = Window.partitionBy( "customer_id") \
                    .orderBy(col("order_purchase_timestamp").desc())

# Latest Order per Customer
latest_order_df = customer_order_payments_df.withColumn("row_number", row_number().over(window_spec))
latest_order_df = latest_order_df.filter(col("row_number") == 1)

# Latest order filter karo.
latest_order_df.select("customer_id", "order_id", "order_purchase_timestamp").show()

# Customer Revenue
customer_revenue_df = customer_order_payments_df.groupBy("customer_id") \
                    .agg(sum("payment_value").alias("total_revenue"))

# Revenue Ranking Window
revenue_window = Window.orderBy(col("total_revenue").desc())

# Rank
customer_rank_df = customer_revenue_df.withColumn("rank", rank().over(revenue_window))
customer_rank_df.show(truncate=False)

# Dense Rank
customer_dense_rank_df = customer_revenue_df.withColumn("dense_rank", dense_rank().over(revenue_window))
customer_dense_rank_df.show(truncate=False)















spark.stop()
