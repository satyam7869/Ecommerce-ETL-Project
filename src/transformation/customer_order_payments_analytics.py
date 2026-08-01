from pathlib import Path
from pyspark.sql.functions import *
from config.app_config import TRANSFORMED_DATA_PATH, ANALYTICS_DATA_PATH
from src.common.file_reader import read_csv
from src.common.spark_session import create_spark_session

spark = create_spark_session()
input_path = Path(TRANSFORMED_DATA_PATH) / "customer_orders_payments.csv"

customer_order_payments_df = read_csv(spark, str(input_path))

print("State-wise Total Orders")
state_orders_df = customer_order_payments_df.groupBy("customer_state").agg(count("order_id").alias("total_orders"))
#state_orders_df.show(truncate=False)

print("State-wise Revenue")
state_revenue_df = customer_order_payments_df.groupBy("customer_state").agg(sum("payment_value").alias("total_revenue"))
#state_revenue_df.show(truncate=False)

print("Total Revenue")
total_revenue_df = customer_order_payments_df.agg(format_number(sum("payment_value"),2).alias("total_revenue"))
total_revenue_df.show(truncate = False)

print("Total Orders")
total_orders_df = customer_order_payments_df.agg(count_distinct("order_id").alias("total_orders"))
total_orders_df.show()

print("Average Order Value")
avg_order_value_df = customer_order_payments_df.agg(format_number(avg("payment_value"),2).alias("avg_order_value"))
avg_order_value_df.show()

print("Windows Functions")
from pyspark.sql.window import Window

print("Defining a window specification")
window_spec = Window.partitionBy( "customer_id") \
                    .orderBy(col("order_purchase_timestamp").desc())

print("Latest Order per Customer")
latest_order_df = customer_order_payments_df.withColumn("row_number", row_number().over(window_spec))
latest_order_df = latest_order_df.filter(col("row_number") == 1)

print("Latest order filter karo.")
latest_order_df.select("customer_id", "order_id", "order_purchase_timestamp").show()

print("Customer Revenue")
customer_revenue_df = customer_order_payments_df.groupBy("customer_id") \
                    .agg(sum("payment_value").alias("total_revenue"))

print("Revenue Ranking Window")
revenue_window = Window.orderBy(col("total_revenue").desc())

print("Rank")
customer_rank_df = customer_revenue_df.withColumn("rank", rank().over(revenue_window))
customer_rank_df.show(truncate=False)

print("Dense Rank")
customer_dense_rank_df = customer_revenue_df.withColumn("dense_rank", dense_rank().over(revenue_window)) 
customer_dense_rank_df.show(truncate=False)

print("Caching DataFrame")
customer_order_payments_df.cache()
customer_order_payments_df.count()  # Trigger an action to cache the DataFrame

print("Explain Plan")
customer_order_payments_df.explain("formatted")

print("Repartitioning DataFrame")
customer_order_payments_df = customer_order_payments_df.repartition("customer_state")

print("Coalescing DataFrame")
customer_order_payments_df = customer_order_payments_df.coalesce(1)

print("Partition checks")
print(customer_order_payments_df.rdd.getNumPartitions())

print("After repartitioning")
customer_order_payments_df = customer_order_payments_df.repartition(4)
print(customer_order_payments_df.rdd.getNumPartitions())

# Total Revenue
output_path = Path(ANALYTICS_DATA_PATH) / "csv" / "total_revenue"
poutput_path = Path(ANALYTICS_DATA_PATH) / "parquet" / "total_revenue"
total_revenue_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(str(output_path)) \
    #.parquet(str(poutput_path))

total_revenue_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(str(poutput_path))

# Customer Rank
output_path = Path(ANALYTICS_DATA_PATH) / "csv" / "customer_rank"
poutput_path = Path(ANALYTICS_DATA_PATH) / "parquet" / "customer_rank"

customer_rank_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(str(output_path)) \
#    .parquet(str(poutput_path))

customer_rank_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(str(poutput_path))

# Customer Dense Rank
output_path = Path(ANALYTICS_DATA_PATH) / "csv" / "customer_dense_rank"
poutput_path = Path(ANALYTICS_DATA_PATH) / "parquet" / "customer_dense_rank"

customer_dense_rank_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(str(output_path)) \
#    .parquet(str(poutput_path))

customer_dense_rank_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .parquet(str(poutput_path))

spark.stop()
