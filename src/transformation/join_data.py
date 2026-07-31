from pathlib import Path
from config.app_config import PROCESSED_DATA_PATH, CUSTOMER_FILE, ORDERS_FILE, JOINED_DATA_PATH
from src.common.spark_session import create_spark_session
from src.common.file_reader import read_csv
from src.schemas.customer_schema import customer_schema
from src.schemas.orders_schema import orders_schema
from pyspark.sql.functions import col

def main():
    spark = create_spark_session()
    print("Reading customers...")
    customer_df = read_csv(
        spark = spark, file_path = str(Path(PROCESSED_DATA_PATH) / CUSTOMER_FILE),
        schema = customer_schema
    )
    print("Reading orders...")
    orders_df = read_csv(
        spark = spark, file_path = str(Path(PROCESSED_DATA_PATH) / ORDERS_FILE),
        schema = orders_schema
    )
    print("Joining...")
    c = customer_df.alias("c")
    o = orders_df.alias("o")
    customer_orders_df = c.join(o, c.customer_id == o.customer_id, "inner")
     
    customer_orders_df.show(truncate = False)    
      
    customer_orders_df = customer_orders_df.select(
        col("c.customer_id"),
        col("customer_unique_id"),
        col("customer_city"),
        col("customer_state"),
        col("o.order_id"),
        col("o.order_status"),
        col("o.order_purchase_timestamp")
)

    customer_orders_df.printSchema()

    customer_orders_df.show(10, truncate=False)

    output_path = Path(JOINED_DATA_PATH) / "customer_orders"

    customer_orders_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(str(output_path))

    spark.stop()  

if __name__ == "__main__":
    main()
