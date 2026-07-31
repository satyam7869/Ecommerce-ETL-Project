from pathlib import Path
from config.app_config import PROCESSED_DATA_PATH, CUSTOMER_FILE, ORDERS_FILE
from src.common.spark_session import create_spark_session
from src.common.file_reader import read_csv
from src.schemas.customer_schema import customer_schema
from src.schemas.orders_schema import orders_schema

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
    customer_orders_df = customer_df.join(orders_df, customer_df.customer_id == orders_df.customer_id, "inner")
     
    customer_orders_df.show(truncate = False)

    spark.stop()  

if __name__ == "__main__":
    main()

      