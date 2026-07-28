from pyspark.sql.functions import *
from src.common.spark_session import create_spark_session
from src.common.file_reader import read_csv
from src.schemas.customer_schema import customer_schema
from src.transformation.clean_data import clean_customer_data

# Cleaning Data 
spark = create_spark_session()

dirty_df = read_csv(spark, "data/dirty/customers_dirty_dataset/dirty_customer_dataset.csv" ,schema = customer_schema)

clean_df =  clean_customer_data(dirty_df)
clean_df.orderBy(col("customer_zip_code_prefix").asc()).show()

clean_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("data/processed/customer_processed_dataset")