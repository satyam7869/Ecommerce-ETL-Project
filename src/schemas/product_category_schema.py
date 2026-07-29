from pyspark.sql.types import *

customer_schema = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", IntegerType(), True)
])