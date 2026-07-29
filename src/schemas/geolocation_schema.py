from pyspark.sql.types import *

customer_schema = StructType([
    StructField("customgeolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", StringType(), True),
    StructField("geolocation_lng", StringType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True)
])