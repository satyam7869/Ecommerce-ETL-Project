from pyspark.sql import SparkSession
from config.app_config import APP_NAME, MASTER

def create_spark_session():
    """
    Create and return a spark session for Ecommerce ETL Project.
    """

    spark = (
        SparkSession.builder \
        .appName(APP_NAME) \
        .master(MASTER) \
        .getOrCreate()
    )
    return spark