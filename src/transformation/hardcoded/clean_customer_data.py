from pyspark.sql.functions import *
from src.common.spark_session import create_spark_session
from src.common.file_reader import read_csv
from src.schemas.customer_schema import customer_schema

# Remove Duplicates
def remove_duplicates(df):
    return df.dropDuplicates()

# Handling Null Values
def handle_nulls(df):
    return (
        df.fillna({
            "customer_city" : "unknown",
            "customer_state" : "NA"
        })
    )    

# Validate States
VALID_STATES = [
    "SP","RJ","MG","BA","PR","RS","SC","GO",
    "DF","ES","MT","MS","TO","PA","AM","RR",
    "AP","RO","AC","MA","PI","CE","RN","PB",
    "PE","AL","SE"
]

def validate_state(df):
    return df.withColumn("customer_state", when(
        col("customer_state").isin(VALID_STATES),
        col("customer_state")
    ).otherwise(lit("Unknown"))
)    

#Cleaning Pipeline
def clean_customer_data(df):
    df = remove_duplicates(df)
    df = handle_nulls(df)
    df = validate_state(df)

    return df