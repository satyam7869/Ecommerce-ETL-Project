from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def remove_duplicates(df: DataFrame):
    return df.dropDuplicates()

def handle_nulls(df: DataFrame, column: str, replacement):
    return df.fillna({column: replacement})

def replace_invalid_values(df: DataFrame, column_name: str, valid_values, replacement_value):
    return df.withColumn(column_name, 
    when(col(column_name).isin(valid_values), col(column_name)
    ).otherwise(replacement_value))        

def clean_dataset(df: DataFrame, clean_config: list):
    for config in clean_config:
        if config["type"] == "duplicate":
            df = remove_duplicates(df)

        elif config["type"] == "null":
            df = handle_nulls(df, config["column"], config["replacement"])

        elif config["type"] == "invalid":
            df = replace_invalid_values(df, config["column"], config["valid_values"], config["replacement"])

    return df                    