from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit

def introduce_nulls(df: DataFrame, column_name: str, condition):

    return df.withColumn(
        column_name,
        when(condition, lit(None))
        .otherwise(col(column_name))
    )

def introduce_duplicates(df, number_of_rows):
    duplicate_df = df.limit(number_of_rows)
    return df.union(duplicate_df)

def introduce_invalid_values(df, column_name, condition, invalid_value):

    return df.withColumn(
        column_name,
        when(condition, lit(invalid_value))
        .otherwise(col(column_name))
    )


def create_dirty_dataset(df, dirty_config):

    for config in dirty_config:
        #print(config)

        if config["type"] == "null":

            df = introduce_nulls(
                df,
                config["column"],
                #config["condition"]
                col(config["condition_column"]) < config["value"]
            )

        elif config["type"] == "duplicate":

            df = introduce_duplicates(
                df,
                config["number_of_rows"]
            )

        elif config["type"] == "invalid":

            df = introduce_invalid_values(
                df,
                config["column"],
                col(config["condition_column"]) > config["value"],
                config["invalid_value"]
)
    return df

    