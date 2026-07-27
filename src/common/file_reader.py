from pyspark.sql import DataFrame

def read_csv(spark, file_path, schema=None):

    reader = (
        spark.read \
        .option("header", "true")
    )

    if schema:
        reader = reader.schema(schema)

    df = reader.csv(file_path)
    return df