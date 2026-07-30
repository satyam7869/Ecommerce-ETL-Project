#from pathlib import Path
#from pyspark.sql.functions import col
#from src.common.spark_session import create_spark_session
#from src.common.file_reader import read_csv
#from src.schemas.customer_schema import customer_schema
#from src.transformation.dirty_data_utils import create_dirty_dataset
#
## Create Spark Session
#spark = create_spark_session()
#
#for dataset_name, config in DATASETS.items():
#    input_path = Path(RAW_PATH_FILE) / config["file"]
#    output_path = Path(DIRTY_PATH_FILE) / config["file"]
#
# # Read Customer Dataset
#    df = read_csv(
#    spark,
#    str(input_path),
#    schema= config["schema"])
#
#
#    dirty_df = create_dirty_dataset(df, config["dirty_config"])
#
#    dirty_df.coalesce(1) \
#        .write \
#        .mode("overwrite") \
#        .option("header", True) \
#        .csv(str(output_path))
#
#spark.stop()


from src.transformation.dirty_data_utils import create_dirty_dataset
from src.common.spark_session import create_spark_session
from config.app_config import DIRTY_DATA_PATH, RAW_DATA_PATH, PROCESSED_DATA_PATH, CUSTOMER_FILE
from config.dataset_config.py import DATASETS

# Create Spark Session
spark = create_spark_session()

for dataset_name, config in DATASETS.items():
    input_path = Path(RAW_DATA_PATH)/ config["file"]

    df = read_csv(spark, 
    str(input_path), 
    schema = config["schema"]) 

    dirty_df = create_dirty_dataset(df, config["dirty_config"])
    output_path = Path(DIRTY_DATA_PATH) / config["file"]

    dirty_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header",True) \
    .csv(str(output_path))

spark.stop()    
