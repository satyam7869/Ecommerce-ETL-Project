from pathlib import Path
from src.coomon/spark_session import create_spark_session
from src.common.file_reader import read_csv
from src.schemas.customer_schema import customer_schema
from src.transformation.clean_Data_utils import clean_dataset
from config.app_config import DIRTY_DATA_PATH, PROCESSED_DATA_PATH, CUSTOMER_FILE

spark = create_spark_session()

dirty_path = Path(DIRTY_DATA_PATH) / CUSTOMER_FILE

customer_df = read_csv(spark, str(dirty_path, schema = customer_schema))

VALID_STATES = [
    "SP","RJ","MG","ES","PR","SC","RS","GO",
    "BA","PE","CE","PB","DF","MT","MS","TO",
    "PA","AM","RR","RO","AC","AP","MA","PI",
    "RN","AL","SE"
]

clean_config = [

    {
        "type": "duplicate"
    },

    {
        "type": "null",
        "column": "customer_city",
        "replacement": "Unknown"
    },

    {
        "type": "invalid",
        "column": "customer_state",
        "valid_values": VALID_STATES,
        "replacement": "Unknown"
    }
]

clean_df = clean_dataset(customer_df, clean_config)

processed_path = Path(PROCESSED_DATA_PATH) / CUSTOMER_FILE

clean_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(str(processed_path))

spark.stop()    

# Import:
from config.dataset_config import DATASETS
from src.transformation.clean_data_utils import clean_dataset

# Loop:
for dataset_name, config in DATASETS.items():

# Read Dirty Dataset
    input_path = Path(DIRTY_DATA_PATH) / config["file"]

    df = read_csv(spark, str(input_path), schema = config["schema"])

# Clean
clean_df = clean_dataset(df, config["clean_config"])

# Write Processed Dataset
ouput_path = Path( PROCESSED_DATA_PATH)/ config["file"]