from pyspark.sql.functions import col

from src.schemas.customer_schema import customer_schema
from src.schemas.geolocation_schema import geolocation_schema
from src.schemas.order_items_schema import order_items_schema
from src.schemas.order_payments_schema import order_payments_schema
from src.schemas.order_reviews_schema import order_reviews_schema
from src.schemas.orders_schema import orders_schema
from src.schemas.product_category_schema import product_category_schema
from src.schemas.products_schema import products_schema
from src.schemas.sellers_schema import sellers_schema

from config.app_config import RAW_DATA_PATH, DIRTY_DATA_PATH, CUSTOMER_FILE, ORDERS_FILE, PRODUCTS_FILE, GEO_LOCATION_FILE, ORDER_ITEMS, ORDER_PAYMENTS, ORDER_REVIEWS, SELLERS_FILE, PRODUCT_CATEGORY

DATASETS = {
    "customers": {"file": CUSTOMER_FILE, "schema": customer_schema, 
    "dirty_config": [

            {
                "type": "null",
                "column": "customer_city",
                "condition": col("customer_zip_code_prefix") < 10005
            },

            {
                "type": "duplicate",
                "number_of_rows": 10
            },

            {
                "type": "invalid",
                "column": "customer_state",
                "condition": col("customer_zip_code_prefix") > 99990,
                "invalid_value": "XYZ"
            }]
            
    "clean_config" = [

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
            }]},

# ORDER DATASET
    "orders": {"file": ORDERS_FILE, "schema": orders_schema,
    "dirty_config": [
            {
                "type": "null",
                "column": "order_status",
                "condition": col("order_id").startswith("00")
            },

            {
                "type": "duplicate",
                "number_of_rows": 10                      
            },

            {

                "type": "invalid",
                "column": "order_status",
                "condition": col("order_id").startswith("ff"),
                "invalid_value": "INVALID_STATUS"
            }]
            
    "clean_config": [
        {
            "type": "duplicate"
        },
        {
            "type": "null",
            "column": "order_status",
            "replacement": "pending"
        },
        {
            "type": "invalid",
            "column": "order_status",
            "valid_values": ["created", "approved", "invoiced", 
            "processing", "shipped", "delivered", "canceled", "unavailable"
            ],
            "replacement": "pending"}]}}