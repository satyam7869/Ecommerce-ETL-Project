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
from src.schemas.order_payments_schema import order_payments_schema

#from src.transformation.run_cleaning import VALID_STATES
from config.app_config import RAW_DATA_PATH, DIRTY_DATA_PATH, CUSTOMER_FILE, ORDERS_FILE, ORDER_PAYMENTS

VALID_STATES = [
    "SP","RJ","MG","ES","PR","SC","RS","GO",
    "BA","PE","CE","PB","DF","MT","MS","TO",
    "PA","AM","RR","RO","AC","AP","MA","PI",
    "RN","AL","SE"
]

DATASETS = {
    "customers": {"file": CUSTOMER_FILE, 
    "schema": customer_schema, 
    "dirty_config": [
            {
                "type": "null",
                "column": "customer_city",
                "condition_column": "customer_zip_code_prefix",
                "operator": "<",
                "value": 10005
            },
            {
                "type": "duplicate",
                "number_of_rows": 10
            },
            {
                "type": "invalid",
                "column": "customer_state",
                "condition_column": "customer_zip_code_prefix",
                "operator": ">",
                "value": 99990,
                "invalid_value": "XYZ"
            }],
            
    "clean_config" : [
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
            "condition_column": "order_id",
            "operator": "startswith",
            "value": "00"
            #"condition": col("order_id").startswith("00")
        },
        {
            "type": "duplicate",
            "number_of_rows": 10                      
        },
        {
            "type": "invalid",
            "column": "order_status",
            "condition_column": "order_id",
            "operator": "startswith",
            "value": "ff",
            #"condition": col("order_id").startswith("ff"),
            "invalid_value": "INVALID_STATUS"
        }],
            
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
            "replacement": "pending"}]},

# ORDER PAYMENTS
    "order_payments": {"file" : ORDER_PAYMENTS, 
    "schema" : order_payments_schema,
    "dirty_config" : [
        {
            "type": "null",
            "column": "payment_type",
            "condition_column": "payment_value",
            "operator": "==",
            "value": 1
        },
        {
            "type": "duplicate",
            "number_of_rows": 10
        },
        {
            "type": "invalid",
            "column": "payment_type",
            "condition_column": "payment_sequential",
            "operator": "==",
            "value": 2,
            "invalid_value": "INVALID_PAYMENT_TYPE"
        }],

    "clean_config": [
        {
            "type": "duplicate"
        },
        {
            "type": "null",
            "column": "payment_type",
            "replacement": "Unknown"
        },
        {
            "type": "invalid",
            "column": "payment_type",
            "valid_values": ["credit_card", "boleto", "voucher", "debit_card"],
            "replacement": "Unknown"
        }]}}
