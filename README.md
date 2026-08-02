# Customer Order Analytics ETL Pipeline using Pyspark

## Project Overview
This project demonstrates a production-style ETL pipeline built using Apache Spark (Pyspark). It process customer, order and payments datasets through multiple stages, including data ingestion, dirty data generation, data cleaning, data integration business transformation and analytical reporting.

This project follows a modular and configuration driven architecture, making it scalable, resuable and, easy to maintain.

## Tech Stack
Python | Apache Spark (PySpark) | Git & GitHub | CSV | Parquet | VS Code

## Key Features
- Configuration-drive ETL pipeline
- Modular project structure
- Dirty data simulation
- Data cleaning framework
- Customer, Orders and Payments integration
- Business Transformation
- Business Analytics
- Window Functions
- Performance optimization
- CSV and Parquet support

## Project Architecture
Raw CSV 
    | 
Dirty Data 
    | 
Processed Data 
    | 
Joined Data 
    | 
Transformed Data 
    | 
Analytics 
    |-- KPIs 
    |-- Revenue 
    |-- Payment Analysis 
    |__ Window Functions


## Folder Structure
project/
|-- config/
|-- data/
    |-- raw/
    |-- dirty/
    |-- processed/
    |-- joined/
    |-- transformed/
    |__ analytics/
|--src/
    |-- common/
    |-- schemas/
    |-- transformation/
    |__ analytics/
|__ README.md         

## ETL Workflow
1. Read raw CSV datasets.
2. Generate dirty data for testing.
3. Clean and validate datasets.
4. Join customer, order, and payment datasets.
5. Apply business transformations.
6. Generate business analytics.
7. Store outputs in CSV and Parquet formats.

## Spark Concepts Used
- SparkSession
- DataFrames
- Manual Schema Definition
- Configuration-Driven ETL
- Narrow & Wide Transformations
- Data Cleaning
- Data Integration (Joins)
- Aggregations
- Window Functions
- Repartition & Coalesce
- Caching
- Explain Plan
- CSV & Parquet File Formats

## Business Analytics

## The project generates the following business insights:
- Total Revenue
- Total Orders
- Average Order Value
- Monthly Revenue Trend
- Revenue by Payment Type
- Latest Order per Customer
- Customer Revenue Ranking

## Performance Optimization
- Used cache() to avoid recomputation.
- Used repartition() for efficient data distribution.
- Used coalesce() before writing output files.
- Analyzed execution plan using explain().

## Output
The project generates analytical outputs in both CSV and Parquet formats.
analytics/
├── total_revenue/
├── monthly_revenue/
├── payment_type/
├── customer_rank/
└── state_revenue/

## How to Run
1. Clone the repository.

```bash
git clone https://github.com/satyam7869/Ecommerce-ETL-Project.git
```

2. Create a virtual environment.

```bash
python -m venv venv
```

3. Activate the virtual environment.

```bash
venv\Scripts\activate
```

4. Install dependencies.

```bash
pip install -r requirements.txt
```

5. Run the ETL pipeline.

```bash
python -m src.transformation.run_dirty_data

python -m src.transformation.run_cleaning

python -m src.transformation.join_data

python -m src.transformation.customer_order_transformation

python -m src.analytics.customer_order_payments_analytics
```

## Future Enhancements
- Integrate AWS S3 for cloud storage.
- Add Apache Airflow for workflow orchestration.
- Support Delta Lake for ACID transactions.
- Add automated data quality validation.
- Develop interactive dashboards using Power BI or Tableau.


## Architecture Diagram

```text
                           Customer Order Analytics ETL Pipeline

                        +-------------------------------+
                        |        Raw CSV Datasets       |
                        | Customers | Orders | Payments |
                        +---------------+---------------+
                                        |
                                        v
                        +-------------------------------+
                        |      Dirty Data Generator      |
                        | Nulls | Duplicates | Invalids  |
                        +---------------+---------------+
                                        |
                                        v
                        +-------------------------------+
                        |       Data Cleaning Layer      |
                        | Remove Duplicates | Fix Nulls  |
                        +---------------+---------------+
                                        |
                                        v
                        +-------------------------------+
                        |        Join Operations         |
                        | Customers + Orders + Payments |
                        +---------------+---------------+
                                        |
                                        v
                        +-------------------------------+
                        | Business Transformations       |
                        | Year | Month | Flags | Casts  |
                        +---------------+---------------+
                                        |
                                        v
                        +-------------------------------+
                        | Business Analytics             |
                        | KPIs | Revenue | Windows       |
                        +---------------+---------------+
                                        |
                                        v
                        +-------------------------------+
                        | CSV Output | Parquet Output    |
                        +-------------------------------+
```