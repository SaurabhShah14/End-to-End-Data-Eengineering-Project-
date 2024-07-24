from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from weather_etl import run_weather_etl
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'weather_dag',
    default_args=default_args,
    description='My first DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)

# Step 1: Fetch data from API and save to S3
run_etl = PythonOperator(
    task_id='complete_weather_etl',
    python_callable=run_weather_etl,
    dag=dag, 
)

# Step 2: Load data from S3 to Databricks
load_to_databricks = DatabricksSubmitRunOperator(
    task_id='load_to_databricks',
    databricks_conn_id = 'my_weather_Databricks',
    json={
        'new_cluster': {
            'spark_version': '3.3.2-scala2.12',
            'node_type_id': 'Standard_DS3_v2',
            'num_workers': 2
        },
        'libraries': [
            {'jar': 'dbfs:/FileStore/jars/spark-csv_2.12-2.4.4.jar'}
        ],
        'notebook_task': {
            'notebook_path': '/notebook/3140293026615150'
        }
    }
)

# Step 3: Transform data using Apache Spark on Databricks
transform_data = DatabricksSubmitRunOperator(
    task_id='transform_data',
    databricks_conn_id = 'my_weather_Databricks',
    json={
        'new_cluster': {
            'spark_version': '3.3.2-scala2.12',
            'node_type_id': 'Standard_DS3_v2',
            'num_workers': 2
        },
        'libraries': [
            {'jar': 'dbfs:/FileStore/jars/spark-csv_2.12-2.4.4.jar'}
        ],
        'notebook_task': {
            'notebook_path': '/notebook/3140293026615150'
        }
    }
)
# Step 4: Load transformed data to Snowflake on Databricks
load_to_snowflake = DatabricksSubmitRunOperator(
    task_id='load_to_snowflake',
    databricks_conn_id = 'my_weather_Databricks',
    json={
        'new_cluster': {
            'spark_version': '3.3.2-scala2.12',
            'node_type_id': 'Standard_DS3_v2',
            'num_workers': 2
        },
        'libraries': [
            {'jar': 'dbfs:/FileStore/jars/spark-csv_2.12-2.4.4.jar'}
        ],
        'notebook_task': {
            'notebook_path': '/notebook/3140293026615150'
        }
    }
)

# Define the dependencies between tasks
run_etl >> load_to_databricks >> transform_data >> load_to_snowflake



