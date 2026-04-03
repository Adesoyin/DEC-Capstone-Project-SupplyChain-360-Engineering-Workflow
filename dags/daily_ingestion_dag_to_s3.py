from airflow import DAG
from airflow.models import Variable

# from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from pendulum import datetime, duration


import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.daily_shipment_inventory import (
    ingest_inventory_csv_to_parquet,
    ingest_shipments_json_to_parquet,
)
from ingestion.store_sales_transaction import ingest_sales_by_date

default_args = {
    "owner": "decfinalproject",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 16),
    "retries": 1,
    "retry_delay": duration(minutes=3),
}

dag = DAG(
    dag_id="supply_chain_daily_ingestion_dag",
    default_args=default_args,
    # to run every day at 10:00PM to ingest the current day's data at 10:00PM (after the work has ended)
    schedule="0 22 * * *",
    #'@daily',
    catchup=False,
    start_date=datetime(2026, 3, 16),
)

task_shipments = PythonOperator(
    task_id="ingest_shipments",
    python_callable=ingest_shipments_json_to_parquet,
    op_kwargs={
        "ds": "2026-03-16",
        #'{{ ds }}',
        "source_bucket": Variable.get("source_bucket"),
        "destination_bucket": Variable.get("destination_bucket"),
    },
    dag=dag,
)

task_inventory = PythonOperator(
    task_id="ingest_inventory",
    python_callable=ingest_inventory_csv_to_parquet,
    op_kwargs={
        "ds": "2026-03-16",
        #'{{ ds }}',
        "source_bucket": Variable.get("source_bucket"),
        "destination_bucket": Variable.get("destination_bucket"),
    },
    dag=dag,
)

task_sales = PythonOperator(
    task_id="ingest_sales",
    python_callable=ingest_sales_by_date,
    op_kwargs={
        "target_date": "2026-03-16",
        #'{{ ds }}',
        "target_bucket": Variable.get("destination_bucket"),
    },
    dag=dag,
)


task_shipments
task_inventory
task_sales
