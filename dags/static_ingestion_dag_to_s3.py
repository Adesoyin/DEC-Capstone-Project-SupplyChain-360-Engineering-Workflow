from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from pendulum import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.ggsheet import ingest_google_sheet_to_s3
from ingestion.rarely_changing_supplier_warehouse_products_csv import (
    ingest_suppliers,
    ingest_warehouses,
    ingest_products,
)

args = {"params": {"env": Variable.get("environment"), "dag owner": "decfinalproject"}}

dag = DAG(
    "supply_chain_static_ingestion_dag",
    start_date=datetime(2026, 1, 1),
    # To be run at 10: 00AM on first day of every month
    schedule="0 10 1 * *",
    catchup=False,
    default_args=args,
)

ingest_suppliers_to_s3 = PythonOperator(
    task_id="ingest_suppliers_to_s3", python_callable=ingest_suppliers, dag=dag
)

ingest_products_to_s3 = PythonOperator(
    task_id="ingest_products_to_s3", python_callable=ingest_products, dag=dag
)

ingest_warehouses_to_s3 = PythonOperator(
    task_id="ingest_warehouses_to_s3", python_callable=ingest_warehouses, dag=dag
)

ingest_google_sheets_to_s3 = PythonOperator(
    task_id="ingest_google_sheets_to_s3",
    python_callable=ingest_google_sheet_to_s3,
    dag=dag,
)


ingest_suppliers_to_s3
ingest_products_to_s3
ingest_warehouses_to_s3
ingest_google_sheets_to_s3
