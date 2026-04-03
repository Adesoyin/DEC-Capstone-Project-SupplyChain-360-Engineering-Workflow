from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
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
    "retries": 1,
    "retry_delay": duration(minutes=3),
}

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"
DBT_LOG_PATH = "/tmp/dbt_logs"

DBT_FLAGS = (
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--log-path {DBT_LOG_PATH}"
)


with DAG(
    dag_id="supply_chain_daily_pipeline",
    default_args=default_args,
    schedule="0 22 * * *",  # 10PM
    start_date=datetime(2026, 3, 16),
    catchup=False,
    tags=["supply_chain", "daily_pipeline"],
) as dag:
    # Shipments

    with TaskGroup("shipments_pipeline") as shipments_pipeline:
        ingest_shipments = PythonOperator(
            task_id="ingest_shipments",
            python_callable=ingest_shipments_json_to_parquet,
            op_kwargs={
                "ds": "2026-03-16",
                #'{{ ds }}',
                "source_bucket": Variable.get("source_bucket"),
                "destination_bucket": Variable.get("destination_bucket"),
            },
        )

        sync_shipments = AirbyteTriggerSyncOperator(
            task_id="sync_shipments_to_snowflake",
            airbyte_conn_id="pat",
            connection_id="3caec654-d3b3-4053-b9d6-f7b78fb62236",
        )

        ingest_shipments >> sync_shipments

    # Inventory

    with TaskGroup("inventory_pipeline") as inventory_pipeline:
        ingest_inventory = PythonOperator(
            task_id="ingest_inventory",
            python_callable=ingest_inventory_csv_to_parquet,
            op_kwargs={
                "ds": "2026-03-16",
                #'{{ ds }}',
                "source_bucket": Variable.get("source_bucket"),
                "destination_bucket": Variable.get("destination_bucket"),
            },
        )

        sync_inventory = AirbyteTriggerSyncOperator(
            task_id="sync_inventory_to_snowflake",
            airbyte_conn_id="pat",
            connection_id="72d463b9-28e6-4411-8c05-2823beb67754",
        )

        ingest_inventory >> sync_inventory

    # Sales

    with TaskGroup("sales_pipeline") as sales_pipeline:
        ingest_sales = PythonOperator(
            task_id="ingest_sales",
            python_callable=ingest_sales_by_date,
            op_kwargs={
                "target_date": "2026-03-16",
                #                '{{ ds }}',
                "target_bucket": Variable.get("destination_bucket"),
            },
        )

        sync_sales = AirbyteTriggerSyncOperator(
            task_id="sync_sales_to_snowflake",
            airbyte_conn_id="pat",
            connection_id="fbc39c88-6c1f-4f51-b3c4-d4801efac28c",
        )

        ingest_sales >> sync_sales

    # dbt Transformation

    with TaskGroup("dbt_transform") as dbt_transform:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"mkdir -p {DBT_LOG_PATH} && dbt deps {DBT_FLAGS}",
        )

        dbt_run = BashOperator(
            task_id="dbt_run_all",
            bash_command=f"dbt run {DBT_FLAGS}",
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"dbt test {DBT_FLAGS}",
        )

        dbt_docs = BashOperator(
            task_id="dbt_docs",
            bash_command=f"dbt docs generate {DBT_FLAGS}",
        )

        dbt_deps >> dbt_run >> dbt_test >> dbt_docs

    [shipments_pipeline, inventory_pipeline, sales_pipeline] >> dbt_transform
