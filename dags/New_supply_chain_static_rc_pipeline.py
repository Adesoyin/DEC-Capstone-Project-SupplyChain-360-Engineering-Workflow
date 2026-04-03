from pendulum import datetime, duration
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.ggsheet import ingest_google_sheet_to_s3
from ingestion.rarely_changing_supplier_warehouse_products_csv import (
    ingest_suppliers,
    ingest_warehouses,
    ingest_products,
)

# Airbyte connection UUIDs for static or rarely changing sources
AIRBYTE_CONN_ID = "pat"
AIRBYTE_CONNECTIONS = {
    "suppliers": "78423849-c6c9-478f-bbae-e6fffc8d5e2e",
    "products": "ecdf3161-cf3f-4d8f-bf28-cecd5744f3d5",
    "warehouses": "73657756-3688-488d-8fb3-ffbf7a0925e0",
    "google_sheets": "16aee0a0-b013-4647-8f0a-41c9c9d46a70",
}

# dbt config
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"
DBT_LOG_PATH = "/tmp/dbt_logs"
DBT_FLAGS = (
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--log-path {DBT_LOG_PATH}"
)

default_args = {
    "owner": "decfinalproject",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": duration(minutes=5),
    "on_failure_callback": lambda ctx: print(
        f"[Alert] Task {ctx['task_instance'].task_id} failed — "
        f"DAG: {ctx['dag'].dag_id}, Date: {ctx['ds']}"
    ),
    "params": {
        "env": Variable.get("environment", default_var="dev"),
        "dag_owner": "decfinalproject",
    },
}

with DAG(
    dag_id="supply_chain_static_monthly_pipeline",
    description=(
        "Monthly pipeline: ingest static master data to S3, sync each source "
        "to Snowflake via Airbyte, then run dbt transformations. "
        "Runs at 10:00 on the first day of every month."
    ),
    default_args=default_args,
    schedule="0 10 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["static", "monthly", "ingestion", "airbyte", "dbt", "snowflake"],
) as dag:
    # Ingestion to S3

    ingest_suppliers_to_s3 = PythonOperator(
        task_id="ingest_suppliers_to_s3",
        python_callable=ingest_suppliers,
    )

    ingest_products_to_s3 = PythonOperator(
        task_id="ingest_products_to_s3",
        python_callable=ingest_products,
    )

    ingest_warehouses_to_s3 = PythonOperator(
        task_id="ingest_warehouses_to_s3",
        python_callable=ingest_warehouses,
    )

    ingest_google_sheets_to_s3 = PythonOperator(
        task_id="ingest_google_sheets_to_s3",
        python_callable=ingest_google_sheet_to_s3,
    )

    # Airbyte syncs S3 to Snowflake

    suppliers_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="suppliers_airbyte_sync_s3_to_snowflake",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=AIRBYTE_CONNECTIONS["suppliers"],
        asynchronous=False,
        timeout=3600,
        wait_seconds=30,
    )

    products_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="products_airbyte_sync_s3_to_snowflake",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=AIRBYTE_CONNECTIONS["products"],
        asynchronous=False,
        timeout=3600,
        wait_seconds=30,
    )

    warehouses_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="warehouses_airbyte_sync_s3_to_snowflake",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=AIRBYTE_CONNECTIONS["warehouses"],
        asynchronous=False,
        timeout=3600,
        wait_seconds=30,
    )

    google_sheets_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="google_sheets_airbyte_sync_s3_to_snowflake",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=AIRBYTE_CONNECTIONS["google_sheets"],
        asynchronous=False,
        timeout=3600,
        wait_seconds=30,
    )

    # dbt transformations

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"mkdir -p {DBT_LOG_PATH} && dbt deps {DBT_FLAGS}",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"dbt run {DBT_FLAGS} --select staging",
    )

    dbt_run_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=f"dbt run {DBT_FLAGS} --select dim",
    )

    dbt_run_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=f"dbt run {DBT_FLAGS} --select fact",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {DBT_FLAGS}",
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"dbt docs generate {DBT_FLAGS}",
    )

    # Task dependency

    ingest_suppliers_to_s3 >> suppliers_airbyte_sync
    ingest_products_to_s3 >> products_airbyte_sync
    ingest_warehouses_to_s3 >> warehouses_airbyte_sync
    ingest_google_sheets_to_s3 >> google_sheets_airbyte_sync

    (
        [
            suppliers_airbyte_sync,
            products_airbyte_sync,
            warehouses_airbyte_sync,
            google_sheets_airbyte_sync,
        ]
        >> dbt_deps
        >> dbt_run_staging
        >> dbt_run_dimensions
        >> dbt_run_facts
        >> dbt_test
        >> dbt_docs
    )
