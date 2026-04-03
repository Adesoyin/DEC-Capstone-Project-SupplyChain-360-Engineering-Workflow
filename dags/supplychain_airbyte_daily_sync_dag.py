from airflow import DAG
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from pendulum import datetime, duration


default_args = {
    "owner": "decfinalproject",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 16),
    "retries": 1,
    "retry_delay": duration(minutes=3),
}

dag = DAG(
    dag_id="airbyte_daily_sync_pipeline",
    default_args=default_args,
    # to run every day at 11:00PM after the daily ingestion dag to warehouse
    schedule="0 23 * * *",
    catchup=False,
    start_date=datetime(2026, 3, 31),
)


#  AIRBYTE SYNC
inventory_airbyte_sync = AirbyteTriggerSyncOperator(
    task_id="inventory_airbyte_sync_s3_to_snowflake",
    airbyte_conn_id="pat",
    connection_id="72d463b9-28e6-4411-8c05-2823beb67754",
)


shipments_airbyte_sync = AirbyteTriggerSyncOperator(
    task_id="shipments_airbyte_sync_s3_to_snowflake",
    airbyte_conn_id="pat",
    connection_id="3caec654-d3b3-4053-b9d6-f7b78fb62236",
)


sales_airbyte_sync = AirbyteTriggerSyncOperator(
    task_id="sales_airbyte_sync_s3_to_snowflake",
    airbyte_conn_id="pat",
    connection_id="fbc39c88-6c1f-4f51-b3c4-d4801efac28c",
)

[inventory_airbyte_sync, shipments_airbyte_sync, sales_airbyte_sync]
