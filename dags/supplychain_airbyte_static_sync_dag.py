from airflow import DAG
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from pendulum import datetime, duration

default_args = {
    'owner': 'decfinalproject',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 16),
    'retries': 1,
    'retry_delay': duration(minutes=3),
}

dag = DAG(
    dag_id='airbyte_rarely_changing_sync_pipeline',
    default_args=default_args,
    # To be run at 11: 00PM frst day of the month, after monthly ingestion dag
    schedule='0 23 1 * *',
    catchup=False,
    start_date=datetime(2026, 3, 31)
)

#  AIRBYTE SYNC
suppliers_airbyte_sync = AirbyteTriggerSyncOperator(
    task_id="suppliers_airbyte_sync_s3_to_snowflake",
    airbyte_conn_id="pat",
    connection_id="78423849-c6c9-478f-bbae-e6fffc8d5e2e"
)

products_airbyte_sync = AirbyteTriggerSyncOperator(
    task_id="products_airbyte_sync_s3_to_snowflake",
    airbyte_conn_id="pat",
    connection_id="ecdf3161-cf3f-4d8f-bf28-cecd5744f3d5"
)

warehouses_airbyte_sync = AirbyteTriggerSyncOperator(
    task_id="warehouses_airbyte_sync_s3_to_snowflake",
    airbyte_conn_id="pat",
    connection_id="73657756-3688-488d-8fb3-ffbf7a0925e0"
)

google_sheets_airbyte_sync = AirbyteTriggerSyncOperator(
    task_id="google_sheets_airbyte_sync_s3_to_snowflake",
    airbyte_conn_id="pat",
    connection_id="16aee0a0-b013-4647-8f0a-41c9c9d46a70"
)   

[suppliers_airbyte_sync,
products_airbyte_sync,
warehouses_airbyte_sync,
google_sheets_airbyte_sync
]

