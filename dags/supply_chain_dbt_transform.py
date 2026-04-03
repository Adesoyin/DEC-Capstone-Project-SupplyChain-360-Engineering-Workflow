from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"
DBT_LOG_PATH = "/tmp/dbt_logs"

DBT_FLAGS = (
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--log-path {DBT_LOG_PATH}"
)

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda ctx: print(
        f"[ALERT] Task {ctx['task_instance'].task_id} failed"
        f"DAG: {ctx['dag'].dag_id}, Date: {ctx['ds']}"
    ),
}

with DAG(
    dag_id="supply_chain_dbt_transform",
    description=(
        "Run dbt models against Snowflake after Airbyte sync completes at 4:00 AM daily"
    ),
    default_args=default_args,
    schedule_interval="0 4 * * *",
    start_date=datetime(2026, 3, 10),
    catchup=False,
    tags=["dbt", "snowflake", "transform"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"mkdir -p {DBT_LOG_PATH} && dbt deps {DBT_FLAGS}",
    )

    # Staging models run
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"dbt run {DBT_FLAGS} --select staging",
    )

    # Dimension models Run
    dbt_run_dimensions = BashOperator(
        task_id="dbt_run_dimensions",
        bash_command=f"dbt run {DBT_FLAGS} --select dim",
    )

    # Run fact models
    dbt_run_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=f"dbt run {DBT_FLAGS} --select fact",
    )

    # Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {DBT_FLAGS}",
    )

    # Generate docs
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"dbt docs generate {DBT_FLAGS}",
    )

    (
        dbt_deps
        >> dbt_run_staging
        >> dbt_run_dimensions
        >> dbt_run_facts
        >> dbt_test
        >> dbt_docs
    )
