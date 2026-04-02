# dags/dbt_transform_dag.py

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"  # where profiles.yml lives
DBT_LOG_PATH     = "/tmp/dbt_logs" 

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
        f"[ALERT] Task {ctx['task_instance'].task_id} failed — "
        f"DAG: {ctx['dag'].dag_id}, Date: {ctx['ds']}"
    ),
}

with DAG(
    dag_id="supply_chain_dbt_transform",
    description="Run dbt models against Snowflake after Airbyte sync completes",
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
        bash_command=f"dbt run {DBT_FLAGS} --select dimensions",
    )

    # Run fact models 
    # Reason: facts reference dims via surrogate keys — must run after dims
    dbt_run_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=f"dbt run {DBT_FLAGS} --select facts",
    )

    # Run dbt tests
    # Reason: validates not_null, unique, FK relationships
    # defined in each schema.yml. Fails the DAG if any test fails — downstream
    # consumers are never served models that have failed quality checks
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {DBT_FLAGS}",
    )

    # Generate docs
    # Reason: keeps your dbt documentation site up to date automatically.
    # Comment this out if you don't have a docs server set up yet
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"dbt docs generate {DBT_FLAGS}",
    )

    dbt_deps >> dbt_run_staging >> dbt_run_dimensions >> dbt_run_facts >> dbt_test >> dbt_docs






















# from airflow import DAG
# from airflow.models import Variable
# from airflow.operators.bash import BashOperator
# #from airflow.providers.standard.operators.bash import BashOperator
# from pendulum import datetime

# args={
#     'params':{
#         'env': Variable.get('environment'),
#         'dag owner': 'decfinalproject'
#     }
# }

# profile_dir = "/opt/airflow/dags/DEC_Capstone_Project_SupplyChain/dbt_project/dbt_project"

# with DAG(
#     dag_id="supplychain_dbt_transformation_pipeline",
#     start_date=datetime(2026, 3, 31),
#     # to run every day at 12:00PM after the Airbyte sync dag
#     schedule='0 12 * * *',
#     #schedule="@daily",
#     catchup=False,
#     default_args=args,

# ) as dag:

#     dbt_run = BashOperator(
#         task_id="dbt_run",
#         bash_command="cd /opt/airflow/dbt_project && dbt run"
#     )

#     dbt_test = BashOperator(
#         task_id="dbt_test",
#         bash_command="docker exec dbt dbt test"
#     )

#     dbt_run >> dbt_test