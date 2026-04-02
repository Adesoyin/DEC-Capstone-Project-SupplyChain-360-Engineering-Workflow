FROM apache/airflow:2.9.1-python3.12

USER root

RUN apt-get update && apt-get install -y \
    git \
    gcc \
    python3-dev \
    build-essential \
    && apt-get clean

USER airflow

# Airflow providers — pinned to constraints for compatibility
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    apache-airflow-providers-airbyte \
    apache-airflow-providers-postgres \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt"

# dbt installed separately WITHOUT Airflow constraints
# Reason: dbt has its own dependency tree that conflicts with Airflow constraints.
# Installing it in a separate layer keeps the conflict isolated and allows
# pip to resolve dbt's dependencies freely
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-snowflake

# General Python dependencies
# Reason: separate layer so a change here doesn't invalidate the dbt layer above
RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    boto3 \
    psycopg2-binary \
    gspread \
    oauth2client \
    python-dotenv

# Copy project files into the image
# Reason: baking files in means the image is self-contained and portable.
# The docker-compose.yml also mounts these as volumes for local development
# so live changes reflect without a rebuild
COPY --chown=airflow:root dags/        /opt/airflow/dags/
COPY --chown=airflow:root src/         /opt/airflow/src/
COPY --chown=airflow:root dbt_project/ /opt/airflow/dbt_project/
COPY --chown=airflow:root creds/       /opt/airflow/creds/

WORKDIR /opt/airflow

# Make src/ importable as a Python package from anywhere inside the container
ENV PYTHONPATH="/opt/airflow/src:${PYTHONPATH:-}"