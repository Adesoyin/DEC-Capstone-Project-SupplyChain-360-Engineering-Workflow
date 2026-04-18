import io
import logging
import os
import sys
from datetime import datetime as dt

import boto3
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../.env"))

# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# S3 client
s3_dest = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)


def get_postgres_conn():
    """
    Use PostgresHook when inside Airflow,
    fall back to direct psycopg2 when running locally.
    """
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="beajan_db")
        return hook.get_conn()

    except Exception:
        logger.info("Airflow connection not available, using local .env credentials...")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT", 5432)
        logger.info(f"Connecting to DB: {host}:{port}")
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            sslmode="require",
            connect_timeout=30,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        print("Connected successfully")
        return conn


def file_already_exists(bucket, key):
    try:
        s3_dest.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def ingest_sales_by_date(target_date, target_bucket, **kwargs):
    ti = kwargs.get("ti")  # for XCom

    db_date_suffix = target_date.replace("-", "_")
    table_name = f"sales_{db_date_suffix}"
    destination_key = f"raw/sales/{target_date}/{table_name}.parquet"

    if file_already_exists(target_bucket, destination_key):
        logger.info(f"Skipping {table_name} - already ingested.")
        return

    try:
        conn = get_postgres_conn()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                )
            """,
                (table_name,),
            )

            if not cur.fetchone()[0]:
                logger.warning(f"Table {table_name} does not exist.")
                return

        logger.info(f"Extracting {table_name} in chunks...")
        query = f'SELECT * FROM public."{table_name}"'

        chunks = pd.read_sql(query, conn, chunksize=50000)

        all_chunks = []
        for i, chunk in enumerate(chunks):
            if "transaction_id" in chunk.columns:
                chunk["transaction_id"] = chunk["transaction_id"].astype(str)
            all_chunks.append(chunk)
            logger.info(f"Processed chunk {i + 1}")

        if not all_chunks:
            logger.warning(f"{table_name} is empty.")
            return

        df = pd.concat(all_chunks, ignore_index=True)
        df["ingestion_timestamp"] = dt.now().isoformat()
        df["business_date"] = target_date

        logger.info(f"Uploading to S3: {destination_key}")
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")

        s3_dest.put_object(
            Bucket=target_bucket, Key=destination_key, Body=buffer.getvalue()
        )

        logger.info(f"Successfully ingested to {destination_key}")

        if ti:
            ti.xcom_push(key="source_destination", value=target_bucket)

    except Exception as e:
        logger.error(f"Ingestion failed for {table_name}: {e}")
        raise
    finally:
        if "conn" in locals() and conn:
            conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_day = sys.argv[1]
    else:
        target_day = dt.now().strftime("%Y-%m-%d")

    dest_bucket = os.getenv("destination_bucket")

    if not dest_bucket:
        logger.error("Destination bucket not found in .env")
        sys.exit(1)

    print("=" * 20)
    logger.info(f"Starting sales ingestion for Business Date: {target_day}")
    ingest_sales_by_date(target_day, dest_bucket)
    
