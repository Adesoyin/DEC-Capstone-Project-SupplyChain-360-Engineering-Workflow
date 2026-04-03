import os
import sys
import logging
import pandas as pd
import psycopg2
from datetime import datetime as dt
from dotenv import load_dotenv
from utils.save_as_parquet import save_to_s3_as_parquet

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DB Config
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


def ingest_sales_by_date(target_date, target_bucket):
    db_date_suffix = target_date.replace("-", "_")
    table_name = f"sales_{db_date_suffix}"

    try:
        # Use a context manager for the connection
        with psycopg2.connect(**DB_CONFIG) as conn:
            # 1. Verify table existss
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = %s)",
                    (table_name,),
                )
                if not cur.fetchone()[0]:
                    logger.error(f"Table {table_name} does not exist.")
                    return

            # 2. Extract Data in Chunks to prevent "Lost Synchronization"
            logger.info(f"Extracting data from {table_name} in chunks...")
            query = f'SELECT * FROM public."{table_name}"'

            # Use chunksize to handle large datasets and memory pressure
            chunks = pd.read_sql(query, conn, chunksize=100000)

            all_chunks = []
            for i, chunk in enumerate(chunks):
                # 3. Fix UUID Error: Convert transaction_id to string immediately
                if "transaction_id" in chunk.columns:
                    chunk["transaction_id"] = chunk["transaction_id"].astype(str)

                all_chunks.append(chunk)
                logger.info(f"Processed chunk {i + 1}")

            if not all_chunks:
                logger.warning(f"Table {table_name} is empty.")
                return

            # Combine chunks into a single DataFrame
            df = pd.concat(all_chunks, ignore_index=True)

            # 4. Add Metadata
            df["ingestion_timestamp"] = dt.now().isoformat()
            df["business_date"] = target_date

            # 5. Save to S3
            destination_key = f"raw/sales/{table_name}.parquet"
            save_to_s3_as_parquet(df, target_bucket, destination_key)

            logger.info(f"✅ Successfully moved {table_name} to S3.")
            print("=" * 20)

    except Exception as e:
        logger.error(f"Ingestion failed for {table_name}: {e}")
        raise


if __name__ == "__main__":
    # --- DATE LOGIC ---
    # 1. Check if a date was passed via command line (e.g. python script.py 2026-03-10)
    # 2. Otherwise, default to Today's date for the daily pipeline
    if len(sys.argv) > 1:
        target_day = sys.argv[1]
    else:
        target_day = dt.now().strftime("%Y-%m-%d")

    dest_bucket = os.getenv("destination_bucket")

    if not dest_bucket:
        logger.error("Destination bucket not found in .env")
        sys.exit(1)

    print("=" * 20)
    logger.info(f"Starting ingestion for Business Date: {target_day}")
    ingest_sales_by_date(target_day, dest_bucket)
