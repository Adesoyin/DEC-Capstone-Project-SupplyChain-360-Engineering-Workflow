import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingestion.utils.static_data_ingest import ingest_static_csv_to_parquet
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

source_bucket = os.getenv("source_bucket")
destination_bucket = os.getenv("destination_bucket")


def ingest_suppliers():

    ingest_static_csv_to_parquet(
        source_bucket=source_bucket,
        source_key="raw/suppliers/suppliers.csv",
        destination_bucket=destination_bucket,
        destination_key="raw/suppliers/suppliers.parquet",
    )


def ingest_products():

    ingest_static_csv_to_parquet(
        source_bucket=source_bucket,
        source_key="raw/products/products.csv",
        destination_bucket=destination_bucket,
        destination_key="raw/products/products.parquet",
    )


def ingest_warehouses():

    ingest_static_csv_to_parquet(
        source_bucket=source_bucket,
        source_key="raw/warehouses/warehouses.csv",
        destination_bucket=destination_bucket,
        destination_key="raw/warehouses/warehouses.parquet",
    )


if __name__ == "__main__":
    print("✅ Starting rarely changing tables ingestion...")

    if not all([source_bucket, destination_bucket]):
        raise ValueError("Missing source and/or destination s3 bucket")

    ingest_products()
    # Added 'f' before the strings below
    logging.info(f"Success ingesting products into {destination_bucket}")
    print("=" * 20)

    ingest_suppliers()
    logging.info(f"Success ingesting suppliers into {destination_bucket}")
    print("=" * 20)

    ingest_warehouses()
    logging.info(f"Success ingesting warehouses into {destination_bucket}")
    print("=" * 20)
