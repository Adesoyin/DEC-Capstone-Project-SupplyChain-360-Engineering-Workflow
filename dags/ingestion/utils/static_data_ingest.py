import sys
import boto3
import os
import pandas as pd
from io import BytesIO
import logging
from dotenv import load_dotenv
from datetime import datetime as dt

load_dotenv()

region = os.getenv("AWS_REGION")
source_bucket = os.getenv("source_bucket")
destination_bucket = os.getenv("destination_bucket")


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Personal destination s3 bucket
s3_dest = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=region,
)

# DEC Source s3 bucket
s3_source = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID2"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY2"),
    region_name=region,
)


def ingest_static_csv_to_parquet(
    source_bucket, source_key, destination_bucket, destination_key
):
    try:
        """ using the source client cred to read from the dec bucket given """
        response = s3_source.get_object(Bucket=source_bucket, Key=source_key)
        df = pd.read_csv(response["Body"])

        # metadata Addition
        df["ingested_at"] = dt.now().isoformat()
        df["source_file"] = source_key

        # parquet conversion using pyarrow engine
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")

        """using the destination client cred to write into my s3 bucket """
        s3_dest.put_object(
            Bucket=destination_bucket, Key=destination_key, Body=buffer.getvalue()
        )
        print("=" * 20)
        logging.info(
            f"Success! Moved {source_key} from DEC_bucket to {destination_bucket}"
        )

    except Exception as e:
        print(f"Error: {e}")
        raise
