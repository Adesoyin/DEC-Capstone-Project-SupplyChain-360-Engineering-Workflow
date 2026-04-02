import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import logging
from datetime import datetime as dt
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

# Load configs once
SOURCE_BUCKET = os.getenv("source_bucket")
DESTINATION_BUCKET = os.getenv("destination_bucket")
REGION = os.getenv("AWS_REGION")


# JSON Shipments

def _s3_source():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID2"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY2"),
        region_name=REGION,
    )

def _s3_dest():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION,
    )

def _clean_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make every column Parquet-safe:
      - Flatten dot-notation column names produced by json_normalize
      - Drop duplicate columns (keeps first occurrence)
      - Serialize any remaining dict/list cells to JSON strings
      - Replace NaN/None with typed nulls — never with empty strings,
        which would coerce numeric columns to object dtype and break
        PyArrow's type inference (root cause of the original error)
    """
    # Flatten nested column names: e.g "address.city" → "address_city"
    df.columns = [col.replace(".", "_") for col in df.columns]

    # Drop duplicate column names that json_normalize can produce
    df = df.loc[:, ~df.columns.duplicated()]

    # Serialize any cell that is still a dict or list
    for col in df.columns:
        mask = df[col].apply(lambda v: isinstance(v, (dict, list)))
        if mask.any():
            df[col] = df[col].apply(
                lambda v: json.dumps(v, default=str) if isinstance(v, (dict, list)) else v
            )
    return df


def _coerce_pyarrow_schema(df: pd.DataFrame) -> pa.Table:
    """
    Convert DataFrame to a PyArrow Table with an explicit, stable schema.
    Explicit schemas prevent type-inference surprises across runs where a
    column might be all-null one day and populated the next.
    """
    sample = df.dropna(how="all")
    if sample.empty:
        sample = df

    inferred = pa.Schema.from_pandas(sample, preserve_index=False)

    # Build a nullable version of every field
    nullable_fields = [
        pa.field(f.name, f.type, nullable=True)
        for f in inferred
    ]
    schema = pa.schema(nullable_fields)

    return pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=False)


def _write_parquet(table: pa.Table) -> bytes:

    buffer = BytesIO()
    pq.write_table(
        table,
        buffer,
        compression="snappy",
        use_dictionary=True,
        row_group_size=50_000,
        data_page_version="2.0",
    )
    buffer.seek(0)
    return buffer.getvalue()


def _validate_parquet(parquet_bytes: bytes) -> None:
    """
    Read back the schema from the bytes to be uploaded
    Raises if the footer is unreadable — Airflow will then retry rather
    than silently storing a corrupt file.
    """
    buffer = BytesIO(parquet_bytes)
    schema = pq.read_schema(buffer)
    logger.info("Parquet validation passed — schema: %s", schema)


def _atomic_upload(s3, bucket: str, key: str, parquet_bytes: bytes) -> None:
    """
    Upload atomically: write to a .tmp key, validate, then copy to the
    real key and delete the temp.  Ensures the destination never holds a
    partial file if the process is interrupted mid-upload.
    """
    tmp_key = key + ".tmp"

    s3.put_object(Bucket=bucket, Key=tmp_key, Body=parquet_bytes)
    logger.info("Uploaded temp file → s3://%s/%s", bucket, tmp_key)

    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": tmp_key},
        Key=key,
    )
    s3.delete_object(Bucket=bucket, Key=tmp_key)
    logger.info("Promoted to final key → s3://%s/%s", bucket, key)


# main function

def ingest_shipments_json_to_parquet(
    ds: str | None = None,
    source_bucket: str | None = None,
    destination_bucket: str | None = None,
    **kwargs,
) -> None:
    """
    Extract one day of shipment JSON from S3, flatten + clean it, and write
    a valid Snappy-compressed Parquet file to the destination bucket.

    ds is used as Execution date string in YYYY-MM-DD format (injected by Airflow or
    passed during my testing but will default to today if not passed.
    """
    ds                 = ds or kwargs.get("ds") or dt.now().strftime("%Y-%m-%d")
    source_bucket      = source_bucket      or SOURCE_BUCKET
    destination_bucket = destination_bucket or DESTINATION_BUCKET

    source_key      = f"raw/shipments/shipments_{ds}.json"
    destination_key = f"raw/shipments/shipments_{ds}.parquet"

    src = _s3_source()
    dst = _s3_dest()

    # Checking if the source file exist
    try:
        src.head_object(Bucket=source_bucket, Key=source_key)
        logger.info("Source file confirmed: s3://%s/%s", source_bucket, source_key)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            logger.warning("Source file missing — skipping: s3://%s/%s", source_bucket, source_key)
            return
        raise

    # download and parse JSON
    response   = src.get_object(Bucket=source_bucket, Key=source_key)
    raw_bytes  = response["Body"].read()

    # support both newline-delimited JSON ({"a":1}\n{"a":2}) and JSON arrays ([{...}])
    raw_text = raw_bytes.decode("utf-8").strip()
    if raw_text.startswith("["):
        records = json.loads(raw_text)
    else:
        records = [
            json.loads(line)
            for line in raw_text.splitlines()
            if line.strip()
        ]                                     

    if not records:
        logger.warning("No records in %s — skipping", source_key)
        return

    logger.info("Parsed %d records from %s", len(records), source_key)

    df = pd.json_normalize(records, sep="_")

    # Clean schema
    df = _clean_schema(df)

    # pipeline metadata
    df["ingested_at"]  = dt.now().isoformat()
    df["source_file"]  = source_key
    df["business_date"] = ds

    logger.info("DataFrame shape after cleaning: %s", df.shape)
    logger.debug("Column dtypes:\n%s", df.dtypes)

    # Convert to PyArrow Table with stable schema
    table = _coerce_pyarrow_schema(df)

    # Serialise to Parquet bytes
    parquet_bytes = _write_parquet(table)
    logger.info("Parquet size: %d bytes", len(parquet_bytes))

    # Validate before uploading 
    _validate_parquet(parquet_bytes)

    # upload to s3 to overwrites any previous version safely
    _atomic_upload(dst, destination_bucket, destination_key, parquet_bytes)

    logger.info(
        "Done: s3://%s/%s → s3://%s/%s (%d rows, %d cols)",
        source_bucket, source_key,
        destination_bucket, destination_key,
        len(df), len(df.columns),
    )



# Inventory

def ingest_inventory_csv_to_parquet(
    ds=None,
    source_bucket=None,
    destination_bucket=None,
    **kwargs
):
    ds = ds or kwargs.get("ds") or dt.now().strftime('%Y-%m-%d')
    source_bucket = source_bucket or SOURCE_BUCKET
    destination_bucket = destination_bucket or DESTINATION_BUCKET

    source_key = f"raw/inventory/inventory_{ds}.csv"
    destination_key = f"raw/inventory/inventory_{ds}.parquet"

    try:
        s3_dest = boto3.client("s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION         
                                    )
        s3_source = boto3.client("s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID2"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY2"),
        region_name=REGION
        )
        logger.info(f"Bucket: {source_bucket}")
        logger.info(f"Key: {source_key}")
        logger.info(f"AWS Source Key ID: {os.getenv('aws_access_key_id')}")

        s3_source.head_object(Bucket=source_bucket, Key=source_key)
    except Exception:
        logger.warning(f"Missing file: {source_key} — skipping")
        return

    try:
        response = s3_source.get_object(Bucket=source_bucket, Key=source_key)
        df = pd.read_csv(response['Body'])

        if df.empty:
            logger.warning(f"No data in {source_key}")
            return

        df["ingested_at"] = dt.now().isoformat()
        df["source_file"] = source_key
        df["business_date"] = ds

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)

        # Delete before write  
        s3_dest.delete_object(Bucket=destination_bucket, Key=destination_key)
        s3_dest.put_object(
            Bucket=destination_bucket,
            Key=destination_key,
            Body=buffer.getvalue()
        )

        logger.info(f"Ingested {source_key}")

    except Exception as e:
        logger.error(str(e))
        raise


# -------------------------------
if __name__ == "__main__":    
    import sys
    import os

    execution_date = (
        sys.argv[1] if len(sys.argv) > 1
        else dt.now().strftime('%Y-%m-%d')
    )

    if not all([SOURCE_BUCKET, DESTINATION_BUCKET]):
        raise ValueError("Check your .env file")

    ingest_shipments_json_to_parquet(ds=execution_date)
    logger.info(f'shipment_delivery_logs for {execution_date} is completed')
    ingest_inventory_csv_to_parquet(ds=execution_date)
    logger.info(f'warehouse_inventory for {execution_date} is completed')

    print(f"✅ Completed for {execution_date}")






































# # Shipments
# def ingest_shipments_json_to_parquet(
#     ds=None,
#     source_bucket=None,
#     destination_bucket=None,
#     **kwargs
# ):

#     # Resolve params
#     ds = ds or kwargs.get("ds") or dt.now().strftime('%Y-%m-%d')
#     source_bucket = source_bucket or SOURCE_BUCKET
#     destination_bucket = destination_bucket or DESTINATION_BUCKET

#     source_key = f"raw/shipments/shipments_{ds}.json"
#     destination_key = f"raw/shipments/shipments_{ds}.parquet"

#     try:  
#         s3_dest = boto3.client("s3",
#         aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID2"),
#         aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY2"),
#         region_name=REGION         
#         )

#         s3_source = boto3.client("s3",
#         aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
#         aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
#         region_name=REGION
#         )

#         s3_source.head_object(Bucket=source_bucket, Key=source_key)
#         logger.info("Succesfully connected to source bucket")
#     except ClientError as e:
#         if e.response['Error']['Code'] == '404':
#             logger.warning(f"Source file truly Missing: {source_bucket}/{source_key} — skipping")
#             return
#         else:
#             logger.error(f"S3 error: {e}")
#             raise
        

#     try:
#         response = s3_source.get_object(Bucket=source_bucket, Key=source_key)
#         df = pd.read_json(response['Body'], lines=True)

#         if df.empty:
#             logger.warning(f"No data in {source_key}")
#             return
        
#         df_flat = pd.json_normalize(df.to_dict(orient='records'))
        
#         df_flat = df_flat.fillna("")
#         df_flat.columns = [col.replace(".", "_") for col in df_flat.columns]

#         # Add metadata
#         df_flat["ingested_at"] = dt.now().isoformat()
#         df_flat["source_file"] = source_key
#         df_flat["business_date"] = ds

#         # df_flat.to_parquet(buffer, index=False)
#         # convert to pyarrow table
#         table = pa.Table.from_pandas(df_flat)

#         # Write parquet
#         buffer = BytesIO()
#         pq.write_table(
#             table,
#             buffer,
#             compression="snappy",
#             use_dictionary=True,
#             row_group_size=50000
#         )

#         # upload to s3 (delete before write)
#         s3_dest.delete_object(Bucket=destination_bucket, Key=destination_key)
#         s3_dest.put_object(
#             Bucket=destination_bucket,
#             Key=destination_key,
#             Body=buffer.getvalue()
#         )

#         logger.info(f"Ingested {source_key} → {destination_key}")

#     except Exception as e:
#         logger.error(f"Error processing {source_key}: {e}")
#         raise
