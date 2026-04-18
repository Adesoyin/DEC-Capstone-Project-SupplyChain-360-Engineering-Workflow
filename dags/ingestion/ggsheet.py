
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
from io import BytesIO
import logging
import os
from datetime import datetime as dt
from dotenv import load_dotenv


load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def ingest_google_sheet_to_s3():

    spreadsheet_id = os.getenv("spreadsheet_id")
    worksheet_name = os.getenv("worksheet_name")
    destination_bucket = os.getenv("destination_bucket")
    destination_key = os.getenv("destination_key")
    creds_path = os.getenv("GOOGLE_CREDS_PATH")

    try:
        #AUTHENTICATION
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name(
            creds_path, scope
        )

        client = gspread.authorize(creds)

        #OPEN SHEET BY ID (BEST PRACTICE)
        sheet = client.open_by_key(spreadsheet_id)

        worksheet = sheet.worksheet(worksheet_name)

        data = worksheet.get_all_records()

        df = pd.DataFrame(data)

        if df.empty:
            logger.warning("Google Sheet returned empty data")
            return

        # METADATA
        df["ingested_at"] = dt.now().isoformat()
        df["source"] = "google_sheets"

        # Parquet
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)

        s3_dest = boto3.client("s3",
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
        region_name=os.getenv("AWS_REGION")

        )

        s3_dest.put_object(
            Bucket=destination_bucket,
            Key=destination_key,
            Body=buffer.getvalue()
        )

        logger.info(f"Loaded Google Sheet → s3://{destination_bucket}/{destination_key}")

    except Exception as e:
        logger.error(f"Error ingesting Google Sheet: {str(e)}")
        raise
sts = boto3.client("sts")
print("AWS IDENTITY:", sts.get_caller_identity())


if __name__ == "__main__":
    spreadsheet_id = os.getenv("spreadsheet_id")
    worksheet_name = os.getenv("worksheet_name")
    destination_bucket = os.getenv("destination_bucket")
    destination_key = os.getenv("destination_key")
    creds_path = os.getenv("GOOGLE_CREDS_PATH")
    print("✅ Starting google sheet store location ingestion...")
    # Validate env vars
    if not all([spreadsheet_id,
    worksheet_name,
    destination_bucket,
    destination_key,creds_path]):
        raise ValueError("Missing environment variables")

    ingest_google_sheet_to_s3(
        # spreadsheet_id=spreadsheet_id,
        # worksheet_name=worksheet_name,
        # destination_bucket=destination_bucket,
        # destination_key=destination_key,
        # creds_path=creds_path
    )

    logger.info("Success ingesting data into {destination_bucket}")
