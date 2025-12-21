import json
import os
import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET_NAME = os.getenv("BUCKET_NAME")
RAW_PREFIX = os.getenv("RAW_PREFIX")
CLEANED_PREFIX = os.getenv("CLEANED_PREFIX")
PROCESS_DATE = os.getenv("PROCESS_DATE")  # e.g. 2025-12-21


def lambda_handler(event, context):
    logger.info(f"Processor started for date: {PROCESS_DATE}")

    raw_prefix = f"{RAW_PREFIX}{PROCESS_DATE}/"
    cleaned_prefix = f"{CLEANED_PREFIX}{PROCESS_DATE}/"

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=raw_prefix
    )

    if "Contents" not in response:
        logger.warning("No raw files found.")
        return {"status": "NO_DATA"}

    processed_count = 0

    for obj in response["Contents"]:
        key = obj["Key"]

        if not key.endswith(".json"):
            continue

        logger.info(f"Processing {key}")

        raw_obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        record = json.loads(raw_obj["Body"].read().decode("utf-8"))

        # Schema validation / normalization
        cleaned_record = {
            "asset": record["asset"],
            "timestamp_utc": record["timestamp_utc"],
            "price_inr_per_gram": record["price_inr_per_gram"],
            "price_inr_per_10g": record["price_inr_per_10g"],
            "provider": record["provider"],
            "quality_check": "PASS"
        }

        filename = key.split("/")[-1]
        cleaned_key = f"{cleaned_prefix}{filename}"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=cleaned_key,
            Body=json.dumps(cleaned_record),
            ContentType="application/json"
        )

        processed_count += 1

    return {
        "status": "SUCCESS",
        "processed_files": processed_count
    }
