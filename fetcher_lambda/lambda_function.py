import json
import os
import boto3
import logging
import urllib3
from datetime import datetime, timezone
from decimal import Decimal

# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -----------------------------
# AWS & HTTP clients
# -----------------------------
http = urllib3.PoolManager()
s3 = boto3.client("s3")

# -----------------------------
# Environment variables
# -----------------------------
BUCKET_NAME = os.getenv("BUCKET_NAME")
RAW_PREFIX = os.getenv("RAW_PREFIX")
ASSET = os.getenv("ASSET", "gold")
API_KEY = os.getenv("TWELVEDATA_API_KEY")

# -----------------------------
# TwelveData endpoints (stable)
# -----------------------------
XAU_URL = f"https://api.twelvedata.com/price?symbol=XAU/USD&apikey={API_KEY}"
USDINR_URL = f"https://api.twelvedata.com/price?symbol=USD/INR&apikey={API_KEY}"

# -----------------------------
# Helper function
# -----------------------------
def call_api(url: str) -> dict:
    response = http.request("GET", url, timeout=10.0)
    if response.status != 200:
        raise Exception(f"API Error {response.status} for URL: {url}")
    return json.loads(response.data.decode("utf-8"))

# -----------------------------
# Lambda handler
# -----------------------------
def lambda_handler(event, context):
    logger.info("Project 8 Fetcher Lambda triggered")

    try:
        # Current UTC time
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        date = now.strftime("%Y-%m-%d")
        time = now.strftime("%H-%M")

        # Fetch prices
        logger.info("Fetching XAU/USD price")
        xau_data = call_api(XAU_URL)

        logger.info("Fetching USD/INR price")
        inr_data = call_api(USDINR_URL)

        xau_usd = Decimal(xau_data["price"])
        usd_inr = Decimal(inr_data["price"])

        # Convert to INR per gram
        price_inr_per_gram = (xau_usd * usd_inr) / Decimal("31.1035")
        price_inr_per_10g = price_inr_per_gram * Decimal("10")

        # Build raw record
        record = {
            "asset": ASSET,
            "timestamp_utc": timestamp,
            "xau_usd": float(xau_usd),
            "usd_inr": float(usd_inr),
            "price_inr_per_gram": round(float(price_inr_per_gram), 4),
            "price_inr_per_10g": round(float(price_inr_per_10g), 4),
            "date": date,
            "hour": now.strftime("%H"),
            "minute": now.strftime("%M"),
            "provider": "twelvedata"
        }

        # Time-partitioned S3 key (append-only)
        s3_key = f"{RAW_PREFIX}{date}/{ASSET}_{time}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(record),
            ContentType="application/json"
        )

        logger.info(f"Saved raw snapshot → s3://{BUCKET_NAME}/{s3_key}")

        return {
            "status": "SUCCESS",
            "saved_file": s3_key,
            "record": record
        }

    except Exception as e:
        logger.error(f"Project 8 Fetcher failed: {str(e)}", exc_info=True)
        raise
