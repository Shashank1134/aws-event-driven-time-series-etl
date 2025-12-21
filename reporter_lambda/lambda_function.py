import json
import os
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET_NAME = os.getenv("BUCKET_NAME")
CLEANED_PREFIX = os.getenv("CLEANED_PREFIX")
REPORTS_PREFIX = os.getenv("REPORTS_PREFIX")
REPORT_DATE = os.getenv("REPORT_DATE")  # e.g. 2025-12-21


def lambda_handler(event, context):
    logger.info(f"Reporter started for {REPORT_DATE}")

    cleaned_prefix = f"{CLEANED_PREFIX}{REPORT_DATE}/"
    reports_prefix = f"{REPORTS_PREFIX}{REPORT_DATE}/"

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=cleaned_prefix
    )

    if "Contents" not in response:
        raise Exception("No cleaned data found for report")

    prices = []

    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue

        data = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        record = json.loads(data["Body"].read().decode("utf-8"))
        prices.append(record["price_inr_per_10g"])

    prices.sort()

    open_price = prices[0]
    close_price = prices[-1]
    high_price = max(prices)
    low_price = min(prices)
    avg_price = round(sum(prices) / len(prices), 4)

    percent_change = round(
        ((close_price - open_price) / open_price) * 100, 4
    )

    if percent_change > 0:
        trend = "UP"
    elif percent_change < 0:
        trend = "DOWN"
    else:
        trend = "FLAT"

    report = {
        "asset": "gold",
        "date": REPORT_DATE,
        "observations": len(prices),
        "open_price": open_price,
        "close_price": close_price,
        "high_price": high_price,
        "low_price": low_price,
        "average_price": avg_price,
        "percent_change": percent_change,
        "trend": trend
    }

    report_key = f"{reports_prefix}daily_summary.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=report_key,
        Body=json.dumps(report, indent=2),
        ContentType="application/json"
    )

    logger.info(f"Report written → s3://{BUCKET_NAME}/{report_key}")

    return {
        "status": "SUCCESS",
        "report_key": report_key
    }
