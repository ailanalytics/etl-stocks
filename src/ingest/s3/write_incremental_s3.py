"""
Connect to EODHD API and retrieve incremental data
Write payloads to S3 bucket
"""

import os
import json
import requests
import boto3
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timezone, timedelta

# -------------------------------------
# Custom Exceptions
# -------------------------------------

class ConfigError(Exception):
    pass

class APIError(Exception):
    pass

class ValidationError(Exception):
    pass

# -------------------------------------
# Load environment variables
# -------------------------------------

load_dotenv()

# -------------------------------------
# Config Path
# -------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]
config_path = PROJECT_ROOT / "config" / "domains" / "sp500_current" / "latest.json"

# -------------------------------------
# API / AWS config
# -------------------------------------

api_key = os.getenv("EOD_APIKEY")
if not api_key:
    raise ConfigError("API key not set in environment")

s3_bucket = os.getenv("S3_RAW_BUCKET")
if not s3_bucket:
    raise ConfigError("S3 bucket not set in environment")

aws_region = os.getenv("AWS_REGION")

client = boto3.client(
    "s3",
    region_name=aws_region
)

# -------------------------------------
# Fetch incremental data
# -------------------------------------

def fetch_incremental(symbols: list[str]) -> list[dict]:
    joined_symbols = ",".join(symbols)

    url = "https://eodhd.com/api/eod-bulk-last-day/US"
    params = {
        "api_token": api_key,
        "symbols": joined_symbols,
        "fmt": "json"
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        raise APIError(f"HTTP error fetching bulk data: {e}") from e
    except ValueError as e:
        raise APIError("Invalid JSON returned from API") from e

    if not isinstance(data, list) or not data:
        raise ValidationError("Unexpected or empty API payload")

    return data

# -------------------------------------
# S3 helpers
# -------------------------------------

def s3_key_exists(bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise

# -------------------------------------
# Write incremental data to S3
# -------------------------------------

def write_incremental(
    api_response: list[dict],
    domain: str = "sp500",
    source: str = "https://eodhd.com/api/eod-bulk-last-day/US"
    ):

    eod_date = datetime.now(timezone.utc).date() - timedelta(days=1)

    key = (
        f"raw/stocks/daily/incremental/"
        f"domain={domain}/"
        f"date={eod_date.isoformat()}/"
        f"eod_incremental.json"
    )

    if s3_key_exists(s3_bucket, key):
        print(f"[SKIP] Incremental data already exists for {eod_date}")
        return

    payload = {
        "eod_date": eod_date.isoformat(),
        "domain": domain,
        "source": source,
        "ingestion_type": "incremental",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "data": api_response
    }

    client.put_object(
        Bucket=s3_bucket,
        Key=key,
        Body=json.dumps(payload, indent=2),
        ContentType="application/json"
    )

    print(f"[OK] {len(api_response)} records written to s3://{s3_bucket}/{key}")

# -------------------------------------
# Orchestration
# -------------------------------------

def get_incremental_data():
    try:
        with config_path.open("r", encoding="utf-8") as f:
            config = json.load(f)
            symbols = config["symbols"]
    except Exception as e:
        raise ConfigError(f"Failed to load symbol config: {e}") from e

    data = fetch_incremental(symbols)
    write_incremental(data)

# -------------------------------------
# Entry point
# -------------------------------------

def main():
    weekday = datetime.now(timezone.utc).weekday()
    # Monday=0, Sunday=6
    if weekday in (0, 6):
        print("[SKIP] Market closed (Sunday/Monday)")
        return

    get_incremental_data()

if __name__ == "__main__":
    main()
