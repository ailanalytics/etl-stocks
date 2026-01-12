"""
Connect to EODHD API and retrieve historical data
Write payloads to S3 bucket

"""

import os
import requests
import json
import boto3
from dotenv import load_dotenv
from pathlib import Path
from datetime import timezone, datetime

# -------------------------------------
# Custom Exceptions
# -------------------------------------

class ConfigError(Exception):
    pass

class APIError(Exception):
    pass

class ValidationError(Exception):
    pass

#-------------------------------------
# Load environment variables
#-------------------------------------

load_dotenv()

#-------------------------------------
# Config Path
#-------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
config_path = (PROJECT_ROOT / "config" / "domains" / "sp500_current" / "latest.json")

#-------------------------------------
#API Details
#-------------------------------------

api_key = os.getenv("EOD_APIKEY")
if not api_key:
    raise ConfigError("API key not set in environment")

s3_bucket = os.getenv("S3_RAW_BUCKET")
if not s3_bucket:
    raise ConfigError("S3 Bucket not set")

aws_region = os.getenv("AWS_REGION")

client = boto3.client("s3")

# -------------------------------------
# Fetch historical data
# -------------------------------------

def fetch_historical(symbol: str) -> list[dict]:

    url = f"https://eodhd.com/api/eod/{symbol}.US"
    params = {
        "api_token": api_key,
        "fmt": "json"
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

    except requests.exceptions.RequestException as e:
        raise APIError(f"HTTP error for {symbol}: {e}") from e
    
    except ValueError as e:
        raise APIError(f"Invalid json returned for {symbol}") from e
    
    if not isinstance(data, list):
        raise ValidationError(f"Unexpected payload structure for {symbol}")
    
    if not data:
        raise ValidationError(f"No data returned for {symbol}")
    
    return data

# -------------------------------------
# Write historical data to S3
# -------------------------------------

def write_historical(symbol: str, api_response: list[dict], domain:str="sp500", source: str="https://eodhd.com/api/eod/"):

    """
    Docstring for write_historical
    
    :param symbol: selected stock symbol
    :type symbol: str
    :param api_response: response payload
    :type api_response: list[dict]
    :param base_path: project root dir, local data_lake
    :type base_path: Path
    :param domain: Stock domain eg sp500
    :type domain: str
    :param source: API Source
    :type source: str
    """

    key = (f"raw/stocks/daily/historical/domain={domain}/symbol={symbol}/eod_history.json")

    payload = {
        "symbol": symbol,
        "domain": domain,
        "source": source,
        "ingestion_type": "historical",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "data": api_response
    }

    client.put_object(
        Bucket=s3_bucket,
        Key=key,
        Body=json.dumps(payload, indent=2),
        ContentType="application/json"
    )

    print(f"[OK] historical data written to s3://{s3_bucket}/{key}")

def get_historical_data():

    try:
        with config_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
            symbol_list = payload["symbols"]

    except Exception as e:
        raise ConfigError(f"Failed to load symbol config: {e}") from e

    for symbol in symbol_list:

        try: 
            data = fetch_historical(symbol)
            write_historical(symbol, data)

        except (APIError, ValidationError) as e:
            print(f"[WARN] {symbol} skipped: {e}")
            continue

        except Exception as e:
            print(f"[ERROR] Unexpected failure for {symbol}: {e}")
            continue

def main():
    get_historical_data()

if __name__ == "__main__":
    main()