"""
Connect to EODHD API and retrieve historical data
"""

import os
import requests
import json
from dotenv import load_dotenv
from pathlib import Path
from datetime import timezone, datetime
from src.utils.custom_exceptions import *

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------

load_dotenv()

# --------------------------------------------------
# API Details
# --------------------------------------------------

api_key = os.getenv("EOD_APIKEY")
if not api_key:
    raise ConfigError("API key not set in environment")

# --------------------------------------------------
# Paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE = PROJECT_ROOT / "data_lake"

# --------------------------------------------------
# Fetch historical data
# --------------------------------------------------

def fetch_historical(symbol: str) -> list[dict]:

    """
    Fetch historical EOD Data from EODHD
    
    :param symbol: Symbol to query
    :type symbol: str
    :return: EOD json for symbol
    :rtype: list[dict]
    """

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

# --------------------------------------------------
# Write historical data
# --------------------------------------------------

def write_historical(symbol: str, api_response: list[dict], base_path: Path, domain:str="sp500", source: str="https://eodhd.com/api/eod/"):

    """
    Write historical json data to local storage
    
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

    output_dir = (base_path/"raw"/"stocks"/"daily"/"historical"/domain/symbol)

    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "eod_history.json"
    temp_file = output_dir / "eod_history.tmp"

    payload = {
        "symbol": symbol,
        "domain": domain,
        "source": source,
        "ingestion_type": "historical",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "data": api_response
    }

    with temp_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    temp_file.replace(output_file)

    print(f"historical data written to {output_file} for {symbol}")

# --------------------------------------------------
# Load symbols and extract/load historical data
# --------------------------------------------------

def get_historical_data():

    """
    Load symbols and fetch historical data
    for each symbol
    """

    config_path = (PROJECT_ROOT / "config" / "domains" / "sp500_current" / "latest.json")

    try:
        with config_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
            symbol_list = payload["symbols"]

    except Exception as e:
        raise ConfigError(f"Failed to load symbol config: {e}") from e

    for symbol in symbol_list:

        try: 
            data = fetch_historical(symbol)
            write_historical(symbol, data, base_path=DATA_LAKE)

        except (APIError, ValidationError) as e:
            print(f"[WARN] {symbol} skipped: {e}")
            continue
        except Exception as e:
            print(f"[ERROR] Unexpected failure for {symbol}: {e}")
            continue

# --------------------------------------------------
# Entry point
# --------------------------------------------------

def main():
    get_historical_data()

if __name__ == "__main__":
    main()