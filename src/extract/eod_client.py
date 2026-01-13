"""
API client for EODHD
"""

import os
import requests
from dotenv import load_dotenv
from utils.custom_exceptions import ConfigError, ValidationError, APIError

# -------------------------------------
# Load environment variables
# -------------------------------------

load_dotenv()

# -------------------------------------
# API / EODHD config
# -------------------------------------

api_key = os.getenv("EOD_APIKEY")
if not api_key:
    raise ConfigError("API key not set in environment")

eod_url = "https://eodhd.com/api/eod-bulk-last-day/US"

# -------------------------------------
# Fetch incremental data
# -------------------------------------

def fetch_incremental(symbols: list[str]) -> list[dict]:

    joined_symbols = ",".join(symbols)

    params = {
        "api_token": api_key,
        "symbols": joined_symbols,
        "fmt": "json"
    }

    try:
        response = requests.get(eod_url, params=params, timeout=15)
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