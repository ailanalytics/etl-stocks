"""
Connect to EODHD API and retrieve historical data
Write payloads to S3 bucket

"""
import json
from extract.eod_client import fetch_historical
from datetime import timezone, datetime
from utils.custom_exceptions import ConfigError, ValidationError, APIError
from utils.get_sp500_tickers import get_symbols
from s3config import s3_bucket, client

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

    symbols = get_symbols()

    for symbol in symbols:

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