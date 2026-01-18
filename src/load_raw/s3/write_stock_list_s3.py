"""
Create json file with list of stock symbol, 
full name and industry
"""

import pandas as pd
import json
import requests
from pathlib import Path
from datetime import datetime, timezone, date
from src.utils.custom_exceptions import *
from src.utils.s3config import s3_bucket, client
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

# Wiki url

URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

def fetch_symbol_meta() -> list[dict]:

    """
    Scrapes Wikipedia's S&P 500 constituent table and returns
    a list of ticker names and sector.
    """

    symbol_meta_list = []

    try:
        response = requests.get(URL, headers=HEADERS, timeout=30)
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        raise APIError(f"HTTP error fetching stock details: {e}") from e


    # Read all tables on the page
    tables = pd.read_html(response.text)
    # The first table on the page contains the S&P 500 list
    df = tables[0]

    for row in df.itertuples(index=False):

        try:
            symbol_name = str(row[0]).replace(".", "-") #API needs - not .
            symbol_meta = {
                "symbol": symbol_name,
                "name": row[1],
                "sector": row[2],
                "sub_industry": row[3],
                "headquarters": row[4],
                "CIK": row[6]
            }

        except ParsingError as e:
            print(f"[ERROR] Unable to parse symbol data for: {symbol_name} : {e}")
            continue


        symbol_meta_list.append(symbol_meta)

    return symbol_meta_list

def payload_meta(data: list[dict]) -> dict:

    payload = {
        "domain": "sp500",
        "source": URL,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "data": data
    }

    return payload

def write_symbol_data_to_s3():

    symbol_list = fetch_symbol_meta()
    payload = payload_meta(data=symbol_list)
    key = (f"raw/stocks/stock_lists/domain={payload["domain"]}/stock_list_{date.today().isoformat()}.json")

    try:
        client.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=json.dumps(payload, indent=2),
            ContentType="application/json"
        )

        print(f"[OK] historical data written to s3://{s3_bucket}/{key}")

    except NoCredentialsError as exc:
        raise RuntimeError("AWS credentials not configured") from exc
    
    except EndpointConnectionError as exc:
        raise RuntimeError("Unable to reach S3") from exc
    
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        raise RuntimeError(f"S3 put_object failed ({error_code}) for {s3_bucket}/{key}") from exc


def main():
    write_symbol_data_to_s3()

if __name__ == "__main__":
    main()