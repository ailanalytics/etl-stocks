"""
Extract and load EOD historical from s3 raw to staging
"""

import json
import sys
from src.utils.s3config import s3_bucket, client
from src.utils.get_sp500_tickers import get_symbols
from src.load_staging.contract_historical import validate_historical_data
from src.utils.db import execute
from src.utils.custom_exceptions import *

# --------------------------------------------------
# SQL
# --------------------------------------------------

INSERT = """

INSERT INTO staging.stocks (
    symbol,
    domain,
    source,
    ingestion_type,
    ingested_at,
    trade_date,
    open,
    high,
    low,
    close,
    adjusted_close,
    volume
) VALUES (
    %(symbol)s,
    %(domain)s,
    %(source)s,
    %(ingestion_type)s,
    %(ingested_at)s,
    %(trade_date)s,
    %(open)s,
    %(high)s,
    %(low)s,
    %(close)s,
    %(adjusted_close)s,
    %(volume)s
)
ON CONFLICT (symbol, trade_date) DO NOTHING;

"""

# --------------------------------------------------
# Load EOD Historical into staging
# --------------------------------------------------

def load_staging_historical():

    """
    Inserts EOD historical staging data into db
    """

    symbols = get_symbols()

    for symbol in symbols:
        key = (f"raw/stocks/daily/historical/domain=sp500/symbol={symbol}/eod_history.json")
        request = client.get_object(
            Bucket=s3_bucket,
            Key=key
        )

        raw = request["Body"].read()
    
        payload = json.loads(raw)

        data = payload["data"] 

        meta = {k: v for k, v in payload.items() if k != "data"}

        for candle in data:
            try:
                grain = validate_historical_data(meta, candle)

                execute(INSERT, grain)

            except RuntimeError as e:
                print(f"[REJECTED] candle: {candle}: {e}")

# --------------------------------------------------
# Entrypoint
# --------------------------------------------------

def main():
    load_staging_historical()

if __name__ == "__main__":
    main()