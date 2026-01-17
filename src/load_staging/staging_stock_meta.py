"""
Extract and load stock meta data from s3 raw to staging
"""

import json
from src.utils.s3config import s3_bucket, client
from src.load_staging.contract_stock_meta import validate_symbol_metadata
from src.utils.db import execute
from src.utils.custom_exceptions import *

# --------------------------------------------------
# SQL
# --------------------------------------------------

INSERT = """

INSERT INTO staging.stocks_meta (
    symbol,
    name,
    sector,
    sub_industry,
    cik,
    domain,
    source,
    ingested_at
) VALUES (
    %(symbol)s,
    %(name)s,
    %(sector)s,
    %(sub_industry)s,
    %(cik)s,
    %(domain)s,
    %(source)s,
    %(ingested_at)s
)
ON CONFLICT (symbol, cik) DO NOTHING;

"""

# --------------------------------------------------
# Load stock meta into staging
# --------------------------------------------------

def load_stock_meta():

    key = (f"raw/stocks/stock_lists/domain=sp500/stock_list_2026-01-17.json")

    request = client.get_object(
        Bucket=s3_bucket,
        Key=key
    )

    raw = request["Body"].read()

    payload = json.loads(raw)

    data = payload["data"] 

    meta = {k: v for k, v in payload.items() if k != "data"}

    for stock in data:

        try:
            grain = validate_symbol_metadata(meta, stock)

            execute(INSERT, grain)

        except StagingError as e:
            print(f"[REJECTED] stock: {stock["symbol"]}: {e}")

    print(f"[OK] Stocks inserted into staging.stocks_meta")

# --------------------------------------------------
# Entrypoint
# --------------------------------------------------

if __name__ == "__main__":
    load_stock_meta()