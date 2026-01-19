"""
Extract and load EOD incremental from s3 raw to staging
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from src.utils.s3config import s3_bucket, client
from src.load_staging.contract_incremental import validate_incremental_data
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
# Load EOD Incremental into staging
# --------------------------------------------------

def load_staging_incremental():

    eod_date = datetime.now(timezone.utc).date() - timedelta(days=1)

    key = (f"raw/stocks/daily/incremental/domain=sp500/date={eod_date.isoformat()}/eod_incremental.json")
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
            grain = validate_incremental_data(meta, candle)

            execute(INSERT, grain)

            print(f"[INSERTED] candle for: {candle["code"]}")

            # sys.exit()

        except SQLError as e:
            print(f"[REJECTED] candle: {candle}: {e}")

# --------------------------------------------------
# Entrypoint
# --------------------------------------------------

if __name__ == "__main__":
    load_staging_incremental()