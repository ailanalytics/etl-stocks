"""
Connect to EODHD API and retrieve incremental data
Write payloads to S3 bucket
"""
import json
from datetime import datetime, timezone, timedelta
from src.extract import eod_client
from src.load_raw import s3config as s3
from src.utils import get_sp500_tickers as get_ticker

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

    if s3.s3_key_exists(s3.s3_bucket, key):
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

    s3.client.put_object(
        Bucket=s3.s3_bucket,
        Key=key,
        Body=json.dumps(payload, indent=2),
        ContentType="application/json"
    )

    print(f"[OK] {len(api_response)} records written to s3://{s3.s3_bucket}/{key}")

# -------------------------------------
# Orchestration
# -------------------------------------

def get_incremental_data():
    
    symbols = get_ticker.get_symbols()

    data = eod_client.fetch_incremental(symbols)
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
