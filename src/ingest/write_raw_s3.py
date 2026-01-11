"""
Connect to EODHD API and retrieve historical data

"""

import os
import requests
import json
from dotenv import load_dotenv
from pathlib import Path
from datetime import timezone, datetime

##########################################
# Load environment variables
##########################################

load_dotenv()

#-------------------------------------
#API Details
#-------------------------------------

api_key = os.getenv("EOD_APIKEY")

URL = f"https://eodhd.com/api/eod/MCD.US?api_token={api_key}&fmt=json"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_LAKE = PROJECT_ROOT / "data_lake"

data = requests.get(URL).json()

def write_historical(symbol: str, api_response: list[dict], base_path: Path, domain:str="sp500", source: str="https://eodhd.com/api/eod/"):

    output_dir = (base_path/"raw"/"stocks"/"daily"/"historical"/f"{domain}"/f"{symbol}")

    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{symbol}_eod_history"

    payload = {
        "symbol": symbol,
        "domain": domain,
        "source": source,
        "ingestion_type": "historical",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "data": api_response
    }

    with output_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"[historical] written {output_file}")

def main():
    write_historical("AMD", data, base_path=DATA_LAKE)

if __name__ == "__main__":
    main()