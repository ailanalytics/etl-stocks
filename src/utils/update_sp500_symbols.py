"""
Creates a json file containing upto date S&P500 tickers

"""

import pandas as pd
import json
import requests
from pathlib import Path
from datetime import datetime

# --------------------------------------------------
# Wiki URL
# --------------------------------------------------

URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# --------------------------------------------------
# Fetch latest sp500 symbols
# --------------------------------------------------

def fetch_sp500() -> list[str]:
    """
    Scrapes Wikipedia's S&P 500 constituent table and returns
    a list of ticker symbols (strings).
    """

    response = requests.get(URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    tables = pd.read_html(response.text)
    df = tables[0]

    symbols = (
        df["Symbol"]
        .str.replace(".", "-", regex=False)
        .tolist()
    )
    return symbols

# --------------------------------------------------
# Saves latest version to local
# --------------------------------------------------

def save_version(symbols: list[str], config_dir: Path):
    """
    Saves the symbol list as a dated versioned JSON and
    also writes a latest.json.
    """
    config_dir.mkdir(parents=True, exist_ok=True)

    date_retrieved = datetime.now().date().isoformat()
    versioned_name = f"{date_retrieved}.json"
    versioned_path = config_dir / versioned_name

    with versioned_path.open("w", encoding="utf-8") as f:
        json.dump({
            "domain": "sp500_current",
            "date": date_retrieved,
            "symbols": symbols
        }, f, indent=2)

    latest_path = config_dir / "latest.json"
    with latest_path.open("w", encoding="utf-8") as f:
        json.dump({
            "domain": "sp500_current",
            "date": date_retrieved,
            "symbols": symbols
        }, f, indent=2)

    print(f"[ok] saved {versioned_path} and updated latest.json")

# --------------------------------------------------
# Entry Point
# --------------------------------------------------

def main():
    config_dir = Path("config/domains/sp500_current")

    print("fetching current S&P 500 list from Wikipedia …")
    symbols = fetch_sp500()
    print(f"retrieved {len(symbols)} symbols")

    save_version(symbols, config_dir)

if __name__ == "__main__":
    main()
