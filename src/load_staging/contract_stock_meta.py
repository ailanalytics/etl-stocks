"""
Contract for validating stock meta data json payload
Ensuring fields are present and casting types
"""

from datetime import datetime, timezone, date
from decimal import ROUND_HALF_UP, Decimal #For converting timestamp
from src.utils.custom_exceptions import *
from src.utils.string_tools import *

# --------------------------------------------------
# Required JSON elements, else raise exception
# --------------------------------------------------

required_fields = [
    "domain",
    "source",
    "ingested_at"
] 

required_data = [
    "symbol",
    "name",
    "sector",
    "sub_industry",
    "cik"
]

# --------------------------------------------------
# Validate JSON Normalise, Type
# --------------------------------------------------

def validate_symbol_metadata(payload: dict, data: dict) -> dict:
    
    for field in required_fields:
        if field not in payload:
            raise ValueError(f"Missing payload field {field}")
        
    for field in required_data:
        if field not in data:
            raise ValueError(f"Missing stock meta data field: {field}")
    
    name = normalize_label(data["name"])
    sector = normalize_label(data["sector"])
    sub_industry = normalize_label(data["sub_industry"])
    cik = int(data["CIK"])
        
    return {
        "symbol": data["symbol"],
        "name": name,
        "sector": sector,
        "sub_industry": sub_industry,
        "cik": cik,
        "domain": payload["domain"],
        "source": payload["source"]
    }