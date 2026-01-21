"""
Contract for validating historical EOD json payload
Ensuring fields are present and casting types
"""

from datetime import datetime, timezone, date
from decimal import ROUND_HALF_UP, Decimal #For converting timestamp
from src.utils.custom_exceptions import *

# --------------------------------------------------
# Required JSON elements, else raise exception
# --------------------------------------------------

required_fields = [
    "symbol",
    "domain",
    "source",
    "ingestion_type",
    "ingested_at"
] 

required_data = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume"
]

# --------------------------------------------------
# Validate JSON Normalise, Type
# --------------------------------------------------

def validate_historical_data(payload: dict, data: dict) -> dict:

    """
    Validates historical payload and data
    Enforces Types
    
    :param payload: Payload meta to validate
    :type payload: dict
    :param data: EOD data
    :type data: dict
    :return: Validated data
    :rtype: dict
    """
    
    for field in required_fields:
        if field not in payload:
            raise ValueError(f"Missing payload field {field}")
        
    if payload["ingestion_type"] != "historical":
        raise ValueError("EOD data not historical")
        
    for field in required_data:
        if field not in data:
            raise ValueError(f"Missing EOD data field: {field}")
        
    open_price = Decimal(str(data["open"])).quantize(
        Decimal("0.0001"),
        rounding=ROUND_HALF_UP
    )

    high_price = Decimal(str(data["high"])).quantize(
        Decimal("0.0001"),
        rounding=ROUND_HALF_UP
    )

    low_price = Decimal(str(data["low"])).quantize(
        Decimal("0.0001"),
        rounding=ROUND_HALF_UP
    )

    close_price = Decimal(str(data["close"])).quantize(
        Decimal("0.0001"),
        rounding=ROUND_HALF_UP
    )

    adjusted_close = Decimal(str(data["adjusted_close"])).quantize(
        Decimal("0.0001"),
        rounding=ROUND_HALF_UP
    )

    volume = int(data["volume"])
    if volume < 0:
        raise ValueError("Negative volume")
        
    return {
        "symbol": payload["symbol"],
        "domain": payload["domain"],
        "source": payload["source"],
        "ingestion_type": "historical",
        "ingested_at": datetime.fromisoformat(payload["ingested_at"]).replace(tzinfo=timezone.utc),
        "trade_date": date.fromisoformat(data["date"]),
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "adjusted_close": adjusted_close,
        "volume": volume
    }