import json
from pathlib import Path
from src.utils.custom_exceptions import ConfigError

# -------------------------------------
# Config Path
# -------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
config_path = PROJECT_ROOT / "config" / "domains" / "sp500_current" / "latest.json"

def get_symbols() -> list[str]:
    try:
        with config_path.open("r", encoding="utf-8") as f:
            config = json.load(f)
            symbols = config["symbols"]
    except Exception as e:
        raise ConfigError(f"Failed to load symbol config: {e}") from e
    
    return symbols