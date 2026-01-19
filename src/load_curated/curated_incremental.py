"""
Extract and load incrimental data from staging into curated
"""

from src.utils.db import *
from src.utils.custom_exceptions import *

# --------------------------------------------------
# SQL
# --------------------------------------------------


INSERT = """
INSERT INTO curated.fact_stock_prices (
    symbol_sk,
    trade_date_sk,
    open,
    high,
    low,
    close,
    adjusted_close,
    volume
)

SELECT
    sm.stock_meta_sk,
    td.date_sk,
    sp.open,
    sp.high,
    sp.low,
    sp.close,
    sp.adjusted_close,
    sp.volume
FROM staging.stocks as sp
JOIN curated.dim_stock_meta as sm
    ON sp.symbol = sm.symbol
JOIN curated.dim_trade_date as td
    ON sp.trade_date = td.date
WHERE sp.trade_date > 
    (
        SELECT MAX(td2.date)
        FROM curated.fact_stock_prices as sp2
        JOIN curated.dim_trade_date as td2
        ON sp2.trade_date_sk = td2.date_sk
    )

ON CONFLICT ON CONSTRAINT fact_stock_grain DO NOTHING
RETURNING 1;

"""

# --------------------------------------------------
# Load curated 
# --------------------------------------------------

def load_curated_incremental():

    try:

        rows = execute_with_rowcount(INSERT)

        print(f"[INSERTED] {rows} into curated inremental data")

    except SQLError as e:

        raise RuntimeError(f"[REJECTED] curated incremental load: {e}")
    

# --------------------------------------------------
# Entry point
# --------------------------------------------------

def main():

    load_curated_incremental()


if __name__ == "__main__":
    main()