"""
Extract and load incrimental data from staging into curated
"""

from src.utils.db import *
from src.utils.custom_exceptions import *

# --------------------------------------------------
# SQL
# --------------------------------------------------

INSERT_DATES = """
INSERT INTO curated.dim_trade_date (
    date,
    day,
    month,
    year,
    day_of_week
)

SELECT DISTINCT 
    td.trade_date,
    EXTRACT(day FROM td.trade_date),
    EXTRACT(month FROM td.trade_date),
    EXTRACT(year FROM td.trade_date),
    EXTRACT(ISODOW FROM td.trade_date)::INT

FROM staging.stocks td
ON CONFLICT (date) DO NOTHING;

"""

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

    """
    Execute SQL to extract incremental data from staging
    and load into curated
    """

    try:

        dates = execute_with_rowcount(INSERT_DATES)

        data = execute_with_rowcount(INSERT)

        print(f"[INSERTED] {dates} into curated dim dates incremental data")

        print(f"[INSERTED] {data} into curated fact stock price incremental data")

    except SQLError as e:

        raise RuntimeError(f"[REJECTED] curated incremental load: {e}")
    
# --------------------------------------------------
# Entry point
# --------------------------------------------------

def main():
    load_curated_incremental()

if __name__ == "__main__":
    main()