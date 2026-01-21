-- =========================================================
-- Stock ETL - Curated Historical
-- =========================================================

-- =========================================================
-- Dimension Stock
-- =========================================================

INSERT INTO curated.dim_stock_meta (
    symbol,
    name,
    sector,
    sub_industry,
    cik,
    domain
)

SELECT DISTINCT 
    sm.symbol,
    sm.name,
    sm.sector,
    sm.sub_industry,
    sm.cik,
    sm.domain

FROM staging.stocks_meta sm
ON CONFLICT (symbol, cik) DO NOTHING;

-- =========================================================
-- Dimension Trade Date
-- =========================================================

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

-- =========================================================
-- Fact Stock Price
-- =========================================================
-- EXPLAIN - Testing efficiency of query

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

ON CONFLICT ON CONSTRAINT fact_stock_grain DO NOTHING;