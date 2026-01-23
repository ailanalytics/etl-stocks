-- =========================================================
-- SQL Insert Mart Performance Snapshot
-- =========================================================

WITH priced AS (
    SELECT
        sm.stock_meta_sk,
        sm.symbol,
        sm.sector,
        sm.sub_industry,
        sp.close,
        td.date,
        td.date_sk,

        LAG(sp.close, 30)  OVER w AS close_30d,
        LAG(sp.close, 60)  OVER w AS close_60d,
        LAG(sp.close, 90)  OVER w AS close_90d,
        LAG(sp.close, 252) OVER w AS close_12m,

        ROW_NUMBER() OVER w_desc AS rn
    FROM curated.fact_stock_prices sp
    JOIN curated.dim_stock_meta sm
        ON sp.symbol_sk = sm.stock_meta_sk
    JOIN curated.dim_trade_date td
        ON sp.trade_date_sk = td.date_sk

    WINDOW
        w AS (
            PARTITION BY sp.symbol_sk
            ORDER BY td.date
        ),
        w_desc AS (
            PARTITION BY sp.symbol_sk
            ORDER BY td.date DESC
        )
)

INSERT INTO mart.stock_perf_current (
    stock_meta_sk,
    symbol,
    sector,
    industry,
    latest_close,
    perf_30d,
    perf_60d,
    perf_90d,
    perf_12m,
    as_of_date
)
SELECT
    stock_meta_sk,
    symbol,
    sector,
    sub_industry,
    close AS latest_close,
    (close / close_30d) - 1 AS perf_30d,
    (close / close_60d) - 1 AS perf_60d,
    (close / close_90d) - 1 AS perf_90d,
    (close / close_12m) - 1 AS perf_12m,
    date

FROM priced
WHERE rn = 1
  AND close_30d IS NOT NULL;
