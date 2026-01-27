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

        LAG(sp.close, 30)  OVER w AS close_30,
        LAG(sp.close, 60)  OVER w AS close_60,
        LAG(sp.close, 90)  OVER w AS close_90,
        LAG(sp.close, 180) OVER w AS close_180,

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
    perf_30td,
    perf_60td,
    perf_90td,
    perf_180td,
    as_of_date
)
SELECT
    stock_meta_sk,
    symbol,
    sector,
    sub_industry,
    close AS latest_close,
    (close / close_30) - 1 AS perf_30td,
    (close / close_60) - 1 AS perf_60td,
    (close / close_90) - 1 AS perf_90td,
    (close / close_180) - 1 AS perf_180td,
    date

FROM priced
WHERE rn = 1
  AND close_30 IS NOT NULL;
