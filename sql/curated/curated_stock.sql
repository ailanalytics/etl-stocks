-- =========================================================
-- Stock ETL - Curated Schema
-- =========================================================

-- -----------------------
-- Create Schema
-- -----------------------

CREATE SCHEMA IF NOT EXISTS curated;

-- =========================================================
-- Dimension Tables
-- =========================================================

CREATE TABLE IF NOT EXISTS curated.dim_stock_meta (
    stock_meta_sk   BIGSERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL,
    name            TEXT NOT NULL,
    sector          TEXT NOT NULL,
    sub_industry    TEXT NOT NULL,
    cik             NUMERIC NOT NULL,
    domain          TEXT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT dim_stock_meta_symbol_cik
    UNIQUE (symbol, cik)
);

CREATE TABLE IF NOT EXISTS curated.dim_trade_date (
    date_sk         BIGSERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    day             INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    year            INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL, -- 1: Mon, 7: Sun
    created_at      TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT dim_trade_date_constraint
    UNIQUE (date)
);

-- =========================================================
-- Fact Table
-- =========================================================

CREATE TABLE IF NOT EXISTS curated.fact_stock_prices (
    stock_price_sk        BIGSERIAL PRIMARY KEY,
    symbol_sk       BIGINT NOT NULL REFERENCES curated.dim_stock_meta(stock_meta_sk),
    trade_date_sk   BIGINT NOT NULL REFERENCES curated.dim_trade_date(date_sk),
    open            NUMERIC(12,4) NOT NULL,
    high            NUMERIC(12,4) NOT NULL,
    low             NUMERIC(12,4) NOT NULL,
    close           NUMERIC(12,4) NOT NULL,
    adjusted_close  NUMERIC(12,4) NOT NULL,
    volume          BIGINT NOT NULL,

    CONSTRAINT fact_stock_grain
    UNIQUE (symbol_sk, trade_date_sk)
);

-- =========================================================
-- Index
-- =========================================================

CREATE INDEX IF NOT EXISTS index_fact_stock_symbol
ON curated.fact_stock_prices (symbol_sk);

CREATE INDEX IF NOT EXISTS index_fact_stock_trade_date
ON curated.fact_stock_prices (trade_date_sk);

CREATE INDEX IF NOT EXISTS index_fact_stock_price_trade_date
ON curated.fact_stock_prices (symbol_sk, trade_date_sk);