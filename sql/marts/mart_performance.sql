-- =========================================================
-- SQL - Creation of Performance Mart Schema and Tables
-- =========================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- =========================================================
-- Create mart tables
-- =========================================================

CREATE TABLE IF NOT EXISTS mart.stock_perf_current (
    stock_perf_sk BIGSERIAL PRIMARY KEY,
    stock_meta_sk BIGINT NOT NULL REFERENCES curated.dim_stock_meta(stock_meta_sk),
    symbol TEXT NOT NULL,
    sector TEXT NOT NULL,
    industry TEXT NOT NULL,
    latest_close NUMERIC(12,4) NOT NULL,
    perf_30d NUMERIC(10,6) NOT NULL,
    perf_60d NUMERIC(10,6) NOT NULL,
    perf_90d NUMERIC(10,6) NOT NULL,
    perf_12m NUMERIC(10,6) NOT NULL,
    as_of_date_sk INTEGER NOT NULL REFERENCES curated.dim_trade_date(date_sk),
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_stock_perf_snapshot
    UNIQUE (stock_meta_sk, as_of_date_sk)
);