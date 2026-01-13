-- =========================================================
-- Stock ETL - Staging Schema
-- =========================================================

-- -----------------------
--  Create Schema
-- -----------------------

CREATE SCHEMA IF NOT EXISTS staging;

-- -----------------------
-- Staging Table
-- -----------------------

CREATE TABLE IF NOT EXISTS staging.stocks (
    symbol          TEXT NOT NULL,
    domain          TEXT NOT NULL,
    source          TEXT NOT NULL,
    ingestion_type  TEXT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL,
    trade_date      DATE NOT NULL,
    open            NUMERIC(12,4) NOT NULL,
    high            NUMERIC(12,4) NOT NULL,
    low             NUMERIC(12,4) NOT NULL,
    close           NUMERIC(12,4) NOT NULL,
    adjusted_close  NUMERIC(12,4) NOT NULL,
    volume          BIGINT NOT NULL
);