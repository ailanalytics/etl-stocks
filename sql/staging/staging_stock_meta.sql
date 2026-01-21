-- =========================================================
-- Stock ETL - Staging Schema
-- =========================================================

-- =========================================================
-- Staging Table - Stock Meta
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stocks_meta (
    symbol          TEXT NOT NULL,
    name            TEXT NOT NULL,
    sector          TEXT NOT NULL,
    sub_industry    TEXT NOT NULL,
    cik             NUMERIC NOT NULL,
    domain          TEXT NOT NULL,
    source          TEXT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL,

    CONSTRAINT staging_stocks_meta_symbol_cik
        UNIQUE (symbol, cik)
);