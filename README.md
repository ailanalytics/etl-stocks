# Stock EOD ETL Pipeline

## Overview

This project implements a production-style ETL pipeline for S&P 500 constituent end-of-day (EOD) market data. The pipeline ingests historical and daily incremental OHLCV data from an external API, persists raw payloads to object storage, and loads validated, idempotent data into PostgreSQL (hosted on a Virtual Private Server) for analytics, backtesting, and BI-style consumption.

The design intentionally mirrors real-world data engineering patterns, including layered storage, schema enforcement, incremental loading, and analytics-ready modelling.

---

## Key Goals

- Practice end-to-end data engineering at realistic scale  
- Separate raw ingestion, validated staging, and curated analytics layers  
- Handle historical backfills and daily incremental updates safely  
- Enforce data integrity at the database level  
- Support re-runs, retries, and cron-based scheduling  
- Produce analytics-ready data models suitable for BI tools  

---

## High-Level Architecture

```
External API
   │
   ▼
Raw Layer (S3, JSON, append-only)
   │
   ▼
Staging Layer (PostgreSQL)
   │
   ▼
Curated / Analytics Layer (PostgreSQL, Star Schema)
```

---

## Raw Layer

- API responses stored as JSON in object storage (S3-compatible)
- Minimal metadata captured (symbol, domain, source, ingestion timestamp)
- Append-only and immutable
- Designed to be replayable to rebuild downstream layers

---

## Staging Layer

- PostgreSQL
- Enforces schema, types, and grain (one symbol per trade date)
- Deduplicates and validates raw inputs
- Acts as the authoritative source for curated transformations

### Staging Table Design

```sql
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
    volume          BIGINT NOT NULL,

    CONSTRAINT staging_stocks_symbol_trade_date_uk
        UNIQUE (symbol, trade_date)
);
```

### Design Rationale

- Natural business key: `(symbol, trade_date)`
- Prevents duplicate candles by construction
- Enables idempotent, retry-safe ingestion
- Enforces correctness at the database level

---

## Incremental Loading Strategy

- Daily cron job retrieves the latest EOD data
- Incremental inserts use conflict-aware logic:

```sql
ON CONFLICT (symbol, trade_date) DO NOTHING;
```

This allows:

- Safe re-runs without duplication
- Tolerance for API retries or partial failures
- Simple, reliable incremental logic

---

## Backfill Notes

- Multi-decade historical backfill completed in ~3 days
- Runtime driven by dataset size, API throughput, and VPS compute constraints
- A single overlapping candle was identified and resolved via enforced constraints

---

## Curated Analytics Layer

The curated layer transforms validated staging data into **analytics-ready star-schema tables** optimised for querying, aggregation, and BI tools.

### Design Principles

- Clear grain definitions
- Surrogate keys for dimensions
- Fact tables optimised for time-series analysis
- Incremental, replayable loads from staging

### Core Tables

#### Dimension Tables

- **`dim_stock_meta`**
  - One row per stock symbol
  - Attributes: symbol, company name, sector, industry, domain
  - Surrogate key: `stock_meta_sk`

- **`dim_trade_date`**
  - One row per calendar date
  - Attributes: date, year, month, day, day_of_week
  - Enables efficient time-based analysis
  - Surrogate key: `date_sk`

#### Fact Table

- **`fact_stock_prices`**
  - Grain: one row per stock per trading day
  - Foreign keys:
    - `stock_meta_sk`
    - `trade_date_sk`
  - Measures:
    - open, high, low, close
    - adjusted_close
    - volume

### Benefits of This Model

- Eliminates duplication of descriptive attributes
- Enables fast analytical queries (grouping by sector, industry, date ranges)
- Directly consumable by BI tools such as Power BI
- Supports future analytical marts without re-engineering core logic

---

## Operational Characteristics

- Cron-driven ingestion and transformation jobs
- Safe to restart at any point
- Logging for job start, progress, and completion
- Curated layer can be fully rebuilt from staging
- Staging can be rebuilt from raw if required

---

## Data Characteristics

- Domain: Current S&P 500 constituents
- Frequency: Daily (EOD candles)
- History: Multi-decade historical backfill
- Scale: ~4.5 million rows in staging

---

## Current Status

- ✔ Historical backfill complete  
- ✔ Daily incremental ingestion operational  
- ✔ Staging layer complete  
- ✔ Curated star schema implemented  
- ✔ ~4.5M validated rows loaded  

---

## Planned Next Steps

- Analytical marts for specific business questions  
  - e.g. *Top 5 performing stocks per industry*  
- Index optimisation for analytical workloads  
- Power BI dashboards and exploratory analytics  
- Additional data quality checks and metrics  

---

## Why This Project Exists

This project is intentionally designed to demonstrate:

- Realistic data volumes and operational constraints  
- Real-world failure and recovery patterns  
- Correct use of database constraints and dimensional modelling  
- Production-style ETL and analytics design  

It is not a trading system.  
It is a **data engineering foundation** for analytics, experimentation, and portfolio demonstration.
