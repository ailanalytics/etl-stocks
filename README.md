# Stock EOD ETL Pipeline

## Overview

This project implements a production‑style ETL pipeline for S&P500 constituent end‑of‑day (EOD) market data. The pipeline ingests historical and daily incremental OHLCV data from an external API, persists raw payloads to object storage, and loads validated, idempotent data into PostgreSQL (hosted on a Virtual Private Server) for analytics and backtesting.

The design aims to intentionally mirror real‑world data engineering patterns.

---

## Key Goals

* Practice end‑to‑end data engineering at realistic scale
* Separate raw ingestion from validated staging and downstream analytics
* Handle historical backfills and incremental updates safely
* Enforce data integrity at the database level
* Support re‑runs, retries, and cron‑based scheduling

---

## High‑Level Architecture

```
External API
   │
   ▼
Raw Layer (JSON, append‑only)
   │
   ▼
Staging Layer (PostgreSQL)
   │
   ▼
Curated / Analytics (PostgreSQL)
```

### Raw Layer

* API responses stored as JSON
* Minimal metadata (symbol, domain, ingestion timestamp, source)
* Append‑only
* Designed to be immutable and replayable

### Staging Layer

* PostgreSQL
* Enforces schema, types, grain (One symbol for one date)
* Acts as the source of truth for downstream models

---

## Data Characteristics

* Domain: Current S&P 500 constituents
* Frequency: Daily (EOD candles)
* History: Multi‑decade historical backfill
* Scale: ~4.5 million rows in staging

---

## Staging Table Design

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
    volume          BIGINT NOT NULL

    CONSTRAINT staging_stocks_symbol_trade_date_uk
        UNIQUE (symbol, trade_date)

);
```

### Design Rationale

* Natural business key: `(symbol, trade_date)`
* Prevents duplicate candles by construction
* Supports idempotent and retry‑safe loads
* Database‑level enforcement

---

## Incremental Loading Strategy

* Daily cron job retrieves the latest EOD data from API
* Inserts use conflict‑aware logic:

```sql
ON CONFLICT (symbol, trade_date) DO NOTHING;
```

This allows:

* Safe re‑runs
* API inconsistencies without duplication

---

## Backfill Notes

* Historical backfill took ~3 days to complete
* The multi-day historical backfill reflects realistic operational constraints, with runtime driven by dataset size, API throughput, and available VPS compute resources.
* Duplicate detection identified a single overlapping candle, resolved via cleanup and enforced constraints

---

## Operational Characteristics

* Cron‑driven jobs
* Safe to restart at any point
* Logging for start, progress, and completion
* Staging can be rebuilt entirely from raw if required

---

## Current Status

✔ Historical data fully loaded
✔ Incremental EOD ingestion operational
✔ Staging layer complete
✔ ~4.5M rows validated

---

## Planned Next Steps

* Curated analytics layer (fact / dimension design)
* Index optimisation for analytical workloads
* Strategy backtesting (trend, relative strength, momentum)
* Data quality checks integrated into job execution

---

## Why This Project Exists

This project is intentionally designed to demonstrate:

* Realistic data volumes
* Real‑world failure modes
* Correct use of database constraints
* Production‑style ETL thinking

It is not intended to be a trading system, but a data engineering foundation for analytics and experimentation.

---
