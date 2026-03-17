## Financial Data Pipeline
This is a data pipeline project ingesting financial data (specifically equity data) from Polygon.io and FRED via APIs and produces decision-ready analytics metrics. The pipeline is batch-oriented, normalizes and validates vendor data, applies corporate actions adjustments, maintains SCD2 security master to produce metrics. 


## Quick start
### 1. Configure .env
```
POLYGON_API_KEY=YOUR_API_KEY
FRED_API_KEY=FRED_API_KEY
```

### Running first data pull
```
python -m financial_data_pipeline.cli --symbol SPY --start 2025-01-01 --end 2025-03-01
```


## Development Roadmap (Incremental Build Plan)

**MVP0**: Raw daily bars ingestion (SPY, AAPL) ✅

**MVP1**: Response validation + error handling

**MVP2**: Raw → structured normalization layer

**MVP3**: Corporate actions modeling

**MVP4**: SCD2 security master

**MVP5**: Daily as-of snapshot + marts




## Key Data Issues Addressed in Pipeline
- Temporal Correctness
- Vendor Data Imperfections
- Auditability
- Operational Reliability

## Main Goals
- Ingest daily OHLCV price data as well as security reference data from vendor (Polygon.io)
- Ingest macroeconomic reference data (e.g. risk free rates) from FRED
- Normalize raw data into consistent schema with validation/anomaly flagging
- Model corporate actions
- Maintain SCD2 security master tracking for shares outstanding, ticker symbols, other security metadata over time
- Materialize a daily as-of snapshot from the SCD2 for efficient downstream joins
- Produce mart tables with decision-ready metrics
- Ensure idempotent backfilss, deterministic transformations, and clear failure visibility

## Out of Scope
The following will be out of the scope of this effort:
- Trading signals or strategy backtesting: scope is a data platform, not alpha research system
- Price prediction or ML modeling: out of scope --> users can build models on top of outputs
- real-time or streaming data: architecture here is batch-oriented (Airflow + dbt)
- ETFs, options, futures, crypto, international equities: Scope is US common stocks only
- Spine-offs, M&A, rights offerings, special dividends: corporate actions scope is limited to splits and ticker changes
- user-facing dashboards or APIs: Output is warehouse tables, not application endpoints
- Enterpirse observability: lightweight alerting only


## Considerations
- Canonical parquet stores `t` as a UTC timestamp normalized from Polygon epoch-millisecond input.