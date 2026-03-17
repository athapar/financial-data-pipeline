# Financial Data Pipeline

A batch-oriented financial data engineering project that ingests daily US equity bars from Polygon.io, validates vendor payloads, and maintains canonical per-symbol parquet datasets with overlap-safe incremental merging.

## Current Implemented Scope

The current implementation focuses on the ingestion core for daily Polygon bars:

- Daily OHLCV bar ingestion from Polygon.io
- Schema validation before write
- Canonical per-symbol parquet storage
- Overlap-safe idempotent merge behavior
- Atomic parquet writes via temp-file replacement
- Incremental sync-state tracking
- Basic pytest coverage for merge and validation behavior

## Current Guarantees

- One canonical parquet dataset is stored per symbol
- Canonical parquet stores `t` as a UTC timestamp normalized from Polygon epoch-millisecond input
- Re-running overlapping backfills does not create duplicate rows
- Incoming duplicate timestamps are deterministically deduplicated
- Missing required fields, invalid timestamps, non-numeric required numeric fields, and non-finite numeric values fail before write
- Empty inputs no-op cleanly
- Core merge and validation behavior is covered by pytest tests

## Quick Start

### 1. Configure `.env`

```env
POLYGON_API_KEY=YOUR_API_KEY
FRED_API_KEY=YOUR_FRED_API_KEY
```

### 2. Run first data pull
`python -m financial_data_pipeline.cli --symbol SPY --start 2025-01-01 --end 2025-03-01`

### 3. Run tests
`python -m pytest -q`

### Example CLI Behavior
- Bootstrap a symbol:
`python -m financial_data_pipeline.cli --symbol TSLA`
- Re-run with no new available data:
`python -m financial_data_pipeline.cli --symbol TSLA`
- Backfill an overlapping historical window safely:
`python -m financial_data_pipeline.cli --symbol TSLA --start 2025-02-01 --end 2026-02-10`

### Reproducible Demo
```python
# 1. bootstrap
python -m financial_data_pipeline.cli --symbol AAPL --start 2026-01-01 --end 2026-01-10

# 2. rerun (no-op)
python -m financial_data_pipeline.cli --symbol AAPL

# 3. overlap backfill
python -m financial_data_pipeline.cli --symbol AAPL --start 2026-01-05 --end 2026-01-15
```



### Repository Structure
```
src/
  financial_data_pipeline/
    cli.py
    polygon.py
    config.py

tests/
  test_validation.py
  test_sync_state.py
  ```

## Roadmap

Implemented:
- MVP0: Raw daily bars ingestion
- Schema validation
- Canonical parquet merge
- Incremental sync-state foundation
- Core pytest coverage

Planned:
- FRED ingestion
- Raw → structured normalization layer
- Corporate actions modeling
- SCD2 security master
- Daily as-of snapshots
- Mart tables / downstream analytics outputs

## Design Notes

- Storage is currently one parquet file per symbol
- Row uniqueness is enforced on timestamp within each symbol dataset
- The current project is intentionally batch-oriented, not streaming
- The current artifact is focused on ingestion correctness and data engineering fundamentals 