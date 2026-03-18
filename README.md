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

## Architecture Overview

The data pipeline is structured as a set of deterministic, testable components:

- Fetch Window Resolution:
    Determines data pull range (manual, incremental, bootstrap)

- Ingestion Layer:
    Retrieves daily OHLCV bars data (Polygon API)

- Validation Layer:
    Enforces schema correctness before any writes

- Storage Layer:
    Merges data into canonical per-symbol parquet datasets with:
    - Idempotent merges 
    - Duplicate timestamp resolution
    - atomic file replacement

- State Layer:
    Maintains monotonic sync-state checkpoints per symbol

- Orchestration Layers:
    Coordinates end-to-end ingestion flow

- Transformation Layer:
    Computes derived analytics (e.g., daily returns)

All components are unit tested and designed for deterministic behavior under reruns and backfills.




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

### Reproducible Demo
```bash
# 1. bootstrap
python -m financial_data_pipeline.cli --symbol AAPL 

# 2. rerun (no-op)
python -m financial_data_pipeline.cli --symbol AAPL

# 3. overlap backfill
python -m financial_data_pipeline.cli --symbol AAPL --start 2025-12-01 --end 2026-01-01
```
Example output (truncated)
```bash
# 1. bootstrap
{"symbol": "AAPL", "incoming_rows": 61, "existing_rows": 0, "final_rows": 61, "duplicates_removed": 0}
Updated sync_state: AAPL_daily -> 2026-03-17

# 2. rerun (no-op)
No new data. start = 2026-03-18, end = 2026-03-17

# 3. overlap backfill
{"symbol": "AAPL", "incoming_rows": 22, "existing_rows": 61, "final_rows": 73, "duplicates_removed": 10}
No state update needed
```

Expected behavior:
- No duplicate timestamps
- Canonical dataset remains deduplicated
- Sync state only advances forward

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