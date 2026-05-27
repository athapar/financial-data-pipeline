from financial_data_pipeline.load_bq import bq_table, batch_load_parquet_to_bigquery
from financial_data_pipeline.config import (
    POLYGON_API_KEY, PROJECT_ROOT, SYNC_STATE_PATH,
    BARS_BASE_DIR, SPLITS_BASE_DIR, TICKER_BASE_DIR,
    FINANCIALS_BASE_DIR, COMPANY_BASE_DIR, DIVIDENDS_BASE_DIR, WAREHOUSE_DIR,
)
from financial_data_pipeline.polygon import PolygonClient
from financial_data_pipeline.orchestrator import (
    run_symbol_ingestion, run_splits_ingestion, run_ticker_ingestion,
    run_financials_ingestion, run_company_overview_ingestion,
    run_dividends_ingestion,
)
from financial_data_pipeline.cli import load_sync_path
from prefect import flow, task
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
import logging
import subprocess
from pathlib import Path

log_path = PROJECT_ROOT / "logs" / "pipeline.log"
log_path.parent.mkdir(parents=True, exist_ok=True)

file_handler = RotatingFileHandler(
    log_path, maxBytes=5 * 1024 * 1024, backupCount=0,
)
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-5s | %(name)s | %(message)s"))
file_handler.setLevel(logging.INFO)

for logger_name in ["prefect", "prefect.flow_runs", "prefect.task_runs", ""]:
    lgr = logging.getLogger(logger_name)
    lgr.addHandler(file_handler)

start = None
end = None

daily_bars_table = "daily_bars"
raw_splits_table = "splits"
raw_tickers_table = "tickers"
raw_financials_table = "quarterly_financials"
raw_company_table = "company_overview"
raw_dividends_table = "dividends"

FRESHNESS_THRESHOLD = timedelta(hours=12)

client = PolygonClient(api_key=POLYGON_API_KEY)


def is_fresh(parquet_path: Path) -> bool:
    if not parquet_path.exists():
        return False
    mtime = datetime.fromtimestamp(parquet_path.stat().st_mtime)
    return (datetime.now() - mtime) < FRESHNESS_THRESHOLD


# ── Phase 1: Ingestion (API → local parquet, skip if fresh) ──────────

@task(log_prints=True)
def ingest_symbol(symbol: str, start=None, end=None):
    sync_state = load_sync_path(SYNC_STATE_PATH)
    return run_symbol_ingestion(symbol, client, sync_state, BARS_BASE_DIR, start, end)

@task(log_prints=True)
def ingest_splits(symbol: str):
    canonical = SPLITS_BASE_DIR / symbol / "splits.parquet"
    if is_fresh(canonical):
        print(f"[SKIP] {symbol} splits parquet is fresh, skipping API call")
        return {"symbol": symbol, "status": "cached"}
    return run_splits_ingestion(symbol, client, SPLITS_BASE_DIR)

@task(log_prints=True)
def ingest_ticker(symbol: str):
    canonical = TICKER_BASE_DIR / symbol / "ticker.parquet"
    if is_fresh(canonical):
        print(f"[SKIP] {symbol} ticker parquet is fresh, skipping API call")
        return {"symbol": symbol, "status": "cached"}
    return run_ticker_ingestion(symbol, client, TICKER_BASE_DIR)

@task(log_prints=True)
def ingest_financials(symbol: str):
    canonical = FINANCIALS_BASE_DIR / symbol / "financials.parquet"
    if is_fresh(canonical):
        print(f"[SKIP] {symbol} financials parquet is fresh, skipping API call")
        return {"symbol": symbol, "status": "cached"}
    return run_financials_ingestion(symbol, client, FINANCIALS_BASE_DIR)

@task(log_prints=True)
def ingest_company_overview(symbol: str):
    canonical = COMPANY_BASE_DIR / symbol / "company.parquet"
    if is_fresh(canonical):
        print(f"[SKIP] {symbol} company parquet is fresh, skipping API call")
        return {"symbol": symbol, "status": "cached"}
    return run_company_overview_ingestion(symbol, client, COMPANY_BASE_DIR)

@task(log_prints=True)
def ingest_dividends(symbol: str):
    canonical = DIVIDENDS_BASE_DIR / symbol / "dividends.parquet"
    if is_fresh(canonical):
        print(f"[SKIP] {symbol} dividends parquet is fresh, skipping API call")
        return {"symbol": symbol, "status": "cached"}
    return run_dividends_ingestion(symbol, client, DIVIDENDS_BASE_DIR)

@task(log_prints=True)
def ingest_all_for_symbol(symbol: str, start=None, end=None) -> dict:
    errors = []

    try:
        ingest_symbol(symbol, start, end)
    except Exception as e:
        errors.append(f"bars: {e}")
        print(f"[ERROR] {symbol} bars failed: {e}")

    try:
        ingest_splits(symbol)
    except Exception as e:
        errors.append(f"splits: {e}")
        print(f"[ERROR] {symbol} splits failed: {e}")

    try:
        ingest_ticker(symbol)
    except Exception as e:
        errors.append(f"ticker: {e}")
        print(f"[ERROR] {symbol} ticker failed: {e}")

    try:
        ingest_financials(symbol)
    except Exception as e:
        errors.append(f"financials: {e}")
        print(f"[ERROR] {symbol} financials failed: {e}")

    try:
        ingest_company_overview(symbol)
    except Exception as e:
        errors.append(f"company: {e}")
        print(f"[ERROR] {symbol} company failed: {e}")

    try:
        ingest_dividends(symbol)
    except Exception as e:
        errors.append(f"dividends: {e}")
        print(f"[ERROR] {symbol} dividends failed: {e}")

    status = "partial_failure" if errors else "success"
    return {"symbol": symbol, "status": status, "errors": errors}


# ── Phase 2: BQ Load (batch concat → single WRITE_TRUNCATE per table) ──

@task(log_prints=True)
def load_all_to_bq(symbols: list[str]) -> None:
    table_configs = [
        (daily_bars_table, [BARS_BASE_DIR / s / "bars.parquet" for s in symbols]),
        (raw_splits_table, [SPLITS_BASE_DIR / s / "splits.parquet" for s in symbols]),
        (raw_tickers_table, [TICKER_BASE_DIR / s / "ticker.parquet" for s in symbols]),
        (raw_financials_table, [FINANCIALS_BASE_DIR / s / "financials.parquet" for s in symbols]),
        (raw_company_table, [COMPANY_BASE_DIR / s / "company.parquet" for s in symbols]),
        (raw_dividends_table, [DIVIDENDS_BASE_DIR / s / "dividends.parquet" for s in symbols]),
    ]

    for table_name, paths in table_configs:
        batch_load_parquet_to_bigquery(paths, bq_table(table_name))


# ── Phase 3: dbt ─────────────────────────────────────────────────────

@task(log_prints=True)
def run_dbt():
    subprocess.run(["dbt", "snapshot"], cwd=WAREHOUSE_DIR, check=True)
    subprocess.run(["dbt", "run"], cwd=WAREHOUSE_DIR, check=True)
    subprocess.run(["dbt", "test"], cwd=WAREHOUSE_DIR, check=True)


# ── Flow ─────────────────────────────────────────────────────────────

@flow(log_prints=True)
def pipeline_flow():
    symbols_path = PROJECT_ROOT / "symbols.txt"
    symbols = [s.strip() for s in symbols_path.read_text().splitlines() if s.strip()]

    # Phase 1: Ingest from API → local parquet (skip if fresh)
    print(f"\n=== Phase 1: Ingestion ({len(symbols)} symbols) ===")
    results = []
    for symbol in symbols:
        result = ingest_all_for_symbol(symbol, start, end)
        results.append(result)

    failed = [r for r in results if r["status"] != "success"]
    print(f"\n=== Ingestion Summary ===")
    print(f"Total: {len(results)} | Succeeded: {len(results) - len(failed)} | Partial failures: {len(failed)}")
    for f in failed:
        print(f"  {f['symbol']}: {f['errors']}")

    # Phase 2: Batch load all local parquet → BQ (single job per table)
    print(f"\n=== Phase 2: BQ Load ===")
    load_all_to_bq(symbols)

    # Phase 3: dbt
    print(f"\n=== Phase 3: dbt ===")
    run_dbt()

pipeline_flow()
