from financial_data_pipeline.load_bq import bq_table, load_parquet_to_bigquery, truncate_table
from financial_data_pipeline.config import BQ_PROJECT_ID, BQ_DATASET_ID, PROJECT_ROOT, SYNC_STATE_PATH, BARS_BASE_DIR, WAREHOUSE_DIR
from financial_data_pipeline.polygon import PolygonClient
from financial_data_pipeline.orchestrator import run_symbol_ingestion
from financial_data_pipeline.config import POLYGON_API_KEY
from financial_data_pipeline.cli import load_sync_path
from prefect import flow, task
import subprocess
from pathlib import Path
start = None
end = None

daily_bars_table = "daily_bars"
fact_table = "fact_daily_prices"

client = PolygonClient(api_key=POLYGON_API_KEY)

@task(log_prints=True)
def truncate_raw_table(table_name):
    truncate_table(bq_table(table_name))
    return

@task(log_prints=True)
def ingest_symbol(symbol:str, start=None, end=None):
    sync_state = load_sync_path(SYNC_STATE_PATH)
    return run_symbol_ingestion(symbol, client, sync_state, BARS_BASE_DIR, start, end)

@task(log_prints=True)     
def load_symbol_to_bq(symbol: str, table: str) -> None:
    canonical_path = BARS_BASE_DIR / symbol / "bars.parquet"
    load_parquet_to_bigquery(canonical_path, bq_table(daily_bars_table), mode="WRITE_APPEND")
    return None

@task(log_prints=True)
def run_dbt():
    subprocess.run(["dbt", "run"], cwd=WAREHOUSE_DIR, check=True)
    subprocess.run(["dbt", "test"], cwd=WAREHOUSE_DIR, check=True)
    
@flow(log_prints=True)
def pipeline_flow():
    symbols_path = PROJECT_ROOT / "symbols.txt"
    symbols = symbols_path.read_text().splitlines()
    truncate_raw_table(daily_bars_table)

    for symbol in symbols:
        result = ingest_symbol(symbol, start, end)
        if result.get("status") == "success":
            load_symbol_to_bq(symbol, daily_bars_table)

    run_dbt()

pipeline_flow()

