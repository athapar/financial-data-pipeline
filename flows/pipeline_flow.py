from financial_data_pipeline.load_bq import bq_table, load_parquet_to_bigquery, truncate_table
from financial_data_pipeline.config import BQ_PROJECT_ID, BQ_DATASET_ID, PROJECT_ROOT, SYNC_STATE_PATH, BARS_BASE_DIR
from financial_data_pipeline.polygon import PolygonClient
from financial_data_pipeline.orchestrator import run_symbol_ingestion
from financial_data_pipeline.config import POLYGON_API_KEY
from financial_data_pipeline.cli import load_sync_path
daily_bars_table = "daily_bars"
fact_table = "fact_daily_prices"

client = PolygonClient(api_key=POLYGON_API_KEY)

def truncate_raw_table(table_name):
    truncate_table(bq_table(table_name))
    return

def ingest_symbol(symbol:str, client: PolygonClient, start=None, end=None):
    sync_state = load_sync_path(SYNC_STATE_PATH)
    return run_symbol_ingestion(symbol, client, sync_state, BARS_BASE_DIR, start, end)
     
def load_symbol_to_bq(symbol: str, table: str) -> None:
    canonical_path = BARS_BASE_DIR / symbol / "bars.parquet"
    load_parquet_to_bigquery(canonical_path, bq_table(daily_bars_table), mode="WRITE_APPEND")
    return None
    
