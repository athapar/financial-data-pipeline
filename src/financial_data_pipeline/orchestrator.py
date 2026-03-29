from financial_data_pipeline.polygon import PolygonClient, merge_df_with_parquet, save_splits_parquet
from financial_data_pipeline.cli import resolve_fetch_window, update_sync_state
from pathlib import Path
from typing import Optional
import pandas as pd

def run_symbol_ingestion(
        symbol: str,
        client: PolygonClient,
        sync_state: dict,
        canonical_base_dir: Path,
        start: Optional[str] = None,
        end: Optional[str] = None
) -> dict:
    """
    Orchestrates full ingestion flow a single symbol

    Steps:
    - resolve fetch window
    - fetch data
    - merge into canonical parquet
    - advance sync state

    Returns structured metadata for observability
    """

    window = resolve_fetch_window(symbol, sync_state, start, end)

    if window.start > window.end:
        return {
            "symbol": symbol,
            "mode": window.mode,
            "status": "no_op",
            "reason": "start_after_end"
        }
    
    payload = client.get_bars_day(symbol, window.start, window.end)

    rows = payload.get("results", [])
    df = pd.DataFrame(rows)

    result_df = merge_df_with_parquet(
        df,
        symbol=symbol,
        canonical_base_dir=canonical_base_dir,
    )

    if result_df is None:
        return {
            "symbol": symbol,
            "mode": window.mode,
            "status": "no_op",
            "reason": "empty_payload",
        }

    max_t = result_df["t"].max()
    new_date = max_t.date().isoformat()

    state_updated = update_sync_state(
        sync_file_path=canonical_base_dir.parent / "sync_state.json",
        symbol=symbol,
        new_date=new_date,
    )

    return {
        "symbol": symbol,
        "mode": window.mode,
        "status": "success",
        "rows": len(result_df),
        "state_updated": state_updated,
    }


def run_splits_ingestion(
        symbol: str,
        client: PolygonClient,
        canonical_base_dir: Path,
) -> dict:
    """
    Orchestrates full splits ingestion flow a single symbol

    Steps:
    - resolve fetch window
    - fetch data
    - merge into canonical parquet
    - advance sync state

    Returns structured metadata for observability
    """
    payload = client.get_splits(symbol)

    results = payload.get("results", [])

    if not results:
        return {
            "symbol": symbol,
            "status": "no_op",
            "reason": "empty_payload",
        }

    df = pd.DataFrame(results)
    result_df = save_splits_parquet(
        df,
        symbol=symbol,
        canonical_base_dir=canonical_base_dir,
    )
    return {
        "symbol": symbol,
        "status": "success",
        "rows": len(result_df),
    }
