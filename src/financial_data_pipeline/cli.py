import argparse
import json
from datetime import date, timedelta, datetime
from pathlib import Path
from financial_data_pipeline.config import POLYGON_API_KEY
from financial_data_pipeline.polygon import PolygonClient, merge_df_with_parquet, validate_schema
import os
from typing import Union, Optional
import pandas as pd
from financial_data_pipeline.polygon import PROJECT_ROOT, BARS_BASE_DIR
from dataclasses import dataclass


def load_sync_path(path: Path) -> dict:
    """
    Loads sync_state.json file if it exists. If it doesn't exist, write new empty json to that file. Returns dictionary.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        path.write_text("{}", encoding="utf-8")
    return json.loads(path.read_text(encoding="utf-8"))

# def set_sidecar_file(initial_path, symbol):
#     jsonl_path = Path(initial_path, symbol, 'bars.jsonl')
#     outpath = Path(initial_path, symbol, "bars_index.txt")
#     with open(jsonl_path, 'r') as file:
#         with open(outpath, 'w') as outfile:
#             for line in file:
#                 _dict = json.loads(line)
#                 t = _dict.get('t')
#                 outfile.write(f"{t}\n")
#     return


def update_sync_state(sync_file_path: Path, symbol: str, new_date: str) -> bool:
    """
    Update sync state for a symbol only if the new date is strictly newer.

    Returns True if state was advanced, False otherwise.
    """
    if sync_file_path.exists():
        with open(sync_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {}

    key = f"{symbol}_daily"
    prev_raw = data.get(key)

    if prev_raw is not None:
        prev_date = date.fromisoformat(prev_raw)
        candidate_date = date.fromisoformat(new_date)

        if candidate_date <= prev_date:
            return False

    data[key] = new_date
    with open(sync_file_path, "w", encoding="utf-8") as newfile:
        json.dump(data, newfile)

    return True



def parse_date(string: Union[str, None]) -> date:
    if string:
        return date.fromisoformat(string)
    else:
        return

def update_sync_state_date(
        symbol: str, 
        sync_state: dict, 
        bars_dir: Path
        ):
    parent_dir = bars_dir / symbol 
    parent_dir.mkdir(parents=True, exist_ok=True)
    max_t = 0
    with open(Path(parent_dir, "bars.jsonl")) as f:
        for line in f.readlines():
            data = json.loads(line)
            if data['t'] > max_t:
                max_t = data['t']
    prev_raw = sync_state.get(symbol + "_daily")
    prev_date = parse_date(prev_raw) if prev_raw else date.min

    max_date = date.fromtimestamp(max_t / 1000)

    if  prev_date >= max_date:
        print("Already up to date")
    else:
        print(f"Previous sync date {prev_date} --> new date {max_date}")
    return max_date.isoformat()

@dataclass(frozen=True)
class FetchWindow:
    start: date
    end: date
    mode: str  # "manual" | "incremental" | "bootstrap"

def resolve_fetch_window(
    symbol: str,
    sync_state: dict,
    start: Optional[str],
    end: Optional[str],
) -> FetchWindow:
    """
    Determine the fetch window for a symbol.

    Modes:
    - manual: user provided start (and optionally end)
    - incremental: advance from last sync state
    - bootstrap: no prior state exists

    Returns:
        FetchWindow with start, end, and mode
    """

    today = date.today()
    key = f"{symbol}_daily" 

    # --- Manual ---
    if start:
        start_date = parse_date(start)
        end_date = parse_date(end) if end else today

        if start_date is None:
            raise ValueError("Invalid start date format")

        if end_date is None:
            raise ValueError("Invalid end date format")

        if start_date > end_date:
            raise ValueError(f"Invalid window: start {start_date} > end {end_date}")

        return FetchWindow(start=start_date, end=end_date, mode="manual")

    # --- Incremental ---
    if key in sync_state:
        prev_date = parse_date(sync_state[key])
        if prev_date is None:
            raise ValueError(f"Invalid stored sync date for {symbol}")

        start_date = prev_date + timedelta(days=1)
        end_date = today

        return FetchWindow(start=start_date, end=end_date, mode="incremental")

    # --- Bootstrap ---
    start_date = today - timedelta(days=90)
    end_date = today

    return FetchWindow(start=start_date, end=end_date, mode="bootstrap")


def main():
    
    project_root = PROJECT_ROOT

    # Ensure data folder exists
    data_root = project_root / "data"
    data_root.mkdir(parents=True, exist_ok=True)

    # Read current sync_state
    sync_path = data_root / "sync_state.json"
    sync_state = load_sync_path(sync_path)

    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--out", default=None, type=Path)
    args = p.parse_args()

    symbol = args.symbol.upper()

    window = resolve_fetch_window(symbol, sync_state, args.start, args.end)

    if window.start > window.end:
        # No new data possible because DB start is ahead of today
        print(f"No new data. start = {window.start}, end = {window.end}")
        return

    else:
        # Set up connection and fetch data
        client = PolygonClient(api_key=POLYGON_API_KEY)
        payload = client.get_bars_day(symbol=symbol, start=window.start, end=window.end)
        rows = payload.get("results", [])

        rows_df = pd.DataFrame.from_dict(rows)

        canonical_path = BARS_BASE_DIR / symbol / "bars.parquet"
        canonical_exists = canonical_path.exists()


        # Set up directories
        prev_raw = sync_state.get(symbol+"_daily")
        prev_date = parse_date(prev_raw) if prev_raw else date.min


        if rows_df.empty:
            if not canonical_exists:
                print("No data returned and no existing Parquet file. Nothing initialized.")
                return
            else:
                print("No new data returned. Canonical storage file exists. Nothing written to it.")
                return

        if args.out:
            out_dir = args.out
            out_dir.mkdir(parents=True, exist_ok=True)
            df = merge_df_with_parquet(rows_df, symbol, out_dir)
        else:
            df = merge_df_with_parquet(rows_df, symbol)

        if df is None:
            return 
        else:
            max_t = df['t'].max()
            new_max_date = max_t.date()

            if new_max_date > prev_date:
                update_sync_state(sync_path, symbol, new_max_date.isoformat()) 
                print(f"Updated sync_state: {symbol}_daily -> {new_max_date.isoformat()}")
            else:
                print("No state update needed")

                      
            

if __name__ == "__main__":
    # init_path = Path('data/raw/polygon/bars')
    # set_sidecar_file(init_path, 'AAPL')
    main()

    # with open('data\sync_state.json','r') as file:
    #     sync_state = json.load(file)

    # max_date = update_sync_state_date('AAPL', sync_state)
    # print(max_date)


