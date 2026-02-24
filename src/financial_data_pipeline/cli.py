import argparse
import json
from datetime import date, timedelta, datetime
from pathlib import Path
from financial_data_pipeline.config import POLYGON_API_KEY
from financial_data_pipeline.polygon import PolygonClient, write_json_raw, write_jsonl_raw, merge_df_with_parquet
import os
from typing import Union
import pandas as pd



def load_sync_path(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        path.write_text("{}", encoding="utf-8")
    return json.loads(path.read_text())

def set_sidecar_file(initial_path, symbol):
    jsonl_path = Path(initial_path, symbol, 'bars.jsonl')
    outpath = Path(initial_path, symbol, "bars_index.txt")
    with open(jsonl_path, 'r') as file:
        with open(outpath, 'w') as outfile:
            for line in file:
                _dict = json.loads(line)
                t = _dict.get('t')
                outfile.write(f"{t}\n")
    return


def update_sync_state(sync_file_path, symbol, new_date):
    if sync_file_path.exists():
        with open(sync_file_path, "r", encoding='utf-8') as f:
            data = json.load(f)
        data[symbol + "_daily"] = new_date

        with open(sync_file_path, "w") as newfile:
            json.dump(data, newfile)

    else:
        data = {}
        data[symbol + "_daily"] = new_date
        with open(sync_file_path, "w") as newfile:
            json.dump(data, newfile)



def parse_date(string: Union[str, None]) -> date:
    if string:
        return date.fromisoformat(string)
    else:
        return

def update_sync_state_date(symbol: str, sync_state: dict, bars_dir: Path):
    parent_dir = bars_dir / symbol 
    parent_dir.mkdir(parents=True, exist_ok=True)
    max_t = 0
    with open(Path(parent_dir, "bars.jsonl")) as f:
        for line in f.readlines():
            data = json.loads(line)
            if data['t'] > max_t:
                max_t = data['t']
    prev_date = parse_date(sync_state.get(symbol + "_daily"))
    max_date = date.fromtimestamp(max_t / 1000)

    if  prev_date >= max_date:
        print("Already up to date")
    else:
        print(f"Previous sync date {prev_date} --> new date {max_date}")
    return max_date.isoformat()


def main():
    # Read current sync_state
    project_root = Path(__file__).resolve().parents[2]
    data_root = project_root / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    sync_path = data_root / "sync_state.json"
    bars_base_dir = data_root / "raw" / "polygon" / "bars"

    sync_state = load_sync_path(sync_path)

    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--out", default=bars_base_dir)
    args = p.parse_args()

    symbol = args.symbol.upper()

    if args.start:
        start = parse_date(args.start)
        end = parse_date(args.end)
    else:
        if sync_state.get(symbol+"_daily"):
            start = parse_date(sync_state[symbol+"_daily"])+timedelta(days=1)
            end = date.today()
        else:
            start=parse_date("2025-01-01")
            end=date.today()
    
    if start > end:
        # No new data possible because DB start is ahead of today
        print(f"No new data. start = {start}, end = {end}")
        return

    else:
        # Set up connection and fetch data
        client = PolygonClient(api_key=POLYGON_API_KEY)
        payload = client.get_bars_day(symbol=symbol, start=start, end=end)
        rows = payload.get("results", [])
        rows_df = pd.DataFrame.from_dict(rows)

        # Set up directories
        out_dir = Path(args.out, symbol)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = Path(out_dir, "bars.parquet")
        prev_date = parse_date(sync_state.get(symbol+"_daily"))
        prev_check = prev_date or date.min

        

        if rows_df.empty and not os.path.exists(bars_base_dir, symbol, "bars.parquet"):
            print("No data returned and no existing Parquet file. Nothing initialized.")
        else:
            df = merge_df_with_parquet(rows_df, symbol)
            max_t = df['t'].max()
            new_max_date = max_t.date()

            if new_max_date > prev_check:
                update_sync_state(sync_path, symbol, new_max_date.isoformat()) 

                      
            

if __name__ == "__main__":
    # init_path = Path('data/raw/polygon/bars')
    # set_sidecar_file(init_path, 'AAPL')
    main()

    # with open('data\sync_state.json','r') as file:
    #     sync_state = json.load(file)

    # max_date = update_sync_state_date('AAPL', sync_state)
    # print(max_date)


