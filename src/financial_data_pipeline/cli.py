import argparse
import json
from datetime import date, timedelta, datetime
from pathlib import Path
from financial_data_pipeline.config import POLYGON_API_KEY
from financial_data_pipeline.polygon import PolygonClient, write_json_raw, write_jsonl_raw, merge_df_with_parquet
import os
from typing import Union
import pandas as pd


def load_sync_path(path):
    with open(path ,'r') as file:
        return json.load(file)

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
    with open(sync_file_path, "r", encoding='utf-8') as f:
        data = json.load(f)
    data[symbol + "_daily"] = new_date

    with open(sync_file_path, "w") as newfile:
        json.dump(data, newfile)



def parse_date(string: Union[str, None]) -> date:
    if string:
        return date.fromisoformat(string)
    else:
        return

def update_sync_state_date(symbol, sync_state):
    max_t = 0
    with open(os.path.join(Path("data/raw/polygon/bars"), symbol, "bars.jsonl")) as f:
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
        print(max_date.isoformat())
    return max_date.isoformat()


def main():
    # Read current sync_state
    sync_state = load_sync_path(Path("data/sync_state.json"))

    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--out", default=Path("data/raw/polygon/bars"))
    args = p.parse_args()

    symbol = args.symbol.upper()

    if args.start:
        start = parse_date(args.start)
        end = parse_date(args.end)
    else:
        start = parse_date(sync_state[symbol+"_daily"])+timedelta(days=1)
        end = date.today()
    
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
        
        

        if rows_df.empty and not os.path.exists(Path("data/raw/polygon/bars", symbol, "bars.parquet")):
            print("No data returned and no existing Parquet file. Nothing initialized.")
        else:
            df = merge_df_with_parquet(rows_df, symbol)
            max_t = df['t'].max()
            new_max_date = max_t.date()

            if new_max_date > prev_date:
                update_sync_state(Path("data/sync_state.json"), symbol, new_max_date.isoformat())        
            

if __name__ == "__main__":
    # init_path = Path('data/raw/polygon/bars')
    # set_sidecar_file(init_path, 'AAPL')
    main()

    # with open('data\sync_state.json','r') as file:
    #     sync_state = json.load(file)

    # max_date = update_sync_state_date('AAPL', sync_state)
    # print(max_date)


