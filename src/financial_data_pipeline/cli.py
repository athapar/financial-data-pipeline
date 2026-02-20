import argparse
import json
from datetime import date, timedelta, datetime
from pathlib import Path
from financial_data_pipeline.config import POLYGON_API_KEY
from financial_data_pipeline.polygon import PolygonClient, write_json_raw, write_jsonl_raw
import os
from typing import Union


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
        print(f"No new data. start = {start}, end = {end}")
        return

    else:
        client = PolygonClient(api_key=POLYGON_API_KEY)
        payload = client.get_bars_day(symbol=symbol, start=start, end=end)

        out_dir = os.path.join(Path(args.out), symbol)
        out_path = Path(os.path.join(out_dir, "bars.jsonl"))
        rows = payload.get("results", [])
        set_sidecar_file(Path("data/raw/polygon/bars"), symbol)


        # if end > parse_date(sync_state[symbol+"_daily"])+timedelta(days=1):
        #     write_jsonl_raw(payload, out_path)
        #     print(f"Wrote to: {out_path}")
        # else:
        #     print("Skipping writes")

        if not rows:
            print("No rows returned")
            return
        else:
            existing_ts_set = set()
            with open(Path("data/raw/polygon/bars", symbol, 'bars_index.txt'), "r") as bt:
                for line in bt:
                    existing_ts_set.add(int(line.strip()))

            max_t = max(r["t"] for r in rows)
            prev_date = parse_date(sync_state.get(symbol + "_daily"))
            new_max_date = date.fromtimestamp(max_t / 1000)
            
            rows_to_write = [r for r in rows if r['t'] not in existing_ts_set]
            if rows_to_write:
                write_jsonl_raw(rows_to_write, out_path)
                print(f"Wrote to: {out_path}")
            else:
                print("No new data- timestamps exist in set")
            

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


