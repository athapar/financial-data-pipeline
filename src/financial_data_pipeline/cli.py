import argparse
import json
from datetime import date, timedelta
from pathlib import Path
from financial_data_pipeline.config import POLYGON_API_KEY
from financial_data_pipeline.polygon import PolygonClient, write_json_raw
import os
from typing import Union

with open('data\sync_state.json','r') as file:
    sync_state = json.load(file)


def parse_date(string: Union[str, None]) -> date:
    if string:
        return date.fromisoformat(string)
    else:
        return

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--out", default="data/raw/polygon/bars")
    args = p.parse_args()

    symbol = args.symbol.upper()

    if args.start:
        start = parse_date(args.start)
        end = parse_date(args.end)
    else:
        start = parse_date(sync_state[symbol+"_daily"])+timedelta(days=1)
        end = date.today()
    
    if start < end:
        client = PolygonClient(api_key=POLYGON_API_KEY)
        payload = client.get_bars_day(symbol=symbol, start=start, end=end)

        out_dir = os.path.join(Path(args.out), symbol)
        out_path = Path(os.path.join(out_dir, f"{start.isoformat()}_{end.isoformat()}.json"))
        write_json_raw(payload, out_path)

        print(f"Wrote to: {out_path}")
    else:
        print(f"No new data. start = {start}, end = {end}")

if __name__ == "__main__":
    main()


