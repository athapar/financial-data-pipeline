import argparse
from datetime import date
from pathlib import Path
from financial_data_pipeline.config import POLYGON_API_KEY
from financial_data_pipeline.polygon import PolygonClient, write_json_raw
import os

def parse_date(string: str) -> date:
    return date.fromisoformat(string)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--out", default="data/raw/polygon/bars")
    args = p.parse_args()

    symbol = args.symbol.upper()
    start = parse_date(args.start)
    end = parse_date(args.end)

    client = PolygonClient(api_key=POLYGON_API_KEY)
    payload = client.get_bars_day(symbol=symbol, start=start, end=end)

    out_dir = os.path.join(Path(args.out), symbol)
    out_path = Path(os.path.join(out_dir, f"{start.isoformat()}_{end.isoformat()}.json"))
    write_json_raw(payload, out_path)

    print(f"Wrote to: {out_path}")

if __name__ == "__main__":
    main()


