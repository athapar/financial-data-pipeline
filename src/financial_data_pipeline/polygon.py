from __future__ import annotations
import json 
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional
import requests
import pandas as pd


# POLYGON_REQUEST_URL = "https://api.massive.com/v3"
POLYGON_REQUEST_URL = "https://api.polygon.io"


@dataclass(frozen=True)
class PolygonClient:
    api_key: str
    session: Optional[requests.Session] = None

    def _session(self) -> requests.Session:
        return self.session or requests.Session()
    
    def get_bars_day(
            self,
            symbol: str,
            start: date,
            end: date,
            adjusted: bool = True, 
            sort: str = "asc",
            limit: int = 50000,
        ) -> Dict[str, Any]:
        # v2 aggreages bars endpoint
        url = f"{POLYGON_REQUEST_URL}/v2/aggs/ticker/{symbol}/range/1/day/{start.isoformat()}/{end.isoformat()}"
        params = {
            "adjusted": "true" if adjusted else "false",
            "sort": sort,
            "limit": limit,
            "apiKey": self.api_key,
        }

        r = self._session().get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    

def write_json_raw(payload, out_path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding = "utf-8")

def write_jsonl_raw(rows, out_path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "a") as file:
        for bar in rows:
            file.write(json.dumps(bar))
            file.write("\n")

def merge_df_with_parquet(rows_df_in, symbol):
    rows_df_in["t"] = pd.to_datetime(rows_df_in['t'], unit='ms', utc=True)
    parquet_parent_dir = Path("data/raw/polygon/bars") / symbol 
    parquet_parent_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = parquet_parent_dir / "bars.parquet"

    if parquet_path.exists():
        prev_data = pd.read_parquet(parquet_path)
        df = pd.concat([prev_data, rows_df_in])
        df = df.drop_duplicates(subset=['t'], keep="last")
    else:
        df = rows_df_in
        df = df.drop_duplicates(subset=['t'], keep="last")
    
    if df.empty:
        print("No data to write")
        return
    else:
        df = df.sort_values(by=['t'])
        df.to_parquet(parquet_path, index=False)
        print(f"Wrote to {parquet_path}")
        return df


    

    

    

        