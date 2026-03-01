from __future__ import annotations
import json 
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional
import requests
import pandas as pd
import shutil

REQUIRED_COLUMNS = {"t", "o", "c", "h", "l", "v"}
OPTIONAL_COLUMNS = {"vw", "n"}
ALLOWED_COLUMNS = REQUIRED_COLUMNS | OPTIONAL_COLUMNS

# POLYGON_REQUEST_URL = "https://api.massive.com/v3"
POLYGON_REQUEST_URL = "https://api.polygon.io"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BARS_BASE_DIR = PROJECT_ROOT / "data" / "raw" / "polygon" / "bars"


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

def validate_schema(df: pd.DataFrame) -> None:
    """
    Enforce minimum schema requirements and check for optional columns
    """
    incoming_cols = set(df.columns)

    missing = REQUIRED_COLUMNS - incoming_cols 

    if missing:
        raise ValueError(
            f"Schema violation: missing required columns in data: {missing}"
        )
    
    extra_cols = incoming_cols - ALLOWED_COLUMNS

    if extra_cols:
        print(f"[WARNING] Extra columns in data detected: {extra_cols}")




def merge_df_with_parquet(rows_df_in, symbol, out_dir: Path = BARS_BASE_DIR):
    """
    Goal is to export data to standard location (source of truth, but if out_path is different save a copy there too)
    """
    if rows_df_in is None or rows_df_in.empty:
        print("No new rows to merge. Quitting...")
        return None
    
    validate_schema(rows_df_in) # Perform schema validation before any writes


    rows_df = rows_df_in.copy() # Make copy to avoid mutating original argument df
    rows_df["t"] = pd.to_datetime(rows_df['t'], unit='ms', utc=True) 

    if rows_df["t"].duplicated().any():
        raise ValueError("Duplicate timestamps detected in vendor payload")


    parquet_path = out_dir / symbol / "bars.parquet"
    canonical_path = BARS_BASE_DIR / symbol / "bars.parquet"
    canonical_path.parent.mkdir(parents=True, exist_ok = True)
    is_export = parquet_path.resolve() != canonical_path.resolve()

    # Merge data with canonical file if it exists
    if canonical_path.exists():
        prev_data = pd.read_parquet(canonical_path)
        prev_data['t'] = pd.to_datetime(prev_data["t"], utc=True)
        df = pd.concat([prev_data, rows_df], ignore_index=True)
    else:
        df = rows_df

    # Remove duplicates (idempotency)
    df = df.drop_duplicates(subset=['t'], keep="last")
    
    # Write to canonical file
    df = df.sort_values(by=['t'])
    df.to_parquet(canonical_path, index=False)
    print(f"Wrote to {canonical_path}")

    # Copy to another location if user specifies
    if is_export:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(canonical_path, parquet_path)
        print(f"Copying to custom path {parquet_path}")

    return df


    

    

    

        