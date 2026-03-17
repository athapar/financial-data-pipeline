from __future__ import annotations
import json 
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional
import requests
import pandas as pd
import shutil
import uuid
import numpy as np

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

    if df["t"].isna().any():
        raise ValueError("Schema violation: null timestamps detected in payload")
    try:
        pd.to_datetime(df["t"], unit="ms", utc=True)
    except Exception as e:
        raise ValueError(f"Schema violation: column 't' could not be parsed as epoch milliseconds: {e}")
    
    numeric_cols = ["o", "h", "l", "c", "v"]
    for col in numeric_cols:
        if df[col].isna().any():
            raise ValueError(
                f"Schema violation: null values detected in required column {col}"
            )
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise ValueError(f"Schema violation: column '{col}' must be numeric")
        
        if not np.isfinite(df[col]).all():
            raise ValueError(f"Schema violation: non-finite values detected in column '{col}'")
        
    optional_numeric_cols = [col for col in ["vw", "n"] if col in df.columns]
    for col in optional_numeric_cols:
        if df[col].isna().any():
            print(
                f"[WARNING] null values detected in optional column {col}"
            )

        if not pd.api.types.is_numeric_dtype(df[col]):
            raise ValueError(f"Schema violation: optional column '{col}' must be numeric when present")
        
        if not np.isfinite(df[col]).all():
            raise ValueError(f"Schema violation: non-finite values detected in column '{col}'")
    




def merge_df_with_parquet(rows_df_in: pd.DataFrame, symbol: str, out_dir: Path = BARS_BASE_DIR) -> pd.DataFrame | None:
    """
    Merge a batch of daily bars into the canonical per-symbol parquet dataset.

    Storage invariant:
    - One parquet file exists per symbol at: BARS_BASE_DIR / {symbol} / "bars.parquet"
    - Therefore row uniqueness within a file is enforced on timestamp `t`

    Behavior:
    - validates schema before write
    - normalizes timestamps to UTC
    - merges with existing canonical parquet if present
    - removes duplicate timestamps deterministically
    - writes canonical parquet
    - optionally copies canonical parquet to a user-specified export directory
    """
    if rows_df_in is None or rows_df_in.empty:
        print("No new rows to merge. Quitting...")
        return None
    
    validate_schema(rows_df_in) 


    rows_df = rows_df_in.copy()
    incoming_count = len(rows_df)
    rows_df["t"] = pd.to_datetime(rows_df['t'], unit='ms', utc=True) 

    dup_count = rows_df['t'].duplicated().sum()
    if dup_count:
        print(f"WARNING: {dup_count} duplicate timestamps in incoming payload. Deduplicating batch")
        rows_df = rows_df.drop_duplicates(subset=['t'], keep='last')

    parquet_path = out_dir / symbol / "bars.parquet"
    canonical_path = BARS_BASE_DIR / symbol / "bars.parquet"
    
    canonical_path.parent.mkdir(parents=True, exist_ok = True)
    is_export = parquet_path.resolve() != canonical_path.resolve()

    # Merge data with canonical file
    existing_count = 0
    if canonical_path.exists():
        prev_data = pd.read_parquet(canonical_path)
        prev_data['t'] = pd.to_datetime(prev_data["t"], utc=True) # Note: canonical parquet stores  `t` as normalized UTC timestamp, not raw epoch milliseconds
        df = pd.concat([prev_data, rows_df], ignore_index=True)
        existing_count = len(prev_data)
    else:
        df = rows_df

    # Remove duplicates (idempotency)
    df = df.drop_duplicates(subset=['t'], keep="last") 
    df = df.sort_values(by=['t']).reset_index(drop=True) # Pipeline assumes data will be for a single symbol and searches for duplicates along timestamps only
    canonical_cols = ["t", "o", "h", "l", "c", "v"]
    optional_cols_present = [col for col in ["vw", "n"] if col in df.columns]
    df = df[canonical_cols + optional_cols_present]

    # Write to canonical file
    tmp_path = canonical_path.with_suffix(f".tmp.{uuid.uuid4().hex}.parquet") # Write to a unique temp file first, then replace (ensure atomic parquet write)
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(canonical_path) 

    
    final_count = len(df)
    duplicates_removed = incoming_count + existing_count - final_count

    print(f"Wrote to {canonical_path}")
    print(
    f"Merged {incoming_count} incoming rows with {existing_count} existing rows "
    f"into {final_count} canonical rows. Removed {duplicates_removed} duplicates"
    )

    # Create copy to another location if user specifies
    if is_export:
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(canonical_path, parquet_path)
        print(f"Copying to custom path {parquet_path}")

    return df


    

    

    

        