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
from financial_data_pipeline.config import PROJECT_ROOT, BARS_BASE_DIR, SPLITS_BASE_DIR, TICKER_BASE_DIR
BARS_REQUIRED_COLUMNS = {"t", "o", "c", "h", "l", "v"}
BARS_OPTIONAL_COLUMNS = {"vw", "n"}
BARS_ALLOWED_COLUMNS = BARS_REQUIRED_COLUMNS | BARS_OPTIONAL_COLUMNS
SPLITS_REQUIRED_COLUMNS = {'execution_date', 'id', 'split_from', 'split_to', 'ticker'}
SPLITS_OPTIONAL_COLUMNS = set()
SPLITS_ALLOWED_COLUMNS = SPLITS_REQUIRED_COLUMNS | SPLITS_OPTIONAL_COLUMNS
TICKER_REQUIRED_COLUMNS = {'ticker', 'name', 'market', 'locale', 'primary_exchange', 'type', 'active'}
TICKER_OPTIONAL_COLUMNS = {'composite_figi', 'share_class_figi', 'cik', 'currency_name', 'last_updated_utc'}
TICKER_ALLOWED_COLUMNS = TICKER_REQUIRED_COLUMNS | TICKER_OPTIONAL_COLUMNS
TICKER_OPTIONAL_COL_ORDER = ['composite_figi', 'share_class_figi', 'cik', 'currency_name', 'last_updated_utc']
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


    def get_splits(
            self,
            symbol: str,
            order: str = "asc",
) -> Dict[str, any]:
        
        url = f"{POLYGON_REQUEST_URL}/v3/reference/splits"
        params = {
            "ticker": symbol,
            "order": order,
            "apiKey": self.api_key,
        }

        r = self._session().get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def get_ticker(
        self,
        symbol: str,
        market: str = "stocks",
        locale: str = 'us',
        _type: str= "CS",
        active: bool= True,
        order: str = "asc"
) -> Dict[str, any]:
        
        url = f"{POLYGON_REQUEST_URL}/v3/reference/tickers"
        params = {
            "ticker": symbol,
            # "adjustment_type.any_of": "forward_split,reverse_split",
            "order": order,
            "apiKey": self.api_key,
            "market": market,
            "locale": locale,
            "type": _type,
            "active": active
        }

        r = self._session().get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

def validate_bars_schema(df: pd.DataFrame) -> None:
    """
    Enforce minimum schema requirements daily prices data and check for optional columns
    """
    incoming_cols = set(df.columns)

    missing = BARS_REQUIRED_COLUMNS - incoming_cols 

    if missing:
        raise ValueError(
            f"Schema violation: missing required columns in data: {missing}"
        )
    
    extra_cols = incoming_cols - BARS_ALLOWED_COLUMNS

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
    
def validate_splits_schema(df: pd.DataFrame) -> None:
    """
    Enforce minimum schema requirements on splits data and check for optional columns
    """
    incoming_cols = set(df.columns)

    missing = SPLITS_REQUIRED_COLUMNS - incoming_cols 

    if missing:
        raise ValueError(
            f"Schema violation: missing required columns in data: {missing}"
        )
    
    extra_cols = incoming_cols - SPLITS_ALLOWED_COLUMNS

    if extra_cols:
        print(f"[WARNING] Extra columns in data detected: {extra_cols}")

    if df["execution_date"].isna().any():
        raise ValueError("Schema violation: null execution_date values detected in payload")
    try:
        pd.to_datetime(df["execution_date"])
    except Exception as e:
        raise ValueError(f"Schema violation: column 'execution_date' could not be parsed as a date: {e}")
    
    numeric_cols = ['split_from', 'split_to']
    for col in numeric_cols:
        if df[col].isna().any():
            raise ValueError(
                f"Schema violation: null values detected in required column {col}"
            )
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise ValueError(f"Schema violation: column '{col}' must be numeric")
        
        if not np.isfinite(df[col]).all():
            raise ValueError(f"Schema violation: non-finite values detected in column '{col}'")
        
def validate_ticker_schema(df: pd.DataFrame) -> None:
    """
    Enforce minimum schema requirements on ticker data and check for optional columns
    """
    incoming_cols = set(df.columns)

    missing = TICKER_REQUIRED_COLUMNS - incoming_cols 

    if missing:
        raise ValueError(
            f"Schema violation: missing required columns in data: {missing}"
        )
    
    extra_cols = incoming_cols - TICKER_ALLOWED_COLUMNS

    if extra_cols:
        print(f"[WARNING] Extra columns in data detected: {extra_cols}")

    if "composite_figi" in df.columns:
        for row in df.loc[df["composite_figi"].isna()].itertuples():
            print(f"[WARNING] ticker {row.ticker} has no composite_figi")
    else:
        print(f"[WARNING] composite_figi column missing entirely from ticker data")


def merge_df_with_parquet(
    rows_df_in: pd.DataFrame,
    symbol: str,
    canonical_base_dir: Path = BARS_BASE_DIR,
    export_base_dir: Path | None = None,
) -> pd.DataFrame | None:
    """
    Merge a batch of daily bars into the canonical per-symbol parquet dataset.

    Storage invariant:
    - One canonical parquet file exists per symbol at:
      canonical_base_dir / {symbol} / "bars.parquet"
    - Therefore row uniqueness within a file is enforced on timestamp `t`

    Behavior:
    - validates schema before write
    - normalizes timestamps to UTC
    - merges with existing canonical parquet if present
    - removes duplicate timestamps deterministically
    - writes canonical parquet atomically
    - optionally copies canonical parquet to a user-specified export directory
    """
    if rows_df_in is None or rows_df_in.empty:
        print("No new rows to merge. Quitting...")
        return None

    validate_bars_schema(rows_df_in)

    rows_df = rows_df_in.copy()
    rows_df['symbol'] = symbol
    incoming_count = len(rows_df)
    rows_df["t"] = pd.to_datetime(rows_df["t"], unit="ms", utc=True)

    dup_count = rows_df["t"].duplicated().sum()
    if dup_count:
        print(f"WARNING: {dup_count} duplicate timestamps in incoming payload. Deduplicating batch")
        rows_df = rows_df.drop_duplicates(subset=["t"], keep="last")

    canonical_path = canonical_base_dir / symbol / "bars.parquet"
    canonical_path.parent.mkdir(parents=True, exist_ok=True)

    existing_count = 0
    if canonical_path.exists():
        prev_data = pd.read_parquet(canonical_path)
        prev_data["t"] = pd.to_datetime(prev_data["t"], utc=True)
        existing_count = len(prev_data)
        df = pd.concat([prev_data, rows_df], ignore_index=True)
    else:
        df = rows_df

    df = df.drop_duplicates(subset=["t"], keep="last")
    df = df.sort_values(by=["t"]).reset_index(drop=True)

    canonical_cols = ["symbol", "t", "o", "h", "l", "c", "v"]
    optional_cols_present = [col for col in ["vw", "n"] if col in df.columns]
    df = df[canonical_cols + optional_cols_present]

    tmp_path = canonical_path.with_suffix(f".tmp.{uuid.uuid4().hex}.parquet")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(canonical_path)

    final_count = len(df)
    duplicates_removed = incoming_count + existing_count - final_count

    print(f"Wrote to {canonical_path}")
    print(json.dumps({
    "symbol": symbol,
    "incoming_rows": incoming_count,
    "existing_rows": existing_count,
    "final_rows": final_count,
    "duplicates_removed": duplicates_removed,
    }))

    if export_base_dir is not None:
        export_path = export_base_dir / symbol / "bars.parquet"
        if export_path.resolve() != canonical_path.resolve():
            export_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(canonical_path, export_path)
            print(f"Copied bars canonical parquet to custom path {export_path}")

    return df


def save_splits_parquet(
    rows_df_in: pd.DataFrame,
    symbol: str,
    canonical_base_dir: Path = SPLITS_BASE_DIR,
    export_base_dir: Path | None = None,
) -> pd.DataFrame | None:
    
    if rows_df_in is None or rows_df_in.empty:
        print("No new splits to merge. Quitting...")
        return None
    
    validate_splits_schema(rows_df_in)

    
    rows_df = rows_df_in.copy()
    incoming_count = len(rows_df)
    
    rows_df["execution_date"] = pd.to_datetime(rows_df["execution_date"]).dt.normalize()

    dup_count = rows_df["execution_date"].duplicated().sum()
    if dup_count:
        print(f"WARNING: {dup_count} duplicate timestamps in incoming payload. Deduplicating batch")
        rows_df = rows_df.drop_duplicates(subset=["execution_date"], keep="last")

    canonical_path = canonical_base_dir / symbol / "splits.parquet"
    canonical_path.parent.mkdir(parents=True, exist_ok=True)

    existing_count = 0
    if canonical_path.exists():
        prev_data = pd.read_parquet(canonical_path)
        prev_data["execution_date"] = pd.to_datetime(prev_data["execution_date"]).dt.normalize()
        existing_count = len(prev_data)
        df = pd.concat([prev_data, rows_df], ignore_index=True)
    else:
        df = rows_df

    df = df.drop_duplicates(subset=["execution_date"], keep="last")
    df = df.sort_values(by=["execution_date"]).reset_index(drop=True)

    canonical_cols = ['ticker', 'execution_date', 'id', 'split_from', 'split_to']
    df = df[canonical_cols]

    tmp_path = canonical_path.with_suffix(f".tmp.{uuid.uuid4().hex}.parquet")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(canonical_path)

    final_count = len(df)
    duplicates_removed = incoming_count + existing_count - final_count

    print(f"Wrote splits data to {canonical_path}")
    print(json.dumps({
    "symbol": symbol,
    "incoming_rows": incoming_count,
    "existing_rows": existing_count,
    "final_rows": final_count,
    "duplicates_removed": duplicates_removed,
    }))

    if export_base_dir is not None:
        export_path = export_base_dir / symbol / "splits.parquet"
        if export_path.resolve() != canonical_path.resolve():
            export_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(canonical_path, export_path)
            print(f"Copied splits canonical parquet to custom path {export_path}")

    return df



def save_ticker_parquet(
    rows_df_in: pd.DataFrame,
    symbol: str,
    canonical_base_dir: Path = TICKER_BASE_DIR,
    export_base_dir: Path | None = None,
) -> pd.DataFrame | None:
    
    if rows_df_in is None or rows_df_in.empty:
        print("No incoming ticker data. Quitting...")
        return None
    
    validate_ticker_schema(rows_df_in)

    df = rows_df_in.copy()

    canonical_path = canonical_base_dir / symbol / "ticker.parquet"
    canonical_path.parent.mkdir(parents=True, exist_ok=True)


    canonical_cols = ['ticker', 'name', 'market', 'locale', 'primary_exchange', 'type', 'active']
    additional_cols_present = [col for col in TICKER_OPTIONAL_COL_ORDER if col in df.columns]
    df = df[canonical_cols + additional_cols_present]

    tmp_path = canonical_path.with_suffix(f".tmp.{uuid.uuid4().hex}.parquet")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(canonical_path)


    print(f"Wrote ticker data to {canonical_path}")
    print(json.dumps({
    "symbol": symbol,
    }))

    if export_base_dir is not None:
        export_path = export_base_dir / symbol / "ticker.parquet"
        if export_path.resolve() != canonical_path.resolve():
            export_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(canonical_path, export_path)
            print(f"Copied ticker canonical parquet to custom path {export_path}")

    return df


    

    

        