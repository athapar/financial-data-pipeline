from __future__ import annotations
import json
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional
import requests
import pandas as pd
import shutil
import uuid
import numpy as np
from financial_data_pipeline.config import PROJECT_ROOT, BARS_BASE_DIR, SPLITS_BASE_DIR, TICKER_BASE_DIR, FINANCIALS_BASE_DIR, COMPANY_BASE_DIR, DIVIDENDS_BASE_DIR
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

    def _get_with_retry(self, url: str, params: dict, max_retries: int = 3) -> requests.Response:
        sess = self._session()
        for attempt in range(max_retries + 1):
            r = sess.get(url, params=params, timeout=30)
            if r.status_code in (429, 502, 503, 504) and attempt < max_retries:
                wait = 2 ** attempt
                print(f"[RETRY] {r.status_code} for {url}, attempt {attempt + 1}/{max_retries}, waiting {wait}s")
                time.sleep(wait)
                continue
            return r
        return r

    def get_bars_day(
            self,
            symbol: str,
            start: date,
            end: date,
            adjusted: bool = False, 
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

        r = self._get_with_retry(url, params)
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

        all_results = []

        while url:
            r = self._get_with_retry(url, params)
            r.raise_for_status()
            body = r.json()
            all_results.extend(body.get("results", []))
            next_url = body.get("next_url")
            if next_url:
                url = next_url
                params = {"apiKey": self.api_key}
            else:
                url = None

        return {"results": all_results}

    def get_ticker(
        self,
        symbol: str,
        market: str = "stocks",
        locale: str = 'us',
        _type: Optional[str] = None,
        active: bool= True,
        order: str = "asc"
) -> Dict[str, any]:

        url = f"{POLYGON_REQUEST_URL}/v3/reference/tickers"
        params = {
            "ticker": symbol,
            "order": order,
            "apiKey": self.api_key,
            "market": market,
            "locale": locale,
            "active": active
        }
        if _type is not None:
            params["type"] = _type

        all_results = []

        while url:
            r = self._get_with_retry(url, params)
            r.raise_for_status()
            body = r.json()
            all_results.extend(body.get("results", []))
            next_url = body.get("next_url")
            if next_url:
                url = next_url
                params = {"apiKey": self.api_key}
            else:
                url = None

        return {"results": all_results}

    def get_financials(
            self,
            symbol: str,
            timeframe: str = "quarterly",
            limit: int = 50,
    ) -> list[Dict[str, Any]]:
        url = f"{POLYGON_REQUEST_URL}/vX/reference/financials"
        params = {
            "ticker": symbol,
            "timeframe": timeframe,
            "order": "desc",
            "limit": limit,
            "apiKey": self.api_key,
        }

        all_results = []

        while url:
            r = self._get_with_retry(url, params)
            if r.status_code == 404:
                print(f"[WARNING] Financials not found for {symbol}, skipping")
                return []
            r.raise_for_status()
            body = r.json()
            all_results.extend(body.get("results", []))
            next_url = body.get("next_url")
            if next_url:
                url = next_url
                params = {"apiKey": self.api_key}
            else:
                url = None

        return all_results

    def get_ticker_details(
            self,
            symbol: str,
    ) -> Dict[str, Any]:
        url = f"{POLYGON_REQUEST_URL}/v3/reference/tickers/{symbol}"
        params = {"apiKey": self.api_key}

        r = self._get_with_retry(url, params)
        if r.status_code == 404:
            print(f"[WARNING] Ticker details not found for {symbol}, skipping")
            return {}
        r.raise_for_status()
        body = r.json()
        return body.get("results", {})

    def get_dividends(
            self,
            symbol: str,
            limit: int = 1000,
    ) -> list[Dict[str, Any]]:
        url = f"{POLYGON_REQUEST_URL}/v3/reference/dividends"
        params = {
            "ticker": symbol,
            "order": "desc",
            "limit": limit,
            "apiKey": self.api_key,
        }

        all_results = []

        while url:
            r = self._get_with_retry(url, params)
            if r.status_code == 404:
                print(f"[WARNING] Dividends not found for {symbol}, skipping")
                return []
            r.raise_for_status()
            body = r.json()
            all_results.extend(body.get("results", []))
            next_url = body.get("next_url")
            if next_url:
                url = next_url
                params = {"apiKey": self.api_key}
            else:
                url = None

        return all_results

FINANCIALS_FIELDS = {
    "income_statement": [
        "revenues",
        "cost_of_revenue",
        "gross_profit",
        "operating_income_loss",
        "net_income_loss",
        "basic_earnings_per_share",
        "diluted_earnings_per_share",
    ],
    "balance_sheet": [
        "assets",
        "current_assets",
        "noncurrent_assets",
        "liabilities",
        "current_liabilities",
        "equity",
    ],
    "cash_flow_statement": [
        "net_cash_flow_from_operating_activities",
        "net_cash_flow_from_investing_activities",
        "net_cash_flow_from_financing_activities",
    ],
}

FINANCIALS_REQUIRED_COLUMNS = {
    "ticker", "fiscal_period", "fiscal_year", "start_date", "end_date", "filing_date",
}

COMPANY_REQUIRED_COLUMNS = {"ticker", "name", "market_cap"}
COMPANY_OPTIONAL_COLUMNS = {
    "sic_code", "sic_description", "total_employees",
    "list_date", "weighted_shares_outstanding", "description",
    "composite_figi",
}
COMPANY_ALLOWED_COLUMNS = COMPANY_REQUIRED_COLUMNS | COMPANY_OPTIONAL_COLUMNS


def flatten_financials(results: list[dict], symbol: str) -> pd.DataFrame | None:
    rows = []
    for filing in results:
        row = {
            "ticker": symbol,
            "fiscal_period": filing.get("fiscal_period"),
            "fiscal_year": filing.get("fiscal_year"),
            "start_date": filing.get("start_date"),
            "end_date": filing.get("end_date"),
            "filing_date": filing.get("filing_date"),
            "source_filing_url": filing.get("source_filing_url"),
        }

        financials = filing.get("financials", {})
        for statement_name, field_keys in FINANCIALS_FIELDS.items():
            statement = financials.get(statement_name, {})
            for key in field_keys:
                item = statement.get(key, {})
                row[key] = item.get("value") if isinstance(item, dict) else None

        rows.append(row)

    if not rows:
        return None

    return pd.DataFrame(rows)


def flatten_company_overview(details: dict, symbol: str) -> pd.DataFrame | None:
    if not details:
        return None

    row = {
        "ticker": symbol,
        "name": details.get("name"),
        "composite_figi": details.get("composite_figi"),
        "sic_code": details.get("sic_code"),
        "sic_description": details.get("sic_description"),
        "market_cap": details.get("market_cap"),
        "weighted_shares_outstanding": details.get("weighted_shares_outstanding"),
        "total_employees": details.get("total_employees"),
        "list_date": details.get("list_date"),
        "description": details.get("description"),
    }

    return pd.DataFrame([row])


def validate_financials_schema(df: pd.DataFrame) -> None:
    incoming_cols = set(df.columns)

    missing = FINANCIALS_REQUIRED_COLUMNS - incoming_cols
    if missing:
        raise ValueError(f"Schema violation: missing required columns in financials data: {missing}")

    if df["filing_date"].isna().all():
        raise ValueError("Schema violation: all filing_date values are null")

    for col in ["fiscal_period", "fiscal_year"]:
        if df[col].isna().any():
            raise ValueError(f"Schema violation: null values in required column {col}")


def validate_company_schema(df: pd.DataFrame) -> None:
    incoming_cols = set(df.columns)

    missing = COMPANY_REQUIRED_COLUMNS - incoming_cols
    if missing:
        raise ValueError(f"Schema violation: missing required columns in company data: {missing}")

    if df["ticker"].isna().any():
        raise ValueError("Schema violation: null ticker in company data")

    if df["market_cap"].isna().any():
        print("[WARNING] null market_cap in company data")


def save_financials_parquet(
    rows_df_in: pd.DataFrame,
    symbol: str,
    canonical_base_dir: Path = FINANCIALS_BASE_DIR,
) -> pd.DataFrame | None:
    if rows_df_in is None or rows_df_in.empty:
        print("No financials data to save. Quitting...")
        return None

    validate_financials_schema(rows_df_in)

    rows_df = rows_df_in.copy()
    incoming_count = len(rows_df)

    for col in ["start_date", "end_date", "filing_date"]:
        rows_df[col] = pd.to_datetime(rows_df[col]).dt.normalize()

    dup_count = rows_df.duplicated(subset=["ticker", "fiscal_period", "fiscal_year"]).sum()
    if dup_count:
        print(f"WARNING: {dup_count} duplicate filings in incoming payload. Deduplicating.")
        rows_df = rows_df.drop_duplicates(subset=["ticker", "fiscal_period", "fiscal_year"], keep="first")

    canonical_path = canonical_base_dir / symbol / "financials.parquet"
    canonical_path.parent.mkdir(parents=True, exist_ok=True)

    existing_count = 0
    if canonical_path.exists():
        prev_data = pd.read_parquet(canonical_path)
        existing_count = len(prev_data)
        df = pd.concat([prev_data, rows_df], ignore_index=True)
    else:
        df = rows_df

    df = df.drop_duplicates(subset=["ticker", "fiscal_period", "fiscal_year"], keep="last")
    df = df.sort_values(by=["fiscal_year", "fiscal_period"]).reset_index(drop=True)

    tmp_path = canonical_path.with_suffix(f".tmp.{uuid.uuid4().hex}.parquet")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(canonical_path)

    final_count = len(df)
    duplicates_removed = incoming_count + existing_count - final_count

    print(f"Wrote financials data to {canonical_path}")
    print(json.dumps({
        "symbol": symbol,
        "incoming_rows": incoming_count,
        "existing_rows": existing_count,
        "final_rows": final_count,
        "duplicates_removed": duplicates_removed,
    }))

    return df


def save_company_parquet(
    rows_df_in: pd.DataFrame,
    symbol: str,
    canonical_base_dir: Path = COMPANY_BASE_DIR,
) -> pd.DataFrame | None:
    if rows_df_in is None or rows_df_in.empty:
        print("No company data to save. Quitting...")
        return None

    validate_company_schema(rows_df_in)

    df = rows_df_in.copy()

    canonical_path = canonical_base_dir / symbol / "company.parquet"
    canonical_path.parent.mkdir(parents=True, exist_ok=True)

    cols = ["ticker", "name", "composite_figi", "sic_code", "sic_description",
            "market_cap", "weighted_shares_outstanding", "total_employees",
            "list_date", "description"]
    cols_present = [c for c in cols if c in df.columns]
    df = df[cols_present]

    tmp_path = canonical_path.with_suffix(f".tmp.{uuid.uuid4().hex}.parquet")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(canonical_path)

    print(f"Wrote company data to {canonical_path}")
    print(json.dumps({"symbol": symbol}))

    return df


DIVIDENDS_REQUIRED_COLUMNS = {"ticker", "cash_amount", "ex_dividend_date"}
DIVIDENDS_OPTIONAL_COLUMNS = {"pay_date", "declaration_date", "record_date", "frequency", "dividend_type"}


def flatten_dividends(results: list[dict], symbol: str) -> pd.DataFrame | None:
    if not results:
        return None

    rows = []
    for d in results:
        rows.append({
            "ticker": symbol,
            "ex_dividend_date": d.get("ex_dividend_date"),
            "pay_date": d.get("pay_date"),
            "declaration_date": d.get("declaration_date"),
            "record_date": d.get("record_date"),
            "cash_amount": d.get("cash_amount"),
            "frequency": d.get("frequency"),
            "dividend_type": d.get("dividend_type"),
        })

    return pd.DataFrame(rows)


def validate_dividends_schema(df: pd.DataFrame) -> None:
    incoming_cols = set(df.columns)

    missing = DIVIDENDS_REQUIRED_COLUMNS - incoming_cols
    if missing:
        raise ValueError(f"Schema violation: missing required columns in dividends data: {missing}")

    if df["ex_dividend_date"].isna().any():
        raise ValueError("Schema violation: null ex_dividend_date values in dividends data")

    if df["cash_amount"].isna().any():
        print("[WARNING] null cash_amount in dividends data")


def save_dividends_parquet(
    rows_df_in: pd.DataFrame,
    symbol: str,
    canonical_base_dir: Path = DIVIDENDS_BASE_DIR,
) -> pd.DataFrame | None:
    if rows_df_in is None or rows_df_in.empty:
        print("No dividends data to save. Quitting...")
        return None

    validate_dividends_schema(rows_df_in)

    rows_df = rows_df_in.copy()
    incoming_count = len(rows_df)

    rows_df["ex_dividend_date"] = pd.to_datetime(rows_df["ex_dividend_date"]).dt.normalize()
    for col in ["pay_date", "declaration_date", "record_date"]:
        if col in rows_df.columns:
            rows_df[col] = pd.to_datetime(rows_df[col], errors="coerce").dt.normalize()

    rows_df["cash_amount"] = rows_df["cash_amount"].astype("float64")

    dup_count = rows_df.duplicated(subset=["ticker", "ex_dividend_date", "cash_amount"]).sum()
    if dup_count:
        print(f"WARNING: {dup_count} duplicate dividends in incoming payload. Deduplicating.")
        rows_df = rows_df.drop_duplicates(subset=["ticker", "ex_dividend_date", "cash_amount"], keep="first")

    canonical_path = canonical_base_dir / symbol / "dividends.parquet"
    canonical_path.parent.mkdir(parents=True, exist_ok=True)

    existing_count = 0
    if canonical_path.exists():
        prev_data = pd.read_parquet(canonical_path)
        existing_count = len(prev_data)
        df = pd.concat([prev_data, rows_df], ignore_index=True)
    else:
        df = rows_df

    df = df.drop_duplicates(subset=["ticker", "ex_dividend_date", "cash_amount"], keep="last")
    df = df.sort_values(by=["ex_dividend_date"]).reset_index(drop=True)

    tmp_path = canonical_path.with_suffix(f".tmp.{uuid.uuid4().hex}.parquet")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(canonical_path)

    final_count = len(df)
    duplicates_removed = incoming_count + existing_count - final_count

    print(f"Wrote dividends data to {canonical_path}")
    print(json.dumps({
        "symbol": symbol,
        "incoming_rows": incoming_count,
        "existing_rows": existing_count,
        "final_rows": final_count,
        "duplicates_removed": duplicates_removed,
    }))

    return df


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
    df["split_from"] = df["split_from"].astype("float64")
    df["split_to"] = df["split_to"].astype("float64")

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


    

    

        