import pandas as pd
import pytest
from pathlib import Path
import json
from datetime import date

from financial_data_pipeline.polygon import validate_schema, merge_df_with_parquet
from financial_data_pipeline.cli import load_sync_path, update_sync_state, parse_date

def make_required_df():
    return pd.DataFrame([
        {
            "t": 1735689600000,
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000.0,
        },
        {
            "t": 1735776000000,
            "o": 101.0,
            "h": 102.0,
            "l": 100.0,
            "c": 101.5,
            "v": 1200.0,
        },
    ])


def test_validate_schema_fails_on_missing_required_column():
    bad_df = pd.DataFrame([
        {
            "t": 1735689600000,
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            # "v" missing
        }
    ])

    with pytest.raises(ValueError, match="missing required columns"):
        validate_schema(bad_df)


def test_merge_writes_parquet_without_optional_columns(tmp_path: Path):
    df = make_required_df()

    result = merge_df_with_parquet(df, symbol="TEST", canonical_base_dir=tmp_path)

    assert result is not None
    assert list(result.columns) == ["t", "o", "h", "l", "c", "v"]

    parquet_path = tmp_path / "TEST" / "bars.parquet"
    assert parquet_path.exists()

    written = pd.read_parquet(parquet_path)
    assert list(written.columns) == ["t", "o", "h", "l", "c", "v"]
    assert len(written) == 2


def test_merge_deduplicates_duplicate_timestamps_in_incoming_batch(tmp_path: Path):
    df = pd.DataFrame([
        {
            "t": 1735689600000,
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000.0,
        },
        {
            "t": 1735689600000,
            "o": 200.0,
            "h": 201.0,
            "l": 199.0,
            "c": 200.5,
            "v": 2000.0,
        },
    ])

    result = merge_df_with_parquet(df, symbol="TEST", canonical_base_dir=tmp_path)

    assert result is not None
    assert len(result) == 1
    assert result.iloc[0]["c"] == 200.5


def test_merge_deduplicates_overlap_with_existing_parquet(tmp_path: Path):
    first_df = pd.DataFrame([
        {
            "t": 1735689600000,
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000.0,
        },
        {
            "t": 1735776000000,
            "o": 101.0,
            "h": 102.0,
            "l": 100.0,
            "c": 101.5,
            "v": 1200.0,
        },
    ])

    second_df = pd.DataFrame([
        {
            "t": 1735776000000,
            "o": 999.0,
            "h": 999.0,
            "l": 999.0,
            "c": 999.0,
            "v": 999.0,
        },
        {
            "t": 1735862400000,
            "o": 102.0,
            "h": 103.0,
            "l": 101.0,
            "c": 102.5,
            "v": 1300.0,
        },
    ])

    merge_df_with_parquet(first_df, symbol="TEST", canonical_base_dir=tmp_path)
    result = merge_df_with_parquet(second_df, symbol="TEST", canonical_base_dir=tmp_path)

    assert result is not None
    assert len(result) == 3

    overlap_row = result[result["c"] == 999.0]
    assert len(overlap_row) == 1


def test_merge_returns_none_on_empty_dataframe(tmp_path: Path):
    empty_df = pd.DataFrame()

    result = merge_df_with_parquet(empty_df, symbol="TEST", canonical_base_dir=tmp_path)

    assert result is None



def test_load_sync_path_bootstraps_empty_file(tmp_path: Path):
    sync_path = tmp_path / "data" / "sync_state.json"

    state = load_sync_path(sync_path)

    assert state == {}
    assert sync_path.exists()
    assert json.loads(sync_path.read_text(encoding="utf-8")) == {}


def test_update_sync_state_writes_symbol_date(tmp_path: Path):
    sync_path = tmp_path / "sync_state.json"

    update_sync_state(sync_path, "AAPL", "2026-03-16")

    state = json.loads(sync_path.read_text(encoding="utf-8"))
    assert state["AAPL_daily"] == "2026-03-16"


def test_parse_date_returns_date_for_valid_iso_string():
    parsed = parse_date("2026-03-16")
    assert parsed == date(2026, 3, 16)


def test_parse_date_returns_none_for_none():
    assert parse_date(None) is None


def test_sync_state_monotonicity_example(tmp_path: Path):
    """
    This tests the monotonicity rule at the state layer:
    if existing sync date is newer than a proposed older date,
    the stored checkpoint should remain at the newer date.

    Since current update_sync_state() blindly writes, this test documents
    the desired behavior and should fail until monotonic logic is enforced.
    """
    sync_path = tmp_path / "sync_state.json"

    # seed newer date
    update_sync_state(sync_path, "TSLA", "2026-03-16")

    # simulate attempted rollback
    state_before = json.loads(sync_path.read_text(encoding="utf-8"))
    assert state_before["TSLA_daily"] == "2026-03-16"

    # Current implementation would overwrite this.
    # Desired behavior is to preserve 2026-03-16.
    # Replace this block once monotonic guard is implemented.