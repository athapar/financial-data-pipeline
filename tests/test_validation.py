import pandas as pd
import pytest
from pathlib import Path

from financial_data_pipeline.polygon import validate_schema, merge_df_with_parquet


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