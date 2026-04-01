import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from financial_data_pipeline.polygon import validate_bars_schema
from financial_data_pipeline.cli import load_sync_path, update_sync_state, parse_date


def test_validate_schema_fails_on_non_numeric_required_column():
    bad_df = pd.DataFrame([
        {
            "t": 1735689600000,
            "o": "bad",
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000.0,
        }
    ])

    with pytest.raises(ValueError, match="column 'o' must be numeric"):
        validate_bars_schema(bad_df)


def test_validate_schema_fails_on_invalid_timestamp():
    bad_df = pd.DataFrame([
        {
            "t": "not_a_timestamp",
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000.0,
        }
    ])

    with pytest.raises(ValueError, match="could not be parsed as epoch milliseconds"):
        validate_bars_schema(bad_df)


def test_validate_schema_fails_on_non_finite_required_numeric():
    bad_df = pd.DataFrame([
        {
            "t": 1735689600000,
            "o": np.inf,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000.0,
        }
    ])

    with pytest.raises(ValueError, match="non-finite values detected in column 'o'"):
        validate_bars_schema(bad_df)


def test_validate_schema_fails_when_optional_column_present_but_non_numeric():
    bad_df = pd.DataFrame([
        {
            "t": 1735689600000,
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000.0,
            "vw": "bad_optional_value",
        }
    ])

    with pytest.raises(ValueError, match="optional column 'vw' must be numeric"):
        validate_bars_schema(bad_df)