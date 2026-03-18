import pandas as pd

from financial_data_pipeline.transforms import compute_daily_returns


def test_compute_daily_returns():
    df = pd.DataFrame([
        {"t": pd.Timestamp("2026-01-01", tz="UTC"), "c": 100.0},
        {"t": pd.Timestamp("2026-01-02", tz="UTC"), "c": 110.0},
        {"t": pd.Timestamp("2026-01-03", tz="UTC"), "c": 99.0},
    ])

    result = compute_daily_returns(df)

    assert "daily_return" in result.columns
    assert pd.isna(result.iloc[0]["daily_return"])
    assert round(result.iloc[1]["daily_return"], 6) == 0.10
    assert round(result.iloc[2]["daily_return"], 6) == -0.10


def test_compute_daily_returns_empty_df():
    df = pd.DataFrame()
    result = compute_daily_returns(df)
    assert result.empty