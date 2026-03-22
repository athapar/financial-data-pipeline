import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from financial_data_pipeline.load_bq import load_parquet_to_bigquery, bq_table


def test_bq_table_format():
    result = bq_table("daily_bars")
    assert result.count(".") == 2  # must be project.dataset.table
    assert result.endswith("daily_bars")


@pytest.fixture
def sample_parquet(tmp_path):
    df = pd.DataFrame({
        "symbol": ["TSLA", "TSLA"],
        "t": pd.to_datetime(["2024-01-02", "2024-01-03"], utc=True),
        "o": [100.0, 101.0],
        "h": [102.0, 103.0],
        "l": [99.0, 100.0],
        "c": [101.0, 102.0],
        "v": [1000.0, 1200.0],
    })
    path = tmp_path / "bars.parquet"
    df.to_parquet(path, index=False)
    return path


def test_write_truncate_disposition(sample_parquet):
    mock_job = MagicMock()
    mock_client = MagicMock()
    mock_client.load_table_from_dataframe.return_value = mock_job

    with patch("financial_data_pipeline.load_bq.bigquery.Client", return_value=mock_client):
        load_parquet_to_bigquery(sample_parquet, "p.d.t", mode="WRITE_TRUNCATE")

    job_config = mock_client.load_table_from_dataframe.call_args.kwargs["job_config"]
    assert job_config.write_disposition == "WRITE_TRUNCATE"


def test_write_append_disposition(sample_parquet):
    mock_job = MagicMock()
    mock_client = MagicMock()
    mock_client.load_table_from_dataframe.return_value = mock_job

    with patch("financial_data_pipeline.load_bq.bigquery.Client", return_value=mock_client):
        load_parquet_to_bigquery(sample_parquet, "p.d.t", mode="WRITE_APPEND")

    job_config = mock_client.load_table_from_dataframe.call_args.kwargs["job_config"]
    assert job_config.write_disposition == "WRITE_APPEND"


def test_job_result_is_awaited(sample_parquet):
    mock_job = MagicMock()
    mock_client = MagicMock()
    mock_client.load_table_from_dataframe.return_value = mock_job

    with patch("financial_data_pipeline.load_bq.bigquery.Client", return_value=mock_client):
        load_parquet_to_bigquery(sample_parquet, "p.d.t")

    mock_job.result.assert_called_once()


def test_loaded_dataframe_has_symbol_column(sample_parquet):
    mock_job = MagicMock()
    mock_client = MagicMock()
    mock_client.load_table_from_dataframe.return_value = mock_job

    with patch("financial_data_pipeline.load_bq.bigquery.Client", return_value=mock_client):
        load_parquet_to_bigquery(sample_parquet, "p.d.t")

    loaded_df = mock_client.load_table_from_dataframe.call_args.args[0]
    assert "symbol" in loaded_df.columns
