import pandas as pd
from pathlib import Path


class MockClient:
    def get_bars_day(self, symbol, start, end):
        return {
            "results": [
                {
                    "t": 1735689600000,
                    "o": 100.0,
                    "h": 101.0,
                    "l": 99.0,
                    "c": 100.5,
                    "v": 1000.0,
                }
            ]
        }


def test_run_symbol_ingestion_bootstrap(tmp_path: Path):
    from financial_data_pipeline.orchestrator import run_symbol_ingestion

    client = MockClient()
    sync_state = {}

    result = run_symbol_ingestion(
        symbol="TEST",
        client=client,
        sync_state=sync_state,
        canonical_base_dir=tmp_path,
    )

    assert result["status"] == "success"
    assert result["mode"] == "bootstrap"

    parquet_path = tmp_path / "TEST" / "bars.parquet"
    assert parquet_path.exists()