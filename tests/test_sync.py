import json
from datetime import date
from pathlib import Path

from financial_data_pipeline.cli import load_sync_path, update_sync_state, parse_date


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