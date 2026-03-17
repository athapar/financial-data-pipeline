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

    changed = update_sync_state(sync_path, "AAPL", "2026-03-16")

    state = json.loads(sync_path.read_text(encoding="utf-8"))
    assert changed is True
    assert state["AAPL_daily"] == "2026-03-16"


def test_update_sync_state_does_not_move_backward(tmp_path: Path):
    sync_path = tmp_path / "sync_state.json"

    changed_first = update_sync_state(sync_path, "TSLA", "2026-03-16")
    changed_second = update_sync_state(sync_path, "TSLA", "2026-03-10")

    state = json.loads(sync_path.read_text(encoding="utf-8"))
    assert changed_first is True
    assert changed_second is False
    assert state["TSLA_daily"] == "2026-03-16"


def test_update_sync_state_does_not_rewrite_same_date(tmp_path: Path):
    sync_path = tmp_path / "sync_state.json"

    changed_first = update_sync_state(sync_path, "TSLA", "2026-03-16")
    changed_second = update_sync_state(sync_path, "TSLA", "2026-03-16")

    state = json.loads(sync_path.read_text(encoding="utf-8"))
    assert changed_first is True
    assert changed_second is False
    assert state["TSLA_daily"] == "2026-03-16"


def test_parse_date_returns_date_for_valid_iso_string():
    parsed = parse_date("2026-03-16")
    assert parsed == date(2026, 3, 16)


def test_parse_date_returns_none_for_none():
    assert parse_date(None) is None