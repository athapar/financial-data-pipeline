from datetime import date, timedelta

from financial_data_pipeline.cli import resolve_fetch_window


def test_manual_window():
    sync_state = {}

    result = resolve_fetch_window(
        symbol="AAPL",
        sync_state=sync_state,
        start="2025-01-01",
        end="2025-01-10",
    )

    assert result.mode == "manual"
    assert result.start.isoformat() == "2025-01-01"
    assert result.end.isoformat() == "2025-01-10"


def test_manual_without_end_uses_today():
    sync_state = {}

    result = resolve_fetch_window(
        symbol="AAPL",
        sync_state=sync_state,
        start="2025-01-01",
        end=None,
    )

    assert result.mode == "manual"
    assert result.start.isoformat() == "2025-01-01"


def test_incremental_window():
    today = date.today()
    prev = today - timedelta(days=5)

    sync_state = {"AAPL_daily": prev.isoformat()}

    result = resolve_fetch_window(
        symbol="AAPL",
        sync_state=sync_state,
        start=None,
        end=None,
    )

    assert result.mode == "incremental"
    assert result.start == prev + timedelta(days=1)
    assert result.end == today


def test_bootstrap_window():
    today = date.today()

    result = resolve_fetch_window(
        symbol="AAPL",
        sync_state={},
        start=None,
        end=None,
    )

    assert result.mode == "bootstrap"
    assert result.start == date(2006, 1, 1)
    assert result.end == today


def test_invalid_manual_window_raises():
    sync_state = {}

    try:
        resolve_fetch_window(
            symbol="AAPL",
            sync_state=sync_state,
            start="2025-02-01",
            end="2025-01-01",
        )
        assert False
    except ValueError:
        assert True