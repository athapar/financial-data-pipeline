"""
One-time script to generate the historical ticker seed for the SCD2 security master.

Pulls ticker events from Polygon's vX/reference/tickers/{id}/events endpoint
for every symbol in symbols.txt, then reconstructs the full ticker-to-FIGI
mapping history as a CSV seed for dbt.

Each event is a ticker_change with a date — the date the symbol started trading
under that ticker. Multiple events for the same FIGI produce multiple SCD2 rows
with non-overlapping validity windows.

Usage:
    python scripts/generate_ticker_history_seed.py
"""
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from financial_data_pipeline.config import POLYGON_API_KEY, PROJECT_ROOT
from financial_data_pipeline.polygon import PolygonClient


def build_history_from_events(symbol: str, client: PolygonClient) -> list[dict]:
    events_data = client.get_ticker_events(symbol)
    if not events_data:
        return []

    composite_figi = events_data.get("composite_figi")
    name = events_data.get("name", "")
    if not composite_figi:
        print(f"  [SKIP] no composite_figi")
        return []

    events = events_data.get("events", [])

    ticker_changes = []
    for event in events:
        if event.get("type") == "ticker_change":
            tc = event.get("ticker_change", {})
            ticker_changes.append({
                "ticker": tc.get("ticker"),
                "date": event.get("date"),
            })

    ticker_changes.sort(key=lambda x: x["date"])

    if not ticker_changes:
        return [{
            "composite_figi": composite_figi,
            "ticker": symbol,
            "name": name,
            "valid_from": "1990-01-01",
            "valid_to": "",
        }]

    rows = []
    for i, tc in enumerate(ticker_changes):
        valid_from = tc["date"]
        valid_to = ticker_changes[i + 1]["date"] if i + 1 < len(ticker_changes) else ""

        rows.append({
            "composite_figi": composite_figi,
            "ticker": tc["ticker"],
            "name": name,
            "valid_from": valid_from,
            "valid_to": valid_to,
        })

    return rows


def main():
    client = PolygonClient(api_key=POLYGON_API_KEY)

    symbols_path = PROJECT_ROOT / "symbols.txt"
    symbols = [s.strip() for s in symbols_path.read_text().splitlines() if s.strip()]

    all_rows = []
    symbols_with_changes = 0

    for symbol in symbols:
        print(f"Processing {symbol}...", end="")
        try:
            rows = build_history_from_events(symbol, client)
            if len(rows) > 1:
                symbols_with_changes += 1
                tickers = [r["ticker"] for r in rows]
                print(f" {' -> '.join(tickers)}")
            else:
                print(f" (stable)")
            all_rows.extend(rows)
        except Exception as e:
            print(f" [ERROR] {e}")

    output_path = PROJECT_ROOT / "warehouse" / "seeds" / "ticker_history_seed.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "composite_figi", "ticker", "name", "valid_from", "valid_to",
        ])
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nWrote {len(all_rows)} rows to {output_path}")
    print(f"Symbols with ticker changes: {symbols_with_changes}")


if __name__ == "__main__":
    main()
