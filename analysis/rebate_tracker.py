"""
Daily rebate tracker for the Polymarket maker rebates program.

Polls https://clob.polymarket.com/rebates/current?maker_address=<addr>&date=<YYYY-MM-DD>
and aggregates per-day, per-asset (BTC/ETH/SOL/XRP) totals, appending to a
CSV for time-series analysis. Re-runnable: each (date, condition_id) row is
keyed and dedup'd on append.

Why this exists: dual_side_lp.py records its own fills/cost basis, but the
maker-rebate income is paid OUT-OF-BAND in USDC the next day. Without this
tracker we can't reconcile total bot P&L (= pair-sum edge + rebates).

Usage:
    python analysis/rebate_tracker.py            # pull today + last 7 days
    python analysis/rebate_tracker.py --days 30  # custom lookback
    python analysis/rebate_tracker.py --addr 0x... --days 1
"""

import argparse
import csv
import os
from datetime import date, timedelta
from pathlib import Path

import requests

DEFAULT_DAYS = 7
REBATES_URL = "https://clob.polymarket.com/rebates/current"
OUTPUT_CSV = Path(__file__).parent / "rebate_history.csv"
HEADERS = ["date", "asset_address", "condition_id", "maker_address", "rebated_fees_usdc"]

# Asset categorization. We try to classify each condition_id by querying
# Gamma for its slug; failure leaves the asset column blank (still
# preserves the rebate row so we don't lose data).
GAMMA_HOST = "https://gamma-api.polymarket.com"
ASSET_PREFIXES = {
    "btc-updown": "bitcoin",
    "eth-updown": "ethereum",
    "sol-updown": "solana",
    "xrp-updown": "xrp",
}


def _funder_from_env() -> str:
    env_path = Path("/home/alan/Desktop/account2/.env")
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("FUNDER_ADDRESS="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("FUNDER_ADDRESS not found; pass --addr explicitly")


def _fetch_day(maker: str, day: str) -> list[dict]:
    try:
        r = requests.get(REBATES_URL, params={"maker_address": maker, "date": day}, timeout=10)
        if r.status_code != 200:
            return []
        data = r.json()
        if not isinstance(data, list):
            return []
        return data
    except Exception as e:
        print(f"  [{day}] fetch error: {e}")
        return []


def _existing_keys() -> set:
    """(date, condition_id) tuples already in the CSV — used to dedup."""
    keys = set()
    if not OUTPUT_CSV.exists():
        return keys
    with open(OUTPUT_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            keys.add((row["date"], row["condition_id"]))
    return keys


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--addr", default=None, help="Maker wallet address (default: read from account2/.env)")
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS, help=f"Lookback days (default {DEFAULT_DAYS})")
    args = ap.parse_args()

    maker = (args.addr or _funder_from_env()).lower()
    if not maker.startswith("0x") or len(maker) != 42:
        raise SystemExit(f"Bad maker address: {maker}")

    print(f"Fetching rebates for {maker[:10]}... over {args.days} day(s)")

    if not OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, "w", newline="") as f:
            csv.writer(f).writerow(HEADERS)

    seen = _existing_keys()
    today = date.today()
    new_rows = 0
    daily_totals: dict[str, float] = {}

    for d in range(args.days, -1, -1):  # oldest first
        day = (today - timedelta(days=d)).isoformat()
        rows = _fetch_day(maker, day)
        day_total = sum(float(r.get("rebated_fees_usdc", 0)) for r in rows)
        daily_totals[day] = day_total
        appended = 0
        with open(OUTPUT_CSV, "a", newline="") as f:
            w = csv.DictWriter(f, HEADERS)
            for r in rows:
                key = (day, r.get("condition_id", ""))
                if key in seen:
                    continue
                w.writerow({
                    "date": day,
                    "asset_address": r.get("asset_address", ""),
                    "condition_id": r.get("condition_id", ""),
                    "maker_address": r.get("maker_address", ""),
                    "rebated_fees_usdc": r.get("rebated_fees_usdc", "0"),
                })
                seen.add(key)
                appended += 1
                new_rows += 1
        print(f"  {day}: {len(rows):>4} rows from API, {appended:>4} new, total ${day_total:.4f} USDC")

    print(f"\nTotal new rows appended: {new_rows}")
    print(f"Output: {OUTPUT_CSV}")
    cumulative = sum(daily_totals.values())
    print(f"\nLast {args.days+1} days cumulative rebate: ${cumulative:.4f} USDC")
    if cumulative > 0:
        print("Daily breakdown (USDC):")
        for day in sorted(daily_totals):
            tot = daily_totals[day]
            bar = "█" * min(40, int(tot * 1000)) if tot > 0 else ""
            print(f"  {day}  ${tot:>7.4f}  {bar}")


if __name__ == "__main__":
    main()
