"""
Backfill missing rows in account2/logs/lp_intervals.csv by reconstructing
each orphan interval from lp_fills.csv + each market's resolved outcome.

Why this exists: dual_side_lp.py used to abort cleanup if the worker thread
was killed mid-interval (SIGTERM/SIGINT). Fills made it to lp_fills.csv but
the per-interval summary row was never written. The bot's been fixed; this
script recovers the historical orphan intervals.

Usage:
    python analysis/backfill_missing_intervals.py [--dry-run]

Reads:
    /home/alan/Desktop/account2/logs/lp_fills.csv
    /home/alan/Desktop/account2/logs/lp_intervals.csv  (so we don't duplicate)

Writes:
    appends rows to /home/alan/Desktop/account2/logs/lp_intervals.csv
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

import requests

ACCOUNT2_LOGS = Path("/home/alan/Desktop/account2/logs")
FILLS_CSV = ACCOUNT2_LOGS / "lp_fills.csv"
INTERVALS_CSV = ACCOUNT2_LOGS / "lp_intervals.csv"
GAMMA_HOST = "https://gamma-api.polymarket.com"

INTERVAL_HEADERS = [
    "timestamp", "asset", "interval", "slug", "beat_price",
    "up_fills", "down_fills", "up_avg_price", "down_avg_price",
    "pairs", "pair_sum", "edge_per_pair", "total_edge",
    "excess_side", "excess_qty",
    "net_pnl", "oracle_side", "total_cost", "revenue", "fill_volume",
    "adverse_events", "duration_secs", "avg_spread_captured",
    "orders_placed", "orders_filled", "fill_rate", "avg_fill_time_secs",
    "leftover_up", "leftover_down", "leftover_cost",
]


def fetch_resolution(slug: str) -> dict | None:
    """Hit Gamma /events?slug=… to get outcomePrices for a resolved market.
    Returns {'closed': bool, 'up_won': bool|None, 'title': str} or None."""
    try:
        r = requests.get(f"{GAMMA_HOST}/events", params={"slug": slug, "limit": 1}, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        if not data:
            return None
        event = data[0]
        markets = event.get("markets", [])
        if not markets:
            return None
        m = markets[0]
        if not m.get("closed"):
            return {"closed": False, "up_won": None, "title": event.get("title", "")}
        # outcomePrices is a JSON-string like '["0", "1"]' — first = "Up", second = "Down"
        op = m.get("outcomePrices") or "[]"
        if isinstance(op, str):
            import json as _json
            op = _json.loads(op)
        outcomes = m.get("outcomes") or ["Up", "Down"]
        # Find the index of "Up" and check its price
        try:
            up_idx = [o.lower() for o in outcomes].index("up")
        except ValueError:
            up_idx = 0
        up_won = float(op[up_idx]) >= 0.5
        return {
            "closed": True,
            "up_won": up_won,
            "title": event.get("title", ""),
            "outcomes": outcomes,
            "outcome_prices": [float(x) for x in op],
        }
    except Exception as e:
        print(f"  fetch_resolution({slug}) error: {e}", file=sys.stderr)
        return None


def aggregate_fills_by_slug() -> dict:
    """Group lp_fills.csv rows by (asset, slug). Returns
        {(asset, slug): {'asset','slug','interval','rows':[fill],'oracle_side','beat_price'}}.
    Only rows where source != 'API_SELL' are counted (we never sold)."""
    by_slug: dict = {}
    with open(FILLS_CSV) as f:
        for row in csv.DictReader(f):
            asset = row["asset"]
            slug = row["slug"]
            interval = int(row["interval"])
            key = (asset, slug)
            bucket = by_slug.setdefault(key, {
                "asset": asset, "slug": slug, "interval": interval,
                "rows": [], "oracle_side_seen": set(), "beat_prices": set(),
                "first_ts": row["timestamp"],
            })
            bucket["rows"].append(row)
            if row.get("oracle_side"):
                bucket["oracle_side_seen"].add(row["oracle_side"])
            if row.get("beat_price"):
                bucket["beat_prices"].add(row["beat_price"])
    return by_slug


def existing_interval_keys() -> set:
    """Set of (asset, slug) already recorded in lp_intervals.csv."""
    keys = set()
    if not INTERVALS_CSV.exists():
        return keys
    with open(INTERVALS_CSV) as f:
        for row in csv.DictReader(f):
            keys.add((row["asset"], row["slug"]))
    return keys


def build_interval_row(bucket: dict, resolution: dict | None) -> dict:
    """Compute an INTERVAL_HEADERS row from one bucket of fills."""
    asset = bucket["asset"]
    slug = bucket["slug"]
    interval_min = bucket["interval"]
    rows = bucket["rows"]

    # Sum BUY fills only (we never placed sell orders in this bot's history)
    up_qty = down_qty = 0
    up_cost = down_cost = 0.0
    for r in rows:
        qty = int(r["qty"])
        price = float(r["price"])
        if qty <= 0:
            continue  # would be a sell — none in our data
        if r["side"] == "UP":
            up_qty += qty
            up_cost += qty * price
        elif r["side"] == "DOWN":
            down_qty += qty
            down_cost += qty * price

    up_avg = up_cost / up_qty if up_qty else 0.0
    down_avg = down_cost / down_qty if down_qty else 0.0
    pairs = min(up_qty, down_qty)
    excess_side = ""
    excess_qty = 0
    if up_qty > down_qty:
        excess_side, excess_qty = "UP", up_qty - down_qty
    elif down_qty > up_qty:
        excess_side, excess_qty = "DOWN", down_qty - up_qty

    pair_sum = (up_avg + down_avg) if pairs > 0 else 0.0
    edge_per_pair = (1.0 - pair_sum) if pairs > 0 else 0.0
    total_edge = pairs * edge_per_pair
    total_cost = up_cost + down_cost

    # Net PnL given the resolved outcome.
    #   - paired shares: each pair pays $1 regardless of outcome → revenue = pairs
    #   - excess shares: pay $1 if winning side, $0 if losing
    revenue = 0.0
    if resolution and resolution.get("closed"):
        revenue += pairs * 1.0  # pairs always net $1
        if excess_qty > 0:
            up_won = resolution["up_won"]
            if (excess_side == "UP" and up_won) or (excess_side == "DOWN" and not up_won):
                revenue += excess_qty * 1.0
    net_pnl = revenue - total_cost if (resolution and resolution.get("closed")) else ""

    # Best-effort beat_price / oracle_side from the fill rows
    beat_price = ""
    if bucket["beat_prices"]:
        # take any single value — they should agree within an interval
        beat_price = next(iter(bucket["beat_prices"]))
    oracle_side = ""
    if len(bucket["oracle_side_seen"]) == 1:
        oracle_side = next(iter(bucket["oracle_side_seen"]))

    leftover_up = up_qty
    leftover_down = down_qty
    leftover_cost = total_cost

    return {
        "timestamp": bucket["first_ts"],
        "asset": asset,
        "interval": interval_min,
        "slug": slug,
        "beat_price": beat_price,
        "up_fills": up_qty,
        "down_fills": down_qty,
        "up_avg_price": f"{up_avg:.4f}" if up_qty else "",
        "down_avg_price": f"{down_avg:.4f}" if down_qty else "",
        "pairs": pairs,
        "pair_sum": f"{pair_sum:.4f}" if pairs else "",
        "edge_per_pair": f"{edge_per_pair:.4f}" if pairs else "",
        "total_edge": f"{total_edge:.4f}" if pairs else "",
        "excess_side": excess_side,
        "excess_qty": excess_qty,
        "net_pnl": f"{net_pnl:.4f}" if isinstance(net_pnl, float) else "",
        "oracle_side": oracle_side,
        "total_cost": f"{total_cost:.4f}",
        "revenue": f"{revenue:.4f}" if (resolution and resolution.get("closed")) else "",
        "fill_volume": "",  # not derivable from fills alone
        "adverse_events": "",
        "duration_secs": interval_min * 60,
        "avg_spread_captured": "",
        "orders_placed": "",
        "orders_filled": "",
        "fill_rate": "",
        "avg_fill_time_secs": "",
        "leftover_up": leftover_up,
        "leftover_down": leftover_down,
        "leftover_cost": f"{leftover_cost:.4f}",
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="Print what would be added; don't write")
    args = ap.parse_args()

    by_slug = aggregate_fills_by_slug()
    existing = existing_interval_keys()
    orphans = [k for k in by_slug if k not in existing]

    if not orphans:
        print("No orphan intervals — every fill already has a corresponding interval row.")
        return

    print(f"Found {len(orphans)} orphan (asset, slug) pairs in fills with no interval row:\n")

    new_rows = []
    for asset, slug in sorted(orphans):
        bucket = by_slug[(asset, slug)]
        up_qty = sum(int(r["qty"]) for r in bucket["rows"] if r["side"] == "UP")
        down_qty = sum(int(r["qty"]) for r in bucket["rows"] if r["side"] == "DOWN")
        print(f"  {asset:8s} {slug:42s}  fills: U{up_qty}/D{down_qty}")
        resolution = fetch_resolution(slug)
        if resolution is None:
            print(f"    └─ couldn't fetch resolution; skipping")
            continue
        if not resolution.get("closed"):
            print(f"    └─ market not closed yet; skipping (will get a row when it resolves)")
            continue
        winner = "UP" if resolution["up_won"] else "DOWN"
        row = build_interval_row(bucket, resolution)
        net = row["net_pnl"]
        cost = row["total_cost"]
        pairs = row["pairs"]
        excess = f"{row['excess_qty']} {row['excess_side']}" if row["excess_qty"] else "—"
        print(f"    └─ winner={winner}  pairs={pairs}  excess={excess}  cost=${cost}  net_pnl=${net}")
        new_rows.append(row)

    if not new_rows:
        print("\nNothing to write.")
        return

    if args.dry_run:
        print(f"\n--dry-run set — would append {len(new_rows)} rows to {INTERVALS_CSV}.")
        return

    with open(INTERVALS_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, INTERVAL_HEADERS)
        for r in new_rows:
            w.writerow(r)
    print(f"\nAppended {len(new_rows)} rows to {INTERVALS_CSV}.")


if __name__ == "__main__":
    main()
