"""
Full P&L accounting for dual_side_lp.py historical runs.

Computes total bot return = pair-sum edge + single-leg P&L + maker rebates,
joining three data sources:

  1. account2/logs/lp_intervals.csv — per-interval pair stats and leftover cost
  2. account2/logs/lp_fills.csv     — fallback for slugs missing from intervals
  3. analysis/rebate_history.csv    — per-day per-condition_id rebates (run rebate_tracker first)

For single-leg outcomes, queries Gamma /events?slug=... and caches results
in analysis/.outcome_cache.json so re-runs are fast.

Usage:
    python analysis/bot_pnl_full_accounting.py
    python analysis/bot_pnl_full_accounting.py --refresh-outcomes  # re-fetch all
"""

import argparse
import csv
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests

ANALYSIS_DIR = Path(__file__).parent
INTERVALS_CSV = Path("/home/alan/Desktop/account2/logs/lp_intervals.csv")
FILLS_CSV = Path("/home/alan/Desktop/account2/logs/lp_fills.csv")
REBATE_CSV = ANALYSIS_DIR / "rebate_history.csv"
OUTCOME_CACHE = ANALYSIS_DIR / ".outcome_cache.json"

GAMMA = "https://gamma-api.polymarket.com"


def _load_outcome_cache() -> dict:
    if OUTCOME_CACHE.exists():
        try:
            return json.loads(OUTCOME_CACHE.read_text())
        except Exception:
            return {}
    return {}


def _save_outcome_cache(cache: dict):
    OUTCOME_CACHE.write_text(json.dumps(cache, indent=2))


def fetch_outcome(slug: str, cache: dict) -> dict | None:
    """Returns {'closed': bool, 'up_won': bool|None, 'condition_id': str} or None.
    Cached after first successful fetch (closed markets are immutable)."""
    if slug in cache:
        c = cache[slug]
        if c.get("closed"):
            return c
    try:
        r = requests.get(f"{GAMMA}/events", params={"slug": slug, "limit": 1}, timeout=10)
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
            cache[slug] = {"closed": False, "up_won": None, "condition_id": m.get("conditionId", "")}
            return cache[slug]
        op = m.get("outcomePrices") or "[]"
        if isinstance(op, str):
            op = json.loads(op)
        outcomes = m.get("outcomes") or ["Up", "Down"]
        try:
            up_idx = [o.lower() for o in outcomes].index("up")
        except ValueError:
            up_idx = 0
        up_won = float(op[up_idx]) >= 0.5
        result = {
            "closed": True,
            "up_won": up_won,
            "condition_id": m.get("conditionId", ""),
        }
        cache[slug] = result
        return result
    except Exception:
        return None


def parse_float(s):
    try:
        return float(s) if s != "" else 0.0
    except (ValueError, TypeError):
        return 0.0


def parse_int(s):
    try:
        return int(s) if s != "" else 0
    except (ValueError, TypeError):
        return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--refresh-outcomes", action="store_true",
                    help="Force re-fetch of every market outcome")
    args = ap.parse_args()

    cache = {} if args.refresh_outcomes else _load_outcome_cache()

    # ── Load intervals ──
    if not INTERVALS_CSV.exists():
        raise SystemExit(f"Missing {INTERVALS_CSV}")

    rows = []
    with open(INTERVALS_CSV) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    print(f"Loaded {len(rows)} interval rows from {INTERVALS_CSV}")

    # ── Resolve outcomes ──
    unique_slugs = sorted({r["slug"] for r in rows if r["slug"]})
    print(f"Unique slugs needing outcome lookup: {len(unique_slugs)}")
    n_fetched = 0
    n_cached = 0
    n_open = 0
    for i, slug in enumerate(unique_slugs):
        if slug in cache and cache[slug].get("closed"):
            n_cached += 1
            continue
        result = fetch_outcome(slug, cache)
        if result is None:
            continue
        if not result.get("closed"):
            n_open += 1
            continue
        n_fetched += 1
        if (i + 1) % 25 == 0:
            print(f"  ... {i+1}/{len(unique_slugs)} done")
            _save_outcome_cache(cache)
        time.sleep(0.05)  # gentle on Gamma
    _save_outcome_cache(cache)
    print(f"Outcomes: {n_cached} cached, {n_fetched} new fetches, {n_open} still open")

    # ── Load rebates by (date, condition_id) ──
    rebates_by_cond_day = defaultdict(float)
    rebates_total_by_day = defaultdict(float)
    if REBATE_CSV.exists():
        with open(REBATE_CSV) as f:
            for r in csv.DictReader(f):
                day = r["date"]
                cid = r["condition_id"]
                amt = parse_float(r["rebated_fees_usdc"])
                rebates_by_cond_day[(day, cid)] += amt
                rebates_total_by_day[day] += amt
    print(f"Loaded rebates from {REBATE_CSV.name}: ${sum(rebates_total_by_day.values()):.4f} total\n")

    # ── Compute per-row P&L ──
    daily = defaultdict(lambda: defaultdict(float))   # daily[(day, asset)][metric]
    asset_totals = defaultdict(lambda: defaultdict(float))
    intervals_by_status = defaultdict(int)

    for r in rows:
        day = r["timestamp"][:10]
        asset = r["asset"]
        slug = r["slug"]
        pairs = parse_int(r["pairs"])
        pair_sum = parse_float(r["pair_sum"])  # sum of avg up + down per pair
        total_edge = parse_float(r["total_edge"])  # pairs * (1 - pair_sum)
        excess_side = r["excess_side"]
        excess_qty = parse_int(r["excess_qty"])
        leftover_up = parse_int(r["leftover_up"])
        leftover_down = parse_int(r["leftover_down"])
        leftover_cost = parse_float(r["leftover_cost"])
        up_avg = parse_float(r["up_avg_price"])
        down_avg = parse_float(r["down_avg_price"])

        # ── Pair-sum P&L (locked, doesn't need outcome) ──
        # cost for paired portion = pairs * pair_sum
        # revenue for paired portion = pairs * 1.00
        pair_revenue = float(pairs)
        pair_cost = pairs * pair_sum
        pair_pnl = pair_revenue - pair_cost  # equals total_edge

        # ── Single-leg P&L (needs outcome) ──
        single_leg_qty = excess_qty
        single_leg_side = excess_side
        # Cost for excess portion = leftover_cost - pair_cost
        single_leg_cost = max(0.0, leftover_cost - pair_cost) if (leftover_up or leftover_down) else 0.0
        single_leg_revenue = 0.0

        outcome = cache.get(slug, {})
        outcome_known = outcome.get("closed") is True
        if outcome_known and single_leg_qty > 0:
            up_won = outcome["up_won"]
            won = (single_leg_side == "UP" and up_won) or (single_leg_side == "DOWN" and not up_won)
            single_leg_revenue = float(single_leg_qty) if won else 0.0
            single_leg_pnl = single_leg_revenue - single_leg_cost
        elif single_leg_qty > 0:
            # Outcome unknown — assume losing-side single-leg (conservative)
            single_leg_pnl = -single_leg_cost
            intervals_by_status["single_leg_outcome_unknown"] += 1
        else:
            single_leg_pnl = 0.0

        # If neither pair nor single-leg, status:
        if pairs == 0 and single_leg_qty == 0 and (leftover_up == 0 and leftover_down == 0):
            intervals_by_status["no_fills"] += 1
        elif pairs > 0 and single_leg_qty == 0:
            intervals_by_status["fully_paired"] += 1
        elif pairs > 0 and single_leg_qty > 0:
            intervals_by_status["paired_plus_excess"] += 1
        elif single_leg_qty > 0:
            intervals_by_status["single_leg_only"] += 1
        else:
            intervals_by_status["other"] += 1

        # ── Aggregate ──
        net_pnl = pair_pnl + single_leg_pnl
        daily[(day, asset)]["intervals"] += 1
        daily[(day, asset)]["pairs"] += pairs
        daily[(day, asset)]["pair_pnl"] += pair_pnl
        daily[(day, asset)]["single_leg_pnl"] += single_leg_pnl
        daily[(day, asset)]["pair_cost"] += pair_cost
        daily[(day, asset)]["single_leg_cost"] += single_leg_cost
        daily[(day, asset)]["net_pnl"] += net_pnl

        asset_totals[asset]["intervals"] += 1
        asset_totals[asset]["pairs"] += pairs
        asset_totals[asset]["pair_pnl"] += pair_pnl
        asset_totals[asset]["single_leg_pnl"] += single_leg_pnl
        asset_totals[asset]["net_pnl"] += net_pnl
        asset_totals[asset]["pair_cost"] += pair_cost
        asset_totals[asset]["single_leg_cost"] += single_leg_cost

    # ── Print per-day report ──
    print("=" * 92)
    print("DAILY ACCOUNTING (pair-sum edge + single-leg + rebate)")
    print("=" * 92)
    print(f"{'Day':<12} {'Asset':<10} {'Int':>4} {'Pairs':>6} "
          f"{'PairPnL':>9} {'SingleLg':>10} {'Trading':>9} "
          f"{'Rebate':>8} {'Total':>9}")
    print("-" * 92)
    days = sorted({day for (day, _) in daily})
    grand_pair = grand_single = grand_rebate = grand_total = 0.0
    grand_intervals = grand_pairs = 0
    for day in days:
        day_rebate = rebates_total_by_day.get(day, 0.0)
        day_trading = 0.0
        printed_for_day = 0
        for (d, asset), m in sorted(daily.items()):
            if d != day:
                continue
            day_trading += m["net_pnl"]
            grand_pair += m["pair_pnl"]
            grand_single += m["single_leg_pnl"]
            grand_intervals += int(m["intervals"])
            grand_pairs += int(m["pairs"])
            print(f"{day:<12} {asset:<10} {int(m['intervals']):>4} {int(m['pairs']):>6} "
                  f"${m['pair_pnl']:>+8.2f} ${m['single_leg_pnl']:>+9.2f} "
                  f"${m['net_pnl']:>+8.2f} "
                  f"{'':>8} "
                  f"{'':>9}")
            printed_for_day += 1
        grand_rebate += day_rebate
        day_total = day_trading + day_rebate
        grand_total += day_total
        # day footer
        print(f"{day:<12} {'(sum)':<10} {'':>4} {'':>6} {'':>9} {'':>10} "
              f"${day_trading:>+8.2f} ${day_rebate:>+7.4f} ${day_total:>+8.2f}")
        print()

    print("=" * 92)
    print("PER-ASSET TOTALS (excludes rebates — rebate is per-day, not per-asset)")
    print("=" * 92)
    print(f"{'Asset':<10} {'Intervals':>10} {'Pairs':>8} {'PairPnL':>10} {'SingleLg':>10} {'Trading':>10}")
    for asset in sorted(asset_totals):
        m = asset_totals[asset]
        print(f"{asset:<10} {int(m['intervals']):>10} {int(m['pairs']):>8} "
              f"${m['pair_pnl']:>+9.2f} ${m['single_leg_pnl']:>+9.2f} "
              f"${m['net_pnl']:>+9.2f}")

    print("\n" + "=" * 92)
    print("GRAND TOTAL")
    print("=" * 92)
    print(f"  Intervals run:            {grand_intervals}")
    print(f"  Pairs locked:             {grand_pairs}")
    print(f"  Pair-sum edge (gross):    ${grand_pair:>+9.2f}")
    print(f"  Single-leg P&L:           ${grand_single:>+9.2f}")
    print(f"  Trading P&L (sub-total):  ${grand_pair + grand_single:>+9.2f}")
    print(f"  Maker rebates earned:     ${grand_rebate:>+9.4f}")
    print(f"  ───────────────────────────────────")
    print(f"  TOTAL BOT P&L:            ${grand_pair + grand_single + grand_rebate:>+9.2f}")
    print()
    print(f"  Status breakdown:")
    for status, n in sorted(intervals_by_status.items(), key=lambda x: -x[1]):
        print(f"    {status:<32} {n}")
    print()
    if grand_pair + grand_single != 0:
        rebate_pct = 100 * grand_rebate / abs(grand_pair + grand_single)
        print(f"  Rebate as % of |trading P&L|: {rebate_pct:.1f}%")


if __name__ == "__main__":
    main()
