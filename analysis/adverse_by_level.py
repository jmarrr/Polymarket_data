"""
Per-level adverse-selection breakdown for dual_side_lp.py.

Reads `dual_side_lp.log` and aggregates ADVERSE events tagged with their
ladder level (L0 / L1 / L2 / L3). Tells us which ladder slot is the
worst pickoff target — a level whose adverse rate is much higher than
its fill rate is a candidate to drop in future sizing tuning.

Usage:
    python analysis/adverse_by_level.py
    python analysis/adverse_by_level.py --asset bitcoin
    python analysis/adverse_by_level.py --since 2026-04-29
"""

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path

LOG_FILE = Path("/home/alan/Desktop/account2/logs/dual_side_lp.log")
INTERVALS_CSV = Path("/home/alan/Desktop/account2/logs/lp_intervals.csv")
FILLS_CSV = Path("/home/alan/Desktop/account2/logs/lp_fills.csv")

# Match per-level adverse log lines. Level segment is OPTIONAL: pre-fix
# rows have "DOWN filled@$0.46" with no level. Post-fix rows have
# "DOWN L2 filled@$0.46". Pre-fix rows count toward fill totals but not
# per-level breakdown.
ADVERSE_RE = re.compile(
    r"^(\S+ \S+) \| ADVERSE \| (\S+-\d+M) \| (UP|DOWN)(?: L(\d+))? "
    r"filled@\$(\d+\.\d+) now@\$(\d+\.\d+) \(move=\$(\d+\.\d+)\) count=(\d+)"
)
# Match per-fill log lines (separately, for fill-rate denominator):
#   2026-04-28 ... | FILL | BITCOIN-5M | DOWN | L0 | 5@$0.41 | margin=...
FILL_RE = re.compile(
    r"^(\S+ \S+) \| FILL \| (\S+-\d+M) \| (UP|DOWN) \| L(\d+) \| (\d+)@\$(\d+\.\d+)"
)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--asset", default=None,
                    help="Filter to one asset (e.g., 'bitcoin'). Case-insensitive.")
    ap.add_argument("--since", default=None,
                    help="Only count rows on/after YYYY-MM-DD")
    args = ap.parse_args()

    asset_filter = args.asset.upper() if args.asset else None
    since = args.since

    if not LOG_FILE.exists():
        raise SystemExit(f"Missing {LOG_FILE}")

    fills_by = defaultdict(int)        # (asset, side, level) -> fill count
    adverse_by = defaultdict(int)       # (asset, side, level) -> adverse count
    total_intervals_with_adverse = defaultdict(int)  # asset -> intervals with ≥1 adverse

    with open(LOG_FILE) as f:
        for line in f:
            if since and not line.startswith(since) and line[:10] < since:
                continue

            m_adv = ADVERSE_RE.match(line.strip())
            if m_adv:
                _, label, side, level, _, _, _, _ = m_adv.groups()
                asset = label.split("-")[0]
                if asset_filter and asset != asset_filter:
                    continue
                if level is None:
                    # Pre-fix log line — count it under (asset, side, "any")
                    # so the user sees something but it doesn't pollute the
                    # per-level breakdown.
                    adverse_by[(asset, side, -1)] += 1
                else:
                    adverse_by[(asset, side, int(level))] += 1
                continue

            m_fill = FILL_RE.match(line.strip())
            if m_fill:
                _, label, side, level, qty, _ = m_fill.groups()
                asset = label.split("-")[0]
                if asset_filter and asset != asset_filter:
                    continue
                fills_by[(asset, side, int(level))] += 1

    if not fills_by and not adverse_by:
        print("No fill/adverse events found in log "
              f"(filter asset={args.asset}, since={args.since})")
        return

    print("=" * 90)
    print("PER-LEVEL ADVERSE-SELECTION BREAKDOWN")
    print("=" * 90)
    print(f"{'asset':<10} {'side':<5} {'level':>5} {'fills':>6} {'adverse':>8} "
          f"{'rate':>7}  {'note'}")
    print("-" * 90)

    assets = sorted({k[0] for k in fills_by} | {k[0] for k in adverse_by})
    for asset in assets:
        rows_for_asset = []
        for side in ("UP", "DOWN"):
            for level in range(0, 6):
                fc = fills_by.get((asset, side, level), 0)
                ac = adverse_by.get((asset, side, level), 0)
                if fc == 0 and ac == 0:
                    continue
                rate = (ac / fc) if fc > 0 else 0.0
                note = ""
                if fc > 0 and rate >= 0.4:
                    note = "  ← HIGH adverse rate (consider dropping)"
                elif fc > 0 and rate >= 0.25:
                    note = "  ← elevated"
                rows_for_asset.append((side, level, fc, ac, rate, note))

            # Pre-fix bucket (no level info)
            ac_unknown = adverse_by.get((asset, side, -1), 0)
            if ac_unknown > 0:
                rows_for_asset.append((side, "?", "—", ac_unknown, 0.0, "  (pre-fix log; level unknown)"))

        for side, level, fc, ac, rate, note in rows_for_asset:
            level_str = f"L{level}" if level != "?" else "L?"
            fc_str = str(fc) if isinstance(fc, int) else fc
            rate_str = f"{rate*100:>5.1f}%" if isinstance(rate, float) and rate > 0 else "    —"
            print(f"{asset:<10} {side:<5} {level_str:<5} {fc_str:>6} {ac:>8} "
                  f"{rate_str} {note}")

    # Summary table — best vs worst level per asset
    print()
    print("=" * 90)
    print("HEADLINE — worst-performing level per asset/side")
    print("=" * 90)
    for asset in assets:
        for side in ("UP", "DOWN"):
            scored = []
            for level in range(6):
                fc = fills_by.get((asset, side, level), 0)
                ac = adverse_by.get((asset, side, level), 0)
                if fc < 5:  # ignore noisy levels with few fills
                    continue
                scored.append((level, fc, ac, ac/fc))
            if not scored:
                continue
            scored.sort(key=lambda t: -t[3])
            worst = scored[0]
            best = scored[-1] if len(scored) > 1 else None
            best_str = (f"  best=L{best[0]} ({best[3]*100:.0f}%, n={best[1]})"
                        if best else "")
            print(f"  {asset:<10} {side:<5}  worst=L{worst[0]} "
                  f"({worst[3]*100:.0f}% adverse, n={worst[1]}){best_str}")


if __name__ == "__main__":
    main()
