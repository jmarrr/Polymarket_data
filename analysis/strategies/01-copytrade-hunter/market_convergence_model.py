"""Reverse-engineer the BTC 5m market's pricing function.

Question: how early do BTC 5m markets "price in" their resolution, and
what model explains the convergence? If the price function is predictable
from (BTC_lead, time_remaining), we can compute a "fair" market price
and trade mispricings vs the orderbook.

Approach:
  1. For each closed BTC 5m market, identify the winning side at resolution
  2. Pull the winner's price trajectory through the interval (every trade)
  3. Bucket by seconds-remaining and compute the median winner price
  4. Show convergence speed: P(winner reaches $0.80, $0.90, $0.95, $0.99
     | time_remaining)
  5. Compare to a textbook Black-Scholes-style binary pricing model

This is the empirical pricing function. If consistent, we can front-run
it: when our Binance lead implies P(UP) = 0.70 but the orderbook is at
0.55, that's a mispricing window.
"""
import sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from lib.db import connect  # noqa: E402


def main():
    con = connect(memory_limit='12GB')
    con.execute('SET preserve_insertion_order=false')
    con.execute('SET threads=8')

    # ── Step 1: identify all closed BTC 5m markets with clean binary outcome
    print('=== STEP 1: BTC 5m market universe ===')
    con.execute("""
        CREATE OR REPLACE TABLE btc5 AS
        SELECT id AS market_id, condition_id, token1, token2,
               answer1, answer2, end_date,
               extract(epoch FROM end_date)::BIGINT AS end_unix,
               extract(epoch FROM end_date)::BIGINT - 300 AS start_unix,
               outcome_prices,
               TRY_CAST(regexp_extract(outcome_prices, '''([0-9.]+)''', 1) AS DOUBLE) AS p1,
               TRY_CAST(regexp_extract(outcome_prices, ''',\\s*''([0-9.]+)''', 1) AS DOUBLE) AS p2,
               -- token1 corresponds to answer1; if answer1='Up' then token1 is the UP token
               CASE WHEN lower(answer1) IN ('up','yes','higher') THEN 'token1' ELSE 'token2' END AS up_token,
               CASE WHEN lower(answer1) IN ('up','yes','higher') THEN 'token2' ELSE 'token1' END AS down_token
        FROM markets
        WHERE slug LIKE 'btc-updown-5m-%'
          AND closed = 1
          AND outcome_prices LIKE '[%'
          AND end_date >= TIMESTAMP '2026-04-01'   -- last 5 weeks of cleaner V2-era data
    """)
    print(con.execute("""
        SELECT COUNT(*) AS markets, MIN(end_date) AS first, MAX(end_date) AS last,
               SUM(CASE WHEN p1=1 OR p2=1 THEN 1 ELSE 0 END) AS clean_binary,
               SUM(CASE WHEN p1 BETWEEN 0.001 AND 0.999 THEN 1 ELSE 0 END) AS fractional
        FROM btc5
    """).df().to_string(index=False))

    # ── Step 2: which side won each market (token1 or token2 resolves to $1)
    con.execute("""
        CREATE OR REPLACE TABLE btc5_w AS
        SELECT *,
               CASE WHEN p1 >= 0.99 THEN 'token1'
                    WHEN p2 >= 0.99 THEN 'token2'
                    ELSE NULL END AS winning_token
        FROM btc5
        WHERE (p1 >= 0.99 OR p2 >= 0.99)        -- exclude unresolved or near-tie
    """)
    n_resolved = con.execute("SELECT COUNT(*) FROM btc5_w").fetchone()[0]
    print(f'\nResolved markets with clean binary outcome: {n_resolved:,}')

    # ── Step 3: pull winning-side trade prices through each interval ───
    # Use `quant` (YES-perspective) — but we need WINNING-side prices.
    # The asset_id in trades gives us which token traded; map to win/lose.
    # `trades.parquet` has asset_id directly.
    print(f'\n=== STEP 2: winning-side price trajectories ===')
    print('(this is the heavy lift — joining 250M+ trades to 100K+ markets)')
    con.execute("""
        CREATE OR REPLACE TABLE w_trades AS
        SELECT m.market_id, m.end_unix, m.start_unix,
               m.winning_token,
               m.up_token AS up_token_label,
               t.timestamp,
               m.end_unix - t.timestamp AS secs_remaining,
               t.price AS winner_price,
               t.usd_amount
        FROM btc5_w m
        JOIN trades t ON t.market_id = m.market_id
        WHERE t.timestamp BETWEEN m.start_unix AND m.end_unix
          -- Filter to winning-side trades: asset_id matches the winner.
          -- token1/token2 in markets correspond to asset_id semantically.
          AND (
              (m.winning_token = 'token1' AND t.asset_id = m.token1) OR
              (m.winning_token = 'token2' AND t.asset_id = m.token2)
          )
    """)
    print(f'winning-side trades materialized:',
          con.execute("SELECT COUNT(*) FROM w_trades").fetchone()[0])

    # ── Step 4: median winner price by seconds remaining ───
    print('\n=== STEP 3: median WINNER price by seconds-remaining bucket ===')
    print('(if markets are predictive, this should rise smoothly from 0.50 → 1.00)')
    print(con.execute("""
        SELECT
            CASE WHEN secs_remaining > 270 THEN 'a) 270-300 (first 30s)'
                 WHEN secs_remaining > 240 THEN 'b) 240-270'
                 WHEN secs_remaining > 180 THEN 'c) 180-240'
                 WHEN secs_remaining > 120 THEN 'd) 120-180'
                 WHEN secs_remaining > 60  THEN 'e)  60-120'
                 WHEN secs_remaining > 30  THEN 'f)  30-60'
                 WHEN secs_remaining > 10  THEN 'g)  10-30'
                 ELSE                            'h)   0-10 (last 10s)'
            END AS time_bucket,
            COUNT(*) AS trades,
            ROUND(MEDIAN(winner_price), 3) AS median_winner_price,
            ROUND(QUANTILE_CONT(winner_price, 0.25), 3) AS q25,
            ROUND(QUANTILE_CONT(winner_price, 0.75), 3) AS q75,
            ROUND(AVG(winner_price), 3) AS avg_winner_price
        FROM w_trades GROUP BY time_bucket ORDER BY time_bucket
    """).df().to_string(index=False))

    # ── Step 5: first-time-crossing thresholds ───
    print('\n=== STEP 4: WHEN does the winner first cross each threshold? ===')
    print('(seconds-before-close at which the winning side first hit $0.X)')
    con.execute("""
        CREATE OR REPLACE TABLE crossings AS
        SELECT market_id,
               MIN(CASE WHEN winner_price >= 0.70 THEN secs_remaining END) AS cross_70,
               MIN(CASE WHEN winner_price >= 0.80 THEN secs_remaining END) AS cross_80,
               MIN(CASE WHEN winner_price >= 0.90 THEN secs_remaining END) AS cross_90,
               MIN(CASE WHEN winner_price >= 0.95 THEN secs_remaining END) AS cross_95,
               MIN(CASE WHEN winner_price >= 0.99 THEN secs_remaining END) AS cross_99
        FROM w_trades GROUP BY market_id
    """)
    print(con.execute("""
        SELECT
            ROUND(MEDIAN(cross_70), 0) AS median_cross_70s,
            ROUND(MEDIAN(cross_80), 0) AS median_cross_80s,
            ROUND(MEDIAN(cross_90), 0) AS median_cross_90s,
            ROUND(MEDIAN(cross_95), 0) AS median_cross_95s,
            ROUND(MEDIAN(cross_99), 0) AS median_cross_99s,
            COUNT(*) AS markets,
            SUM(CASE WHEN cross_70 IS NOT NULL THEN 1 ELSE 0 END) AS markets_with_70_cross,
            SUM(CASE WHEN cross_95 IS NOT NULL THEN 1 ELSE 0 END) AS markets_with_95_cross,
            SUM(CASE WHEN cross_99 IS NOT NULL THEN 1 ELSE 0 END) AS markets_with_99_cross
        FROM crossings
    """).df().to_string(index=False))

    print('\n=== STEP 5: distribution of cross_95 — how variable is convergence? ===')
    print(con.execute("""
        SELECT
            CASE WHEN cross_95 IS NULL THEN 'z) never hit 0.95'
                 WHEN cross_95 > 240 THEN 'a) > 240s remaining (>4 min)'
                 WHEN cross_95 > 180 THEN 'b) 180-240s'
                 WHEN cross_95 > 120 THEN 'c) 120-180s'
                 WHEN cross_95 > 60  THEN 'd)  60-120s'
                 WHEN cross_95 > 30  THEN 'e)  30-60s'
                 WHEN cross_95 > 10  THEN 'f)  10-30s'
                 ELSE                       'g)   0-10s (last second)'
            END AS bucket,
            COUNT(*) AS markets,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM crossings GROUP BY bucket ORDER BY bucket
    """).df().to_string(index=False))

    # ── Step 6: now look at LOSING-side price trajectory — does the loser
    # ── stay near $0.50 or actively drop? Tells us if there's "consensus
    # ── early" or "consensus only at the end."
    print('\n=== STEP 6: LOSING-side median price by time bucket ===')
    con.execute("""
        CREATE OR REPLACE TABLE l_trades AS
        SELECT m.market_id, m.end_unix - t.timestamp AS secs_remaining, t.price AS loser_price
        FROM btc5_w m
        JOIN trades t ON t.market_id = m.market_id
        WHERE t.timestamp BETWEEN m.start_unix AND m.end_unix
          AND (
              (m.winning_token = 'token1' AND t.asset_id = m.token2) OR
              (m.winning_token = 'token2' AND t.asset_id = m.token1)
          )
    """)
    print(con.execute("""
        SELECT
            CASE WHEN secs_remaining > 270 THEN 'a) 270-300'
                 WHEN secs_remaining > 240 THEN 'b) 240-270'
                 WHEN secs_remaining > 180 THEN 'c) 180-240'
                 WHEN secs_remaining > 120 THEN 'd) 120-180'
                 WHEN secs_remaining > 60  THEN 'e)  60-120'
                 WHEN secs_remaining > 30  THEN 'f)  30-60'
                 WHEN secs_remaining > 10  THEN 'g)  10-30'
                 ELSE                            'h)   0-10'
            END AS time_bucket,
            COUNT(*) AS trades,
            ROUND(MEDIAN(loser_price), 3) AS median_loser_price,
            ROUND(QUANTILE_CONT(loser_price, 0.75), 3) AS q75_loser
        FROM l_trades GROUP BY time_bucket ORDER BY time_bucket
    """).df().to_string(index=False))

    # ── Step 7: spread analysis — what's the typical price_sum (UP+DOWN)?
    # If sum < $1, there's a guaranteed arb. Useful baseline.
    print('\n=== STEP 7: instantaneous UP+DOWN price-sum distribution ===')
    print('(if sum < 1, market is implying < 100% probability somewhere — pricing inefficiency)')
    print(con.execute("""
        WITH paired AS (
            SELECT m.market_id, m.end_unix - t.timestamp AS secs_remaining,
                   t.price,
                   CASE WHEN t.asset_id = m.token1 THEN 't1' ELSE 't2' END AS side
            FROM btc5_w m
            JOIN trades t ON t.market_id = m.market_id
            WHERE t.timestamp BETWEEN m.start_unix AND m.end_unix
        ),
        -- For each market, take the LAST trade on each side at each second
        latest_per_sec AS (
            SELECT market_id, secs_remaining, side,
                   LAST_VALUE(price) OVER (PARTITION BY market_id, secs_remaining, side
                                            ORDER BY price ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS price
            FROM paired
        ),
        -- Sum up t1 + t2 at each second per market
        sums AS (
            SELECT market_id, secs_remaining,
                   MAX(CASE WHEN side='t1' THEN price END) +
                   MAX(CASE WHEN side='t2' THEN price END) AS price_sum
            FROM latest_per_sec
            GROUP BY market_id, secs_remaining
            HAVING price_sum IS NOT NULL
        )
        SELECT
            CASE WHEN secs_remaining > 180 THEN 'a) >180s'
                 WHEN secs_remaining > 60  THEN 'b)  60-180s'
                 WHEN secs_remaining > 10  THEN 'c)  10-60s'
                 ELSE                            'd)  <=10s'
            END AS time_bucket,
            COUNT(*) AS observations,
            ROUND(MEDIAN(price_sum), 3) AS median_sum,
            ROUND(QUANTILE_CONT(price_sum, 0.10), 3) AS p10,
            ROUND(QUANTILE_CONT(price_sum, 0.90), 3) AS p90
        FROM sums GROUP BY time_bucket ORDER BY time_bucket
    """).df().to_string(index=False))


if __name__ == '__main__':
    main()
