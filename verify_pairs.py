#!/usr/bin/env python3
"""
Pair Verification CLI
=====================

The matcher generates candidates; a human verifies them. Only pairs marked
'verified' here are paper-traded cross-venue. This is the anti-garbage gate.

Usage:
    .venv/bin/python verify_pairs.py                 # show pending pairs
    .venv/bin/python verify_pairs.py --show 5        # first 5
    .venv/bin/python verify_pairs.py --approve 3 7   # approve pairs by row #
    .venv/bin/python verify_pairs.py --reject 3      # reject pair by row #
    .venv/bin/python verify_pairs.py --list-verified
"""

import argparse
import sqlite3
import sys
import time

DB = "data/market.db"


def conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB)
    c.execute("PRAGMA busy_timeout=5000")
    return c


def show(c: sqlite3.Connection, status: str, limit: int) -> list:
    rows = c.execute(
        """SELECT p.id, p.poly_market, p.kalshi_ticker, p.similarity,
                  mp.title, mk.title
           FROM pairs p
           LEFT JOIN market_metadata mp ON mp.market = p.poly_market AND mp.venue='polymarket'
           LEFT JOIN market_metadata mk ON mk.market = p.kalshi_ticker AND mk.venue='kalshi'
           WHERE p.status = ?
           ORDER BY p.similarity DESC""", (status,)).fetchall()
    if not rows:
        print(f"No pairs with status '{status}'.")
        return rows
    print(f"=== {status.upper()} pairs ({len(rows)}) ===")
    for i, (pid, poly, kalshi, sim, pt, kt) in enumerate(rows[:limit] if limit else rows):
        print(f"[{pid:>4}] sim={sim:.2f}")
        print(f"   POLY:   {pt or poly}")
        print(f"   KALSHI: {kt or kalshi}")
    if limit and len(rows) > limit:
        print(f"... {len(rows) - limit} more (use --show N)")
    return rows


def set_status(c: sqlite3.Connection, ids: list[int], status: str) -> None:
    now = time.time()
    for pid in ids:
        cur = c.execute("UPDATE pairs SET status=?, updated_ts=? WHERE id=?",
                        (status, now, pid))
        if cur.rowcount:
            print(f"pair {pid} -> {status}")
        else:
            print(f"pair {pid} not found")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", type=int, default=0, help="show first N pending pairs")
    ap.add_argument("--approve", nargs="+", type=int, help="pair ids to verify")
    ap.add_argument("--reject", nargs="+", type=int, help="pair ids to reject")
    ap.add_argument("--list-verified", action="store_true")
    args = ap.parse_args()

    c = conn()
    try:
        if args.approve:
            set_status(c, args.approve, "verified")
        if args.reject:
            set_status(c, args.reject, "rejected")
        c.commit()
        if args.list_verified:
            show(c, "verified", 0)
        elif args.approve or args.reject:
            pass
        else:
            rows = show(c, "pending", args.show)
            if rows:
                print("\nVerify by reading BOTH titles: same event, same resolution?"
                      "\n  .venv/bin/python verify_pairs.py --approve <id...>")
    finally:
        c.close()


if __name__ == "__main__":
    sys.exit(main())
