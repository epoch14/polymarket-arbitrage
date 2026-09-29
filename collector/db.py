"""
Collector Storage
=================

SQLite (WAL) persistence for the data collector. All writes go through
the Recorder's per-table queues and are flushed in batches every second
by a single writer task — the hot path never blocks on disk.

Tables:
    book_snapshots         top-of-book + sampled depth (raw, pruned at retention_days)
    book_rollups_5m        5-minute OHLC-style rollups (kept forever — research backbone)
    trades                 public trade prints
    market_metadata        market reference data (resolution rules, end dates, volume)
    pairs                  cross-venue candidate pairs + verification status
    opportunity_snapshots  every computed edge snapshot (taken or not)
    spot_prices            crypto spot prints
    bot_events             lifecycle / integrity events
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS book_snapshots (
    id INTEGER PRIMARY KEY,
    ts_epoch REAL NOT NULL,
    venue TEXT NOT NULL,
    market TEXT NOT NULL,
    token TEXT NOT NULL,
    best_bid REAL, best_ask REAL,
    best_bid_size REAL, best_ask_size REAL,
    depth TEXT
);
CREATE INDEX IF NOT EXISTS idx_books_lookup ON book_snapshots(venue, market, token, ts_epoch);
CREATE INDEX IF NOT EXISTS idx_books_ts ON book_snapshots(ts_epoch);

CREATE TABLE IF NOT EXISTS book_rollups_5m (
    venue TEXT NOT NULL,
    market TEXT NOT NULL,
    token TEXT NOT NULL,
    bucket_epoch INTEGER NOT NULL,
    bid_min REAL, bid_max REAL, bid_avg REAL,
    ask_min REAL, ask_max REAL, ask_avg REAL,
    n INTEGER,
    PRIMARY KEY (venue, market, token, bucket_epoch)
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY,
    ts_epoch REAL NOT NULL,
    venue TEXT NOT NULL,
    market TEXT NOT NULL,
    token TEXT,
    price REAL, size REAL, side TEXT,
    trade_id TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_trades_lookup ON trades(venue, market, ts_epoch);

CREATE TABLE IF NOT EXISTS market_metadata (
    venue TEXT NOT NULL,
    market TEXT NOT NULL,
    title TEXT,
    category TEXT,
    end_date TEXT,
    volume_24h REAL,
    liquidity REAL,
    tokens_json TEXT,
    active INTEGER,
    extra_json TEXT,
    updated_ts REAL,
    PRIMARY KEY (venue, market)
);

CREATE TABLE IF NOT EXISTS pairs (
    id INTEGER PRIMARY KEY,
    poly_market TEXT NOT NULL,
    kalshi_ticker TEXT NOT NULL,
    similarity REAL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','verified','rejected')),
    notes TEXT,
    created_ts REAL,
    updated_ts REAL,
    UNIQUE (poly_market, kalshi_ticker)
);

CREATE TABLE IF NOT EXISTS opportunity_snapshots (
    id INTEGER PRIMARY KEY,
    ts_epoch REAL NOT NULL,
    strategy TEXT NOT NULL,
    poly_market TEXT,
    kalshi_ticker TEXT,
    poly_yes_bid REAL, poly_yes_ask REAL, poly_no_bid REAL, poly_no_ask REAL,
    kalshi_yes_bid REAL, kalshi_yes_ask REAL, kalshi_no_bid REAL, kalshi_no_ask REAL,
    direction TEXT,
    gross_edge REAL, net_edge REAL, edge_pct REAL, score REAL,
    details_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_opps_lookup ON opportunity_snapshots(strategy, poly_market, ts_epoch);

CREATE TABLE IF NOT EXISTS spot_prices (
    id INTEGER PRIMARY KEY,
    ts_epoch REAL NOT NULL,
    symbol TEXT NOT NULL,
    price REAL
);

CREATE TABLE IF NOT EXISTS bot_events (
    id INTEGER PRIMARY KEY,
    ts_epoch REAL NOT NULL,
    level TEXT,
    component TEXT,
    message TEXT
);
"""


def now() -> tuple[float, str]:
    """(epoch, iso) timestamp pair."""
    t = time.time()
    return t, datetime.fromtimestamp(t, tz=timezone.utc).isoformat()


class Recorder:
    """Batched async writer. Feeds put rows; one task flushes every interval."""

    TABLES = ("book_snapshots", "trades", "market_metadata", "pairs",
              "opportunity_snapshots", "spot_prices", "bot_events")
    # market_metadata/pairs are UPSERTs (keyed by natural key)
    UPSERT = {
        "market_metadata": """INSERT INTO market_metadata
            (venue, market, title, category, end_date, volume_24h, liquidity,
             tokens_json, active, extra_json, updated_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(venue, market) DO UPDATE SET
              title=excluded.title, category=excluded.category,
              end_date=excluded.end_date, volume_24h=excluded.volume_24h,
              liquidity=excluded.liquidity, tokens_json=excluded.tokens_json,
              active=excluded.active, extra_json=excluded.extra_json,
              updated_ts=excluded.updated_ts""",
        "pairs": """INSERT INTO pairs
            (poly_market, kalshi_ticker, similarity, status, notes, created_ts, updated_ts)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(poly_market, kalshi_ticker) DO UPDATE SET
              similarity=excluded.similarity, notes=excluded.notes,
              updated_ts=excluded.updated_ts""",
    }
    # Column order per table for positional inserts
    COLUMNS = {
        "book_snapshots": ["ts_epoch", "venue", "market", "token",
                           "best_bid", "best_ask", "best_bid_size", "best_ask_size", "depth"],
        "trades": ["ts_epoch", "venue", "market", "token", "price", "size", "side", "trade_id"],
        "market_metadata": ["venue", "market", "title", "category", "end_date",
                            "volume_24h", "liquidity", "tokens_json", "active",
                            "extra_json", "updated_ts"],
        "pairs": ["poly_market", "kalshi_ticker", "similarity", "status",
                  "notes", "created_ts", "updated_ts"],
        "opportunity_snapshots": ["ts_epoch", "strategy", "poly_market", "kalshi_ticker",
                                  "poly_yes_bid", "poly_yes_ask", "poly_no_bid", "poly_no_ask",
                                  "kalshi_yes_bid", "kalshi_yes_ask", "kalshi_no_bid",
                                  "kalshi_no_ask", "direction", "gross_edge", "net_edge",
                                  "edge_pct", "score", "details_json"],
        "spot_prices": ["ts_epoch", "symbol", "price"],
        "bot_events": ["ts_epoch", "level", "component", "message"],
    }

    def __init__(self, db_path: str, flush_interval: float = 1.0):
        self.db_path = db_path
        self.flush_interval = flush_interval
        self._queues: dict[str, asyncio.Queue] = {t: asyncio.Queue(maxsize=50_000) for t in self.TABLES}
        self._task: asyncio.Task | None = None
        self._running = False
        self.dropped = 0
        self.written = 0

    def put(self, table: str, row: dict) -> None:
        """Queue one row. Never blocks the caller; drops on overflow (counted)."""
        q = self._queues[table]
        try:
            q.put_nowait(row)
        except asyncio.QueueFull:
            self.dropped += 1

    def event(self, level: str, component: str, message: str) -> None:
        self.put("bot_events", {"ts_epoch": time.time(), "level": level,
                                "component": component, "message": message[:2000]})

    async def start(self) -> None:
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)
        self._conn.commit()
        self._running = True
        self._task = asyncio.create_task(self._run(), name="recorder")
        logger.info("Recorder started: %s", self.db_path)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            await self._task
        if getattr(self, "_conn", None):
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self._conn.close()
        logger.info("Recorder stopped: written=%d dropped=%d", self.written, self.dropped)

    async def _run(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval)
                self._flush()
            except asyncio.CancelledError:
                self._flush()
                raise
            except Exception:
                logger.exception("Recorder flush error")
                await asyncio.sleep(5)

    def _flush(self) -> None:
        for table, q in self._queues.items():
            rows = []
            while not q.empty():
                try:
                    rows.append(q.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if not rows:
                continue
            cols = self.COLUMNS[table]
            placeholders = ",".join("?" * len(cols))
            if table in self.UPSERT:
                sql = self.UPSERT[table]
            elif table == "trades":  # re-polls re-fetch the same prints
                sql = (f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) "
                       f"VALUES ({placeholders})")
            else:
                sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
            try:
                self._conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
                self._conn.commit()
                self.written += len(rows)
            except sqlite3.Error:
                logger.exception("Insert failed for %s (n=%d)", table, len(rows))

    # ------------------------------------------------------------------
    # Maintenance jobs
    # ------------------------------------------------------------------

    def rollup(self) -> int:
        """Aggregate the last window of raw snapshots into 5-minute rollups."""
        since = time.time() - 3 * 3600  # re-aggregate a 3h tail (idempotent, OR REPLACE)
        cur = self._conn.execute("""
            INSERT OR REPLACE INTO book_rollups_5m
            SELECT venue, market, token,
                   CAST(ts_epoch/300 AS INTEGER)*300,
                   MIN(best_bid), MAX(best_bid), AVG(best_bid),
                   MIN(best_ask), MAX(best_ask), AVG(best_ask),
                   COUNT(*)
            FROM book_snapshots
            WHERE ts_epoch >= ?
              AND best_bid IS NOT NULL AND best_ask IS NOT NULL
            GROUP BY venue, market, token, CAST(ts_epoch/300 AS INTEGER)*300
        """, (since,))
        self._conn.commit()
        return cur.rowcount

    def prune(self, retention_days: int) -> int:
        """Delete raw rows past retention. Rollups are kept forever."""
        cutoff = time.time() - retention_days * 86400
        total = 0
        for table, days_col in (("book_snapshots", cutoff), ("trades", cutoff)):
            cur = self._conn.execute(f"DELETE FROM {table} WHERE ts_epoch < ?", (days_col,))
            total += cur.rowcount
        self._conn.commit()
        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        return total

    def gap_check(self, max_age: float = 120.0) -> dict[str, float]:
        """Seconds since the newest raw snapshot per venue (staleness alarm)."""
        out = {}
        for venue in ("polymarket", "kalshi"):
            row = self._conn.execute(
                "SELECT MAX(ts_epoch) FROM book_snapshots WHERE venue=?", (venue,)
            ).fetchone()
            out[venue] = time.time() - row[0] if row and row[0] else float("inf")
        stale = {v: round(a, 1) for v, a in out.items() if a > max_age}
        if stale:
            self.event("WARNING", "integrity", f"stale feeds: {stale}")
        return out

    def stats(self) -> dict:
        """Row counts for health reporting."""
        counts = {}
        for table in self.TABLES:
            counts[table] = self._conn.execute(
                f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return counts
