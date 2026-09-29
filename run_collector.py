#!/usr/bin/env python3
"""
Collector Entrypoint
====================

24/7 market data collector. One asyncio process, two paths:

  HOT:  feeds -> in-memory books -> OpportunityScorer (passive paper layer)
  COLD: Recorder (batched SQLite WAL) -> 5m rollups -> retention pruning

Ops:
  - data/health.json refreshed every health_interval (atomic rename)
  - optional Uptime Kuma push via KUMA_PUSH_URL env
  - systemd watchdog (WATCHDOG=1) when NOTIFY_SOCKET is present
  - graceful SIGINT/SIGTERM shutdown with final flush

Usage:
    python3 run_collector.py [-c config.yaml] [-v]
"""

import argparse
import asyncio
import json
import logging
import os
import shutil
import signal
import socket
import sys
import time
from logging.handlers import RotatingFileHandler

import httpx
import yaml

from collector.db import Recorder
from collector.feeds import PolymarketFeed, KalshiFeed, SpotFeed
from collector.universe import PairManager
from collector.features import OpportunityScorer

logger = logging.getLogger("collector")

DEFAULTS = {
    "poly_universe_size": 150,
    "poly_poll_interval": 2.0,
    "poly_book_chunk": 50,
    "poly_snapshot_interval": 5.0,
    "depth_interval": 30.0,
    "kalshi_rate_limit": 8.0,
    "kalshi_workers": 4,
    "kalshi_slow_interval": 90.0,
    "kalshi_extra_size": 50,
    "spot_interval": 5.0,
    "spot_symbols": ["BTC", "ETH", "SOL"],
    "trades_interval": 120.0,
    "universe_interval": 3600.0,
    "rollup_interval": 300.0,
    "integrity_interval": 600.0,
    "prune_interval": 86400.0,
    "health_interval": 30.0,
    "db_path": "data/market.db",
    "log_file": "data/collector.log",
    "min_pair_similarity": 0.6,
    "max_book_age": 30.0,
    "poly_taker_bps": 150.0,
    "retention_days": 14,
    "min_free_disk_gb": 10.0,
    "max_rss_mb": 350.0,
}

KRAKEN_FALLBACK_OK = True


def load_config(path: str) -> dict:
    cfg = dict(DEFAULTS)
    try:
        with open(path) as f:
            user = yaml.safe_load(f) or {}
        cfg.update(user.get("collector", {}) or {})
    except FileNotFoundError:
        logger.warning("Config %s not found — using defaults", path)
    cfg["kuma_push_url"] = os.environ.get("KUMA_PUSH_URL", "") or cfg.get("kuma_push_url", "")
    return cfg


def rss_mb() -> float:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1]) / 1024.0
    except OSError:
        pass
    return 0.0


def sd_notify(message: str) -> None:
    """Minimal sd_notify(3) for the systemd watchdog."""
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        s.connect(addr)
        s.send(message.encode())
        s.close()
    except OSError as e:
        logger.debug("sd_notify failed: %s", e)


class Collector:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.started = time.time()
        self.recorder = Recorder(cfg["db_path"])
        self.poly = PolymarketFeed(cfg["poly_universe_size"], cfg["poly_book_chunk"])
        self.kalshi = KalshiFeed(cfg["kalshi_rate_limit"])
        self.spot = SpotFeed(cfg["spot_symbols"])
        self.pairs = PairManager(self.recorder, self.kalshi, cfg["min_pair_similarity"])
        self.scorer = OpportunityScorer(
            self.recorder, poly_taker_bps=cfg["poly_taker_bps"],
            max_book_age=cfg["max_book_age"])
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._last_depth = 0.0
        self._last_written: dict[tuple, float] = {}   # (venue, market, token) -> ts_epoch
        self._fast_queue: asyncio.Queue = asyncio.Queue()
        self._slow_queue: asyncio.Queue = asyncio.Queue()

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        await self.recorder.start()
        await self.kalshi.client.__aenter__()
        self.recorder.event("INFO", "lifecycle", "collector starting")

        # initial universe synchronously so the first cycle has data;
        # pair matching is slow (minutes) — runs in background, loops start now
        await self.poly.refresh_universe()
        await self.kalshi.refresh_universe()
        self.kalshi.set_poll_sets(
            matched=[],
            extra=[m["ticker"] for m in getattr(self.kalshi, "_raw_markets", [])
                   [:self.cfg["kalshi_extra_size"]]],
        )
        self.recorder.event("INFO", "lifecycle",
                            f"universe: poly={len(self.poly.universe)} "
                            f"kalshi_pairs=matching")

        loops = {
            "poly_cycle": self._poly_loop(),
            "kalshi_fast": self._kalshi_fast_loop(),
            "kalshi_slow": self._kalshi_slow_loop(),
            "book_writer": self._book_writer_loop(),
            "scorer": self._scorer_loop(),
            "spot": self._simple_loop(self.cfg["spot_interval"], self._spot_tick),
            "trades": self._simple_loop(self.cfg["trades_interval"], self._trades_tick),
            "rollup": self._simple_loop(self.cfg["rollup_interval"], self._rollup_tick),
            "integrity": self._simple_loop(self.cfg["integrity_interval"], self._integrity_tick),
            "prune": self._simple_loop(self.cfg["prune_interval"], self._prune_tick),
            "health": self._simple_loop(self.cfg["health_interval"], self._health_tick),
        }
        for name, coro in loops.items():
            self._tasks.append(asyncio.create_task(coro, name=name))
        self._tasks.append(asyncio.create_task(self._refresh_pairs(), name="refresh_pairs"))
        logger.info("Collector running with %d loops", len(self._tasks))

    async def _refresh_pairs(self) -> None:
        """Matcher is O(n*m) and sleeps — run as a loop task, first pass now."""
        await self.pairs.refresh(self.poly)
        self.kalshi.set_poll_sets(
            matched=[t for _, t in self.pairs.matched_pairs],
            extra=[m["ticker"] for m in getattr(self.kalshi, "_raw_markets", [])
                   [:self.cfg["kalshi_extra_size"]]],
        )
        await self._write_metadata()
        self.recorder.event("INFO", "lifecycle",
                            f"pairs matched: {len(self.pairs.matched_pairs)}")
        while self._running:
            await asyncio.sleep(self.cfg["universe_interval"])
            try:
                await self.poly.refresh_universe()
                await self.kalshi.refresh_universe()
                await self._refresh_pairs_once()
            except Exception:
                logger.exception("universe refresh error")

    async def _refresh_pairs_once(self) -> None:
        await self.pairs.refresh(self.poly)
        self.kalshi.set_poll_sets(
            matched=[t for _, t in self.pairs.matched_pairs],
            extra=[m["ticker"] for m in getattr(self.kalshi, "_raw_markets", [])
                   [:self.cfg["kalshi_extra_size"]]],
        )
        await self._write_metadata()

    async def _write_metadata(self) -> None:
        """Persist reference data (resolution rules, end dates, tokens) for both venues."""
        import json as _json
        now = time.time()
        for m in self.poly.universe.values():
            self.recorder.put("market_metadata", {
                "venue": "polymarket", "market": m.condition_id,
                "title": m.question, "category": None, "end_date": m.end_date,
                "volume_24h": m.volume_24h, "liquidity": m.liquidity,
                "tokens_json": _json.dumps([m.yes_token, m.no_token]),
                "active": 1, "extra_json": None, "updated_ts": now,
            })
        for m in getattr(self.kalshi, "_raw_markets", []):
            extra = {
                "rules": (m.get("rules_primary") or "")[:1500],
                "close_time": m.get("close_time"),
                "event_ticker": m.get("event_ticker"),
                "yes_bid": m.get("yes_bid_dollars"), "yes_ask": m.get("yes_ask_dollars"),
            }
            try:
                vol24 = float(str(m.get("volume_24h_fp") or 0).replace(",", ""))
            except ValueError:
                vol24 = 0.0
            self.recorder.put("market_metadata", {
                "venue": "kalshi", "market": m.get("ticker"),
                "title": m.get("title"), "category": None,
                "end_date": m.get("close_time"),
                "volume_24h": vol24,
                "liquidity": float(m.get("liquidity_dollars") or 0),
                "tokens_json": None,
                "active": 1 if m.get("status") == "active" else 0,
                "extra_json": _json.dumps(extra),
                "updated_ts": now,
            })

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        try:
            await self.kalshi.client.__aexit__(None, None, None)
        except Exception:
            pass
        await self.poly.close()
        await self.spot.close()
        self.recorder.event("INFO", "lifecycle", "collector stopped")
        await self.recorder.stop()

    # ------------------------------------------------------------------
    # loops
    # ------------------------------------------------------------------

    async def _poly_loop(self) -> None:
        interval = self.cfg["poly_poll_interval"]
        while self._running:
            t0 = time.time()
            try:
                depth_due = (t0 - self._last_depth) >= self.cfg["depth_interval"]
                n = await self.poly.poll_books(include_depth=depth_due)
                if depth_due:
                    self._last_depth = t0
                if n == 0 and self.poly.errors:
                    logger.warning("poly cycle fetched 0 books (errors=%d)", self.poly.errors)
            except Exception:
                logger.exception("poly cycle error")
            await asyncio.sleep(max(0.2, interval - (time.time() - t0)))

    async def _kalshi_fast_loop(self) -> None:
        """Refill the fast queue each sweep; workers drain it under a shared bucket."""
        while self._running:
            for ticker in self.kalshi.tickers:
                await self._fast_queue.put(ticker)
            await asyncio.sleep(1.0)

    async def _kalshi_slow_loop(self) -> None:
        while self._running:
            for ticker in self.kalshi.slow_tickers:
                await self._slow_queue.put(ticker)
            await asyncio.sleep(self.cfg["kalshi_slow_interval"])

    async def _kalshi_worker(self, queue: asyncio.Queue, name: str) -> None:
        while self._running:
            try:
                ticker = await asyncio.wait_for(queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            try:
                if self.kalshi._backoff > time.time():
                    continue
                await self.kalshi.bucket.acquire()
                data = await self.kalshi.client._get(f"/markets/{ticker}/orderbook")
                self.kalshi.requests += 1
                ob = data.get("orderbook_fp") or data.get("orderbook") or {}
                if ob:
                    self.kalshi._absorb(ticker, ob)
                    self.kalshi.last_success = time.time()
                    self.kalshi.empty_streak = 0
                else:
                    # empty book after internal 429 retries — treat as rate pressure
                    self.kalshi.empty_streak += 1
                    if self.kalshi.empty_streak >= 10:
                        self.kalshi.empty_streak = 0
                        old = self.kalshi.bucket.rate
                        self.kalshi.bucket.rate = max(2.0, old * 0.8)
                        self.kalshi._backoff = time.time() + 5.0
                        logger.warning("kalshi rate %.1f -> %.1f/s (empty streak)",
                                       old, self.kalshi.bucket.rate)
                        self.recorder.event("WARNING", "kalshi",
                                            f"rate cut {old:.1f}->{self.kalshi.bucket.rate:.1f}/s")
            except Exception:
                self.kalshi.errors += 1

    async def _book_writer_loop(self) -> None:
        """Snapshot in-memory books to the DB every poly_snapshot_interval."""
        interval = self.cfg["poly_snapshot_interval"]
        while self._running:
            await asyncio.sleep(interval)
            try:
                now = time.time()
                include_depth = (now - self._last_depth) < interval * 2  # depth just refreshed?
                rows = []
                for feed in (self.poly, self.kalshi):
                    for snap in feed.books.values():
                        for token in ("YES", "NO"):
                            bid = getattr(snap, f"{token.lower()}_bid")
                            ask = getattr(snap, f"{token.lower()}_ask")
                            key = (snap.venue, snap.market, token)
                            if snap.ts_epoch <= self._last_written.get(key, 0):
                                continue
                            self._last_written[key] = snap.ts_epoch
                            rows.append({
                                "ts_epoch": snap.ts_epoch, "venue": snap.venue,
                                "market": snap.market, "token": token,
                                "best_bid": bid, "best_ask": ask,
                                "best_bid_size": getattr(snap, f"{token.lower()}_bid_sz"),
                                "best_ask_size": getattr(snap, f"{token.lower()}_ask_sz"),
                                "depth": getattr(snap, f"{token.lower()}_depth") if include_depth else None,
                            })
                for r in rows:
                    self.recorder.put("book_snapshots", r)
                logger.debug("book_writer: %d rows", len(rows))
            except Exception:
                logger.exception("book_writer error")

    async def _scorer_loop(self) -> None:
        while self._running:
            await asyncio.sleep(2.0)
            try:
                x = self.scorer.score_pairs(self.poly.books, self.kalshi.books,
                                            self.pairs.matched_pairs)
                b = self.scorer.score_bundles(self.poly.books)
                if x or b:
                    logger.debug("scorer: %d xplat, %d bundle rows", x, b)
            except Exception:
                logger.exception("scorer error")

    # ------------------------------------------------------------------

    async def _spot_tick(self) -> None:
        prices = await self.spot.poll()
        for sym, price in prices.items():
            self.recorder.put("spot_prices", {"ts_epoch": time.time(), "symbol": sym, "price": price})

    async def _trades_tick(self) -> None:
        for t in await self.poly.poll_trades():
            self.recorder.put("trades", t)

    def _rollup_tick(self) -> None:
        n = self.recorder.rollup()
        logger.info("rollup: %d buckets updated", n)

    def _integrity_tick(self) -> None:
        ages = self.recorder.gap_check(max_age=self.cfg["max_book_age"] * 4)
        logger.info("integrity: feed ages %s", {k: round(v, 1) for k, v in ages.items()})

    def _prune_tick(self) -> None:
        n = self.recorder.prune(self.cfg["retention_days"])
        self.recorder.event("INFO", "prune", f"deleted {n} rows past retention")

    async def _health_tick(self) -> None:
        health = {
            "ts": time.time(),
            "uptime_s": round(time.time() - self.started, 1),
            "rss_mb": round(rss_mb(), 1),
            "disk_free_gb": round(shutil.disk_usage("/").free / 1e9, 1),
            "poly": {
                "universe": len(self.poly.universe),
                "books": len(self.poly.books),
                "last_success_ago": round(time.time() - self.poly.last_success, 1) if self.poly.last_success else None,
                "errors": self.poly.errors, "requests": self.poly.requests,
            },
            "kalshi": {
                "fast": len(self.kalshi.tickers), "slow": len(self.kalshi.slow_tickers),
                "books": len(self.kalshi.books),
                "last_success_ago": round(time.time() - self.kalshi.last_success, 1) if self.kalshi.last_success else None,
                "errors": self.kalshi.errors, "rate": round(self.kalshi.bucket.rate, 1),
            },
            "spot": {"symbols": self.spot.prices, "errors": self.spot.errors},
            "pairs": len(self.pairs.matched_pairs),
            "opportunities_logged": self.scorer.count,
            "recorder": {"written": self.recorder.written, "dropped": self.recorder.dropped},
        }
        # alarms + rate recovery
        if health["rss_mb"] > self.cfg["max_rss_mb"]:
            self.recorder.event("WARNING", "health", f"rss {health['rss_mb']}MB over budget")
        if health["disk_free_gb"] < self.cfg["min_free_disk_gb"]:
            self.recorder.event("CRITICAL", "health", f"disk free {health['disk_free_gb']}GB")
        if self.kalshi.bucket.rate < self.cfg["kalshi_rate_limit"] \
                and time.time() - self.kalshi.last_success < 60:
            self.kalshi.bucket.rate = min(self.cfg["kalshi_rate_limit"],
                                          self.kalshi.bucket.rate + 0.25)
        # atomic write
        os.makedirs("data", exist_ok=True)
        tmp = "data/health.json.tmp"
        with open(tmp, "w") as f:
            json.dump(health, f)
        os.replace(tmp, "data/health.json")
        # kuma push
        url = self.cfg.get("kuma_push_url")
        if url:
            try:
                async with httpx.AsyncClient(timeout=5.0) as c:
                    await c.get(url, params={"status": "up", "msg": "OK", "ping": ""})
            except Exception as e:
                logger.debug("kuma push failed: %s", e)

    async def _simple_loop(self, interval: float, tick):
        # first run promptly after start
        await asyncio.sleep(5)
        while self._running:
            try:
                r = tick()
                if asyncio.iscoroutine(r):
                    await r
            except Exception:
                logger.exception("loop %s error", getattr(tick, "__name__", tick))
            await asyncio.sleep(interval)

    # ------------------------------------------------------------------

    async def run(self) -> None:
        await self.start()
        # spin up kalshi workers
        for i in range(self.cfg["kalshi_workers"]):
            self._tasks.append(asyncio.create_task(
                self._kalshi_worker(self._fast_queue, f"kw{i}"), name=f"kw{i}"))
        self._tasks.append(asyncio.create_task(
            self._kalshi_worker(self._slow_queue, "kw-slow"), name="kw-slow"))
        sd_notify("READY=1")
        factories = {
            "poly_cycle": self._poly_loop,
            "kalshi_fast": self._kalshi_fast_loop,
            "kalshi_slow": self._kalshi_slow_loop,
            "book_writer": self._book_writer_loop,
            "scorer": self._scorer_loop,
            "refresh_pairs": self._refresh_pairs,
            "spot": lambda: self._simple_loop(self.cfg["spot_interval"], self._spot_tick),
            "trades": lambda: self._simple_loop(self.cfg["trades_interval"], self._trades_tick),
            "rollup": lambda: self._simple_loop(self.cfg["rollup_interval"], self._rollup_tick),
            "integrity": lambda: self._simple_loop(self.cfg["integrity_interval"], self._integrity_tick),
            "prune": lambda: self._simple_loop(self.cfg["prune_interval"], self._prune_tick),
            "health": lambda: self._simple_loop(self.cfg["health_interval"], self._health_tick),
        }
        while self._running:
            await asyncio.sleep(1)
            sd_notify("WATCHDOG=1")
            dead = [t for t in self._tasks if t.done()]
            for t in dead:
                self._tasks.remove(t)
                crashed = (not t.cancelled()) and t.exception() is not None
                if crashed:
                    logger.error("loop %s died: %r — restarting", t.get_name(), t.exception())
                    self.recorder.event("ERROR", "lifecycle",
                                        f"loop {t.get_name()} died: {t.exception()}")
                try:
                    name = t.get_name()
                    if name == "kw-slow":
                        new = asyncio.create_task(self._kalshi_worker(self._slow_queue, name), name=name)
                    elif name.startswith("kw"):
                        new = asyncio.create_task(self._kalshi_worker(self._fast_queue, name), name=name)
                    else:
                        new = asyncio.create_task(factories[name](), name=name)
                    self._tasks.append(new)
                except Exception:
                    logger.exception("restart of %s failed", t.get_name())
                if crashed:
                    await asyncio.sleep(2)  # backoff against tight crash loops


async def amain(cfg: dict) -> None:
    collector = Collector(cfg)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    run_task = asyncio.create_task(collector.run())
    await stop.wait()
    if run_task.done() and not run_task.cancelled() and run_task.exception():
        logger.error("collector run failed", exc_info=run_task.exception())
    logger.info("shutdown signal — draining")
    collector._running = False
    sd_notify("STOPPING=1")
    run_task.cancel()
    await asyncio.gather(run_task, return_exceptions=True)
    await collector.stop()


def main() -> None:
    ap = argparse.ArgumentParser(description="24/7 market data collector")
    ap.add_argument("-c", "--config", default="config.yaml")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            RotatingFileHandler(DEFAULTS["log_file"], maxBytes=20e6, backupCount=3),
        ],
    )
    asyncio.run(amain(load_config(args.config)))


if __name__ == "__main__":
    main()
