"""
Collector Feeds
===============

Lean async feeds for the collector:

- PolymarketFeed: universe discovery via Gamma (top-N by 24h volume) and
  batched order books via POST clob/books (multiple token_ids per request).
- KalshiFeed: round-robin orderbook polling over matched candidate tickers,
  rate-limited with a token bucket and 429-aware global backoff.
- SpotFeed: crypto spot prices (Coinbase primary, Kraken fallback).

All feeds are tolerant: failures are logged and counted, never fatal.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_URL = "https://clob.polymarket.com"
DATA_API_URL = "https://data-api.polymarket.com"
KALSHI_URL = "https://api.elections.kalshi.com/trade-api/v2"
COINBASE_URL = "https://api.coinbase.com/v2/prices"
KRAKEN_URL = "https://api.kraken.com/0/public/Ticker"


class TokenBucket:
    """Simple async token bucket for per-venue rate budgets."""

    def __init__(self, rate: float, burst: float | None = None):
        self.rate = rate
        self.capacity = burst or max(rate, 1.0)
        self.tokens = self.capacity
        self.updated = time.monotonic()

    async def acquire(self) -> None:
        while True:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            if self.tokens >= 1:
                self.tokens -= 1
                return
            await asyncio.sleep((1 - self.tokens) / self.rate)


@dataclass
class PolyMarket:
    """A Polymarket market in the collector universe."""
    condition_id: str
    question: str
    yes_token: str
    no_token: str
    end_date: str = ""
    volume_24h: float = 0.0
    liquidity: float = 0.0
    category: str = ""


@dataclass
class BookSnapshot:
    """Top-of-book (+ optional depth) for one binary market on one venue."""
    venue: str
    market: str                       # condition_id or kalshi ticker
    ts_epoch: float
    yes_bid: float | None = None
    yes_ask: float | None = None
    no_bid: float | None = None
    no_ask: float | None = None
    yes_bid_sz: float | None = None
    yes_ask_sz: float | None = None
    no_bid_sz: float | None = None
    no_ask_sz: float | None = None
    yes_depth: str | None = None      # JSON, sampled at depth_interval
    no_depth: str | None = None

    def price_hash(self) -> tuple:
        """Identity of the price state — used to dedup DB writes."""
        return (self.yes_bid, self.yes_ask, self.no_bid, self.no_ask,
                self.yes_bid_sz, self.yes_ask_sz, self.no_bid_sz, self.no_ask_sz)


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

class PolymarketFeed:
    """Gamma discovery + batched CLOB books. One collector, no auth needed."""

    def __init__(self, universe_size: int = 150, chunk_size: int = 50):
        self.universe_size = universe_size
        self.chunk_size = chunk_size
        self._client = httpx.AsyncClient(timeout=15.0)
        self.books: dict[str, BookSnapshot] = {}       # condition_id -> latest
        self.universe: dict[str, PolyMarket] = {}
        self.errors = 0
        self.requests = 0
        self.last_success: float = 0.0

    async def close(self) -> None:
        await self._client.aclose()

    async def refresh_universe(self) -> int:
        """Top-N active markets by 24h volume. Returns universe size."""
        try:
            seen: dict[str, PolyMarket] = {}
            for page in range(4):  # 4 x 100 = 400 candidates, keep top N
                r = await self._client.get(f"{GAMMA_URL}/markets", params={
                    "active": "true", "closed": "false", "limit": 100,
                    "offset": page * 100,
                    "order": "volume24hr", "ascending": "false",
                })
                r.raise_for_status()
                for m in r.json():
                    cid = m.get("conditionId")
                    tokens_raw = m.get("clobTokenIds")
                    if not cid or not tokens_raw:
                        continue
                    try:
                        tokens = json_loads(tokens_raw)
                    except Exception:
                        continue
                    if len(tokens) < 2:
                        continue
                    seen[cid] = PolyMarket(
                        condition_id=cid,
                        question=m.get("question", ""),
                        yes_token=str(tokens[0]),
                        no_token=str(tokens[1]),
                        end_date=m.get("endDate") or "",
                        volume_24h=float(m.get("volume24hr") or 0),
                        liquidity=float(m.get("liquidityNum") or 0),
                    )
            self.universe = dict(
                sorted(seen.items(), key=lambda kv: kv[1].volume_24h, reverse=True)
                [:self.universe_size]
            )
            logger.info("Universe refreshed: %d markets (from %d candidates)",
                        len(self.universe), len(seen))
            return len(self.universe)
        except Exception:
            self.errors += 1
            logger.exception("Universe refresh failed")
            return len(self.universe)

    async def poll_books(self, include_depth: bool = False) -> int:
        """Fetch books for the whole universe in chunked batch calls."""
        markets = list(self.universe.values())
        if not markets:
            return 0
        token_to_market: dict[str, PolyMarket] = {}
        chunks: list[list[dict]] = []
        chunk: list[dict] = []
        for m in markets:
            token_to_market[m.yes_token] = m
            token_to_market[m.no_token] = m
            chunk.append({"token_id": m.yes_token})
            chunk.append({"token_id": m.no_token})
            if len(chunk) >= self.chunk_size:
                chunks.append(chunk)
                chunk = []
        if chunk:
            chunks.append(chunk)

        fetched = 0
        ts = time.time()
        for c in chunks:
            try:
                r = await self._client.post(f"{CLOB_URL}/books", json=c)
                self.requests += 1
                if r.status_code == 404:
                    # Some token died; universe refresh will prune it shortly
                    continue
                r.raise_for_status()
                for book in r.json():
                    m = token_to_market.get(book.get("asset_id", ""))
                    if not m:
                        continue
                    self._absorb(m, book, ts, include_depth)
                    fetched += 1
                self.last_success = time.time()
            except Exception:
                self.errors += 1
                await asyncio.sleep(0.5)
        return fetched

    def _absorb(self, m: PolyMarket, book: dict, ts: float, include_depth: bool) -> None:
        bids = sorted(
            (float(b["price"]), float(b["size"])) for b in book.get("bids", [])
        )
        asks = sorted(
            (float(a["price"]), float(a["size"])) for a in book.get("asks", [])
        )
        is_yes = book.get("asset_id") == m.yes_token
        snap = self.books.get(m.condition_id) or BookSnapshot(
            venue="polymarket", market=m.condition_id, ts_epoch=ts)
        if is_yes:
            snap.yes_bid, snap.yes_bid_sz = (bids[-1] if bids else (None, None))
            snap.yes_ask, snap.yes_ask_sz = (asks[0] if asks else (None, None))
            if include_depth and (bids or asks):
                snap.yes_depth = json_dumps({"bids": bids[-5:], "asks": asks[:5]})
        else:
            snap.no_bid, snap.no_bid_sz = (bids[-1] if bids else (None, None))
            snap.no_ask, snap.no_ask_sz = (asks[0] if asks else (None, None))
            if include_depth and (bids or asks):
                snap.no_depth = json_dumps({"bids": bids[-5:], "asks": asks[:5]})
        snap.ts_epoch = ts
        self.books[m.condition_id] = snap

    async def poll_trades(self, since_trade_id: str | None = None) -> list[dict]:
        """Public trade prints (data-api). Tolerant — returns [] on failure."""
        trades: list[dict] = []
        try:
            for cid in list(self.universe)[:self.universe_size]:
                r = await self._client.get(f"{DATA_API_URL}/trades", params={
                    "market": cid, "limit": 50, "takerOnly": "false"})
                self.requests += 1
                if r.status_code != 200:
                    continue
                r.raise_for_status()
                for t in r.json():
                    trades.append({
                        "ts_epoch": parse_iso_ts(t.get("timestamp")),
                        "venue": "polymarket",
                        "market": cid,
                        "token": "YES" if t.get("side") == "BUY" else None,  # refined below
                        "price": float(t.get("price") or 0),
                        "size": float(t.get("size") or 0),
                        "side": t.get("side"),
                        "trade_id": t.get("transactionHash"),
                    })
        except Exception:
            self.errors += 1
        return trades


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

class KalshiFeed:
    """Rate-limited round-robin orderbook polling. Reuses KalshiClient parsing."""

    def __init__(self, rate_limit: float = 8.0):
        from kalshi_client.api import KalshiClient
        self.client = KalshiClient(timeout=10.0, max_retries=2)
        self.bucket = TokenBucket(rate_limit)
        self.tickers: list[str] = []                  # active poll set (matched pairs first)
        self.slow_tickers: list[str] = []             # lower-priority, polled at slow cadence
        self.books: dict[str, BookSnapshot] = {}      # ticker -> latest
        self.errors = 0
        self.requests = 0
        self.empty_streak = 0                         # consecutive empty books (429 smell)
        self.last_success: float = 0.0
        self._backoff: float = 0.0                    # global 429 cooldown

    async def refresh_universe(self, max_series: int = 120, top_n: int = 400) -> int:
        """
        Discover real (non-parlay) Kalshi markets.

        The /markets listing is flooded by auto-generated MVE combo markets,
        so discovery routes through /events -> series -> /markets?series_ticker.
        Markets carry dollar-string fields (yes_bid_dollars, volume_24h_fp, ...).
        """
        try:
            # 1. events -> distinct series (skip MVE combos)
            cursor, series = None, []
            seen_series = set()
            for _ in range(8):
                params = {"status": "open", "limit": 200}
                if cursor:
                    params["cursor"] = cursor
                data = await self.client._get("/events", params)
                for e in data.get("events", []):
                    st = e.get("series_ticker")
                    if st and not e.get("event_ticker", "").startswith("KXMVE") \
                            and st not in seen_series:
                        seen_series.add(st)
                        series.append(st)
                cursor = data.get("cursor")
                if not cursor or len(series) >= max_series:
                    break

            # 2. per-series market fetch, ranked by 24h volume
            raw_markets: list[dict] = []
            for st in series[:max_series]:
                await self.bucket.acquire()
                try:
                    data = await self.client._get(
                        "/markets", {"series_ticker": st, "status": "open", "limit": 1000})
                    raw_markets.extend(data.get("markets", []))
                except Exception:
                    continue

            def vol24(m: dict) -> float:
                try:
                    return float(str(m.get("volume_24h_fp") or 0).replace(",", ""))
                except ValueError:
                    return 0.0

            raw_markets = [m for m in raw_markets
                           if m.get("ticker") and not m.get("custom_strike")]
            raw_markets.sort(key=vol24, reverse=True)
            self._raw_markets = raw_markets[:top_n]
            logger.info("Kalshi universe: %d series -> %d markets (top %d kept)",
                        len(series), len(raw_markets), len(self._raw_markets))
            return len(self._raw_markets)
        except Exception:
            self.errors += 1
            logger.exception("Kalshi universe refresh failed")
            return 0

    def set_poll_sets(self, matched: list[str], extra: list[str]) -> None:
        """Matched-pair tickers poll fast; extra tickers poll slow."""
        self.tickers = matched
        self.slow_tickers = [t for t in extra if t not in set(matched)]
        logger.info("Kalshi poll sets: %d fast, %d slow", len(self.tickers), len(self.slow_tickers))

    def _absorb(self, ticker: str, ob: dict) -> None:
        """Parse orderbook_fp: {'yes_dollars': [[price, size], ...], 'no_dollars': [...]}."""
        def levels(side: list) -> list[tuple[float, float]]:
            out = []
            for price, size in side or []:
                try:
                    out.append((float(price), float(size)))
                except (TypeError, ValueError):
                    continue
            return sorted(out)

        yes_bids = levels(ob.get("yes_dollars") or ob.get("yes"))
        no_bids = levels(ob.get("no_dollars") or ob.get("no"))
        self.books[ticker] = BookSnapshot(
            venue="kalshi", market=ticker, ts_epoch=time.time(),
            yes_bid=yes_bids[-1][0] if yes_bids else None,
            yes_ask=1.0 - no_bids[0][0] if no_bids else None,
            no_bid=no_bids[-1][0] if no_bids else None,
            no_ask=1.0 - yes_bids[0][0] if yes_bids else None,
            yes_bid_sz=yes_bids[-1][1] if yes_bids else None,
            no_bid_sz=no_bids[-1][1] if no_bids else None,
        )


# ---------------------------------------------------------------------------
# Spot
# ---------------------------------------------------------------------------

class SpotFeed:
    """Crypto spot: Coinbase primary, Kraken fallback, one symbol per call."""

    KRAKEN_MAP = {"BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD"}

    def __init__(self, symbols: list[str]):
        self.symbols = symbols
        self._client = httpx.AsyncClient(timeout=10.0)
        self.prices: dict[str, float] = {}
        self.errors = 0
        self.last_success: float = 0.0

    async def close(self) -> None:
        await self._client.aclose()

    async def poll(self) -> dict[str, float]:
        for sym in self.symbols:
            price = await self._coinbase(sym) or await self._kraken(sym)
            if price:
                self.prices[sym] = price
                self.last_success = time.time()
            else:
                self.errors += 1
        return self.prices

    async def _coinbase(self, sym: str) -> float | None:
        try:
            r = await self._client.get(f"{COINBASE_URL}/{sym}-USD/spot")
            r.raise_for_status()
            return float(r.json()["data"]["amount"])
        except Exception:
            return None

    async def _kraken(self, sym: str) -> float | None:
        try:
            r = await self._client.get(KRAKEN_URL, params={"pair": self.KRAKEN_MAP.get(sym, sym + "USD")})
            r.raise_for_status()
            result = r.json().get("result", {})
            first = next(iter(result.values()), None)
            if first:
                return float(first["c"][0])  # last trade price
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def json_loads(s):
    import json
    return json.loads(s)


def json_dumps(obj) -> str:
    import json
    return json.dumps(obj, separators=(",", ":"))


def parse_iso_ts(raw) -> float:
    """Parse a timestamp (unix seconds, ms, or ISO) into epoch seconds."""
    if raw is None:
        return time.time()
    try:
        v = float(raw)
        return v / 1000.0 if v > 1e12 else v
    except (TypeError, ValueError):
        pass
    try:
        from datetime import datetime, timezone
        s = str(raw).replace("Z", "+00:00")
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return time.time()
