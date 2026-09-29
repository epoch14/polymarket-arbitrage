"""
Universe & Pair Management
==========================

Keeps the collector's view of "what should we watch" current:

- Polymarket universe: handled by PolymarketFeed.refresh_universe (Gamma top-N).
- Kalshi universe: ranked open markets (KalshiFeed.refresh_universe).
- Cross-venue pairs: the existing MarketMatcher (fuzzy + sports/person heuristics)
  runs hourly as a *candidate generator*. Every pair lands in the `pairs` table
  with status='pending' — Phase 1 adds verification; unverified pairs never signal.
"""

import logging
import time
from types import SimpleNamespace

from collector.db import Recorder

logger = logging.getLogger(__name__)


class PairManager:
    """Runs the candidate matcher and persists pairs with pending status."""

    def __init__(self, recorder: Recorder, kalshi_feed, min_similarity: float = 0.6):
        self.recorder = recorder
        self.kalshi_feed = kalshi_feed
        self.min_similarity = min_similarity
        self.pairs: dict[str, tuple[str, str]] = {}   # pair_id -> (poly_market, kalshi_ticker)
        self.matched_pairs: list[tuple[str, str]] = []
        self.last_run: float = 0.0

    async def refresh(self, poly_feed) -> int:
        """Match poly universe against kalshi universe. Returns pair count."""
        from core.cross_platform_arb import MarketMatcher

        poly_shims = [
            SimpleNamespace(market_id=m.condition_id, question=m.question, active=True)
            for m in poly_feed.universe.values()
        ]
        kalshi_shims = []
        for raw in getattr(self.kalshi_feed, "_raw_markets", []):
            title = raw.get("title") or ""
            # multi-outcome list markets ("yes A, yes B, ...") are not binary
            # equivalents — exclude from candidate matching
            low = title.lower()
            if low.startswith(("yes ", "no ")) and (",yes " in low or ",no " in low):
                continue
            kalshi_shims.append(SimpleNamespace(
                ticker=raw.get("ticker"), title=title, is_active=True))
        if not poly_shims or not kalshi_shims:
            logger.warning("Pair match skipped: poly=%d kalshi=%d", len(poly_shims), len(kalshi_shims))
            return 0

        matcher = MarketMatcher(min_similarity=self.min_similarity)
        t0 = time.time()
        matches = await matcher.find_matches(poly_shims, kalshi_shims)
        logger.info("Matcher: %d candidate pairs in %.1fs", len(matches), time.time() - t0)

        now = time.time()
        fresh: list[tuple[str, str]] = []
        for p in matches:
            self.recorder.put("pairs", {
                "poly_market": p.polymarket_id,
                "kalshi_ticker": p.kalshi_ticker,
                "similarity": p.similarity_score,
                "status": "pending",
                "notes": f"[auto] {p.category} sim={p.similarity_score:.2f}",
                "created_ts": now,
                "updated_ts": now,
            })
            fresh.append((p.polymarket_id, p.kalshi_ticker))
        self.matched_pairs = fresh
        self.pairs = {f"poly:{a}|kalshi:{b}": (a, b) for a, b in fresh}
        self.last_run = now
        return len(fresh)
