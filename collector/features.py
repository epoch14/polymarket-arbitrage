"""
Opportunity Scorer (Phase 0 stub)
=================================

Passive paper layer: computes edges from live books and records EVERY
snapshot (including sub-threshold near-misses) to `opportunity_snapshots`.
This is the dataset decay curves, thresholds, and the Kalman fair-value
model (Phase 1) are trained on — no execution, no signals.

Strategies recorded:
  xplat_yes/     cross-venue divergence (best direction of 4 legs)
  xplat_no/
  bundle_long    Polymarket YES ask + NO ask < 1
  bundle_short   Polymarket YES bid + NO bid > 1

Fee model (Phase 1 replaces with per-market schedules):
  kalshi_fee(p) = 0.07 * p * (1-p)   per share
  poly_taker    = bps of notional
"""

import json
import logging
import time

from collector.db import Recorder
from collector.feeds import BookSnapshot

logger = logging.getLogger(__name__)


def kalshi_fee(p: float) -> float:
    return 0.07 * p * (1.0 - p)


def poly_fee(p: float, taker_bps: float) -> float:
    return (taker_bps / 10000.0) * p


class OpportunityScorer:
    def __init__(
        self,
        recorder: Recorder,
        poly_taker_bps: float = 150.0,
        kalshi_extra_bps: float = 0.0,
        max_book_age: float = 20.0,
    ):
        self.recorder = recorder
        self.poly_taker_bps = poly_taker_bps
        self.kalshi_extra_bps = kalshi_extra_bps
        self.max_book_age = max_book_age
        self._pair_state: dict[str, tuple] = {}    # pair_key -> (hash, last_write)
        self._bundle_state: dict[str, tuple] = {}
        self.count = 0

    # ------------------------------------------------------------------

    def score_pairs(self, poly_books: dict[str, BookSnapshot],
                    kalshi_books: dict[str, BookSnapshot],
                    pairs: list[tuple[str, str]]) -> int:
        """Cross-venue edges for every pair with fresh books on both sides."""
        now = time.time()
        written = 0
        for poly_id, ticker in pairs:
            pb = poly_books.get(poly_id)
            kb = kalshi_books.get(ticker)
            if not pb or not kb:
                continue
            if now - pb.ts_epoch > self.max_book_age or now - kb.ts_epoch > self.max_book_age:
                continue
            h = (pb.price_hash(), kb.price_hash())
            key = f"{poly_id}|{ticker}"
            prev = self._pair_state.get(key)
            # Dedup: write on change, or heartbeat every 60s
            if prev and prev[0] == h and now - prev[1] < 60:
                continue

            best = self._best_cross_edge(pb, kb)
            if best is None:
                self._pair_state[key] = (h, now)
                continue
            direction, gross, net, details = best
            self.recorder.put("opportunity_snapshots", {
                "ts_epoch": now,
                "strategy": f"xplat_{direction.split('_')[0].lower()}",
                "poly_market": poly_id,
                "kalshi_ticker": ticker,
                "poly_yes_bid": pb.yes_bid, "poly_yes_ask": pb.yes_ask,
                "poly_no_bid": pb.no_bid, "poly_no_ask": pb.no_ask,
                "kalshi_yes_bid": kb.yes_bid, "kalshi_yes_ask": kb.yes_ask,
                "kalshi_no_bid": kb.no_bid, "kalshi_no_ask": kb.no_ask,
                "direction": direction,
                "gross_edge": round(gross, 6),
                "net_edge": round(net, 6),
                "edge_pct": round(net / details["buy_price"], 6) if details["buy_price"] else 0,
                "score": round(net, 6),
                "details_json": json.dumps(details, separators=(",", ":")),
            })
            self._pair_state[key] = (h, now)
            self.count += 1
            written += 1
        return written

    def score_bundles(self, poly_books: dict[str, BookSnapshot]) -> int:
        """Single-venue bundle parity checks (Polymarket)."""
        now = time.time()
        written = 0
        for cid, pb in poly_books.items():
            if now - pb.ts_epoch > self.max_book_age:
                continue
            if None in (pb.yes_bid, pb.yes_ask, pb.no_bid, pb.no_ask):
                continue
            total_ask = pb.yes_ask + pb.no_ask
            total_bid = pb.yes_bid + pb.no_bid

            gross_l = 1.0 - total_ask
            net_l = gross_l - poly_fee(total_ask, self.poly_taker_bps)
            gross_s = total_bid - 1.0
            net_s = gross_s - poly_fee(total_bid, self.poly_taker_bps)

            if net_l >= net_s:
                direction, gross, net, strat = "BUNDLE_LONG", gross_l, net_l, "bundle_long"
            else:
                direction, gross, net, strat = "BUNDLE_SHORT", gross_s, net_s, "bundle_short"

            # Record real/near edges only — table stays research-relevant
            if net < -0.01:
                continue
            h = (pb.price_hash(),)
            prev = self._bundle_state.get(cid)
            if prev and prev[0] == h and now - prev[1] < 300:
                continue

            self.recorder.put("opportunity_snapshots", {
                "ts_epoch": now,
                "strategy": strat,
                "poly_market": cid,
                "kalshi_ticker": None,
                "poly_yes_bid": pb.yes_bid, "poly_yes_ask": pb.yes_ask,
                "poly_no_bid": pb.no_bid, "poly_no_ask": pb.no_ask,
                "kalshi_yes_bid": None, "kalshi_yes_ask": None,
                "kalshi_no_bid": None, "kalshi_no_ask": None,
                "direction": direction,
                "gross_edge": round(gross, 6),
                "net_edge": round(net, 6),
                "edge_pct": round(net, 6),
                "score": round(net, 6),
                "details_json": None,
            })
            self._bundle_state[cid] = (h, now)
            self.count += 1
            written += 1
        return written

    # ------------------------------------------------------------------

    def _best_cross_edge(self, pb: BookSnapshot, kb: BookSnapshot):
        """Best net edge across the 4 cross-venue directions."""
        candidates = []
        legs = [
            # (direction, buy_price, sell_price, buy_on_poly, token)
            ("YES_buy_poly", pb.yes_ask, kb.yes_bid, True, "YES"),
            ("YES_buy_kalshi", kb.yes_ask, pb.yes_bid, False, "YES"),
            ("NO_buy_poly", pb.no_ask, kb.no_bid, True, "NO"),
            ("NO_buy_kalshi", kb.no_ask, pb.no_bid, False, "NO"),
        ]
        for direction, buy, sell, buy_on_poly, token in legs:
            if buy is None or sell is None or buy <= 0 or buy >= 1 or sell <= 0 or sell >= 1:
                continue
            if buy_on_poly:
                fees = poly_fee(buy, self.poly_taker_bps) + kalshi_fee(sell)
            else:
                fees = kalshi_fee(buy) + poly_fee(sell, self.poly_taker_bps)
            fees += (self.kalshi_extra_bps / 10000.0) * (buy + sell)
            gross = sell - buy
            net = gross - fees
            candidates.append((direction, gross, net, {
                "buy_price": buy, "sell_price": sell,
                "buy_on_poly": buy_on_poly, "token": token,
                "fees": round(fees, 6),
                "buy_size": (pb.yes_ask_sz if token == "YES" else pb.no_ask_sz) if buy_on_poly
                            else (kb.yes_bid_sz if token == "YES" else kb.no_bid_sz),
                "sell_size": (kb.yes_bid_sz if token == "YES" else kb.no_bid_sz) if buy_on_poly
                             else (pb.yes_bid_sz if token == "YES" else pb.no_bid_sz),
            }))
        if not candidates:
            return None
        return max(candidates, key=lambda c: c[2])
