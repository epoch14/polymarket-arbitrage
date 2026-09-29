"""
Paper Trading Engine
====================

Simulated execution against LIVE in-memory books — the honest bridge between
"detection" and "live trading". No real orders. Every fill is:

  - latency-exposed: intent executes on the NEXT scorer tick (~2s later)
  - slippage-exposed: pays one tick adverse from the intended price
  - depth-limited: qty capped at 50% of displayed best size AND notional budget
  - fee-charged: full fee model per fill

Strategies gated:
  bundle_long/short  — single venue, no match risk: paper-trades immediately
  xplat_yes/no       — only for pairs with status='verified' in the pairs table

Positions are held (no exit logic yet) and marked to market every tick;
the equity curve in `paper_equity` is the strategy's first report card.
"""

import json
import logging
import time

logger = logging.getLogger(__name__)


class PaperTrader:
    def __init__(
        self,
        recorder,
        balance: float = 100_000.0,
        notional_per_trade: float = 1_000.0,
        poly_taker_bps: float = 150.0,
        kalshi_fee_factor: float = 0.07,
        max_book_age: float = 30.0,
        verified_pairs: set | None = None,
    ):
        self.recorder = recorder
        self.balance = balance
        self.start_balance = balance
        self.notional_per_trade = notional_per_trade
        self.poly_taker_bps = poly_taker_bps
        self.kalshi_fee_factor = kalshi_fee_factor
        self.max_book_age = max_book_age
        self.verified_pairs: set = verified_pairs or set()   # {(poly_id, ticker)}

        self.cash = balance
        self.positions: dict[str, dict] = {}   # pos_key -> {qty, cost, legs:[...], detail}
        self._pending: list[dict] = []          # intents awaiting next-tick fill
        self._seen_keys: set = set()
        self.trades = 0
        self.rejected = {"stale": 0, "cash": 0, "dup": 0, "gate": 0, "nodepth": 0}

    # ------------------------------------------------------------------
    # intake — called right after the scorer writes edge rows
    # ------------------------------------------------------------------

    def on_edges(self, rows: list[dict], poly_books, kalshi_books) -> None:
        """Queue intents from fresh edge rows. rows = scorer's last output."""
        now = time.time()
        for r in rows:
            strat = r["strategy"]
            key = f"{r.get('poly_market')}|{r.get('kalshi_ticker')}|{r['direction']}"
            if key in self._seen_keys:
                self.rejected["dup"] += 1
                continue
            if r["net_edge"] is None or r["net_edge"] <= 0:
                continue
            if strat.startswith("xplat"):
                pair = (r.get("poly_market"), r.get("kalshi_ticker"))
                if pair not in self.verified_pairs:
                    self.rejected["gate"] += 1
                    continue
            self._seen_keys.add(key)
            self._pending.append({"row": r, "queued_at": now})

        # execute intents queued on a PREVIOUS tick (latency simulation)
        still_pending = []
        for intent in self._pending:
            if now - intent["queued_at"] < 1.5:
                still_pending.append(intent)      # executes next tick
                continue
            self._try_fill(intent, poly_books, kalshi_books, now)
        self._pending = still_pending
        self._seen_keys = set(list(self._seen_keys)[-5000:])   # bound memory

    # ------------------------------------------------------------------

    def _try_fill(self, intent: dict, poly_books, kalshi_books, now: float) -> None:
        r = intent["row"]
        direction = r["direction"]
        try:
            if direction == "BUNDLE_LONG":
                pb = poly_books.get(r["poly_market"])
                if not pb or now - pb.ts_epoch > self.max_book_age \
                        or None in (pb.yes_ask, pb.no_ask, pb.yes_ask_sz, pb.no_ask_sz):
                    self.rejected["stale"] += 1
                    return
                qty = self._qty(pb.yes_ask, min(pb.yes_ask_sz, pb.no_ask_sz))
                if qty <= 0:
                    self.rejected["nodepth"] += 1
                    return
                leg_a = ("YES", pb.yes_ask + 0.01)
                leg_b = ("NO", pb.no_ask + 0.01)
                self._open("bundle_long", r["poly_market"], [leg_a, leg_b], qty,
                           f"edge={r['net_edge']:.4f}")
            else:
                # cross-venue: buy leg fills at ask+1tick on buy venue
                buy_on_poly = r["details_json"] and \
                    json.loads(r["details_json"]).get("buy_on_poly")
                token = (r["details_json"] and json.loads(r["details_json"]).get("token")) or "YES"
                pb = poly_books.get(r["poly_market"])
                kb = kalshi_books.get(r["kalshi_ticker"])
                if not pb or not kb:
                    self.rejected["stale"] += 1
                    return
                if buy_on_poly:
                    px, sz = (pb.yes_ask, pb.yes_ask_sz) if token == "YES" \
                        else (pb.no_ask, pb.no_ask_sz)
                    if px is None or now - pb.ts_epoch > self.max_book_age:
                        self.rejected["stale"] += 1
                        return
                    qty = self._qty(px + 0.01, sz)
                    if qty <= 0:
                        self.rejected["nodepth"] += 1
                        return
                    self._open(f"xplat_{token.lower()}", f"poly:{r['poly_market']}",
                               [(token, px + 0.01)], qty, f"vs kalshi:{r['kalshi_ticker']}")
                else:
                    px = kb.yes_bid if token == "YES" else kb.no_bid
                    sz = kb.yes_bid_sz if token == "YES" else kb.no_bid_sz
                    # kalshi sells execute against the derived ask: 1 - opposite bid
                    opp_bid = kb.no_bid if token == "YES" else kb.yes_bid
                    if px is None or opp_bid is None or now - kb.ts_epoch > self.max_book_age:
                        self.rejected["stale"] += 1
                        return
                    fill_px = round(1.0 - opp_bid - 0.01, 4)   # derived ask, 1 tick worse
                    qty = self._qty(fill_px, sz)
                    if qty <= 0:
                        self.rejected["nodepth"] += 1
                        return
                    self._open(f"xplat_{token.lower()}", f"kalshi:{r['kalshi_ticker']}",
                               [(token, fill_px)], qty, f"vs poly:{r['poly_market']}")
        except Exception:
            logger.exception("paper fill failed")

    def _qty(self, price: float, depth_size) -> float:
        if not price or price <= 0 or price >= 1 or not depth_size:
            return 0.0
        by_notional = self.notional_per_trade / price
        by_depth = float(depth_size) * 0.5
        qty = min(by_notional, by_depth)
        cost = qty * price
        if cost > self.cash:
            return 0.0
        return round(qty, 2)

    def _open(self, strategy: str, market: str, legs: list, qty: float, detail: str) -> None:
        now = time.time()
        fee = 0.0
        cost = 0.0
        priced_legs = []
        for token, px in legs:
            fee += self._fee(strategy, px)
            cost += qty * px
            priced_legs.append([token, px])
        if cost + fee > self.cash:
            self.rejected["cash"] += 1
            return
        self.cash -= (cost + fee)
        pos_key = f"{strategy}|{market}"
        pos = self.positions.get(pos_key)
        if pos:
            total_qty = pos["qty"] + qty
            pos["cost"] = (pos["cost"] * pos["qty"] + cost + fee) / total_qty
            pos["qty"] = total_qty
        else:
            self.positions[pos_key] = {
                "qty": qty, "cost": cost + fee,
                "strategy": strategy, "market": market,
                "legs": priced_legs, "detail": detail, "opened": now,
            }
        self.trades += 1
        self.recorder.put("paper_trades", {
            "ts_epoch": now, "strategy": strategy, "market": market,
            "detail": detail, "side": "BUY", "qty": qty,
            "price": round(cost / qty, 4), "notional": round(cost, 2),
            "fee": round(fee, 4), "trade_key": f"{pos_key}|{now}",
        })

    def _fee(self, strategy: str, px: float) -> float:
        if strategy.startswith("xplat") and "kalshi:" in strategy:
            return self.kalshi_fee_factor * px * (1 - px)
        return (self.poly_taker_bps / 10000.0) * px

    # ------------------------------------------------------------------
    # mark to market
    # ------------------------------------------------------------------

    def mark(self, poly_books, kalshi_books) -> dict:
        """MTM all positions. Returns equity snapshot."""
        now = time.time()
        total_val = 0.0
        stale_positions = 0
        for pos_key, pos in list(self.positions.items()):
            val = 0.0
            ok = True
            for token, _entry_px in pos["legs"]:
                market = pos["market"]
                if market.startswith("poly:"):
                    pb = poly_books.get(market[5:])
                    if not pb or now - pb.ts_epoch > self.max_book_age:
                        ok = False
                        break
                    bid = pb.yes_bid if token == "YES" else pb.no_bid
                    ask = pb.yes_ask if token == "YES" else pb.no_ask
                    mid = (bid + ask) / 2 if (bid is not None and ask is not None) else None
                else:
                    kb = kalshi_books.get(market[7:])
                    if not kb or now - kb.ts_epoch > self.max_book_age:
                        ok = False
                        break
                    bid = kb.yes_bid if token == "YES" else kb.no_bid
                    opp = kb.no_bid if token == "YES" else kb.yes_bid
                    ask = 1.0 - opp if opp is not None else None
                    mid = (bid + ask) / 2 if (bid is not None and ask is not None) else None
                if mid is None:
                    ok = False
                    break
                val += pos["qty"] * mid
            if ok:
                total_val += val
            else:
                # value at cost until books return (conservative: no fake gains)
                total_val += pos["qty"] * pos["cost"]
                stale_positions += 1
        equity = self.cash + total_val
        snap = {
            "ts_epoch": now,
            "balance": round(self.cash, 2),
            "positions_value": round(total_val, 2),
            "equity": round(equity, 2),
            "open_positions": len(self.positions),
        }
        if self.trades or self.positions:
            self.recorder.put("paper_equity", snap)
        self._last_snap = {**snap, "stale_valued": stale_positions,
                           "pnl": round(equity - self.start_balance, 2),
                           "trades": self.trades, "rejected": dict(self.rejected)}
        return self._last_snap

    @property
    def snapshot(self) -> dict:
        return getattr(self, "_last_snap", {"pnl": 0.0, "trades": 0})
