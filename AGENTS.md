# AGENTS.md — polymarket-arbitrage

Quant research + trading system for Polymarket/Kalshi prediction markets.
Strategy phases and gates: see the quant-shop plan (Phase 0 collector → paper → tiny live).

## Commands

```bash
# venv: uv-managed (system python has no pip)
uv venv .venv && uv pip install --python .venv/bin/python httpx pyyaml websockets aiohttp

# collector (the 24/7 data asset) — DO NOT run manually while service is live
systemctl --user status|restart|stop polymarket-collector
journalctl --user -u polymarket-collector -f        # logs
cat data/health.json                                # liveness/metrics snapshot

# tests
.venv/bin/python -m pytest tests/ -v
```

## Architecture

- `run_collector.py` + `collector/` — 24/7 data collector (systemd user service,
  watchdog via Type=notify). Hot path (books → scorer) never blocks on disk;
  cold path batches to `data/market.db` (SQLite WAL) every 1s.
- `collector/db.py` — schema, batched Recorder, 5-min rollups (`book_rollups_5m`
  kept forever), 14-day raw retention, gap checks.
- `collector/feeds.py` — Polymarket (Gamma universe top-150 by 24h vol, batched
  `POST clob/books`), Kalshi (token-bucket 8 req/s, events→series discovery),
  Spot (Coinbase→Kraken fallback).
- `collector/features.py` — passive OpportunityScorer: logs EVERY edge snapshot
  (incl. near-misses) to `opportunity_snapshots`. No execution.
- `collector/universe.py` — candidate pairs via `core/cross_platform_arb.py`
  MarketMatcher; all pairs `status='pending'`; unverified pairs never signal.
- `core/`, `polymarket_client/`, `kalshi_client/` — legacy trading bot (dry_run).
  The cross-platform arb engine here is UNWIRED from main.py (known issue).

## Kalshi API gotchas (hard-won)

- Market listing is flooded by auto-generated MVE parlay markets — discover via
  `/events` (skip `KXMVE*`) → `series_ticker` → `/markets?series_ticker=...`.
- API migrated to dollar-string fields: `yes_bid_dollars`, `volume_24h_fp`, ...
  `kalshi_client/` parsers are STALE against this (still expect cent ints).
- Orderbook response key is `orderbook_fp`:
  `{"yes_dollars": [[price, size], ...], "no_dollars": [...]}` (dollars).
- Public rate limit is tight (~8-10 req/s) — token bucket + 429 backoff in feeds.

## Secrets & ops

- `deploy/collector.env` holds `KUMA_PUSH_URL` (Uptime Kuma push token — SECRET,
  gitignored, chmod 600). Service file references it via `EnvironmentFile=-`.
- `logs/` is root-owned (legacy) — collector logs go to `data/collector.log`
  and journald instead.
- Never commit `data/`, `.venv/`, `deploy/collector.env`.

## Repo quirks

- `polymarket_client/api.py` has uncommitted local modifications (pre-existing).
- Box is RAM-constrained (~1.5G free) — collector budget 500M (MemoryMax), keep it lean.
