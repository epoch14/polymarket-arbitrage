#!/usr/bin/env python3
"""
Report Generator
================

Builds daily/weekly/monthly HTML reports from data/market.db:

  - KPI cards (equity, PnL windows, trades, buys/sells, opportunities, coverage)
  - Inline-SVG equity curve (email-safe, no JS)
  - Paper trades table + buy/sell breakdown
  - Opportunity edge stats + top edges
  - Feed/data health

Writes reports/<stamp>-<period>.html and POSTs {period, subject, html} to the
n8n webhook in REPORT_WEBHOOK_URL (deploy/reporting.env) which emails it.

Usage:
    .venv/bin/python generate_report.py daily|weekly|monthly [--no-send]
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

import httpx
import yaml

DB = "data/market.db"
REPORTS_DIR = "reports"
WEBHOOK_ENV = "deploy/reporting.env"

DARK = "#0f172a"
ACCENT = "#22d3ee"
GREEN = "#34d399"
RED = "#f87171"
MUTED = "#94a3b8"

CSS = f"""
body {{ margin:0; padding:0; background:#f1f5f9; font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; }}
.wrap {{ max-width:760px; margin:0 auto; background:#ffffff; }}
.header {{ background:{DARK}; padding:28px 32px; }}
.header h1 {{ color:#ffffff; margin:0; font-size:22px; }}
.header .sub {{ color:{ACCENT}; font-size:13px; margin-top:6px; letter-spacing:0.5px; }}
.section {{ padding:20px 32px; }}
.section h2 {{ font-size:15px; color:{DARK}; border-bottom:2px solid {ACCENT}; padding-bottom:6px; margin:18px 0 12px; }}
.cards {{ display:flex; flex-wrap:wrap; gap:10px; }}
.card {{ flex:1 1 30%; min-width:150px; background:#f8fafc; border:1px solid #e2e8f0; border-radius:8px; padding:12px 14px; }}
.card .label {{ font-size:11px; color:{MUTED}; text-transform:uppercase; letter-spacing:0.5px; }}
.card .value {{ font-size:20px; font-weight:700; color:{DARK}; margin-top:4px; }}
table.data {{ width:100%; border-collapse:collapse; font-size:12px; }}
table.data th {{ background:{DARK}; color:#fff; text-align:left; padding:7px 9px; font-size:11px; text-transform:uppercase; letter-spacing:0.4px; }}
table.data td {{ padding:6px 9px; border-bottom:1px solid #e2e8f0; color:#334155; }}
table.data tr:nth-child(even) td {{ background:#f8fafc; }}
.pos {{ color:{GREEN}; font-weight:600; }}
.neg {{ color:{RED}; font-weight:600; }}
.muted {{ color:{MUTED}; font-size:11px; }}
.footer {{ padding:16px 32px; color:{MUTED}; font-size:11px; border-top:1px solid #e2e8f0; }}
"""


def q(c: sqlite3.Connection, sql: str, args: tuple = ()) -> list:
    return c.execute(sql, args).fetchall()


def one(c, sql, args=()):
    r = c.execute(sql, args).fetchone()
    return r[0] if r else None


def fmt_usd(v) -> str:
    if v is None:
        return "—"
    cls = "pos" if v >= 0 else "neg"
    return f'<span class="{cls}">${v:,.2f}</span>'


def esc(s) -> str:
    s = str(s) if s is not None else ""
    return (s.replace("&", "&amp;").replace("<", "&lt;")
             .replace(">", "&gt;").replace('"', "&quot;"))


def equity_chart(c, since: float, width: int = 680, height: int = 160) -> str:
    rows = q(c, """SELECT ts_epoch, equity FROM paper_equity
                   WHERE ts_epoch >= ? ORDER BY ts_epoch""", (since,))
    if len(rows) < 2:
        bal = one(c, "SELECT balance FROM paper_equity ORDER BY id DESC LIMIT 1") or 100000.0
        return (f'<div style="border:1px solid #e2e8f0;border-radius:8px;padding:10px">'
                f'<svg width="100%" height="{height}" viewBox="0 0 {width} {height}" '
                f'preserveAspectRatio="none"><line x1="0" y1="{height//2}" x2="{width}" '
                f'y2="{height//2}" stroke="{MUTED}" stroke-dasharray="4,4"/>'
                f'<text x="10" y="{height//2 - 8}" fill="{MUTED}" font-size="11">'
                f'No equity curve yet — flat at ${bal:,.0f}. Curve appears with the first paper trade.</text></svg></div>')
    pts = [(ts, eq) for ts, eq in rows]
    vals = [eq for _, eq in pts]
    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1.0
    t0, t1 = pts[0][0], pts[-1][0]
    tspan = (t1 - t0) or 1.0
    coords = [(int((ts - t0) / tspan * (width - 20)) + 10,
               int((1 - (eq - lo) / span) * (height - 30)) + 15) for ts, eq in pts]
    poly = " ".join(f"{x},{y}" for x, y in coords)
    last = vals[-1]
    color = GREEN if last >= vals[0] else RED
    return (f'<div style="border:1px solid #e2e8f0;border-radius:8px;padding:10px">'
            f'<svg width="100%" height="{height}" viewBox="0 0 {width} {height}" preserveAspectRatio="none">'
            f'<polyline points="{poly}" fill="none" stroke="{color}" stroke-width="2"/>'
            f'</svg><div class="muted">Equity {datetime.fromtimestamp(t0, tz=timezone.utc).strftime("%m/%d %H:%M")} → '
            f'{datetime.fromtimestamp(t1, tz=timezone.utc).strftime("%m/%d %H:%M")} UTC · '
            f'low ${lo:,.2f} · high ${hi:,.2f} · now ${last:,.2f}</div></div>')


def window_pnl(c, since: float) -> tuple:
    """(pnl_since_window, first_equity, last_equity) from paper_equity snapshots."""
    first = one(c, "SELECT equity FROM paper_equity WHERE ts_epoch >= ? ORDER BY id ASC LIMIT 1", (since,))
    last = one(c, "SELECT equity FROM paper_equity ORDER BY id DESC LIMIT 1")
    if first is None or last is None:
        return None, first, last
    return round(last - first, 2), first, last


def build(period: str) -> tuple[str, str]:
    c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    now = time.time()
    gen_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    wins = {"daily": 86400, "weekly": 7 * 86400, "monthly": 30 * 86400}
    win = wins.get(period, 86400)
    since = now - win

    # ---- paper trading ----
    bal = one(c, "SELECT balance FROM paper_equity ORDER BY id DESC LIMIT 1") or 100000.0
    eq_now = one(c, "SELECT equity FROM paper_equity ORDER BY id DESC LIMIT 1") or bal
    n_trades = one(c, "SELECT COUNT(*) FROM paper_trades") or 0
    n_trades_win = one(c, "SELECT COUNT(*) FROM paper_trades WHERE ts_epoch >= ?", (since,)) or 0
    open_pos = one(c, "SELECT COUNT(DISTINCT market || strategy) FROM paper_trades") or 0
    buys = one(c, "SELECT COUNT(*) FROM paper_trades WHERE side='BUY'") or 0
    sells = one(c, "SELECT COUNT(*) FROM paper_trades WHERE side='SELL'") or 0
    notional = one(c, "SELECT SUM(notional) FROM paper_trades") or 0.0
    fees = one(c, "SELECT SUM(fee) FROM paper_trades") or 0.0
    pnl_win, eq_first, eq_last = window_pnl(c, since)
    pnl_all = round(eq_now - 100000.0, 2)

    # ---- opportunities ----
    opps_win = one(c, "SELECT COUNT(*) FROM opportunity_snapshots WHERE ts_epoch >= ?", (since,)) or 0
    by_strat = q(c, """SELECT strategy, COUNT(*), ROUND(AVG(net_edge),4), ROUND(MAX(net_edge),4)
                       FROM opportunity_snapshots WHERE ts_epoch >= ? GROUP BY strategy""", (since,))
    top_edges = q(c, """SELECT strategy, poly_market, kalshi_ticker, direction, net_edge, ts_epoch
                        FROM opportunity_snapshots WHERE ts_epoch >= ?
                        ORDER BY net_edge DESC LIMIT 5""", (since,))
    pairs_total = one(c, "SELECT COUNT(*) FROM pairs") or 0
    pairs_verified = one(c, "SELECT COUNT(*) FROM pairs WHERE status='verified'") or 0
    pairs_rejected = one(c, "SELECT COUNT(*) FROM pairs WHERE status='rejected'") or 0

    # ---- data coverage ----
    books_win = q(c, "SELECT venue, COUNT(*) FROM book_snapshots WHERE ts_epoch >= ? GROUP BY venue", (since,))
    books_total = q(c, "SELECT venue, COUNT(*) FROM book_snapshots GROUP BY venue")
    trades_n = one(c, "SELECT COUNT(*) FROM trades") or 0
    meta_n = one(c, "SELECT COUNT(*) FROM market_metadata") or 0
    rollup_n = one(c, "SELECT COUNT(*) FROM book_rollups_5m") or 0

    # ---- feed health (from health.json) ----
    feed = {}
    try:
        with open("data/health.json") as f:
            h = json.load(f)
        feed = {
            "uptime_h": round(h.get("uptime_s", 0) / 3600, 1),
            "rss_mb": h.get("rss_mb"),
            "kalshi_fast": h.get("kalshi", {}).get("fast"),
            "kalshi_slow": h.get("kalshi", {}).get("slow"),
            "kalshi_books": h.get("kalshi", {}).get("books"),
            "kalshi_rate": h.get("kalshi", {}).get("rate"),
            "poly_universe": h.get("poly", {}).get("universe"),
            "poly_errors": h.get("poly", {}).get("errors"),
            "kalshi_errors": h.get("kalshi", {}).get("errors"),
            "disk_gb": h.get("disk_free_gb"),
        }
    except Exception:
        pass

    def coverage_pct(n, expected):
        return f"{min(100, round(100 * n / expected))}%" if expected else "0%"

    cov = coverage_pct(sum(n for _, n in books_win), int(win / 5 * 300))

    # ---- html ----
    period_label = period.capitalize()
    subject = f"[Quant Bot] {period_label} Report — PnL {pnl_win if pnl_win is not None else 0:+.2f}$ · {n_trades_win} trades"
    kpis = f"""
    <div class="cards">
      <div class="card"><div class="label">Paper Equity</div><div class="value">${eq_now:,.2f}</div></div>
      <div class="card"><div class="label">PnL ({period_label})</div><div class="value">{fmt_usd(pnl_win)}</div></div>
      <div class="card"><div class="label">PnL (All Time)</div><div class="value">{fmt_usd(pnl_all)}</div></div>
      <div class="card"><div class="label">Paper Trades (window)</div><div class="value">{n_trades_win}</div></div>
      <div class="card"><div class="label">Buys / Sells</div><div class="value">{buys} / {sells}</div></div>
      <div class="card"><div class="label">Open Positions</div><div class="value">{open_pos}</div></div>
      <div class="card"><div class="label">Opportunities (window)</div><div class="value">{opps_win:,}</div></div>
      <div class="card"><div class="label">Data Coverage (window)</div><div class="value">{cov}</div></div>
      <div class="card"><div class="label">Fees Paid (paper)</div><div class="value">${fees:,.2f}</div></div>
    </div>"""

    chart = f'<h2>Equity Curve</h2>{equity_chart(c, max(since, (one(c, "SELECT MIN(ts_epoch) FROM paper_equity") or since)))}'

    trades_rows = q(c, """SELECT ts_epoch, strategy, market, detail, side, qty, price, notional, fee
                          FROM paper_trades ORDER BY id DESC LIMIT 25""")
    if trades_rows:
        tr_html = "".join(
            f"<tr><td>{datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%m/%d %H:%M')}</td>"
            f"<td>{esc(strategy)}</td><td title='{esc(detail)}'>{esc(market[:44])}</td>"
            f"<td>{esc(side)}</td><td>{qty:,.2f}</td><td>${price:,.4f}</td>"
            f"<td>${notional:,.2f}</td><td>${fee:,.2f}</td></tr>"
            for ts, strategy, market, detail, side, qty, price, notional, fee in trades_rows)
        trades_section = (f'<h2>Paper Trades (latest 25)</h2><table class="data">'
                          f'<tr><th>Time</th><th>Strategy</th><th>Market</th><th>Side</th>'
                          f'<th>Qty</th><th>Price</th><th>Notional</th><th>Fee</th></tr>{tr_html}</table>')
    else:
        trades_section = ('<h2>Paper Trades</h2><p class="muted">No paper trades yet — '
                          'bundle strategy fires when net edge &gt; 0 with fresh books; '
                          'cross-venue waits for verified pairs (run verify_pairs.py).</p>')

    if by_strat:
        strat_rows = "".join(
            f"<tr><td>{esc(s)}</td><td>{n:,}</td><td>{avg:+.4f}</td><td>{mx:+.4f}</td></tr>"
            for s, n, avg, mx in by_strat)
        opp_section = (f'<h2>Opportunity Stats (window)</h2><table class="data">'
                       f'<tr><th>Strategy</th><th>Snapshots</th><th>Avg Net Edge</th><th>Max Net Edge</th></tr>'
                       f'{strat_rows}</table>'
                       f'<p class="muted">Pairs: {pairs_total} candidates · {pairs_verified} verified · '
                       f'{pairs_rejected} rejected (unverified never trade)</p>')
    else:
        opp_section = '<h2>Opportunity Stats</h2><p class="muted">No opportunities logged in this window.</p>'

    if top_edges:
        edges_rows = "".join(
            f"<tr><td>{esc(s)}</td><td>{esc((pm or '')[:30])}</td><td>{esc((kt or '')[:30])}</td>"
            f"<td>{esc(d)}</td><td>{ne:+.4f}</td></tr>"
            for s, pm, kt, d, ne, _ts in top_edges)
        opp_section += (f'<h3 style="font-size:13px;margin:14px 0 6px;">Top 5 edges (window)</h3>'
                        f'<table class="data"><tr><th>Strategy</th><th>Poly</th><th>Kalshi</th>'
                        f'<th>Direction</th><th>Net Edge</th></tr>{edges_rows}</table>')

    books_str = " · ".join(f"{v}: {n:,}" for v, n in books_win) or "0"
    books_tot_str = " · ".join(f"{v}: {n:,}" for v, n in books_total) or "0"
    data_section = f"""
    <h2>Data Collection Health</h2>
    <table class="data">
      <tr><th>Metric</th><th>Value</th></tr>
      <tr><td>Book snapshots (window)</td><td>{books_str}</td></tr>
      <tr><td>Book snapshots (total)</td><td>{books_tot_str}</td></tr>
      <tr><td>Trade prints collected</td><td>{trades_n:,}</td></tr>
      <tr><td>Markets with metadata</td><td>{meta_n:,}</td></tr>
      <tr><td>5-min rollups (kept forever)</td><td>{rollup_n:,}</td></tr>
      <tr><td>Collector uptime / RSS</td><td>{feed.get('uptime_h','—')} h · {feed.get('rss_mb','—')} MB</td></tr>
      <tr><td>Kalshi poll set (fast/slow)</td><td>{feed.get('kalshi_fast','—')} / {feed.get('kalshi_slow','—')} · books in memory: {feed.get('kalshi_books','—')}</td></tr>
      <tr><td>Feed errors (poly / kalshi)</td><td>{feed.get('poly_errors','—')} / {feed.get('kalshi_errors','—')}</td></tr>
      <tr><td>Disk free</td><td>{feed.get('disk_gb','—')} GB</td></tr>
    </table>"""

    html = f"""<!DOCTYPE html><html><body style="margin:0;background:#f1f5f9;">
    <div class="wrap">
      <div class="header">
        <h1>Polymarket × Kalshi — {period_label} Report</h1>
        <div class="sub">PAPER TRADING · generated {gen_at}</div>
      </div>
      <div class="section">{kpis}</div>
      <div class="section">{chart}</div>
      <div class="section">{trades_section}</div>
      <div class="section">{opp_section}</div>
      <div class="section">{data_section}</div>
      <div class="footer">Generated by the collector on the VPS · data: data/market.db ·
        raw snapshots retained 14 days · 5-min rollups forever · Not financial advice — paper results only.</div>
    </div></body></html>"""
    c.close()
    return subject, html


def send_to_n8n(period: str, subject: str, html: str) -> bool:
    url = os.environ.get("REPORT_WEBHOOK_URL", "")
    if not url and os.path.exists(WEBHOOK_ENV):
        for line in open(WEBHOOK_ENV):
            if line.startswith("REPORT_WEBHOOK_URL="):
                url = line.strip().split("=", 1)[1].strip().strip('"')
    if not url:
        print("No REPORT_WEBHOOK_URL — saved locally only")
        return False
    try:
        r = httpx.post(url, json={"period": period, "subject": subject, "html": html}, timeout=30)
        print(f"webhook: {r.status_code} {r.text[:100]}")
        return r.status_code == 200
    except Exception as e:
        print(f"webhook failed: {e}")
        return False


def main() -> None:
    period = sys.argv[1] if len(sys.argv) > 1 else "daily"
    no_send = "--no-send" in sys.argv
    if period not in ("daily", "weekly", "monthly"):
        print("period must be daily|weekly|monthly")
        sys.exit(1)
    subject, html = build(period)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    stamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    path = f"{REPORTS_DIR}/{stamp}-{period}.html"
    with open(path, "w") as f:
        f.write(html)
    print(f"saved {path} ({len(html)} bytes)")
    if not no_send:
        send_to_n8n(period, subject, html)


if __name__ == "__main__":
    main()
