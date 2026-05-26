# USDT-M Perpetual K-Line Momentum Screener

> A lightweight, production-grade momentum screener for Binance USDⓈ-M perpetual futures.
> Pulls only closed 5-minute candles via REST, aggregates higher timeframes (15m / 30m / 1h / 4h) locally,
> persists everything to SQLite, and streams live prices to an HTML dashboard via Binance WebSocket.

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Code Size](https://img.shields.io/badge/code-2.5k%20LOC-orange.svg)]()
[![Status](https://img.shields.io/badge/status-active-success.svg)]()

中文版 README: [README.zh-CN.md](README.zh-CN.md)

---

## ✨ Highlights

- **One source, many timeframes.** Only one REST endpoint (`/fapi/v1/klines` at 5m) is polled per cycle.
  15m / 30m / 1h / 4h candles are reconstructed locally by aggregation, drastically cutting API weight
  and avoiding rate-limit risk.
- **Closed candles only.** Every ranking decision is made on fully-closed bars — no flicker, no
  forward-looking bias.
- **SQLite-backed.** WAL-mode SQLite store with composite primary keys, auto-cleanup of old rows,
  and re-entrant upserts. Crash-safe and trivially exportable.
- **Multi-timeframe composite ranking.** A weighted score (5m / 15m / 30m / 1h / 4h) surfaces symbols
  that are moving across timeframes, not just on a single bar spike.
- **Live HTML dashboard.** A self-contained HTML page is regenerated on every cycle. The page itself
  opens a Binance WebSocket connection to refresh live price / drift-from-record-price in-browser,
  so the Python loop never has to poll for prices.
- **Resilience.** Retries with backoff on 418 / 429, coverage-ratio gating (skip a noisy round rather
  than corrupting rankings), and a rolling 72h backfill loop that heals any 5m gaps.
- **Liquidity & spread filters.** Pluggable filters for 24h quote volume, top-of-book spread, and
  optional order-book depth within ±0.2 % of mid.

## 🧱 Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                          Main Loop (12s tick)                          │
└──────────────┬─────────────────────────┬───────────────────────────────┘
               │                         │
               ▼                         ▼
   ┌──────────────────────┐   ┌──────────────────────────┐
   │  BinanceClient       │   │  Store (SQLite, WAL)     │
   │  - exchangeInfo      │   │  - kline_records         │
   │  - ticker/24hr       │◀──┤  - ranking_runs / items  │
   │  - klines (5m only)  │   │  - upsert + cleanup      │
   │  - depth (optional)  │   └──────────────────────────┘
   └──────────┬───────────┘                ▲
              │                            │
              ▼                            │
   ┌──────────────────────┐                │
   │  Aggregator          │                │
   │  5m → 15m/30m/1h/4h  │────────────────┘
   └──────────┬───────────┘
              │
              ▼
   ┌──────────────────────┐      ┌──────────────────────────┐
   │  Ranker              │─────▶│  Console (rich tables)   │
   │  - per-interval      │      └──────────────────────────┘
   │  - composite (Σ w·r) │      ┌──────────────────────────┐
   │  - focus table       │─────▶│  HTML Dashboard (+ WS)   │
   └──────────────────────┘      └──────────────────────────┘
```

**Data flow contract:** the Python process is the *system of record* for closed bars
and writes a flat HTML dashboard. The browser is the *real-time view layer* and is
responsible for live prices via WebSocket — Python never polls prices.

## 🚀 Quick Start

```bash
# 1. Clone
git clone https://github.com/<your-username>/usdt-perp-kline-screener.git
cd usdt-perp-kline-screener

# 2. Install
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Run (foreground, default settings)
python usdt_perp_kline_screener.py

# 4. Run (compact console + HTML dashboard refreshed every 60s)
python usdt_perp_kline_screener.py \
    --compact \
    --html screener_dashboard.html \
    --html-refresh 60

# 5. Run in background (Linux/macOS)
nohup python usdt_perp_kline_screener.py \
    --compact --html screener_dashboard.html \
    > screener.log 2>&1 &
```

Open `screener_dashboard.html` in your browser — it will auto-refresh and stream live prices.

## ⚙️ Key Configuration

All knobs are CLI flags or live on the `Config` dataclass. Defaults are tuned for "scan the whole
USDT-M perpetual market without getting rate-limited":

| Setting                       | Default                | What it does                                       |
| ----------------------------- | ---------------------- | -------------------------------------------------- |
| `min_24h_quote_volume`        | 30,000,000 USDT        | Drops illiquid symbols before the kline fetch.     |
| `loop_seconds`                | 12                     | Main loop tick.                                    |
| `post_close_delay_ms`         | 2500                   | Wait after a 5m close before querying (settle).    |
| `max_workers`                 | 4                      | Thread pool for parallel kline fetches.            |
| `min_coverage` / `hard_min_coverage` | 0.85 / 0.70     | Skip a round if coverage is too low.               |
| `keep_days`                   | 14                     | Auto-prune older rows.                             |
| `long_backfill_window_hours`  | 72                     | Hourly gap-fill window for 5m candles.             |

## 📁 Project Layout

```
usdt-perp-kline-screener/
├── usdt_perp_kline_screener.py   # single-file implementation (~2.5k LOC)
├── requirements.txt              # runtime deps (requests, rich)
├── README.md                     # this file (EN)
├── README.zh-CN.md               # Chinese version
├── LICENSE                       # MIT
└── .gitignore
```

Generated at runtime (ignored by git):

```
screener_records_v9.sqlite3   # local persistence
screener_dashboard.html       # browser-facing dashboard
screener.log                  # nohup log
```

## 🛣️ Roadmap

- [ ] Unit tests for the 5m → higher-TF aggregator
- [ ] Dockerfile + docker-compose for one-command deploy
- [ ] Optional Telegram / Discord webhook on focus-table changes
- [ ] Pluggable exchange backends (Bybit, OKX)

## ⚠️ Disclaimer

This project is for **research and educational purposes only**. It is *not* financial advice and
does *not* place orders. Cryptocurrency markets are highly volatile; you are solely responsible
for any decisions made using this software.

## 📄 License

MIT — see [LICENSE](LICENSE).
