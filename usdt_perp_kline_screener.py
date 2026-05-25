#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
USDT-M 永续已收盘K线动量筛选器（轻量稳定版：REST只拉5m）

功能：
1. 只扫描 Binance USDⓈ-M Futures / USDT 永续 / TRADING 合约。
2. REST 只拉 5m 已收盘 K 线；15m、30m、1h、4h 由本地 5m K线聚合。
3. 后台循环运行，自动记录到 SQLite。
4. 每次有新K线收盘时，打印：
   - 当前周期涨幅 Top 15
   - 当前周期跌幅 Top 15
   - 多周期综合涨幅 Top 15
   - 多周期综合跌幅 Top 15
   - 重点关注表：记录周期、记录价、各周期涨跌、阳线数量与窗口涨跌
5. 默认只用 24h 成交额做全市场流动性过滤；价差过滤可选开启。
6. 盘口深度过滤默认关闭；需要时可用 --use-depth 开启。
7. 只统计已收盘 K 线；阳线定义为 close > open。
8. HTML 端通过 Binance Futures WebSocket 实时更新“现价/现价相对记录价”，不再为现价额外轮询 REST。

安装：
    pip install -r requirements_screener_v9_cn_clean.txt

运行：
    python usdt_perp_closed_kline_screener_v9_cn_clean.py

紧凑盯盘：
    python usdt_perp_closed_kline_screener_v9_stableplus.py --compact --html screener_dashboard.html --html-refresh 60

后台运行示例：
    nohup python usdt_perp_closed_kline_screener_v9_cn_clean.py --compact --html screener_dashboard.html > screener.log 2>&1 &
"""

from __future__ import annotations

import argparse
import html as html_lib
import concurrent.futures as futures
import datetime as dt
import math
import os
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich import box
    RICH_AVAILABLE = True
except Exception:
    Console = None  # type: ignore
    Table = None  # type: ignore
    Panel = None  # type: ignore
    Text = None  # type: ignore
    box = None  # type: ignore
    RICH_AVAILABLE = False


BASE_URL = "https://fapi.binance.com"
REQUEST_PROXIES: Dict[str, str] = {}

INTERVAL_MS = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
}

INTERVALS = ["5m", "15m", "30m", "1h", "4h"]

COMBINED_WEIGHTS = {
    "5m": 0.25,
    "15m": 0.25,
    "30m": 0.20,
    "1h": 0.20,
    "4h": 0.10,
}

_thread_local = threading.local()
_shutdown = False
USE_RICH_OUTPUT = False
NO_COLOR_OUTPUT = False
CONSOLE = Console(highlight=False) if RICH_AVAILABLE else None

BEIJING_TZ = dt.timezone(dt.timedelta(hours=8), name="Asia/Shanghai")
PRICE_HEADER_TIME_MS: Optional[int] = None
RECORD_PRICE_HEADER_TIME_MS: Optional[int] = None


def get_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": "usdt-perp-closed-kline-screener/1.0"})
        if REQUEST_PROXIES:
            session.proxies.update(REQUEST_PROXIES)
        _thread_local.session = session
    return session


def handle_shutdown(signum: int, frame: Any) -> None:
    global _shutdown
    _shutdown = True


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


@dataclass
class Config:
    db_path: str = "screener_records_v9.sqlite3"
    top_n: int = 15
    hit_top_n: int = 15
    min_24h_quote_volume: float = 30_000_000.0
    max_spread_pct: float = 0.08
    use_spread_filter: bool = False
    use_depth_filter: bool = False
    depth_range_pct: float = 0.002
    depth_limit: int = 100
    min_depth_usdt: float = 25_000.0
    depth_candidate_pool: int = 80
    history_windows_hours: Tuple[int, ...] = (1, 4, 24, 72)
    max_workers: int = 4
    loop_seconds: int = 12
    post_close_delay_ms: int = 2500
    liquidity_refresh_seconds: int = 120
    symbol_refresh_seconds: int = 3600
    request_timeout: float = 20.0
    retries: int = 3
    proxy_url: str = ""
    base_url: str = BASE_URL
    rich_output: bool = True
    no_color: bool = False
    compact_output: bool = False
    html_path: str = "screener_dashboard.html"
    html_refresh_seconds: int = 60
    ws_price_enabled: bool = True
    min_coverage: float = 0.85
    hard_min_coverage: float = 0.70
    preload_on_start: bool = True
    keep_days: int = 14
    cleanup_interval_seconds: int = 24 * 60 * 60
    long_backfill_window_hours: int = 72
    long_backfill_interval_seconds: int = 3600  # 每小时扫描一次长窗口5m缺口；0 表示关闭


class BinanceClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.base_url = (cfg.base_url or BASE_URL).rstrip("/")

    def get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = self.base_url + path
        last_err: Optional[Exception] = None

        for attempt in range(self.cfg.retries):
            try:
                r = get_session().get(url, params=params, timeout=self.cfg.request_timeout)
                if r.status_code in (418, 429):
                    time.sleep(1.5 * (attempt + 1))
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as exc:
                last_err = exc
                time.sleep(0.4 * (attempt + 1))

        raise RuntimeError(f"GET {path} failed: {last_err}")

    def server_time_ms(self) -> int:
        return int(self.get_json("/fapi/v1/time")["serverTime"])

    def exchange_symbols(self) -> List[str]:
        info = self.get_json("/fapi/v1/exchangeInfo")
        out: List[str] = []
        for s in info.get("symbols", []):
            if (
                s.get("contractType") == "PERPETUAL"
                and s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"
            ):
                out.append(str(s["symbol"]))
        return sorted(out)

    def tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        data = self.get_json("/fapi/v1/ticker/24hr")
        return {x["symbol"]: x for x in data}

    def book_tickers(self) -> Dict[str, Dict[str, Any]]:
        data = self.get_json("/fapi/v1/ticker/bookTicker")
        return {x["symbol"]: x for x in data}

    def kline_at(self, symbol: str, interval: str, open_time: int, close_time: int) -> Optional[Dict[str, Any]]:
        data = self.get_json(
            "/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": open_time,
                "endTime": close_time,
                "limit": 1,
            },
        )
        if not data:
            return None

        k = data[0]
        if int(k[0]) != open_time:
            return None
        if int(k[6]) != close_time:
            return None

        open_p = float(k[1])
        high = float(k[2])
        low = float(k[3])
        close = float(k[4])
        ret_pct = ((close / open_p) - 1.0) * 100.0 if open_p > 0 else math.nan
        close_pos = ((close - low) / (high - low)) if high > low else 0.5

        return {
            "symbol": symbol,
            "interval": interval,
            "open_time": int(k[0]),
            "close_time": int(k[6]),
            "open": open_p,
            "high": high,
            "low": low,
            "close": close,
            "volume": float(k[5]),
            "quote_volume": float(k[7]),
            "trades": int(k[8]),
            "taker_buy_base_volume": float(k[9]),
            "taker_buy_quote_volume": float(k[10]),
            "ret_pct": ret_pct,
            "close_pos": close_pos,
        }

    def depth_stats(self, symbol: str, mid_price: float) -> Dict[str, float]:
        data = self.get_json(
            "/fapi/v1/depth",
            params={"symbol": symbol, "limit": self.cfg.depth_limit},
        )
        lower = mid_price * (1.0 - self.cfg.depth_range_pct)
        upper = mid_price * (1.0 + self.cfg.depth_range_pct)

        bid_depth = 0.0
        ask_depth = 0.0

        for price, qty in data.get("bids", []):
            p = float(price)
            q = float(qty)
            if p >= lower:
                bid_depth += p * q

        for price, qty in data.get("asks", []):
            p = float(price)
            q = float(qty)
            if p <= upper:
                ask_depth += p * q

        return {
            "bid_depth": bid_depth,
            "ask_depth": ask_depth,
            "min_depth": min(bid_depth, ask_depth),
        }


class Store:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.init_schema()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS kline_records (
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                close_time INTEGER NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                quote_volume REAL,
                trades INTEGER,
                taker_buy_quote_volume REAL,
                ret_pct REAL,
                close_pos REAL,
                quote_volume_24h REAL,
                spread_pct REAL,
                bid REAL,
                ask REAL,
                observed_at_ms INTEGER NOT NULL,
                PRIMARY KEY(symbol, interval, close_time)
            );

            CREATE INDEX IF NOT EXISTS idx_kline_interval_close
                ON kline_records(interval, close_time);

            CREATE INDEX IF NOT EXISTS idx_kline_symbol_interval_close
                ON kline_records(symbol, interval, close_time);

            CREATE TABLE IF NOT EXISTS ranking_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                mode TEXT NOT NULL,
                interval TEXT,
                side TEXT NOT NULL,
                close_time INTEGER,
                created_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_ranking_runs_history
                ON ranking_runs(mode, side, created_at_ms);

            CREATE TABLE IF NOT EXISTS ranking_items (
                run_id INTEGER NOT NULL,
                rank INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                ret_pct REAL,
                hit_count INTEGER,
                score REAL,
                ret_5m REAL,
                ret_15m REAL,
                ret_30m REAL,
                ret_1h REAL,
                ret_4h REAL,
                quote_volume REAL,
                quote_volume_24h REAL,
                spread_pct REAL,
                bid_depth REAL,
                ask_depth REAL,
                min_depth REAL,
                close_pos REAL,
                PRIMARY KEY(run_id, rank)
            );

            CREATE INDEX IF NOT EXISTS idx_ranking_items_symbol
                ON ranking_items(symbol);
            """
        )
        self.conn.commit()

    def upsert_klines(self, rows: List[Dict[str, Any]], observed_at_ms: int) -> None:
        if not rows:
            return
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO kline_records (
                symbol, interval, open_time, close_time,
                open, high, low, close, volume, quote_volume, trades,
                taker_buy_quote_volume, ret_pct, close_pos,
                quote_volume_24h, spread_pct, bid, ask, observed_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["symbol"], r["interval"], r["open_time"], r["close_time"],
                    r["open"], r["high"], r["low"], r["close"], r["volume"],
                    r["quote_volume"], r["trades"], r["taker_buy_quote_volume"],
                    r["ret_pct"], r["close_pos"], r.get("quote_volume_24h"),
                    r.get("spread_pct"), r.get("bid"), r.get("ask"), observed_at_ms,
                )
                for r in rows
            ],
        )
        self.conn.commit()

    def save_ranking(
        self,
        mode: str,
        interval: Optional[str],
        side: str,
        close_time: Optional[int],
        rows: List[Dict[str, Any]],
        created_at_ms: int,
    ) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO ranking_runs(mode, interval, side, close_time, created_at_ms)
            VALUES (?, ?, ?, ?, ?)
            """,
            (mode, interval, side, close_time, created_at_ms),
        )
        run_id = int(cur.lastrowid)
        cur.executemany(
            """
            INSERT INTO ranking_items (
                run_id, rank, symbol, ret_pct, hit_count, score,
                ret_5m, ret_15m, ret_30m, ret_1h, ret_4h,
                quote_volume, quote_volume_24h, spread_pct,
                bid_depth, ask_depth, min_depth, close_pos
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    idx + 1,
                    r.get("symbol"),
                    r.get("ret_pct"),
                    r.get("hit_count"),
                    r.get("score"),
                    r.get("ret_5m"),
                    r.get("ret_15m"),
                    r.get("ret_30m"),
                    r.get("ret_1h"),
                    r.get("ret_4h"),
                    r.get("quote_volume"),
                    r.get("quote_volume_24h"),
                    r.get("spread_pct"),
                    r.get("bid_depth"),
                    r.get("ask_depth"),
                    r.get("min_depth"),
                    r.get("close_pos"),
                )
                for idx, r in enumerate(rows)
            ],
        )
        self.conn.commit()

    def candle_window_stats(
        self,
        symbols: List[str],
        now_ms: int,
        window_ms: int,
    ) -> Dict[str, Dict[str, Any]]:
        """
        统计窗口内已入库 5m K线，并在本地临时聚合 15m / 1h / 4h 阳线数量。

        阳线定义：聚合K线 close > open。
        输出里的 x/y(%) 表示：阳线数量 / 已完整聚合K线数量（阳线比例）。
        注意：数据库仍只需要稳定写入5m；15m/1h/4h 只在统计时由5m临时聚合。
        """
        if not symbols:
            return {}

        cutoff = now_ms - window_ms
        placeholders = ",".join("?" for _ in symbols)
        sql = f"""
            SELECT symbol, open_time, close_time, open, high, low, close
            FROM kline_records
            WHERE interval = '5m'
              AND close_time >= ?
              AND close_time <= ?
              AND symbol IN ({placeholders})
            ORDER BY symbol ASC, close_time ASC
        """
        params: List[Any] = [cutoff, now_ms] + symbols
        rows = self.conn.execute(sql, params).fetchall()

        expected = {
            "5m": max(1, int(window_ms // INTERVAL_MS["5m"])),
            "15m": max(1, int(window_ms // INTERVAL_MS["15m"])),
            "1h": max(1, int(window_ms // INTERVAL_MS["1h"])),
            "4h": max(1, int(window_ms // INTERVAL_MS["4h"])),
        }

        out: Dict[str, Dict[str, Any]] = {}
        grouped_5m: Dict[str, List[Tuple[int, int, float, float, float, float]]] = {}

        for symbol, open_time, close_time, open_p, high_p, low_p, close_p in rows:
            symbol = str(symbol)
            grouped_5m.setdefault(symbol, []).append((
                int(open_time),
                int(close_time),
                float(open_p),
                float(high_p),
                float(low_p),
                float(close_p),
            ))

        def empty_stats() -> Dict[str, Any]:
            return {
                "5m_up": 0, "5m_bars": 0, "5m_expected": expected["5m"],
                "15m_up": 0, "15m_bars": 0, "15m_expected": expected["15m"],
                "1h_up": 0, "1h_bars": 0, "1h_expected": expected["1h"],
                "4h_up": 0, "4h_bars": 0, "4h_expected": expected["4h"],
                "window_ret_pct": None,
            }

        def aggregate_up(xs: List[Tuple[int, int, float, float, float, float]], interval: str) -> Tuple[int, int]:
            """把5m记录按官方K线边界聚合，返回 完整聚合K线中的阳线数/完整聚合K线数。"""
            if not xs:
                return 0, 0
            target_ms = INTERVAL_MS[interval]
            need = max(1, int(target_ms // INTERVAL_MS["5m"]))
            buckets: Dict[int, List[Tuple[int, int, float, float, float, float]]] = {}
            for item in xs:
                ot = item[0]
                bucket_open = (ot // target_ms) * target_ms
                buckets.setdefault(bucket_open, []).append(item)

            up = 0
            bars = 0
            for bucket_open, parts in buckets.items():
                parts.sort(key=lambda x: x[0])
                bucket_close = bucket_open + target_ms - 1
                if len(parts) != need:
                    continue
                if parts[0][0] != bucket_open or parts[-1][1] != bucket_close:
                    continue
                open_p = parts[0][2]
                close_p = parts[-1][5]
                if open_p <= 0 or close_p <= 0:
                    continue
                bars += 1
                if close_p > open_p:
                    up += 1
            return up, bars

        for symbol in symbols:
            xs = grouped_5m.get(symbol, [])
            xs.sort(key=lambda x: x[1])
            st = empty_stats()

            st["5m_bars"] = len(xs)
            st["5m_up"] = sum(1 for _, _, open_p, _, _, close_p in xs if close_p > open_p)

            for interval in ("15m", "1h", "4h"):
                up, bars = aggregate_up(xs, interval)
                st[f"{interval}_up"] = up
                st[f"{interval}_bars"] = bars

            if xs:
                first_open = xs[0][2]
                last_close = xs[-1][5]
                if first_open > 0:
                    st["window_ret_pct"] = ((last_close / first_open) - 1.0) * 100.0

            out[symbol] = st

        return out

    def cleanup_old_records(self, now_ms: int, keep_days: int = 14) -> None:
        """清理过旧记录，避免 SQLite 无限膨胀。keep_days <= 0 表示不清理。"""
        if keep_days <= 0:
            return
        cutoff = int(now_ms) - int(keep_days) * 24 * 60 * 60 * 1000
        cur = self.conn.cursor()
        cur.execute("DELETE FROM kline_records WHERE close_time < ?", (cutoff,))
        cur.execute("DELETE FROM ranking_runs WHERE created_at_ms < ?", (cutoff,))
        cur.execute("""
            DELETE FROM ranking_items
            WHERE run_id NOT IN (SELECT run_id FROM ranking_runs)
        """)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class DepthCache:
    def __init__(self, client: BinanceClient):
        self.client = client
        self.cache: Dict[str, Dict[str, float]] = {}

    def get(self, symbol: str, mid_price: float) -> Dict[str, float]:
        if symbol not in self.cache:
            self.cache[symbol] = self.client.depth_stats(symbol, mid_price)
        return self.cache[symbol]


def utc_str(ms: Optional[int]) -> str:
    if ms is None:
        return "-"
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")




def fmt_time(ms: Optional[int]) -> str:
    """按本机本地时区显示时间，便于盯盘。"""
    if ms is None:
        return "-"
    try:
        return dt.datetime.fromtimestamp(int(ms) / 1000).strftime("%m-%d %H:%M")
    except Exception:
        return "-"


def fmt_hhmmss(ms: Optional[int] = None) -> str:
    """只显示北京时间 HH:MM:SS；优先使用 Binance serverTime，避免受本机时区影响。"""
    try:
        if ms is None:
            ms = PRICE_HEADER_TIME_MS
        if ms is None:
            return dt.datetime.now(BEIJING_TZ).strftime("%H:%M:%S")
        return dt.datetime.fromtimestamp(int(ms) / 1000, tz=dt.timezone.utc).astimezone(BEIJING_TZ).strftime("%H:%M:%S")
    except Exception:
        return "--:--:--"


def fmt_hhmm_bar_boundary(ms: Optional[int]) -> str:
    """K线 close_time 显示为下一根开盘边界时间，例如 13:09:59.999 显示为 13:10。"""
    if ms is None:
        return "--:--"
    try:
        return dt.datetime.fromtimestamp((int(ms) + 1) / 1000, tz=dt.timezone.utc).astimezone(BEIJING_TZ).strftime("%H:%M")
    except Exception:
        return "--:--"


def current_price_header(ms: Optional[int] = None) -> str:
    """现价列表头：初始显示榜单生成秒级时间；HTML端按现价WS更新时间改写。"""
    return f"现价({fmt_hhmmss(ms)})"


def record_price_header(ms: Optional[int] = None) -> str:
    """记录价表头：只在表头显示记录价对应的K线时间，行内只显示价格。"""
    if ms is None:
        ms = RECORD_PRICE_HEADER_TIME_MS
    return f"记录价({fmt_hhmm_bar_boundary(ms)})"


def fmt_price(x: Optional[float]) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    try:
        v = float(x)
    except Exception:
        return "-"
    if abs(v) >= 1000:
        return f"{v:,.2f}"
    if abs(v) >= 100:
        return f"{v:,.3f}"
    if abs(v) >= 1:
        return f"{v:,.4f}"
    return f"{v:.8f}"

def fmt_pct(x: Optional[float], digits: int = 3) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    return f"{x:+.{digits}f}%"


def fmt_num(x: Optional[float], digits: int = 2) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    return f"{x:,.{digits}f}"


def fmt_usdt(x: Optional[float]) -> str:
    """USDT金额格式化为中文单位，便于盯盘阅读。"""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    abs_x = abs(x)
    if abs_x >= 100_000_000:
        return f"{x / 100_000_000:.2f}亿"
    if abs_x >= 10_000:
        return f"{x / 10_000:.2f}万"
    return f"{x:.0f}"


def table_cell(row: Dict[str, Any], row_idx: int, key: str) -> Tuple[str, Optional[str]]:
    """Return display text and optional Rich style for a table cell."""
    style: Optional[str] = None

    if key == "#":
        return str(row_idx), "dim"

    if key.startswith("time:"):
        field = key[5:]
        return fmt_time(row.get(field)), "dim"

    if key.startswith("price_time:"):
        parts = key.split(":", 2)
        if len(parts) != 3:
            return "-", None
        price_field = parts[1]
        return fmt_price(row.get(price_field)), None

    if key.startswith("price:"):
        field = key[6:]
        return fmt_price(row.get(field)), None

    if key.startswith("pct:"):
        field = key[4:]
        value = row.get(field)
        text = fmt_pct(value)
        if value is not None and not (isinstance(value, float) and math.isnan(value)):
            try:
                v = float(value)
                if field == "spread_pct":
                    if v <= 0.05:
                        style = "green"
                    elif v <= 0.08:
                        style = "yellow"
                    else:
                        style = "red"
                elif v > 0:
                    style = "green"
                elif v < 0:
                    style = "red"
            except Exception:
                pass
        return text, style

    if key.startswith("usdt:"):
        field = key[5:]
        value = row.get(field)
        style = "cyan" if field in {"quote_volume_24h", "min_depth", "bid_depth", "ask_depth"} else None
        return fmt_usdt(value), style

    if key.startswith("num:"):
        field = key[4:]
        value = row.get(field)
        return fmt_num(value), None

    if key.startswith("int:"):
        field = key[4:]
        value = row.get(field)
        if value is None:
            return "-", None
        style = "yellow" if field in {"hit_count", "hits", "interval_count"} else None
        return str(int(value)), style

    if key.startswith("bars:"):
        base = key[5:]
        up = row.get(f"{base}_up")
        bars = row.get(f"{base}_bars")
        if up is None or bars is None:
            return "-", None
        try:
            up_i = int(up)
            bars_i = int(bars)
        except Exception:
            return "-", None
        if bars_i <= 0:
            return "-", None
        ratio = up_i / bars_i
        style = "green" if ratio >= 0.65 else ("yellow" if ratio >= 0.50 else None)
        return f"{up_i}/{bars_i}({ratio * 100:.0f}%)", style

    value = row.get(key)
    if value is None:
        return "-", None
    style = "bold cyan" if key == "symbol" else None
    return str(value), style


def print_table(title: str, rows: List[Dict[str, Any]], columns: List[Tuple[str, str]]) -> None:
    if USE_RICH_OUTPUT and RICH_AVAILABLE and CONSOLE is not None and Table is not None:
        table = Table(title=title, box=box.SIMPLE_HEAVY if box is not None else None, show_lines=False, pad_edge=False)
        for header, key in columns:
            numeric = key == "#" or key.startswith(("pct:", "usdt:", "num:", "int:", "price:", "time:"))
            table.add_column(header, justify="right" if numeric else "left", no_wrap=True)

        if rows:
            for i, r in enumerate(rows, start=1):
                cells = []
                for _, key in columns:
                    text, style = table_cell(r, i, key)
                    if NO_COLOR_OUTPUT or style is None or Text is None:
                        cells.append(text)
                    else:
                        cells.append(Text(text, style=style))
                table.add_row(*cells)
        CONSOLE.print(table)
        if not rows:
            CONSOLE.print("[dim]无符合条件的数据[/dim]" if not NO_COLOR_OUTPUT else "无符合条件的数据")
        return

    print("\n" + title)
    print("=" * len(title))
    if not rows:
        print("无符合条件的数据")
        return

    rendered: List[List[str]] = []
    headers = [h for h, _ in columns]
    for i, r in enumerate(rows, start=1):
        line: List[str] = []
        for _, key in columns:
            text, _ = table_cell(r, i, key)
            line.append(text)
        rendered.append(line)

    widths = [len(h) for h in headers]
    for line in rendered:
        for i, cell in enumerate(line):
            widths[i] = max(widths[i], len(cell))

    def make_row(parts: List[str]) -> str:
        return " | ".join(part.rjust(widths[i]) for i, part in enumerate(parts))

    print(make_row(headers))
    print("-+-".join("-" * w for w in widths))
    for line in rendered:
        print(make_row(line))


def latest_closed_open_close(server_ms: int, interval: str, post_close_delay_ms: int) -> Optional[Tuple[int, int]]:
    interval_ms = INTERVAL_MS[interval]
    current_open = (server_ms // interval_ms) * interval_ms
    elapsed_since_open = server_ms - current_open
    if elapsed_since_open < post_close_delay_ms:
        return None
    close_time = current_open - 1
    open_time = close_time + 1 - interval_ms
    return open_time, close_time


def latest_closed_open_close_force(server_ms: int, interval: str) -> Tuple[int, int]:
    """无论当前距离新K线开始多久，都返回最近一根理论上已经收盘的K线。用于启动预热。"""
    interval_ms = INTERVAL_MS[interval]
    current_open = (server_ms // interval_ms) * interval_ms
    close_time = current_open - 1
    open_time = close_time + 1 - interval_ms
    return open_time, close_time


def coverage_ratio(rows: List[Dict[str, Any]], expected_symbols: List[str]) -> float:
    if not expected_symbols:
        return 0.0
    got = {str(r.get("symbol")) for r in rows if r.get("symbol")}
    return len(got) / max(1, len(expected_symbols))


def db_size_text(path: str) -> str:
    try:
        size = os.path.getsize(path)
        wal = path + "-wal"
        if os.path.exists(wal):
            size += os.path.getsize(wal)
        if size >= 1024 * 1024 * 1024:
            return f"{size / (1024 * 1024 * 1024):.2f} GB"
        if size >= 1024 * 1024:
            return f"{size / (1024 * 1024):.1f} MB"
        return f"{size / 1024:.1f} KB"
    except Exception:
        return "-"


def remember_warning(status: Dict[str, Any], message: str, max_items: int = 8) -> None:
    warnings = status.setdefault("warnings", [])
    warnings.append(message)
    if len(warnings) > max_items:
        del warnings[:-max_items]


def enrich_liquidity(
    symbols: Iterable[str],
    tickers_24h: Dict[str, Dict[str, Any]],
    book_tickers: Dict[str, Dict[str, Any]],
    cfg: Config,
) -> Dict[str, Dict[str, Any]]:
    """
    流动性过滤。

    轻量版默认只按 24h 成交额过滤，不再强制请求/依赖 bookTicker。
    只有开启价差过滤或盘口深度过滤时，才需要 bid/ask/mid/spread_pct。
    """
    out: Dict[str, Dict[str, Any]] = {}
    need_book = bool(cfg.use_spread_filter or cfg.use_depth_filter)

    for symbol in symbols:
        t = tickers_24h.get(symbol)
        if not t:
            continue

        try:
            quote_volume_24h = float(t["quoteVolume"])
            last_price = float(t["lastPrice"])
        except Exception:
            continue

        if quote_volume_24h < cfg.min_24h_quote_volume:
            continue

        bid = None
        ask = None
        mid = last_price if last_price > 0 else None
        spread_pct = None

        if need_book:
            b = book_tickers.get(symbol)
            if not b:
                continue
            try:
                bid = float(b["bidPrice"])
                ask = float(b["askPrice"])
            except Exception:
                continue
            if bid <= 0 or ask <= 0 or ask < bid:
                continue
            mid = (bid + ask) / 2.0
            spread_pct = ((ask - bid) / mid) * 100.0
            if cfg.use_spread_filter and spread_pct > cfg.max_spread_pct:
                continue

        out[symbol] = {
            "quote_volume_24h": quote_volume_24h,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread_pct": spread_pct,
            "last_price": last_price,
        }
    return out


def fetch_klines_for_interval(
    client: BinanceClient,
    symbols: List[str],
    liquidity: Dict[str, Dict[str, float]],
    interval: str,
    open_time: int,
    close_time: int,
    cfg: Config,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    def task(symbol: str) -> Optional[Dict[str, Any]]:
        k = client.kline_at(symbol, interval, open_time, close_time)
        if k is None:
            return None
        liq = liquidity.get(symbol, {})
        k.update(liq)
        last_price = k.get("last_price")
        close_price = k.get("close")
        k["current_price"] = last_price
        if last_price and close_price:
            try:
                k["current_change_pct"] = (float(last_price) / float(close_price) - 1.0) * 100.0
            except Exception:
                k["current_change_pct"] = None
        else:
            k["current_change_pct"] = None
        return k

    with futures.ThreadPoolExecutor(max_workers=cfg.max_workers) as ex:
        future_map = {ex.submit(task, s): s for s in symbols}
        for f in futures.as_completed(future_map):
            symbol = future_map[f]
            try:
                row = f.result()
                if row is not None and not math.isnan(row["ret_pct"]):
                    rows.append(row)
            except Exception as exc:
                print(f"[警告] K线请求失败 {symbol} {interval}: {exc}", file=sys.stderr)
    return rows




def _row_from_kline_array(symbol: str, interval: str, k: List[Any]) -> Optional[Dict[str, Any]]:
    """Binance /klines 数组 → 本脚本统一 row schema。"""
    try:
        open_p = float(k[1])
        high = float(k[2])
        low = float(k[3])
        close = float(k[4])
        if open_p <= 0 or close <= 0:
            return None
        ret_pct = ((close / open_p) - 1.0) * 100.0
        close_pos = ((close - low) / (high - low)) if high > low else 0.5
        return {
            "symbol": symbol,
            "interval": interval,
            "open_time": int(k[0]),
            "close_time": int(k[6]),
            "open": open_p,
            "high": high,
            "low": low,
            "close": close,
            "volume": float(k[5]),
            "quote_volume": float(k[7]),
            "trades": int(k[8]),
            "taker_buy_base_volume": float(k[9]),
            "taker_buy_quote_volume": float(k[10]),
            "ret_pct": ret_pct,
            "close_pos": close_pos,
        }
    except Exception:
        return None


def fetch_recent_5m_history(
    client: BinanceClient,
    symbols: List[str],
    liquidity: Dict[str, Dict[str, float]],
    limit: int,
    now_ms: int,
    cfg: Config,
) -> List[Dict[str, Any]]:
    """
    启动预热专用：只拉 5m 历史K线。

    这取代旧版本分别拉 5m/15m/30m/1h/4h 的预热方式。
    limit 建议覆盖 max(4h窗口, 阳线统计窗口)，最大不超过 Binance 单次 1500。
    """
    rows: List[Dict[str, Any]] = []
    if not symbols or limit <= 0:
        return rows

    closed_cutoff = now_ms - max(0, int(cfg.post_close_delay_ms))
    safe_limit = min(1500, max(1, int(limit)))

    def task(symbol: str) -> List[Dict[str, Any]]:
        data = client.get_json(
            "/fapi/v1/klines",
            params={"symbol": symbol, "interval": "5m", "limit": safe_limit},
        )
        out: List[Dict[str, Any]] = []
        liq = liquidity.get(symbol, {})
        for k in data:
            row = _row_from_kline_array(symbol, "5m", k)
            if row is None:
                continue
            # 丢掉正在形成或刚刚收盘但可能还没稳定的K线。
            if int(row["close_time"]) >= closed_cutoff:
                continue
            row.update(liq)
            last_price = row.get("last_price")
            close_price = row.get("close")
            row["current_price"] = last_price
            if last_price and close_price:
                try:
                    row["current_change_pct"] = (float(last_price) / float(close_price) - 1.0) * 100.0
                except Exception:
                    row["current_change_pct"] = None
            else:
                row["current_change_pct"] = None
            out.append(row)
        return out

    preload_workers = max(1, min(cfg.max_workers, 6))
    with futures.ThreadPoolExecutor(max_workers=preload_workers) as ex:
        future_map = {ex.submit(task, s): s for s in symbols}
        for f in futures.as_completed(future_map):
            symbol = future_map[f]
            try:
                rows.extend(f.result())
            except Exception as exc:
                print(f"[警告] 5m历史预热失败 {symbol}: {exc}", file=sys.stderr)
    return rows


def preload_5m_limit(cfg: Config) -> int:
    """计算启动时需要拉多少根 5m 历史K线。"""
    max_interval_bars = max(int(INTERVAL_MS[i] // INTERVAL_MS["5m"]) for i in INTERVALS)
    max_window_bars = 0
    if cfg.history_windows_hours:
        max_window_bars = max(int(h * 60 * 60 * 1000 // INTERVAL_MS["5m"]) for h in cfg.history_windows_hours)
    return min(1500, max(max_interval_bars, max_window_bars, 1) + 5)


def aggregate_interval_from_5m_store(
    store: Store,
    symbols: List[str],
    liquidity: Dict[str, Dict[str, float]],
    interval: str,
    open_time: int,
    close_time: int,
) -> List[Dict[str, Any]]:
    """
    从本地 SQLite 的 5m 已收盘K线聚合出官方边界上的 15m/30m/1h/4h K线。

    重要：这里不是滚动窗口，而是按 Binance 官方K线边界聚合。
    例如 15m 只会聚合 14:15、14:20、14:25 三根 5m，得到 14:15-14:29:59。
    """
    if not symbols:
        return []
    if interval == "5m":
        expected = 1
    else:
        expected = int(INTERVAL_MS[interval] // INTERVAL_MS["5m"])

    placeholders = ",".join("?" for _ in symbols)
    sql = f"""
        SELECT symbol, open_time, close_time, open, high, low, close,
               volume, quote_volume, trades, taker_buy_quote_volume
        FROM kline_records
        WHERE interval = '5m'
          AND close_time >= ?
          AND close_time <= ?
          AND symbol IN ({placeholders})
        ORDER BY symbol ASC, close_time ASC
    """
    params: List[Any] = [open_time, close_time] + symbols
    db_rows = store.conn.execute(sql, params).fetchall()

    grouped: Dict[str, List[Tuple[int, int, float, float, float, float, float, float, int, float]]] = {}
    for symbol, ot, ct, op, hi, lo, cl, vol, qv, trades, tbqv in db_rows:
        grouped.setdefault(str(symbol), []).append((
            int(ot), int(ct), float(op), float(hi), float(lo), float(cl),
            float(vol or 0.0), float(qv or 0.0), int(trades or 0), float(tbqv or 0.0),
        ))

    out: List[Dict[str, Any]] = []
    for symbol in symbols:
        xs = grouped.get(symbol, [])
        if len(xs) < expected:
            continue
        # 只取窗口内最后 expected 根，并严格校验官方边界。
        xs = xs[-expected:]
        if xs[0][0] != open_time or xs[-1][1] != close_time:
            continue
        open_p = xs[0][2]
        close_p = xs[-1][5]
        high = max(x[3] for x in xs)
        low = min(x[4] for x in xs)
        if open_p <= 0 or close_p <= 0:
            continue
        ret_pct = ((close_p / open_p) - 1.0) * 100.0
        close_pos = ((close_p - low) / (high - low)) if high > low else 0.5
        row: Dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "open_time": open_time,
            "close_time": close_time,
            "open": open_p,
            "high": high,
            "low": low,
            "close": close_p,
            "volume": sum(x[6] for x in xs),
            "quote_volume": sum(x[7] for x in xs),
            "trades": sum(x[8] for x in xs),
            "taker_buy_base_volume": None,
            "taker_buy_quote_volume": sum(x[9] for x in xs),
            "ret_pct": ret_pct,
            "close_pos": close_pos,
        }
        liq = liquidity.get(symbol, {})
        row.update(liq)
        last_price = row.get("last_price")
        row["current_price"] = last_price
        if last_price and close_p:
            try:
                row["current_change_pct"] = (float(last_price) / float(close_p) - 1.0) * 100.0
            except Exception:
                row["current_change_pct"] = None
        else:
            row["current_change_pct"] = None
        out.append(row)
    return out


def backfill_missing_5m_window(
    client: BinanceClient,
    store: Store,
    symbols: List[str],
    liquidity: Dict[str, Dict[str, float]],
    open_time: int,
    close_time: int,
    cfg: Config,
    observed_at_ms: int,
    status: Optional[Dict[str, Any]] = None,
    label: str = "4h",
) -> int:
    """
    修复高周期聚合缺口：高周期由 5m 本地聚合，只要中间少一根 5m，4h 就会显示为空。
    这里仅对指定窗口内 5m 数量不足的合约做补拉，避免全量重复请求。
    """
    if not symbols:
        return 0

    expected = int((close_time - open_time + 1) // INTERVAL_MS["5m"])
    if expected <= 0:
        return 0

    placeholders = ",".join("?" for _ in symbols)
    sql = f"""
        SELECT symbol, COUNT(*)
        FROM kline_records
        WHERE interval = '5m'
          AND close_time >= ?
          AND close_time <= ?
          AND symbol IN ({placeholders})
        GROUP BY symbol
    """
    params: List[Any] = [open_time, close_time] + symbols
    counts = {str(sym): int(cnt) for sym, cnt in store.conn.execute(sql, params).fetchall()}
    missing = [s for s in symbols if counts.get(s, 0) < expected]
    if not missing:
        return 0

    rows: List[Dict[str, Any]] = []

    def task(symbol: str) -> List[Dict[str, Any]]:
        data = client.get_json(
            "/fapi/v1/klines",
            params={
                "symbol": symbol,
                "interval": "5m",
                "startTime": open_time,
                "endTime": close_time,
                "limit": min(1500, expected + 2),
            },
        )
        out: List[Dict[str, Any]] = []
        liq = liquidity.get(symbol, {})
        for k in data:
            row = _row_from_kline_array(symbol, "5m", k)
            if row is None:
                continue
            if int(row["close_time"]) < open_time or int(row["close_time"]) > close_time:
                continue
            row.update(liq)
            last_price = row.get("last_price")
            close_price = row.get("close")
            row["current_price"] = last_price
            if last_price and close_price:
                try:
                    row["current_change_pct"] = (float(last_price) / float(close_price) - 1.0) * 100.0
                except Exception:
                    row["current_change_pct"] = None
            else:
                row["current_change_pct"] = None
            out.append(row)
        return out

    with futures.ThreadPoolExecutor(max_workers=max(1, min(cfg.max_workers, 6))) as ex:
        future_map = {ex.submit(task, s): s for s in missing}
        for f in futures.as_completed(future_map):
            symbol = future_map[f]
            try:
                rows.extend(f.result())
            except Exception as exc:
                print(f"[警告] {label} 缺口补拉失败 {symbol}: {exc}", file=sys.stderr)

    if rows:
        store.upsert_klines(rows, observed_at_ms=observed_at_ms)

    msg = f"{label} 5m缺口补拉：缺口合约 {len(missing)} 个，写入/覆盖 {len(rows)} 条5m"
    print(f"[{utc_str(observed_at_ms)}] {msg}")
    if status is not None:
        status["last_5m_backfill"] = msg
    return len(rows)



def build_latest_from_5m_store(
    store: Store,
    symbols: List[str],
    liquidity: Dict[str, Dict[str, float]],
    cfg: Config,
    server_ms: int,
    status: Optional[Dict[str, Any]] = None,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """基于本地 5m K线，重建 5m/15m/30m/1h/4h 的最新官方已收盘K线。"""
    latest: Dict[str, Dict[str, Dict[str, Any]]] = {i: {} for i in INTERVALS}
    for interval in INTERVALS:
        oc = latest_closed_open_close(server_ms, interval, cfg.post_close_delay_ms)
        if oc is None:
            oc = latest_closed_open_close_force(server_ms, interval)
        open_time, close_time = oc
        rows = aggregate_interval_from_5m_store(store, symbols, liquidity, interval, open_time, close_time)
        latest[interval] = {r["symbol"]: r for r in rows}
        if status is not None:
            cov = coverage_ratio(rows, symbols)
            status.setdefault("coverage", {})[interval] = {
                "rows": len(rows),
                "expected": len(symbols),
                "ratio": cov,
                "close_time": close_time,
                "preload": False,
                "source": "5m本地聚合" if interval != "5m" else "5m REST入库",
            }
    return latest


def fetch_candle_window_stats_db_only(
    store: Store,
    symbols: List[str],
    windows: Tuple[int, ...],
    now_ms: int,
    status: Optional[Dict[str, Any]] = None,
) -> Dict[int, Dict[str, Dict[str, Any]]]:
    """阳线统计只使用本地 SQLite，不再为阳线统计额外请求 REST。"""
    out: Dict[int, Dict[str, Dict[str, Any]]] = {}
    for h in windows:
        out[h] = store.candle_window_stats(symbols, now_ms, int(h) * 60 * 60 * 1000)
    if status is not None:
        status["candle_stats_source"] = "SQLite only"
        status["candle_stats_rest_symbols"] = 0
    return out

def html_status_panel(status: Optional[Dict[str, Any]]) -> str:
    if not status:
        return ""

    pieces: List[str] = []
    coverage = status.get("coverage", {}) or {}
    if coverage:
        cov_parts = []
        for interval in INTERVALS:
            st = coverage.get(interval)
            if not st:
                continue
            ratio = float(st.get("ratio") or 0.0)
            rows = int(st.get("rows") or 0)
            expected = int(st.get("expected") or 0)
            cls = "good" if ratio >= 0.95 else ("warn" if ratio >= 0.85 else "bad")
            cov_parts.append(f"<span class='{cls}'>{html_lib.escape(interval)}覆盖率 {rows}/{expected}={ratio:.1%}</span>")
        if cov_parts:
            pieces.append(" ｜ ".join(cov_parts))

    source = status.get("candle_stats_source")
    if source:
        extra = status.get("candle_stats_rest_symbols")
        if extra is not None:
            pieces.append(f"阳线统计：{html_lib.escape(str(source))}，REST补齐 {int(extra)} 个合约")
        else:
            pieces.append(f"阳线统计：{html_lib.escape(str(source))}")

    db_size = status.get("db_size")
    if db_size:
        pieces.append(f"数据库：{html_lib.escape(str(db_size))}")

    price_ok = status.get("last_price_update")
    if price_ok:
        price_symbols = status.get("price_symbols")
        if price_symbols is not None:
            pieces.append(f"现价更新：{html_lib.escape(str(price_ok))}，监控 {int(price_symbols)} 个合约")
        else:
            pieces.append(f"现价更新：{html_lib.escape(str(price_ok))}")

    html_ok = status.get("last_html_write")
    if html_ok:
        pieces.append(f"HTML写入：{html_lib.escape(str(html_ok))}")

    warnings = status.get("warnings") or []
    if warnings:
        warning_text = "；".join(str(x) for x in warnings[-3:])
        pieces.append(f"<span class='warn'>最近警告：{html_lib.escape(warning_text)}</span>")

    if not pieces:
        return ""
    return "<div class='hint status-panel'>" + "<br>".join(pieces) + "</div>"


def add_depth_to_candidates(
    rows: List[Dict[str, Any]],
    depth_cache: DepthCache,
    cfg: Config,
) -> List[Dict[str, Any]]:
    if not cfg.use_depth_filter:
        return rows

    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            d = depth_cache.get(r["symbol"], float(r["mid"]))
            r = dict(r)
            r.update(d)
            if d["min_depth"] >= cfg.min_depth_usdt:
                out.append(r)
        except Exception as exc:
            print(f"[警告] 深度请求失败 {r.get('symbol')}: {exc}", file=sys.stderr)
    return out


def rank_interval_rows(
    rows: List[Dict[str, Any]],
    side: str,
    depth_cache: DepthCache,
    cfg: Config,
) -> List[Dict[str, Any]]:
    # 上涨榜只允许涨幅 > 0；下跌榜只允许涨幅 < 0。
    # 否则在全市场很弱/很强时，负数也可能混进“上涨榜”，正数也可能混进“下跌榜”。
    if side == "UP":
        side_rows = [r for r in rows if r.get("ret_pct") is not None and r["ret_pct"] > 0]
        reverse = True
    else:
        side_rows = [r for r in rows if r.get("ret_pct") is not None and r["ret_pct"] < 0]
        reverse = False

    prelim = sorted(side_rows, key=lambda r: r["ret_pct"], reverse=reverse)[: cfg.depth_candidate_pool]
    filtered = add_depth_to_candidates(prelim, depth_cache, cfg)
    ranked = sorted(filtered, key=lambda r: r["ret_pct"], reverse=reverse)[: cfg.top_n]
    return ranked


def compute_combined(
    latest: Dict[str, Dict[str, Dict[str, Any]]],
    liquidity: Dict[str, Dict[str, float]],
    side: str,
    depth_cache: DepthCache,
    cfg: Config,
) -> List[Dict[str, Any]]:
    """
    多周期综合榜。

    修正点：
    1. 不再只按“名次”给分，加入实际涨跌幅，避免市场很平时 +0.01% 也被排成强动能。
    2. UP/DOWN 使用方向化收益：UP 用 ret，DOWN 用 -ret。
    3. 反方向周期会轻微扣分，避免 5m 很强但 1h/4h 明显反向的标的误入综合强势榜。
    4. 记录价统一使用最近5m已收盘价，保证表头“记录价(HH:MM)”对所有行一致；主导周期只保存在 dominant_interval。
    """
    rank_maps: Dict[str, Dict[str, int]] = {}
    ret_maps: Dict[str, Dict[str, float]] = {}
    qv_maps: Dict[str, Dict[str, float]] = {}
    close_maps: Dict[str, Dict[str, float]] = {}
    close_time_maps: Dict[str, Dict[str, int]] = {}

    symbols_all = set(liquidity.keys())
    direction = 1.0 if side == "UP" else -1.0

    for interval in INTERVALS:
        all_interval_rows = [
            latest[interval][s]
            for s in latest.get(interval, {})
            if s in symbols_all and "ret_pct" in latest[interval][s]
        ]

        ret_maps[interval] = {r["symbol"]: r["ret_pct"] for r in all_interval_rows}
        qv_maps[interval] = {r["symbol"]: r.get("quote_volume") for r in all_interval_rows}
        close_maps[interval] = {r["symbol"]: r.get("close") for r in all_interval_rows}
        close_time_maps[interval] = {r["symbol"]: r.get("close_time") for r in all_interval_rows}

        if side == "UP":
            interval_rows = [r for r in all_interval_rows if r.get("ret_pct") is not None and r["ret_pct"] > 0]
            reverse = True
        else:
            interval_rows = [r for r in all_interval_rows if r.get("ret_pct") is not None and r["ret_pct"] < 0]
            reverse = False

        interval_rows = sorted(interval_rows, key=lambda r: r["ret_pct"], reverse=reverse)
        rank_maps[interval] = {r["symbol"]: idx + 1 for idx, r in enumerate(interval_rows)}

    candidates: List[Dict[str, Any]] = []
    for symbol in symbols_all:
        hit_count = 0
        rank_score = 0.0
        momentum_score = 0.0
        best_interval: Optional[str] = None
        best_contribution = -1.0
        has_top_hit = False

        row: Dict[str, Any] = {"symbol": symbol}
        row.update(liquidity[symbol])

        for interval in INTERVALS:
            rank = rank_maps.get(interval, {}).get(symbol)
            ret = ret_maps.get(interval, {}).get(symbol)
            row[f"ret_{interval}"] = ret
            row[f"rank_{interval}"] = rank
            row[f"qv_{interval}"] = qv_maps.get(interval, {}).get(symbol)
            row[f"close_{interval}"] = close_maps.get(interval, {}).get(symbol)
            row[f"close_time_{interval}"] = close_time_maps.get(interval, {}).get(symbol)

            if ret is None:
                continue

            try:
                signed_ret = float(ret) * direction
            except Exception:
                continue

            weight = COMBINED_WEIGHTS[interval]

            # 实际动能分：同方向加分，反方向扣一半分。
            if signed_ret > 0:
                momentum_score += signed_ret * weight
            elif signed_ret < 0:
                momentum_score += signed_ret * weight * 0.50

            if rank is not None and rank <= cfg.hit_top_n:
                has_top_hit = True
                hit_count += 1

                # 名次分 × 实际幅度。这样既保留“周期内排名”，又避免微小涨跌幅被过度放大。
                contribution = (cfg.hit_top_n + 1 - rank) * weight * max(signed_ret, 0.0)
                rank_score += contribution

                if contribution > best_contribution:
                    best_contribution = contribution
                    best_interval = interval

        score = rank_score + momentum_score

        # 必须至少在某个周期进入同方向前N，并且综合方向化得分为正。
        if not has_top_hit or score <= 0:
            continue

        # 主导周期只用于内部解释；记录价统一使用最近一根5m已收盘价。
        # 这样表头“记录价(HH:MM)”可以代表所有行，避免有的行来自1h/4h但表头显示5m时间。
        if best_interval is None:
            for interval in INTERVALS:
                if row.get(f"ret_{interval}") is not None:
                    best_interval = interval
                    break

        record_interval = "5m"
        record_price = row.get("close_5m")
        record_time = row.get("close_time_5m")
        if record_price is None or record_time is None:
            continue

        current_price = row.get("last_price")
        row["dominant_interval"] = best_interval
        row["record_interval"] = record_interval
        row["record_price"] = record_price
        row["record_time"] = record_time
        row["current_price"] = current_price
        if record_price and current_price:
            try:
                row["current_change_pct"] = (float(current_price) / float(record_price) - 1.0) * 100.0
            except Exception:
                row["current_change_pct"] = None
        else:
            row["current_change_pct"] = None

        row["hit_count"] = hit_count
        row["score"] = score
        row["rank_score"] = rank_score
        row["momentum_score"] = momentum_score
        candidates.append(row)

    candidates.sort(
        key=lambda r: (
            r.get("score", 0.0),
            r.get("hit_count", 0),
            r.get("quote_volume_24h", 0.0),
            -(r.get("spread_pct") if r.get("spread_pct") is not None else 0.0),
        ),
        reverse=True,
    )
    prelim = candidates[: cfg.depth_candidate_pool]
    filtered = add_depth_to_candidates(prelim, depth_cache, cfg)
    filtered.sort(
        key=lambda r: (
            r.get("score", 0.0),
            r.get("hit_count", 0),
            r.get("quote_volume_24h", 0.0),
            -(r.get("spread_pct") if r.get("spread_pct") is not None else 0.0),
        ),
        reverse=True,
    )
    return filtered[: cfg.top_n]

def interval_columns() -> List[Tuple[str, str]]:
    """单周期榜单列名。内部字段仍保持英文，表头显示中文。"""
    return [
        ("#", "#"),
        ("合约", "symbol"),
        (current_price_header(), "price:current_price"),
        ("现价/收盘", "pct:current_change_pct"),
        ("收盘时间", "time:close_time"),
        ("收盘价", "price:close"),
        ("涨跌幅", "pct:ret_pct"),
        ("本K成交额", "usdt:quote_volume"),
        ("24h成交额", "usdt:quote_volume_24h"),
        ("价差", "pct:spread_pct"),
        ("最小深度", "usdt:min_depth"),
        ("收盘位置", "num:close_pos"),
    ]


def combined_columns() -> List[Tuple[str, str]]:
    """多周期综合榜单列名：现价前置，价格只作展示，不参与排名。"""
    return [
        ("#", "#"),
        ("合约", "symbol"),
        (current_price_header(), "price:current_price"),
        ("现价/记录", "pct:current_change_pct"),
        (record_price_header(), "price_time:record_price:record_time"),
        ("5m", "pct:ret_5m"),
        ("15m", "pct:ret_15m"),
        ("30m", "pct:ret_30m"),
        ("1h", "pct:ret_1h"),
        ("4h", "pct:ret_4h"),
        ("5m成交额", "usdt:qv_5m"),
        ("15m成交额", "usdt:qv_15m"),
        ("1h成交额", "usdt:qv_1h"),
        ("24h成交额", "usdt:quote_volume_24h"),
        ("价差", "pct:spread_pct"),
        ("深度", "usdt:min_depth"),
    ]



def focus_columns(windows: Tuple[int, ...]) -> List[Tuple[str, str]]:
    """重点关注榜列名：按窗口使用更合适的阳线周期。"""
    cols: List[Tuple[str, str]] = [
        ("#", "#"),
        ("合约", "symbol"),
        (current_price_header(), "price:current_price"),
        ("现价/记录", "pct:current_change_pct"),
        (record_price_header(), "price_time:record_price:record_time"),
        ("5m", "pct:ret_5m"),
        ("15m", "pct:ret_15m"),
        ("30m", "pct:ret_30m"),
        ("1h", "pct:ret_1h"),
        ("4h", "pct:ret_4h"),
    ]

    # 阳线显示格式：阳线数量/完整聚合K线数量(阳线比例)。
    # 1h 默认保留 5m 阳线；4h 显示 5m + 15m；24h 显示 15m + 1h；72h 显示 1h + 4h。
    for h in windows:
        h_int = int(h)
        if h_int == 4:
            cols.append(("4h 5m阳线", "bars:candle_4h_5m"))
            cols.append(("4h 15m阳线", "bars:candle_4h_15m"))
        elif h_int == 24:
            cols.append(("24h 15m阳线", "bars:candle_24h_15m"))
            cols.append(("24h 1h阳线", "bars:candle_24h_1h"))
        elif h_int == 72:
            cols.append(("72h 1h阳线", "bars:candle_72h_1h"))
            cols.append(("72h 4h阳线", "bars:candle_72h_4h"))
        else:
            cols.append((f"{h_int}h 5m阳线", f"bars:candle_{h_int}h_5m"))
        cols.append((f"{h_int}h涨跌幅", f"pct:candle_{h_int}h_ret"))

    cols.extend([
        ("5m成交额", "usdt:qv_5m"),
        ("1h成交额", "usdt:qv_1h"),
        ("24h成交额", "usdt:quote_volume_24h"),
        ("价差", "pct:spread_pct"),
        ("深度", "usdt:min_depth"),
    ])
    return cols


def make_focus_rows(
    combined_rows: List[Dict[str, Any]],
    candle_by_hour: Dict[int, Dict[str, Dict[str, Any]]],
    windows: Tuple[int, ...],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in combined_rows:
        row = dict(r)
        symbol = row.get("symbol")
        for h in windows:
            h_int = int(h)
            st = candle_by_hour.get(h_int, {}).get(symbol, {})
            for interval in ("5m", "15m", "1h", "4h"):
                row[f"candle_{h_int}h_{interval}_up"] = st.get(f"{interval}_up", 0)
                row[f"candle_{h_int}h_{interval}_bars"] = st.get(f"{interval}_bars", 0)
                row[f"candle_{h_int}h_{interval}_expected"] = st.get(f"{interval}_expected")
            row[f"candle_{h_int}h_ret"] = st.get("window_ret_pct")
        out.append(row)
    return out


def cell_css_class(row: Dict[str, Any], key: str) -> str:
    if key.startswith("pct:"):
        field = key[4:]
        value = row.get(field)
        try:
            v = float(value)
        except Exception:
            return ""
        if field == "spread_pct":
            if v <= 0.05:
                return "good"
            if v <= 0.08:
                return "warn"
            return "bad"
        if v > 0:
            return "pos"
        if v < 0:
            return "neg"
    if key == "symbol":
        return "symbol"
    if key.startswith("int:"):
        field = key[4:]
        if field in {"hit_count", "hits", "interval_count"}:
            return "hit"
    if key.startswith("bars:"):
        return "hit"
    if key.startswith("usdt:"):
        field = key[5:]
        if field in {"min_depth", "bid_depth", "ask_depth", "quote_volume_24h"}:
            return "liquidity"
    return ""


def html_sort_value(row: Dict[str, Any], row_idx: int, key: str) -> str:
    """HTML表格排序用的原始值。"""
    if key == "#":
        return str(row_idx)
    if key == "symbol":
        return str(row.get("symbol") or "")
    if key.startswith("pct:"):
        value = row.get(key[4:])
        try:
            return str(float(value))
        except Exception:
            return ""
    if key.startswith("usdt:"):
        value = row.get(key[5:])
        try:
            return str(float(value))
        except Exception:
            return ""
    if key.startswith("num:"):
        value = row.get(key[4:])
        try:
            return str(float(value))
        except Exception:
            return ""
    if key.startswith("int:"):
        value = row.get(key[4:])
        try:
            return str(int(value))
        except Exception:
            return ""
    if key.startswith("price_time:"):
        parts = key.split(":", 2)
        if len(parts) != 3:
            return ""
        value = row.get(parts[1])
        try:
            return str(float(value))
        except Exception:
            return ""
    if key.startswith("price:"):
        value = row.get(key[6:])
        try:
            return str(float(value))
        except Exception:
            return ""
    if key.startswith("time:"):
        value = row.get(key[5:])
        try:
            return str(int(value))
        except Exception:
            return ""
    if key.startswith("bars:"):
        base = key[5:]
        up = row.get(f"{base}_up")
        bars = row.get(f"{base}_bars")
        try:
            up_i = int(up)
            bars_i = int(bars)
        except Exception:
            return ""
        if bars_i <= 0:
            return ""
        # 先按阳线比例排，比例相同再按阳线数量排。
        return f"{up_i / bars_i:.8f}|{up_i:04d}"
    value = row.get(key)
    return "" if value is None else str(value)


def html_table(title: str, rows: List[Dict[str, Any]], columns: List[Tuple[str, str]]) -> str:
    title_e = html_lib.escape(title)
    parts = [f"<section class='card'><h2>{title_e}</h2>"]
    if not rows:
        parts.append("<p class='empty'>无符合条件的数据</p></section>")
        return "\n".join(parts)

    parts.append(f"<div class='table-wrap'><table class='sortable' data-title='{title_e}'><thead><tr>")
    for header, key in columns:
        key_attr = html_lib.escape(key)
        parts.append(f"<th data-key='{key_attr}' onclick='sortTable(this)'>{html_lib.escape(header)}<span class='sort-mark'>↕</span></th>")
    parts.append("</tr></thead><tbody>")
    for i, row in enumerate(rows, start=1):
        symbol_attr = html_lib.escape(str(row.get("symbol") or ""))
        parts.append(f"<tr data-symbol='{symbol_attr}'>")
        for _, key in columns:
            text, _ = table_cell(row, i, key)
            cls = cell_css_class(row, key)
            cls_attr = f" class='{cls}'" if cls else ""
            sort_attr = html_lib.escape(html_sort_value(row, i, key))
            key_attr = html_lib.escape(key)
            parts.append(f"<td{cls_attr} data-key='{key_attr}' data-sort='{sort_attr}'>{html_lib.escape(text)}</td>")
        parts.append("</tr>")
    parts.append("</tbody></table></div></section>")
    return "\n".join(parts)


def write_dashboard(
    path: str,
    created_at_ms: int,
    cfg: Config,
    symbol_count: int,
    liquidity_count: int,
    last_interval_rankings: Dict[Tuple[str, str], List[Dict[str, Any]]],
    combined_up: List[Dict[str, Any]],
    combined_down: List[Dict[str, Any]],
    focus_up: List[Dict[str, Any]],
    focus_down: List[Dict[str, Any]],
    status: Optional[Dict[str, Any]] = None,
) -> None:
    if not path:
        return

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    created = dt.datetime.fromtimestamp(created_at_ms / 1000, tz=dt.timezone.utc).astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S 北京时间")
    sections: List[str] = []
    sections.append("<div class='hint'>点击任意表头可排序；记录价=综合榜内部参考的已收盘K线价格；现价由浏览器WebSocket实时更新，只更新展示，不改变排名；现价表头时间会随现价WS最近更新时间按秒改写；记录价时间只显示在表头；页面自动刷新会保留滚动位置与展开状态。</div>")
    status_html = html_status_panel(status)
    if status_html:
        sections.append(status_html)
    sections.append(html_table(f"重点关注-上涨 前{cfg.top_n}", focus_up, focus_columns(cfg.history_windows_hours)))
    sections.append(html_table(f"重点关注-下跌 前{cfg.top_n}", focus_down, focus_columns(cfg.history_windows_hours)))

    combined_sections = [
        html_table(f"多周期综合-上涨 前{cfg.top_n}", combined_up, combined_columns()),
        html_table(f"多周期综合-下跌 前{cfg.top_n}", combined_down, combined_columns()),
    ]
    sections.append("<details id='details-combined'><summary>多周期综合榜（展开查看）</summary>" + "\n".join(combined_sections) + "</details>")

    interval_sections: List[str] = []
    for interval in INTERVALS:
        interval_sections.append(html_table(f"{interval} 已收盘上涨榜 前{cfg.top_n}", last_interval_rankings.get((interval, "UP"), []), interval_columns()))
        interval_sections.append(html_table(f"{interval} 已收盘下跌榜 前{cfg.top_n}", last_interval_rankings.get((interval, "DOWN"), []), interval_columns()))

    sections.append("<details id='details-interval'><summary>单周期已收盘最新榜单（展开查看）</summary>" + "\n".join(interval_sections) + "</details>")

    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>USDT永续已收盘K线筛选器</title>
<style>
:root {{
  --bg: #0b0f17; --card: #141a24; --card2:#101620; --text: #d7dde8; --muted: #8b94a7;
  --grid: #263043; --pos: #16c784; --neg: #ea3943; --warn: #f59e0b;
  --good: #4ade80; --bad: #f87171; --link: #7dd3fc; --head:#1f2937;
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans SC", Arial, sans-serif; }}
header {{ position: sticky; top: 0; z-index: 30; background: rgba(11,15,23,.95); border-bottom: 1px solid var(--grid); padding: 12px 16px; backdrop-filter: blur(8px); }}
h1 {{ margin: 0 0 6px 0; font-size: 18px; }}
.meta {{ color: var(--muted); font-size: 13px; display: flex; gap: 14px; flex-wrap: wrap; }}
main {{ padding: 12px; display: grid; grid-template-columns: repeat(auto-fit, minmax(780px, 1fr)); gap: 12px; }}
.hint {{ grid-column: 1 / -1; color: var(--muted); background: var(--card2); border: 1px solid var(--grid); border-radius: 10px; padding: 8px 10px; font-size: 13px; }}
.card, details {{ background: var(--card); border: 1px solid var(--grid); border-radius: 12px; padding: 10px; box-shadow: 0 6px 24px rgba(0,0,0,.18); }}
details {{ grid-column: 1 / -1; }}
summary {{ cursor: pointer; color: var(--link); font-weight: 700; margin-bottom: 10px; }}
h2 {{ margin: 0 0 8px 0; font-size: 15px; color: #f8fafc; }}
.table-wrap {{ overflow: auto; max-height: 680px; border-radius: 8px; }}
table {{ border-collapse: separate; border-spacing: 0; width: 100%; font-variant-numeric: tabular-nums; font-size: 12px; }}
th, td {{ border-bottom: 1px solid var(--grid); padding: 6px 8px; text-align: right; white-space: nowrap; }}
th {{ position: sticky; top: 0; background: var(--head); color: #cbd5e1; z-index: 5; cursor: pointer; user-select: none; }}
th:hover {{ color: #fff; background: #2a3648; }}
.sort-mark {{ color: #64748b; font-size: 10px; margin-left: 3px; }}
tbody tr:nth-child(even) td {{ background: rgba(255,255,255,.018); }}
tbody tr:hover td {{ background: rgba(125,211,252,.08); }}
td:nth-child(1), th:nth-child(1) {{ position: sticky; left: 0; z-index: 6; background: #161d29; }}
td:nth-child(2), th:nth-child(2) {{ position: sticky; left: 42px; z-index: 6; background: #161d29; text-align: left; }}
th:nth-child(1), th:nth-child(2) {{ z-index: 8; background: var(--head); }}
.symbol {{ color: var(--link); font-weight: 800; }}
.pos {{ color: var(--pos); font-weight: 700; }}
.neg {{ color: var(--neg); font-weight: 700; }}
.good {{ color: var(--good); }}
.warn {{ color: var(--warn); }}
.bad {{ color: var(--bad); }}
.hit {{ color: #fde68a; font-weight: 700; }}
.liquidity {{ color: #a5b4fc; }}
.empty {{ color: var(--muted); }}
@media (max-width: 900px) {{ main {{ grid-template-columns: 1fr; }} }}
</style>
<script>
const AUTO_REFRESH_SECONDS = {int(cfg.html_refresh_seconds)};
const WS_PRICE_ENABLED = {str(bool(cfg.ws_price_enabled)).lower()};
const BINANCE_PRICE_WS_URL = "wss://fstream.binance.com/market/stream?streams=!miniTicker@arr";

function beijingTimeFromDate(date, withSeconds = false) {{
  try {{
    return new Intl.DateTimeFormat('zh-CN', {{
      timeZone: 'Asia/Shanghai',
      hour: '2-digit',
      minute: '2-digit',
      second: withSeconds ? '2-digit' : undefined,
      hour12: false
    }}).format(date);
  }} catch (e) {{
    return withSeconds ? '--:--:--' : '--:--';
  }}
}}

function updateCurrentPriceHeadersByWs(timeText) {{
  document.querySelectorAll('th[data-key="price:current_price"]').forEach(th => {{
    const markEl = th.querySelector('.sort-mark');
    const mark = markEl ? markEl.textContent : '↕';
    th.innerHTML = '现价(' + timeText + ')<span class="sort-mark">' + mark + '</span>';
  }});
}}

function parseSortValue(v) {{
  if (v === null || v === undefined || v === '') return {{empty: true, value: ''}};
  if (String(v).includes('|')) {{
    const parts = String(v).split('|');
    const n = Number(parts[0]);
    const m = Number(parts[1] || 0);
    if (!Number.isNaN(n)) return {{empty:false, value:n * 100000 + m}};
  }}
  const n = Number(v);
  if (!Number.isNaN(n)) return {{empty:false, value:n}};
  return {{empty:false, value:String(v)}};
}}

function sortTable(th, forcedAsc = null, skipSave = false) {{
  const table = th.closest('table');
  const tbody = table.querySelector('tbody');
  const idx = Array.from(th.parentNode.children).indexOf(th);
  const asc = forcedAsc === null ? th.dataset.asc !== 'true' : forcedAsc;
  table.querySelectorAll('th').forEach(h => {{ h.dataset.asc = ''; h.querySelector('.sort-mark').textContent = '↕'; }});
  th.dataset.asc = String(asc);
  th.querySelector('.sort-mark').textContent = asc ? '↑' : '↓';
  table.dataset.sortIndex = String(idx);
  table.dataset.sortAsc = String(asc);
  const rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort((a, b) => {{
    const av = parseSortValue(a.children[idx].dataset.sort);
    const bv = parseSortValue(b.children[idx].dataset.sort);
    if (av.empty && bv.empty) return 0;
    if (av.empty) return 1;
    if (bv.empty) return -1;
    let cmp;
    if (typeof av.value === 'number' && typeof bv.value === 'number') cmp = av.value - bv.value;
    else cmp = String(av.value).localeCompare(String(bv.value), 'zh-Hans-CN');
    return asc ? cmp : -cmp;
  }});
  rows.forEach((r, i) => {{ r.children[0].textContent = String(i + 1); tbody.appendChild(r); }});
  if (!skipSave) savePageState();
}}

function storageKey() {{
  return 'usdt_momentum_dashboard_state:' + location.pathname;
}}

function savePageState() {{
  const details = {{}};
  document.querySelectorAll('details[id]').forEach(d => {{ details[d.id] = d.open; }});
  const sorts = {{}};
  document.querySelectorAll('table.sortable[data-title]').forEach(t => {{
    if (t.dataset.sortIndex !== undefined && t.dataset.sortIndex !== '') {{
      sorts[t.dataset.title] = {{idx: Number(t.dataset.sortIndex), asc: t.dataset.sortAsc === 'true'}};
    }}
  }});
  localStorage.setItem(storageKey(), JSON.stringify({{scrollY: window.scrollY, details, sorts}}));
}}

function restorePageState() {{
  let state = null;
  try {{ state = JSON.parse(localStorage.getItem(storageKey()) || 'null'); }} catch (e) {{ state = null; }}
  if (!state) return;

  if (state.details) {{
    Object.entries(state.details).forEach(([id, open]) => {{
      const d = document.getElementById(id);
      if (d) d.open = Boolean(open);
    }});
  }}

  if (state.sorts) {{
    document.querySelectorAll('table.sortable[data-title]').forEach(t => {{
      const saved = state.sorts[t.dataset.title];
      if (!saved) return;
      const th = t.querySelectorAll('th')[saved.idx];
      if (th) sortTable(th, Boolean(saved.asc), true);
    }});
  }}

  requestAnimationFrame(() => window.scrollTo(0, Number(state.scrollY || 0)));
}}


function fmtPriceLive(x) {{
  const v = Number(x);
  if (!Number.isFinite(v)) return '-';
  const av = Math.abs(v);
  if (av >= 1000) return v.toLocaleString(undefined, {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
  if (av >= 100) return v.toLocaleString(undefined, {{minimumFractionDigits: 3, maximumFractionDigits: 3}});
  if (av >= 1) return v.toLocaleString(undefined, {{minimumFractionDigits: 4, maximumFractionDigits: 4}});
  return v.toFixed(8);
}}

function fmtPctLive(x, digits = 3) {{
  const v = Number(x);
  if (!Number.isFinite(v)) return '-';
  const sign = v > 0 ? '+' : '';
  return sign + v.toFixed(digits) + '%';
}}

function setWsStatus(text) {{
  const el = document.getElementById('ws-price-status');
  if (el) el.textContent = text;
}}

function cssSafe(s) {{
  if (window.CSS && CSS.escape) return CSS.escape(s);
  return String(s).replace(/"/g, '\\"');
}}

let SYMBOL_ROW_CACHE = null;
function rowsForSymbol(symbol) {{
  if (SYMBOL_ROW_CACHE === null) {{
    SYMBOL_ROW_CACHE = new Map();
    document.querySelectorAll('tr[data-symbol]').forEach(row => {{
      const sym = row.dataset.symbol;
      if (!sym) return;
      if (!SYMBOL_ROW_CACHE.has(sym)) SYMBOL_ROW_CACHE.set(sym, []);
      SYMBOL_ROW_CACHE.get(sym).push(row);
    }});
  }}
  return SYMBOL_ROW_CACHE.get(symbol) || [];
}}


function updateLivePrice(symbol, price) {{
  if (!symbol || !Number.isFinite(price)) return;
  const rows = rowsForSymbol(symbol);
  if (!rows.length) return;
  rows.forEach(row => {{
    row.querySelectorAll('td[data-key="price:current_price"]').forEach(td => {{
      td.textContent = fmtPriceLive(price);
      td.dataset.sort = String(price);
    }});

    const refCell = row.querySelector('td[data-key="price_time:record_price:record_time"]') || row.querySelector('td[data-key="price:record_price"]') || row.querySelector('td[data-key="price:close"]');
    const ref = refCell ? Number(refCell.dataset.sort) : NaN;
    if (Number.isFinite(ref) && ref > 0) {{
      const pct = (price / ref - 1.0) * 100.0;
      row.querySelectorAll('td[data-key="pct:current_change_pct"]').forEach(td => {{
        td.textContent = fmtPctLive(pct);
        td.dataset.sort = String(pct);
        td.classList.remove('pos', 'neg');
        if (pct > 0) td.classList.add('pos');
        else if (pct < 0) td.classList.add('neg');
      }});
    }}
  }});
}}

function startPriceWebSocket() {{
  if (!WS_PRICE_ENABLED || !('WebSocket' in window)) {{
    setWsStatus('现价WS：关闭');
    return;
  }}
  let retry = 0;
  let ws = null;

  const connect = () => {{
    try {{
      ws = new WebSocket(BINANCE_PRICE_WS_URL);
      setWsStatus('现价WS：连接中');
    }} catch (e) {{
      retry += 1;
      setWsStatus('现价WS：创建失败，重连中');
      setTimeout(connect, Math.min(30000, 1000 * Math.pow(2, Math.min(retry, 5))));
      return;
    }}

    ws.onopen = () => {{
      retry = 0;
      setWsStatus('现价WS：已连接，等待价格推送');
    }};

    ws.onmessage = (event) => {{
      let payload;
      try {{ payload = JSON.parse(event.data); }} catch (e) {{ return; }}
      const data = Array.isArray(payload.data) ? payload.data : (Array.isArray(payload) ? payload : []);
      let updated = 0;
      let maxEventTimeMs = null;
      for (const item of data) {{
        const symbol = item && item.s;
        const price = Number(item && item.c);
        if (!symbol || !Number.isFinite(price)) continue;
        const rows = rowsForSymbol(symbol);
        if (rows.length) {{
          updateLivePrice(symbol, price);
          updated += rows.length;
          const eventTime = Number(item && item.E);
          if (Number.isFinite(eventTime)) {{
            maxEventTimeMs = maxEventTimeMs === null ? eventTime : Math.max(maxEventTimeMs, eventTime);
          }}
        }}
      }}
      if (updated > 0) {{
        const eventDate = maxEventTimeMs === null ? new Date() : new Date(maxEventTimeMs);
        const now = beijingTimeFromDate(eventDate, true);
        updateCurrentPriceHeadersByWs(now);
        setWsStatus('现价WS：已更新 ' + updated + ' 行｜' + now);
      }} else if (data.length > 0) {{
        setWsStatus('现价WS：已收到推送，但当前榜单暂无匹配合约');
      }}
    }};

    ws.onerror = () => {{
      setWsStatus('现价WS：错误');
      try {{ ws.close(); }} catch (e) {{}}
    }};

    ws.onclose = () => {{
      retry += 1;
      const delay = Math.min(30000, 1000 * Math.pow(2, Math.min(retry, 5)));
      setWsStatus('现价WS：断开，' + Math.round(delay / 1000) + '秒后重连');
      setTimeout(connect, delay);
    }};
  }};

  connect();
}}

window.addEventListener('beforeunload', savePageState);
window.addEventListener('DOMContentLoaded', () => {{
  restorePageState();
  startPriceWebSocket();
  document.querySelectorAll('details[id]').forEach(d => d.addEventListener('toggle', savePageState));
  if (AUTO_REFRESH_SECONDS > 0) {{
    setInterval(() => {{
      savePageState();
      location.reload();
    }}, AUTO_REFRESH_SECONDS * 1000);
  }}
}});
</script>
</head>
<body>
<header>
  <h1>USDT 永续已收盘 K 线动量榜｜阳线统计</h1>
  <div class="meta">
    <span>更新时间：{html_lib.escape(created)}</span>
    <span>交易对：{symbol_count}</span>
    <span>流动性过滤后：{liquidity_count}</span>
    <span>输出前：{cfg.top_n}</span>
    <span>24h成交额 ≥ {html_lib.escape(fmt_usdt(cfg.min_24h_quote_volume))}</span>
    <span>价差过滤：{'≤ %.4f%%' % cfg.max_spread_pct if cfg.use_spread_filter else '关闭'}</span>
    <span>深度过滤：{'开启' if cfg.use_depth_filter else '关闭'}</span>
    <span id="ws-price-status">现价WS：等待启动</span>
    <span>HTML自动刷新：{str(cfg.html_refresh_seconds) + '秒' if cfg.html_refresh_seconds > 0 else '关闭'}</span>
  </div>
</header>
<main>
{''.join(sections)}
</main>
</body>
</html>
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


def parse_history_windows(raw: str) -> Tuple[int, ...]:
    raw = (raw or "").strip()
    if not raw:
        return tuple()

    out: List[int] = []
    for part in raw.split(","):
        p = part.strip().lower()
        if not p:
            continue
        if p.endswith("小时"):
            p = p[:-2]
        elif p.endswith("h"):
            p = p[:-1]
        value = int(p)
        if value <= 0:
            raise ValueError("history window must be positive")
        out.append(value)

    return tuple(dict.fromkeys(out))



def parse_args() -> Config:
    p = argparse.ArgumentParser(description="USDT-M 永续已收盘K线动量筛选器（轻量稳定版：REST只拉5m）")
    p.add_argument("--db", default="screener_records_v9.sqlite3", help="SQLite记录文件")
    p.add_argument("--min-24h-qv", type=float, default=30_000_000.0, help="24h成交额过滤，单位USDT，默认3000万")
    p.add_argument("--max-spread", type=float, default=0.08, help="最大买卖价差，单位百分比，例如0.08表示0.08%%；只有 --use-spread 时生效")
    p.add_argument("--use-spread", action="store_true", help="开启买卖价差过滤；默认关闭以减少REST请求和放宽筛选范围")
    p.add_argument("--no-spread", action="store_true", help="兼容参数：关闭买卖价差过滤")
    p.add_argument("--min-depth", type=float, default=25_000.0, help="候选盘口±0.2%%最小深度，单位USDT")
    p.add_argument("--use-depth", action="store_true", help="开启盘口深度过滤；默认关闭以减少REST请求")
    p.add_argument("--no-depth", action="store_true", help="兼容旧参数：关闭盘口深度过滤")
    p.add_argument("--top", type=int, default=15, help="输出前N名")
    p.add_argument("--hit-top", type=int, default=15, help="内部排名阈值：每个周期取前N名参与综合评分")
    p.add_argument("--history-top", type=int, default=15, help="已废弃参数：保留兼容，不影响输出")
    p.add_argument("--history-windows", default="1,4,24,72", help="阳线统计窗口，单位小时，逗号分隔；默认1,4,24,72；留空表示关闭")
    p.add_argument("--loop", type=int, default=12, help="主循环检查间隔，秒，默认12")
    p.add_argument("--workers", type=int, default=4, help="并发请求数量，默认4；代理不稳可降到2")
    p.add_argument("--timeout", type=float, default=20.0, help="单次HTTP请求超时时间，秒，默认20")
    p.add_argument("--liquidity-refresh", type=int, default=120, help="流动性数据刷新秒数，默认120")
    p.add_argument("--proxy", default=os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or os.environ.get("ALL_PROXY") or os.environ.get("all_proxy") or "", help="HTTP/SOCKS代理，例如 socks5h://127.0.0.1:51081 或 http://127.0.0.1:7890")
    p.add_argument("--base-url", default=BASE_URL, help="Binance USDⓈ-M Futures REST基础地址，默认 https://fapi.binance.com")
    p.add_argument("--plain", action="store_true", help="关闭Rich彩色表格，使用普通文本输出")
    p.add_argument("--no-color", action="store_true", help="保留Rich表格，但关闭颜色")
    p.add_argument("--compact", action="store_true", default=True, help="紧凑输出：默认开启，只打印重点关注表")
    p.add_argument("--full-output", action="store_true", help="打印单周期榜和多周期综合榜，输出会更多")
    p.add_argument("--html", default="screener_dashboard.html", help="HTML仪表盘输出路径；留空表示不生成")
    p.add_argument("--no-html", action="store_true", help="关闭HTML仪表盘输出")
    p.add_argument("--html-refresh", type=int, default=60, help="HTML页面自动刷新秒数；0表示不自动刷新，默认60；现价由WebSocket局部更新")
    p.add_argument("--current-price-refresh", type=int, default=0, help="已废弃兼容参数：现价不再通过REST轮询，改由HTML端WebSocket实时更新")
    p.add_argument("--no-ws-price", action="store_true", help="关闭HTML端Binance WebSocket现价更新")
    p.add_argument("--min-coverage", type=float, default=0.85, help="5m K线覆盖率提醒阈值，默认0.85")
    p.add_argument("--hard-min-coverage", type=float, default=0.70, help="5m K线硬覆盖率阈值，低于该值跳过本轮，默认0.70")
    p.add_argument("--no-preload", action="store_true", help="关闭启动预热5m历史K线")
    p.add_argument("--keep-days", type=int, default=14, help="SQLite记录保留天数；0表示不自动清理，默认14")
    p.add_argument("--long-backfill-window", type=int, default=72, help="长窗口5m缺口扫描的窗口大小（小时），默认72；对应24h/72h阳线精度")
    p.add_argument("--long-backfill-interval", type=int, default=3600, help="长窗口5m缺口扫描间隔（秒），默认3600；0表示关闭")
    args = p.parse_args()
    use_depth = bool(args.use_depth and not args.no_depth)
    return Config(
        db_path=args.db,
        top_n=args.top,
        hit_top_n=args.hit_top,
        min_24h_quote_volume=args.min_24h_qv,
        max_spread_pct=args.max_spread,
        use_spread_filter=bool(args.use_spread and not args.no_spread),
        min_depth_usdt=args.min_depth,
        use_depth_filter=use_depth,
        history_windows_hours=parse_history_windows(args.history_windows),
        loop_seconds=args.loop,
        max_workers=max(1, int(args.workers)),
        request_timeout=args.timeout,
        liquidity_refresh_seconds=max(30, int(args.liquidity_refresh)),
        proxy_url=args.proxy.strip(),
        base_url=args.base_url.strip() or BASE_URL,
        rich_output=(not args.plain),
        no_color=args.no_color,
        compact_output=(not args.full_output),
        html_path="" if args.no_html else args.html,
        html_refresh_seconds=max(0, int(args.html_refresh)),
        ws_price_enabled=not args.no_ws_price,
        min_coverage=max(0.0, min(1.0, float(args.min_coverage))),
        hard_min_coverage=max(0.0, min(1.0, float(args.hard_min_coverage))),
        preload_on_start=(not args.no_preload),
        keep_days=max(0, int(args.keep_days)),
        long_backfill_window_hours=max(4, int(args.long_backfill_window)),
        long_backfill_interval_seconds=max(0, int(args.long_backfill_interval)),
    )



def main() -> None:
    global USE_RICH_OUTPUT, NO_COLOR_OUTPUT, REQUEST_PROXIES, PRICE_HEADER_TIME_MS, RECORD_PRICE_HEADER_TIME_MS
    cfg = parse_args()
    if cfg.proxy_url:
        REQUEST_PROXIES = {"http": cfg.proxy_url, "https": cfg.proxy_url}
    USE_RICH_OUTPUT = bool(cfg.rich_output and RICH_AVAILABLE)
    NO_COLOR_OUTPUT = bool(cfg.no_color)
    client = BinanceClient(cfg)
    store = Store(cfg.db_path)

    print("USDT-M 永续已收盘K线筛选器已启动（轻量稳定版：REST只拉5m）")
    print("核心变化：只用 REST 拉 5m 已收盘K线；15m/30m/1h/4h 从本地5m聚合；成交额默认3000万；价差和深度过滤默认关闭；阳线统计只读SQLite，并由5m本地聚合15m/1h/4h阳线。")
    print(f"数据库：{os.path.abspath(cfg.db_path)}")
    print(f"API地址：{cfg.base_url.rstrip('/')}")
    print(f"代理：{cfg.proxy_url if cfg.proxy_url else '未设置，使用系统/直连环境'}")
    print(
        f"过滤条件：24h成交额 >= {fmt_usdt(cfg.min_24h_quote_volume)}, "
        f"价差过滤：{'<= %.4f%%' % cfg.max_spread_pct if cfg.use_spread_filter else '关闭'}, "
        f"深度过滤：{'开启' if cfg.use_depth_filter else '关闭'}"
        + (f", 最小深度 >= {fmt_usdt(cfg.min_depth_usdt)}" if cfg.use_depth_filter else "")
    )
    print(
        f"运行参数：workers={cfg.max_workers}, loop={cfg.loop_seconds}s, timeout={cfg.request_timeout}s, "
        f"流动性刷新={cfg.liquidity_refresh_seconds}s, 覆盖率提醒={cfg.min_coverage:.0%}, 硬跳过={cfg.hard_min_coverage:.0%}"
    )
    if cfg.history_windows_hours:
        print(
            "K线统计窗口："
            + ", ".join(f"{h}h" for h in cfg.history_windows_hours)
            + "；阳线统计只来自本地SQLite，24h显示15m/1h阳线，72h显示1h/4h阳线，刚启动时记录数量可能不足。"
        )

    status_state: Dict[str, Any] = {
        "coverage": {},
        "warnings": [],
        "last_html_write": "-",
        "mode": "REST只拉5m；高周期本地聚合",
    }
    symbols: List[str] = []
    last_symbol_refresh = 0.0
    liquidity: Dict[str, Dict[str, float]] = {}
    last_liquidity_refresh = 0.0
    latest: Dict[str, Dict[str, Dict[str, Any]]] = {i: {} for i in INTERVALS}
    last_fetched_5m_close: Optional[int] = None
    last_rank_saved: Dict[str, int] = {}
    last_interval_rankings: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    did_preload = False
    last_cleanup = 0.0
    last_long_backfill = 0.0

    def refresh_rankings(server_ms: int, depth_cache: DepthCache) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """重建各周期最新榜、综合榜、重点关注榜。"""
        nonlocal latest, last_interval_rankings, last_rank_saved
        ranked_symbols = sorted(liquidity.keys())
        oc4 = latest_closed_open_close(server_ms, "4h", cfg.post_close_delay_ms)
        if oc4 is None:
            oc4 = latest_closed_open_close_force(server_ms, "4h")
        backfill_missing_5m_window(
            client=client,
            store=store,
            symbols=ranked_symbols,
            liquidity=liquidity,
            open_time=oc4[0],
            close_time=oc4[1],
            cfg=cfg,
            observed_at_ms=server_ms,
            status=status_state,
            label="4h",
        )

        latest = build_latest_from_5m_store(
            store=store,
            symbols=ranked_symbols,
            liquidity=liquidity,
            cfg=cfg,
            server_ms=server_ms,
            status=status_state,
        )

        for interval in INTERVALS:
            rows = list(latest.get(interval, {}).values())
            if not rows:
                continue
            close_time = int(rows[0].get("close_time"))
            up_rows = rank_interval_rows(rows, side="UP", depth_cache=depth_cache, cfg=cfg)
            down_rows = rank_interval_rows(rows, side="DOWN", depth_cache=depth_cache, cfg=cfg)
            last_interval_rankings[(interval, "UP")] = up_rows
            last_interval_rankings[(interval, "DOWN")] = down_rows

            # 只在官方周期真的出现新close_time时入库；避免每5分钟重复保存同一个15m/1h榜。
            if last_rank_saved.get(interval) != close_time:
                last_rank_saved[interval] = close_time
                store.save_ranking("interval", interval, "UP", close_time, up_rows, server_ms)
                store.save_ranking("interval", interval, "DOWN", close_time, down_rows, server_ms)
                if not cfg.compact_output:
                    print_table(f"{interval} 已收盘上涨榜 前{cfg.top_n} | 收盘={utc_str(close_time)}", up_rows, interval_columns())
                    print_table(f"{interval} 已收盘下跌榜 前{cfg.top_n} | 收盘={utc_str(close_time)}", down_rows, interval_columns())

        combined_up = compute_combined(latest, liquidity, side="UP", depth_cache=depth_cache, cfg=cfg)
        combined_down = compute_combined(latest, liquidity, side="DOWN", depth_cache=depth_cache, cfg=cfg)
        store.save_ranking("combined", None, "UP", None, combined_up, server_ms)
        store.save_ranking("combined", None, "DOWN", None, combined_down, server_ms)

        focus_symbols = sorted({r.get("symbol") for r in (combined_up + combined_down) if r.get("symbol")})
        candle_by_hour = fetch_candle_window_stats_db_only(
            store=store,
            symbols=focus_symbols,
            windows=cfg.history_windows_hours,
            now_ms=server_ms,
            status=status_state,
        )
        focus_up = make_focus_rows(combined_up, candle_by_hour, cfg.history_windows_hours)
        focus_down = make_focus_rows(combined_down, candle_by_hour, cfg.history_windows_hours)
        return combined_up, combined_down, focus_up, focus_down

    try:
        while not _shutdown:
            loop_start = time.time()
            server_ms = client.server_time_ms()
            PRICE_HEADER_TIME_MS = server_ms
            if last_fetched_5m_close is not None:
                RECORD_PRICE_HEADER_TIME_MS = last_fetched_5m_close
            status_state["db_size"] = db_size_text(cfg.db_path)

            if cfg.keep_days > 0 and (last_cleanup <= 0 or loop_start - last_cleanup >= cfg.cleanup_interval_seconds):
                try:
                    store.cleanup_old_records(server_ms, keep_days=cfg.keep_days)
                    last_cleanup = loop_start
                    status_state["last_cleanup"] = fmt_time(server_ms)
                    status_state["db_size"] = db_size_text(cfg.db_path)
                except Exception as exc:
                    msg = f"数据库清理失败: {exc}"
                    print(f"[警告] {msg}", file=sys.stderr)
                    remember_warning(status_state, msg)

            if not symbols or loop_start - last_symbol_refresh >= cfg.symbol_refresh_seconds:
                symbols = client.exchange_symbols()
                last_symbol_refresh = loop_start
                print(f"\n[{utc_str(server_ms)}] 已加载交易对：{len(symbols)} 个 USDT 永续交易中合约")

            if not liquidity or loop_start - last_liquidity_refresh >= cfg.liquidity_refresh_seconds:
                tickers = client.tickers_24h()
                books = client.book_tickers() if (cfg.use_spread_filter or cfg.use_depth_filter) else {}
                liquidity = enrich_liquidity(symbols, tickers, books, cfg)
                last_liquidity_refresh = loop_start
                print(f"[{utc_str(server_ms)}] 流动性过滤通过：{len(liquidity)} 个合约")

            eligible_symbols = sorted(liquidity.keys())
            if not eligible_symbols:
                print(f"[{utc_str(server_ms)}] 没有合约通过流动性过滤")
                time.sleep(cfg.loop_seconds)
                continue

            depth_cache = DepthCache(client)
            scanned_any = False

            if cfg.preload_on_start and not did_preload:
                limit = preload_5m_limit(cfg)
                print(f"[{utc_str(server_ms)}] 启动预热：拉取每个合约最近 {limit} 根 5m 已收盘K线")
                preload_rows = fetch_recent_5m_history(
                    client=client,
                    symbols=eligible_symbols,
                    liquidity=liquidity,
                    limit=limit,
                    now_ms=server_ms,
                    cfg=cfg,
                )
                if preload_rows:
                    store.upsert_klines(preload_rows, observed_at_ms=server_ms)
                    latest_5m_close = max(int(r["close_time"]) for r in preload_rows if r.get("interval") == "5m")
                    last_fetched_5m_close = latest_5m_close
                    RECORD_PRICE_HEADER_TIME_MS = latest_5m_close
                    scanned_any = True
                    # 用覆盖率判断预热是否真正完成；不足则下一轮继续重试
                    preloaded_syms = {r["symbol"] for r in preload_rows if r.get("interval") == "5m"}
                    preload_cov = len(preloaded_syms) / max(1, len(eligible_symbols))
                    if preload_cov >= cfg.min_coverage:
                        did_preload = True
                        print(f"[{utc_str(server_ms)}] 启动预热完成：写入 {len(preload_rows)} 条5m历史K线；覆盖率 {preload_cov:.1%}；最新5m={utc_str(latest_5m_close)}")
                    else:
                        msg = f"启动预热覆盖率偏低：{len(preloaded_syms)}/{len(eligible_symbols)}={preload_cov:.1%}，下一轮继续重试"
                        print(f"[警告] {msg}", file=sys.stderr)
                        remember_warning(status_state, msg)
                else:
                    msg = "启动预热没有拿到5m历史K线，下一轮继续重试"
                    print(f"[警告] {msg}", file=sys.stderr)
                    remember_warning(status_state, msg)

            # 长窗口（默认72h）5m缺口扫描：每小时一次，保证 24h/72h 阳线统计精确。
            # 4h 之外的 5m 缺口由这里追，避免预热漏掉的 symbol 长期数据不全。
            if (
                cfg.long_backfill_interval_seconds > 0
                and did_preload
                and (last_long_backfill <= 0 or loop_start - last_long_backfill >= cfg.long_backfill_interval_seconds)
            ):
                try:
                    five_min_ms = INTERVAL_MS["5m"]
                    last_5m_open = (server_ms // five_min_ms) * five_min_ms
                    long_close = last_5m_open - 1
                    long_open = last_5m_open - cfg.long_backfill_window_hours * 60 * 60 * 1000
                    backfill_missing_5m_window(
                        client=client,
                        store=store,
                        symbols=eligible_symbols,
                        liquidity=liquidity,
                        open_time=long_open,
                        close_time=long_close,
                        cfg=cfg,
                        observed_at_ms=server_ms,
                        status=status_state,
                        label=f"{cfg.long_backfill_window_hours}h",
                    )
                    last_long_backfill = loop_start
                except Exception as exc:
                    msg = f"{cfg.long_backfill_window_hours}h 缺口补拉失败: {exc}"
                    print(f"[警告] {msg}", file=sys.stderr)
                    remember_warning(status_state, msg)

            oc5 = latest_closed_open_close(server_ms, "5m", cfg.post_close_delay_ms)
            if oc5 is not None:
                open_time, close_time = oc5
                if last_fetched_5m_close != close_time:
                    print(f"\n[{utc_str(server_ms)}] 扫描 5m 已收盘K线：{utc_str(open_time)} -> {utc_str(close_time)}")
                    rows = fetch_klines_for_interval(
                        client=client,
                        symbols=eligible_symbols,
                        liquidity=liquidity,
                        interval="5m",
                        open_time=open_time,
                        close_time=close_time,
                        cfg=cfg,
                    )
                    cov = coverage_ratio(rows, eligible_symbols)
                    status_state.setdefault("coverage", {})["5m"] = {
                        "rows": len(rows),
                        "expected": len(eligible_symbols),
                        "ratio": cov,
                        "close_time": close_time,
                        "preload": False,
                        "source": "REST",
                    }
                    if cov < cfg.hard_min_coverage:
                        # 不把这根5m永久标记为已处理；下一轮继续重试，避免缺一根5m导致4h长期聚合不出来。
                        msg = f"5m K线覆盖率过低：{len(rows)}/{len(eligible_symbols)} = {cov:.1%}，跳过本轮排名和HTML刷新；下一轮会重试"
                        print(f"[警告] {msg}", file=sys.stderr)
                        remember_warning(status_state, msg)
                    else:
                        if cov < cfg.min_coverage:
                            msg = f"5m K线覆盖率偏低：{len(rows)}/{len(eligible_symbols)} = {cov:.1%}，本轮继续处理但结果需谨慎"
                            print(f"[警告] {msg}", file=sys.stderr)
                            remember_warning(status_state, msg)
                        store.upsert_klines(rows, observed_at_ms=server_ms)
                        last_fetched_5m_close = close_time
                        RECORD_PRICE_HEADER_TIME_MS = close_time
                        scanned_any = True

            if scanned_any:
                combined_up, combined_down, focus_up, focus_down = refresh_rankings(server_ms, depth_cache)
                last_combined_up, last_combined_down = combined_up, combined_down
                last_focus_up, last_focus_down = focus_up, focus_down

                status_state["last_price_update"] = "HTML端WebSocket实时更新"
                status_state["price_symbols"] = len({
                    r.get("symbol") for r in (last_focus_up + last_focus_down) if r.get("symbol")
                })

                print_table(f"重点关注-上涨 前{cfg.top_n} | 5m REST + 高周期本地聚合", last_focus_up, focus_columns(cfg.history_windows_hours))
                print_table(f"重点关注-下跌 前{cfg.top_n} | 5m REST + 高周期本地聚合", last_focus_down, focus_columns(cfg.history_windows_hours))

                if not cfg.compact_output:
                    print_table(f"多周期综合-上涨 前{cfg.top_n} | 内部排名阈值=前{cfg.hit_top_n}", last_combined_up, combined_columns())
                    print_table(f"多周期综合-下跌 前{cfg.top_n} | 内部排名阈值=前{cfg.hit_top_n}", last_combined_down, combined_columns())

            # 现价不再由 Python REST 轮询刷新；HTML 页面直接通过 Binance WebSocket 局部更新。
            price_refresh_due = False

            if cfg.html_path and (scanned_any or price_refresh_due):
                try:
                    write_dashboard(
                        path=cfg.html_path,
                        created_at_ms=server_ms,
                        cfg=cfg,
                        symbol_count=len(symbols),
                        liquidity_count=len(liquidity),
                        last_interval_rankings=last_interval_rankings,
                        combined_up=last_combined_up,
                        combined_down=last_combined_down,
                        focus_up=last_focus_up,
                        focus_down=last_focus_down,
                        status=status_state,
                    )
                    status_state["last_html_write"] = fmt_time(server_ms)
                except Exception as exc:
                    msg = f"HTML仪表盘写入失败: {exc}"
                    print(f"[警告] {msg}", file=sys.stderr)
                    remember_warning(status_state, msg)

            elapsed = time.time() - loop_start
            sleep_for = max(1.0, cfg.loop_seconds - elapsed)
            time.sleep(sleep_for)

    finally:
        store.close()
        print("\n已停止。")


if __name__ == "__main__":
    main()
