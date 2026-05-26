# USDT-M 永续合约 K线动量筛选器

> 一个面向 Binance USDⓈ-M 永续合约的轻量、生产级动量筛选工具。
> REST 只拉 5 分钟已收盘 K 线,15m / 30m / 1h / 4h 全部由本地聚合;
> 数据持久化到 SQLite,HTML 仪表盘通过 Binance WebSocket 实时刷新现价。

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Code Size](https://img.shields.io/badge/code-2.5k%20LOC-orange.svg)]()
[![Status](https://img.shields.io/badge/status-active-success.svg)]()

English version: [README.md](README.md)

---

## ✨ 项目亮点

- **单一数据源,多周期聚合。** 每轮只调用一次 5m K 线接口,15m / 30m / 1h / 4h 全部本地聚合,
  大幅降低 API 权重消耗,几乎不会触发限流。
- **只用已收盘 K 线。** 所有排名和阳线统计都基于完全收盘的 bar,无闪烁、无前视偏差。
- **SQLite 持久化。** 使用 WAL 模式,复合主键防重,自动清理过期数据,断电重启可恢复。
- **多周期综合排名。** 5m / 15m / 30m / 1h / 4h 加权打分,发现真正"多周期同向"的标的,
  而不是单根爆量的假突破。
- **HTML 实时仪表盘。** Python 进程定期重写一份自包含 HTML,浏览器自己开 WebSocket 拉现价、
  计算"现价相对记录价"的偏离,Python 端不再为现价轮询 REST。
- **健壮性设计。** 418 / 429 自动退避重试;覆盖率不足时跳过本轮排名而不是污染数据;
  滚动 72h 缺口扫描自动修补 5m K 线漏洞。
- **流动性 / 价差过滤可插拔。** 默认按 24h 成交额过滤,可选启用顶档价差和 ±0.2% 深度过滤。

## 🧱 系统架构

```
┌────────────────────────────────────────────────────────────────────────┐
│                          主循环 (每 12 秒)                              │
└──────────────┬─────────────────────────┬───────────────────────────────┘
               │                         │
               ▼                         ▼
   ┌──────────────────────┐   ┌──────────────────────────┐
   │  BinanceClient       │   │  Store (SQLite, WAL)     │
   │  - exchangeInfo      │   │  - kline_records         │
   │  - ticker/24hr       │◀──┤  - ranking_runs / items  │
   │  - klines (只拉5m)   │   │  - upsert + 清理          │
   │  - depth (可选)      │   └──────────────────────────┘
   └──────────┬───────────┘                ▲
              │                            │
              ▼                            │
   ┌──────────────────────┐                │
   │  本地聚合器           │                │
   │  5m → 15m/30m/1h/4h  │────────────────┘
   └──────────┬───────────┘
              │
              ▼
   ┌──────────────────────┐      ┌──────────────────────────┐
   │  排名引擎             │─────▶│  控制台 (rich 表格)       │
   │  - 单周期 Top         │      └──────────────────────────┘
   │  - 多周期综合得分     │      ┌──────────────────────────┐
   │  - 重点关注表         │─────▶│  HTML 仪表盘 (+ WebSocket) │
   └──────────────────────┘      └──────────────────────────┘
```

**职责划分:** Python 进程是已收盘数据的"系统记录方",负责拉数据、聚合、排名、写 HTML;
浏览器是"实时视图层",自己开 WebSocket 拉现价 —— Python 不再轮询现价。

## 🚀 快速开始

```bash
# 1. 克隆
git clone https://github.com/<你的用户名>/usdt-perp-kline-screener.git
cd usdt-perp-kline-screener

# 2. 安装依赖
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. 直接运行(默认参数)
python usdt_perp_kline_screener.py

# 4. 紧凑盯盘 + 每 60 秒刷新 HTML
python usdt_perp_kline_screener.py \
    --compact \
    --html screener_dashboard.html \
    --html-refresh 60

# 5. 后台运行(Linux/macOS)
nohup python usdt_perp_kline_screener.py \
    --compact --html screener_dashboard.html \
    > screener.log 2>&1 &
```

运行后用浏览器打开 `screener_dashboard.html`,页面会自动刷新并通过 WebSocket 实时拉现价。

## ⚙️ 主要配置项

所有参数都可以通过命令行传入,或者修改 `Config` 数据类。默认参数已经针对
"扫描整个 USDT-M 永续市场又不触发限流"做过调校:

| 参数                          | 默认值                 | 作用                                              |
| ----------------------------- | ---------------------- | ------------------------------------------------- |
| `min_24h_quote_volume`        | 30,000,000 USDT        | 流动性下限,过滤冷门合约                          |
| `loop_seconds`                | 12                     | 主循环间隔                                        |
| `post_close_delay_ms`         | 2500                   | 5m 收盘后等待多久再去拉数据(给行情结算时间)     |
| `max_workers`                 | 4                      | 并发拉 K 线的线程数                               |
| `min_coverage` / `hard_min_coverage` | 0.85 / 0.70     | K 线覆盖率不足时,本轮排名直接跳过                 |
| `keep_days`                   | 14                     | 旧数据自动清理                                    |
| `long_backfill_window_hours`  | 72                     | 长窗口 5m 缺口扫描范围                             |

## 📁 项目结构

```
usdt-perp-kline-screener/
├── usdt_perp_kline_screener.py   # 单文件实现(约 2547 行)
├── requirements.txt              # 运行依赖(requests, rich)
├── README.md                     # 英文说明
├── README.zh-CN.md               # 本文件
├── LICENSE                       # MIT
└── .gitignore
```

运行时产物(已被 `.gitignore` 忽略):

```
screener_records_v9.sqlite3   # 本地数据库
screener_dashboard.html       # 浏览器仪表盘
screener.log                  # 后台运行日志
```

## 🛣️ Roadmap

- [ ] 5m → 高周期聚合的单元测试
- [ ] Dockerfile + docker-compose 一键部署
- [ ] 重点关注表变化时推送 Telegram / Discord
- [ ] 支持其他交易所(Bybit、OKX)

## ⚠️ 免责声明

本项目仅供**研究和学习用途**,不构成任何投资建议,也**不会主动下单**。
加密货币市场波动剧烈,使用本工具进行的任何决策由使用者自行负责。

## 📄 开源协议

MIT — 详见 [LICENSE](LICENSE)
