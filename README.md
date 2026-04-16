# ARBSCANNER V5 — Multi-Crypto Frequency-Optimized Trader

A high-frequency paper/shadow/live trading bot for Polymarket crypto prediction markets. Trades 6 cryptocurrencies across 5-minute and 15-minute up/down markets using real-time WebSocket price feeds from both Polymarket CLOB and Binance.

## Features

- **Multi-Crypto Trading** — Simultaneously trades BTC, ETH, SOL, XRP, DOGE, and BNB markets
- **3 Trading Modes:**
  - **Paper** — Simulated trades only (default, no real money)
  - **Shadow ($1)** — Places real $1 orders alongside paper trades for validation
  - **Live** — Full-size real orders via Polymarket CLOB API (requires confirmation)
- **WebSocket-First Architecture** — Real-time CLOB order book data + Binance price feeds
- **Safety System** — Daily loss limits, consecutive loss tracking, max concurrent positions, kill switch
- **Dashboard** — Web UI on port 8085 with live P&L, equity curves, trade history, mode controls
- **Frequency Optimized** — Discovers and trades new 5m/15m markets as they appear (~every 5 minutes)

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Dashboard (FastAPI)                 │
│              Port 8085                           │
├─────────────────────────────────────────────────┤
│         live_paper_trader.py                     │
│  ┌──────────┐ ┌──────────┐ ┌──────────────┐     │
│  │ Strategy │ │ Trading  │ │   Safety     │     │
│  │  Engine  │ │  Client  │ │   Manager    │     │
│  └────┬─────┘ └────┬─────┘ └──────┬───────┘     │
│       │             │              │             │
│  ┌────┴─────────────┴──────────────┴───────┐     │
│  │     Market Discovery & Evaluation       │     │
│  │  (WebSocket CLOB + REST + Binance)      │     │
│  └─────────────────────────────────────────┘     │
└─────────────────────────────────────────────────┘
```

## Files

| File | Description |
|------|-------------|
| `config.py` | All configuration: API endpoints, crypto configs, strategy params, safety limits, wallet settings |
| `live_paper_trader.py` | Core trading engine — market discovery, evaluation, entry/exit logic, WebSocket feeds |
| `strategy.py` | Trading strategy — entry signals, position sizing, Kelly criterion |
| `dashboard.py` | FastAPI web server — REST API, mode switching, kill switch, safety endpoints |
| `trading_client.py` | Polymarket CLOB client wrapper — order placement, balance checks, API credential management |
| `safety.py` | Safety manager — daily loss limits, kill switch, halt logic, persistent state |
| `models.py` | Data models for markets and trades |
| `check_question.py` | Utility to inspect a specific Polymarket question/market |
| `templates/index.html` | Dashboard UI — charts, trade tables, mode controls, safety panel |
| `static/style.css` | Dashboard styling |

## Setup

### Prerequisites
- Python 3.10+
- A Polymarket wallet with USDC (for shadow/live modes)

### Installation

```bash
# Clone
git clone https://github.com/benrukundo/ARBSCANNER_V5.git
cd ARBSCANNER_V5

# Virtual environment
python3 -m venv venv
source venv/bin/activate

# Dependencies
pip install -r requirements.txt

# Configuration
cp .env.example .env
# Edit .env with your Polymarket wallet credentials (only needed for shadow/live modes)
```

### Running

```bash
# Start the dashboard + trader
uvicorn dashboard:app --host 0.0.0.0 --port 8085

# Or run in background
nohup uvicorn dashboard:app --host 0.0.0.0 --port 8085 > logs/v21.log 2>&1 &
```

Then open `http://<your-ip>:8085` in a browser.

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/health` | Health check with mode and trader status |
| `GET` | `/api/paper/state` | Full trading state (positions, P&L, trades) |
| `GET` | `/api/paper/equity-curve` | Equity curve data for charting |
| `POST` | `/api/paper/start` | Start the trading engine |
| `POST` | `/api/paper/stop` | Stop the trading engine |
| `POST` | `/api/mode/switch` | Switch mode: `{"mode": "paper"}` or `"shadow"` or `"live"` |
| `POST` | `/api/kill-switch/activate` | Emergency stop — cancels all orders |
| `POST` | `/api/kill-switch/deactivate` | Re-enable trading after kill switch |
| `GET` | `/api/safety/state` | Current safety manager state |
| `POST` | `/api/safety/unhalt` | Manually unhalt after safety trigger |

## Trading Modes

### Paper Mode (Default)
No real money involved. The bot discovers markets, evaluates entry conditions, and records simulated trades with a virtual bankroll.

### Shadow Mode ($1 Trades)
Places real $1 orders on Polymarket alongside paper trades. Useful for validating execution and slippage without significant risk.

### Live Mode
Places full-size real orders based on Kelly criterion position sizing (capped at `MAX_TRADE_SIZE`). Requires typing "CONFIRM" in the dashboard to enable.

## Safety System

- **Daily Loss Limit** — Auto-halts if daily losses exceed $15 (configurable)
- **Consecutive Losses** — Auto-halts after 5 consecutive losses
- **Max Concurrent Positions** — Limits to 3 open positions at a time
- **Kill Switch** — Instant emergency stop, cancels all open orders
- **Minimum Balance** — Won't trade if wallet balance drops below $10

## Supported Cryptocurrencies

| Crypto | Markets | Binance Symbol |
|--------|---------|----------------|
| Bitcoin (BTC) | 5m, 15m | BTCUSDT |
| Ethereum (ETH) | 5m, 15m | ETHUSDT |
| Solana (SOL) | 5m, 15m | SOLUSDT |
| XRP | 5m, 15m | XRPUSDT |
| Dogecoin (DOGE) | 5m, 15m | DOGEUSDT |
| BNB | 5m, 15m | BNBUSDT |

## Infrastructure

Optimized for deployment on Azure North Europe (Dublin) for minimum latency to Polymarket servers (~80ms API, ~100ms WebSocket).

## License

Private — All rights reserved.
