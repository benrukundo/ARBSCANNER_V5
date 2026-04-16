"""Configuration for V2.1 — Frequency Optimized Paper Trader."""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Version ────────────────────────────────────────────────────
VERSION = "v2.1-multicrypto"

# ── API Endpoints ──────────────────────────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
BINANCE_API = "https://api.binance.com/api/v3"

# ── WebSocket Endpoints ───────────────────────────────────────
WS_CLOB_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_LIVE_DATA_URL = "wss://ws-live-data.polymarket.com"
POLYMARKET_WS = "wss://ws-live-data.polymarket.com"

# ── Fetcher Settings ──────────────────────────────────────────
FETCH_TIMEOUT = 15
REQUEST_DELAY = 0.3

# ── BTC Market Identification ─────────────────────────────────
BTC_SLUG_PREFIXES = ["btc-updown-5m-", "btc-updown-15m-"]
SLUG_DURATION_MAP = {
    "5m": 300,
    "15m": 900,
}


def slug_to_duration(slug: str) -> int:
    for tf, secs in SLUG_DURATION_MAP.items():
        if f"-{tf}-" in slug:
            return secs
    return 300


def slug_to_start_ts(slug: str) -> int | None:
    parts = slug.split("-")
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return None


def is_btc_updown_slug(slug: str) -> bool:
    return any(slug.startswith(p) for p in BTC_SLUG_PREFIXES)

# ═════════════════════════════════════════════════════════════
# MULTI-CRYPTO CONFIGURATION
# ═════════════════════════════════════════════════════════════

# TODO: Consider making min_distance dynamic (e.g., 0.07% of current price)
# rather than hardcoded. This would auto-adapt to price changes over time.
# NOTE: volatility_1sec values are fallbacks only. Live trading uses RollingVolatility from Binance feed.
CRYPTO_CONFIGS = {
    "btc": {
        "name": "Bitcoin",
        "slug_prefix": "btc-updown",
        "binance_symbol": "BTCUSDT",
        "binance_ws": "wss://stream.binance.com:9443/ws/btcusdt@trade",
        "polymarket_ws_symbol": "btcusdt",
        "chainlink_ws_symbol": "btc/usd",
        "timeframes": ["5m", "15m"],
        "volatility_1sec": 2.5,
        "min_distance": 50.0,
        "position_size": 0.05,
        "min_ptb": 1000,  # price-to-beat must be > this to be valid
    },
    "eth": {
        "name": "Ethereum",
        "slug_prefix": "eth-updown",
        "binance_symbol": "ETHUSDT",
        "binance_ws": "wss://stream.binance.com:9443/ws/ethusdt@trade",
        "polymarket_ws_symbol": "ethusdt",
        "chainlink_ws_symbol": "eth/usd",
        "timeframes": ["5m", "15m"],
        "volatility_1sec": 0.35,
        "min_distance": 5.0,
        "position_size": 0.05,
        "min_ptb": 100,
    },
    "sol": {
        "name": "Solana",
        "slug_prefix": "sol-updown",
        "binance_symbol": "SOLUSDT",
        "binance_ws": "wss://stream.binance.com:9443/ws/solusdt@trade",
        "polymarket_ws_symbol": "solusdt",
        "chainlink_ws_symbol": "sol/usd",
        "timeframes": ["5m", "15m"],
        "volatility_1sec": 0.015,
        "min_distance": 0.30,
        "position_size": 0.05,
        "min_ptb": 1,
    },
    "xrp": {
        "name": "XRP",
        "slug_prefix": "xrp-updown",
        "binance_symbol": "XRPUSDT",
        "binance_ws": "wss://stream.binance.com:9443/ws/xrpusdt@trade",
        "polymarket_ws_symbol": "xrpusdt",
        "chainlink_ws_symbol": "xrp/usd",
        "timeframes": ["5m", "15m"],
        "volatility_1sec": 0.00015,
        "min_distance": 0.005,          # increased from 0.003 to reduce false entries
        "position_size": 0.05,
        "min_ptb": 0.01,
    },
    "doge": {
        "name": "Dogecoin",
        "slug_prefix": "doge-updown",
        "binance_symbol": "DOGEUSDT",
        "binance_ws": "wss://stream.binance.com:9443/ws/dogeusdt@trade",
        "polymarket_ws_symbol": "dogeusdt",
        "chainlink_ws_symbol": "doge/usd",
        "timeframes": ["5m", "15m"],
        "volatility_1sec": 0.000015,
        "min_distance": 0.0005,         # increased from 0.0003 to reduce false entries
        "position_size": 0.05,
        "min_ptb": 0.001,
    },
    "bnb": {
        "name": "BNB",
        "slug_prefix": "bnb-updown",
        "binance_symbol": "BNBUSDT",
        "binance_ws": "wss://stream.binance.com:9443/ws/bnbusdt@trade",
        "polymarket_ws_symbol": "bnbusdt",
        "chainlink_ws_symbol": "bnb/usd",
        "timeframes": ["5m", "15m"],
        "volatility_1sec": 0.08,
        "min_distance": 1.5,
        "position_size": 0.05,
        "min_ptb": 10,
    },
}

# Which cryptos to actively trade
ACTIVE_CRYPTOS = ["btc", "eth", "sol", "xrp", "doge", "bnb"]

def is_crypto_updown_slug(slug: str, crypto_key: str) -> bool:
    prefix = CRYPTO_CONFIGS[crypto_key]["slug_prefix"]
    return slug.startswith(prefix)




# ── Predictive Discovery ─────────────────────────────────────
PREDICTIVE_PREFETCH_SECS = 45   # fetch market details N seconds before boundary
PREDICTIVE_CHECK_INTERVAL = 1   # check for upcoming boundaries every N seconds

# ── WebSocket Settings ────────────────────────────────────────
WS_RECONNECT_DELAY = 2
WS_PING_INTERVAL = 10
WS_FALLBACK_POLL_INTERVAL = 1   # was 2, reduced for faster fallback on 5-min markets

# ── Strategy Defaults (wider 300s entry window) ──────────────
DEFAULT_STRATEGY_PARAMS = {
    "MAX_TIME_REMAINING": 300,      # 5 minutes — wider than V1's 120s
    "MIN_BTC_DISTANCE": 50.0,
    "MAX_ENTRY_PRICE": 0.95,
    "MIN_EDGE": 0.03,
    "POSITION_SIZE": 0.05,
    "BTC_1SEC_VOLATILITY": 2.5,
}

# ── Staged Entry Thresholds ──────────────────────────────────
# Enter earlier if price is low; enter later at higher prices
STAGED_ENTRY = [
    {"min_time": 180, "max_time": 300, "max_price": 0.60},  # Early: only cheap
    {"min_time": 120, "max_time": 180, "max_price": 0.80},  # Mid: moderate
    {"min_time": 5,   "max_time": 120, "max_price": 0.95},  # Late: standard
]

# ── Paper Trading ─────────────────────────────────────────────
PAPER_INITIAL_BANKROLL = 100.0
PAPER_CHECK_INTERVAL = 2   # faster fallback polling than V1's 5s

# ── Safety ────────────────────────────────────────────────────
MIN_ENTRY_PRICE = 0.05     # reject phantom quotes below this

# ── Fees ──────────────────────────────────────────────────────
POLY_CRYPTO_FEE_RATE = 0.072

# Simulated slippage for paper trading realism (0.5%)
PAPER_SLIPPAGE_BPS = 50

# ── Paths ─────────────────────────────────────────────────────
DATA_DIR = "data"
LOGS_DIR = "logs"
PAPER_TRADES_FILE = "logs/paper_trades.jsonl"
PERFORMANCE_LOG_FILE = "logs/v21_performance.jsonl"


# ═════════════════════════════════════════════════════════════
# TRADING MODE — paper / shadow / live
# ═════════════════════════════════════════════════════════════
TRADING_MODE = "paper"          # "paper", "shadow", "live"
SHADOW_TRADE_SIZE = 1.0         # $ per trade in shadow mode
# Position sizing: 5% of bankroll, capped at $100 max per trade
# This means the bot scales naturally up to $2,000 bankroll before the cap kicks in
# $100 stays within single-level liquidity and won't dominate market volume
MAX_TRADE_SIZE = 100.0           # hard cap on any single trade
MIN_BALANCE_TO_TRADE = 10.0     # minimum wallet balance to trade

# ── Safety Limits ─────────────────────────────────────────────
DAILY_LOSS_LIMIT = 15.0         # $ max daily loss before halt
MAX_CONSECUTIVE_LOSSES = 5      # halt after N consecutive losses
MAX_CONCURRENT_POSITIONS = 3    # max open positions at once
KILL_SWITCH = False             # emergency stop (also file-based)
KILL_SWITCH_FILE = os.path.join(DATA_DIR, "KILL_SWITCH")

# ── Logging Paths ─────────────────────────────────────────────
TRADE_LOG = os.path.join(LOGS_DIR, "trades.jsonl")
ERROR_LOG = os.path.join(LOGS_DIR, "errors.jsonl")
ALERTS_LOG = os.path.join(LOGS_DIR, "alerts.jsonl")
STATE_FILE = os.path.join(DATA_DIR, "v21_state.json")

# ── Polymarket CLOB ───────────────────────────────────────────
CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137

# ── Wallet (from .env) ───────────────────────────────────────
PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER_ADDRESS", "")
SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "2"))
API_KEY = os.getenv("POLYMARKET_API_KEY", "")
API_SECRET = os.getenv("POLYMARKET_API_SECRET", "")
API_PASSPHRASE = os.getenv("POLYMARKET_PASSPHRASE", "")

# ── Dashboard ─────────────────────────────────────────────────
DASHBOARD_PORT = 8085
