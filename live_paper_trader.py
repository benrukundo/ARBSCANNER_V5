"""V2.1 Frequency-Optimized Paper Trader.

KEY IMPROVEMENTS over V1 (port 8081):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. PREDICTIVE DISCOVERY  — slugs computed from timestamp boundaries,
   market details pre-fetched 45s BEFORE the window opens.
2. WEBSOCKET ODDS        — real-time CLOB price feed via WebSocket,
   strategy evaluated on every price tick (not 5s polling).
3. CONCURRENT TRACKING   — ALL active 5m/15m markets tracked simultaneously,
   multiple positions allowed.
4. STAGED ENTRY          — wider 300s window with price-tiered thresholds:
   300s → max 0.60 | 180s → max 0.80 | 120s → max 0.95.
5. PERFORMANCE LOGGING   — every market window logged to v21_performance.jsonl
   for gap analysis.
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from redeem import run_redeem_cycle, get_redeem_status

try:
    import fcntl  # POSIX-only; trader runs on Linux VM
except ImportError:
    fcntl = None

try:
    import websockets
except ImportError:
    websockets = None

from config import (
    GAMMA_API, CLOB_API,
    FETCH_TIMEOUT, PAPER_INITIAL_BANKROLL, PAPER_CHECK_INTERVAL,
    DEFAULT_STRATEGY_PARAMS, POLY_CRYPTO_FEE_RATE,
    LOGS_DIR, PAPER_TRADES_FILE, PERFORMANCE_LOG_FILE,
    PREDICTIVE_PREFETCH_SECS, PREDICTIVE_CHECK_INTERVAL,
    STAGED_ENTRY, MIN_ENTRY_PRICE, VERSION, PRICE_BUFFER,
    WS_CLOB_URL, WS_RECONNECT_DELAY, WS_FALLBACK_POLL_INTERVAL, POLYMARKET_WS,
    SLUG_DURATION_MAP,
    CRYPTO_CONFIGS, ACTIVE_CRYPTOS, TRADING_MODE, SHADOW_TRADE_SIZE, MAX_TRADE_SIZE, MIN_TRADE_SIZE, TRADE_LOG, STATE_FILE,
    )
from strategy import should_enter_staged, calculate_fee, calculate_payout
from config import (
    KELLY_FRACTION, MIN_BET_SIZE, POSITION_SIZE_MODE, FIXED_POSITION_SIZE,
    ENABLE_DYNAMIC_EXIT, TAKE_PROFIT_PRICE, STOP_LOSS_PRICE,
    POSITION_MONITOR_INTERVAL, HEDGE_INSTEAD_OF_STOP, MAX_HEDGE_TOTAL_COST,
    POLY_CRYPTO_FEE_RATE as POLY_FEE_RATE_V6,
    MIN_POSITION_AGE_FOR_EXIT, STOP_LOSS_CONSECUTIVE_SCANS,
    MIN_EST_PROB, MAX_TIME_REMAINING_ENTRY,
    MIN_DISTANCE_PCT, DEFAULT_MIN_DISTANCE_PCT,
    KELLY_BOUNDARY_MULTIPLIER, KELLY_BOUNDARY_FRACTION,
    NY_SESSION_START_UTC, NY_SESSION_END_UTC,
    NY_MIN_ENTRY_PRICE, NY_KELLY_FRACTION, NY_MAX_TRADE_SIZE,
    NY_SESSION_LOSS_LIMIT,
    TRUSTED_PTB_SOURCES, STRIKE_GATE_MODE,
    STRIKE_GATE_SIZE_FACTOR, STRIKE_GATE_MIN_EDGE_BUMP,
)
from trading_client import PolymarketTrader, append_jsonl
from safety import SafetyManager

logger = logging.getLogger("v6.trader")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)


def _extract_price_from_question(question: str) -> float | None:
    """Extract the strike price from a market question.

    E.g. 'Will BTC be above or below $75,100.00 at 10:00 PM?'
    returns 75100.0
    """
    m = re.search(r'\$([0-9,]+(?:\.\d+)?)', question)
    if m:
        try:
            return float(m.group(1).replace(',', ''))
        except ValueError:
            pass
    return None


def _coerce_float(value) -> float | None:
    """Best-effort float conversion for API metadata values."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _parse_jsonish(value):
    """Decode JSON strings while leaving other types unchanged."""
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{") or text.startswith("["):
            try:
                return json.loads(text)
            except Exception:
                return value
    return value


def _acquire_singleton_lock(mode: str) -> "object | None":
    """Acquire an exclusive flock on data/trader_<mode>.lock.

    The bot is configured to talk to a single Polymarket wallet. Two
    concurrent processes (e.g. live_paper_trader.py and dashboard.py) each
    instantiating FrequencyOptimizedTrader will independently submit
    orders, producing the duplicate trades seen on 2026-04-28. The lock
    survives only as long as the holding process; if the previous owner
    dies the kernel releases it automatically.

    Returns the open file handle (must be kept alive) or exits the
    process if another instance already holds the lock.
    """
    if fcntl is None:
        # Non-POSIX (dev box on Windows). Skip the check rather than crash.
        return None
    from config import DATA_DIR
    os.makedirs(DATA_DIR, exist_ok=True)
    lock_path = os.path.join(DATA_DIR, f"trader_{mode}.lock")
    # Open without truncating so we can read the holder's PID on failure.
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    fh = os.fdopen(fd, "r+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        try:
            fh.seek(0)
            existing = fh.read().strip() or "?"
        except Exception:
            existing = "?"
        fh.close()
        logger.critical(
            "Another trader instance (mode=%s) already holds %s (pid=%s). "
            "Refusing to start — this prevents duplicate orders on the same "
            "wallet. Stop the other instance first.",
            mode, lock_path, existing,
        )
        sys.exit(1)
    # We hold the lock — overwrite with our PID.
    fh.seek(0)
    fh.truncate()
    fh.write(str(os.getpid()))
    fh.flush()
    logger.info("Singleton lock acquired: %s (pid=%d)", lock_path, os.getpid())
    return fh




# ═══════════════════════════════════════════════════════════════
#  BINANCE MULTI-STREAM FEED — single WS for all crypto prices
# ═══════════════════════════════════════════════════════════════

class BinanceMultiFeed:
    """Single WebSocket connection to Binance combined stream for all cryptos."""

    def __init__(self, crypto_keys: list[str], on_price_callback):
        """
        crypto_keys: list of keys like ["btc", "eth", "sol", ...]
        on_price_callback: fn(crypto_key, price, timestamp_ms)
        """
        streams = []
        self._symbol_to_key = {}
        for key in crypto_keys:
            cfg = CRYPTO_CONFIGS[key]
            symbol = cfg["binance_symbol"].lower()
            streams.append(f"{symbol}@trade")
            self._symbol_to_key[symbol.upper()] = key

        self.url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)
        self.on_price = on_price_callback
        self._connected = False
        self._msg_count = 0
        self.latest_prices: dict[str, float] = {}
        self.latest_timestamps: dict[str, int] = {}

    async def connect(self):
        if websockets is None:
            logger.error("websockets not installed — BinanceMultiFeed disabled")
            return
        while True:
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=10) as ws:
                    self._connected = True
                    logger.info("[Binance Multi WS] Connected (%d streams)", len(self._symbol_to_key))
                    async for msg in ws:
                        data = json.loads(msg)
                        payload = data.get("data", data)
                        symbol = payload.get("s", "")
                        crypto_key = self._symbol_to_key.get(symbol)
                        if crypto_key and "p" in payload:
                            price = float(payload["p"])
                            ts = payload.get("T", int(time.time() * 1000))
                            self.latest_prices[crypto_key] = price
                            self.latest_timestamps[crypto_key] = ts
                            self._msg_count += 1
                            self.on_price(crypto_key, price, ts)
            except asyncio.CancelledError:
                logger.info("[Binance Multi WS] Shutting down")
                break
            except Exception as e:
                self._connected = False
                logger.warning("[Binance Multi WS] Disconnected: %s, reconnecting in 2s...", e)
                await asyncio.sleep(2)

    def get_price(self, crypto_key: str) -> float:
        return self.latest_prices.get(crypto_key, 0.0)

    def age_ms(self, crypto_key: str) -> float:
        ts = self.latest_timestamps.get(crypto_key)
        if ts is None:
            return 99999
        return time.time() * 1000 - ts


class ChainlinkPriceFeed:
    """Polymarket RTDS WebSocket feed for Chainlink crypto prices."""

    PING_INTERVAL = 20

    def __init__(self, crypto_keys: list[str], on_price_callback):
        self.url = POLYMARKET_WS
        self.on_price = on_price_callback
        self._connected = False
        self._msg_count = 0
        self.latest_prices: dict[str, float] = {}
        self.latest_timestamps: dict[str, int] = {}
        self._symbol_to_key: dict[str, str] = {}

        for key in crypto_keys:
            symbol = str(CRYPTO_CONFIGS[key].get("chainlink_ws_symbol", "")).lower()
            if symbol:
                self._symbol_to_key[symbol] = key

    async def connect(self):
        if websockets is None:
            logger.error("websockets not installed — ChainlinkPriceFeed disabled")
            return
        while True:
            try:
                async with websockets.connect(
                    self.url,
                    additional_headers={
                        "Origin": "https://polymarket.com",
                        "User-Agent": "Mozilla/5.0",
                    },
                ) as ws:
                    self._connected = True
                    logger.info("[Chainlink RTDS] Connected")
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "subscriptions": [{
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": "",
                        }],
                    }))
                    logger.info("[Chainlink RTDS] Subscribed to crypto_prices_chainlink")

                    last_ping = time.time()
                    while True:
                        now = time.time()
                        if now - last_ping >= self.PING_INTERVAL:
                            await ws.send("PING")
                            last_ping = now

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        except asyncio.TimeoutError:
                            continue

                        if isinstance(raw, bytes) or raw in ("PONG", "pong", ""):
                            continue

                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        if msg.get("topic") != "crypto_prices_chainlink":
                            continue

                        payload = msg.get("payload", {})
                        if not isinstance(payload, dict):
                            continue

                        symbol = str(payload.get("symbol", "")).lower()
                        crypto_key = self._symbol_to_key.get(symbol)
                        if crypto_key is None:
                            continue

                        value = payload.get("value")
                        if value is None:
                            continue

                        ts = int(payload.get("timestamp", int(time.time() * 1000)))
                        price = float(value)
                        self.latest_prices[crypto_key] = price
                        self.latest_timestamps[crypto_key] = ts
                        self._msg_count += 1
                        self.on_price(crypto_key, price, ts)

            except asyncio.CancelledError:
                logger.info("[Chainlink RTDS] Shutting down")
                break
            except Exception as e:
                self._connected = False
                logger.warning("[Chainlink RTDS] Disconnected: %s, reconnecting in 3s...", e)
                await asyncio.sleep(3)

    def get_price(self, crypto_key: str) -> float:
        return self.latest_prices.get(crypto_key, 0.0)

    def age_ms(self, crypto_key: str) -> float:
        ts = self.latest_timestamps.get(crypto_key)
        if ts is None:
            return 99999
        return time.time() * 1000 - ts

# ═══════════════════════════════════════════════════════════════
#  PREDICTIVE MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════════

class PredictiveDiscovery:
    """Predicts upcoming market slugs from fixed timestamp boundaries."""

    def __init__(self):
        self.prefetched: set[str] = set()   # slugs already pre-fetched
        self.predicted_count: int = 0
        self.fallback_count: int = 0
        self.discovery_times: list[float] = []  # ms to discover each market

    def get_upcoming_slugs(self, now: int) -> list[dict]:
        """Return candidate slugs for current + upcoming windows.

        For each active crypto and each timeframe:
          - Current window: boundary <= now < boundary + duration
          - Next window:    if within PREFETCH seconds of next boundary
          - Also 1 previous window (might still be open)
        """
        candidates = []
        for crypto_key in ACTIVE_CRYPTOS:
            cfg = CRYPTO_CONFIGS[crypto_key]
            prefix = cfg["slug_prefix"]
            for tf_label in cfg.get("timeframes", SLUG_DURATION_MAP.keys()):
                duration = SLUG_DURATION_MAP.get(tf_label, 300)
                current_boundary = now - (now % duration)
                next_boundary = current_boundary + duration
                prev_boundary = current_boundary - duration

                # Current window
                slug_cur = f"{prefix}-{tf_label}-{current_boundary}"
                if slug_cur not in self.prefetched:
                    candidates.append({
                        "slug": slug_cur,
                        "start_ts": current_boundary,
                        "end_ts": current_boundary + duration,
                        "tf": tf_label,
                        "crypto_key": crypto_key,
                        "source": "predicted",
                    })

                # Next window (pre-fetch if close)
                secs_until_next = next_boundary - now
                if secs_until_next <= PREDICTIVE_PREFETCH_SECS:
                    slug_next = f"{prefix}-{tf_label}-{next_boundary}"
                    if slug_next not in self.prefetched:
                        candidates.append({
                            "slug": slug_next,
                            "start_ts": next_boundary,
                            "end_ts": next_boundary + duration,
                            "tf": tf_label,
                            "crypto_key": crypto_key,
                            "source": "predicted-prefetch",
                        })

                # Previous window (might still have time remaining)
                slug_prev = f"{prefix}-{tf_label}-{prev_boundary}"
                end_prev = prev_boundary + duration
                if end_prev > now and slug_prev not in self.prefetched:
                    candidates.append({
                        "slug": slug_prev,
                        "start_ts": prev_boundary,
                        "end_ts": end_prev,
                        "tf": tf_label,
                        "crypto_key": crypto_key,
                        "source": "predicted-prev",
                    })

        return candidates

    def mark_prefetched(self, slug: str):
        self.prefetched.add(slug)

    def record_discovery(self, delay_ms: float, source: str):
        self.discovery_times.append(delay_ms)
        if source.startswith("predicted"):
            self.predicted_count += 1
        else:
            self.fallback_count += 1


# ═══════════════════════════════════════════════════════════════
#  CLOB WEBSOCKET — REAL-TIME ODDS MONITORING
# ═══════════════════════════════════════════════════════════════

class CLOBWebSocket:
    """Maintains WebSocket connection to Polymarket CLOB for real-time prices."""

    def __init__(self, on_price_update=None):
        self.prices: dict[str, float] = {}          # token_id → best ask price
        self.best_bids: dict[str, float] = {}       # token_id → best bid price
        self.ask_sizes: dict[str, float] = {}        # token_id → total $ available at asks
        self.ask_levels: dict[str, list] = {}        # token_id → sorted [(price, size), ...] top 20
        self.last_update: dict[str, float] = {}     # token_id → timestamp
        self._subscribed_tokens: set[str] = set()
        self._pending_tokens: set[str] = set()      # tokens to add
        self._ws = None
        self._connected = False
        self._msg_count = 0
        self._on_price_update = on_price_update     # callback(token_id, price)
        self._running = True

    def add_tokens(self, token_ids: list[str]):
        """Queue tokens for subscription (thread-safe via asyncio)."""
        for tid in token_ids:
            if tid and tid not in self._subscribed_tokens:
                self._pending_tokens.add(tid)

    def get_price(self, token_id: str) -> float | None:
        """Get cached best ask price for a token. None if no data."""
        return self.prices.get(token_id)

    def get_best_bid(self, token_id: str) -> float | None:
        """Get cached best bid price for a token. None if no data."""
        return self.best_bids.get(token_id)

    def get_best_bid_or_estimate(self, token_id: str, other_token_id: str = None) -> float | None:
        """Get best bid for token. Falls back to estimating from opposite token's ask.
        
        For binary markets: bid(A) ≈ 1 - ask(B), minus a small spread adjustment.
        This is needed because WS book events often don't include bid data.
        """
        # Try direct WS best_bid first
        bid = self.best_bids.get(token_id)
        if bid and bid > 0:
            return bid
        # Fallback: estimate from opposite token's best ask
        if other_token_id:
            other_ask = self.prices.get(other_token_id)
            if other_ask and 0 < other_ask < 1:
                # In binary market: bid(A) ≈ 1 - ask(B)
                # Apply small spread discount (0.5 cents) to be conservative
                estimated_bid = 1.0 - other_ask - 0.005
                if estimated_bid > 0:
                    return round(estimated_bid, 4)
        return None

    def get_ask_liquidity(self, token_id: str) -> float:
        """Get total $ liquidity available on ask side. 0 if unknown."""
        return self.ask_sizes.get(token_id, 0.0)

    def get_available_liquidity(self, token_id: str, max_price: float) -> tuple:
        """Returns (total_shares, total_usd) available on ask side at or below max_price.
        Uses cached ask levels from WS book events."""
        levels = self.ask_levels.get(token_id, [])
        total_shares = 0.0
        total_usd = 0.0
        for price, size in levels:
            if price <= max_price:
                total_shares += size
                total_usd += price * size
        return total_shares, total_usd

    def price_age_ms(self, token_id: str) -> float:
        """Milliseconds since last price update for this token."""
        ts = self.last_update.get(token_id, 0)
        return (time.time() - ts) * 1000 if ts > 0 else float("inf")

    async def run(self):
        """Main WebSocket loop with auto-reconnect."""
        if websockets is None:
            logger.warning("websockets not installed — WS disabled, REST-only mode")
            return

        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                logger.warning(f"CLOB WS error: {e}")
            self._connected = False
            if self._running:
                await asyncio.sleep(WS_RECONNECT_DELAY)

    async def _connect_and_listen(self):
        """Connect, subscribe, and process messages."""
        logger.info(f"CLOB WS connecting to {WS_CLOB_URL}...")
        async with websockets.connect(
            WS_CLOB_URL,
            ping_interval=20,
            ping_timeout=10,
            open_timeout=10,
        ) as ws:
            self._ws = ws
            self._connected = True
            logger.info("CLOB WS connected")

            # Subscribe to any already-known tokens
            all_tokens = list(self._subscribed_tokens | self._pending_tokens)
            if all_tokens:
                await self._subscribe(ws, all_tokens)
                self._subscribed_tokens.update(self._pending_tokens)
                self._pending_tokens.clear()

            while self._running:
                # Check for new tokens to subscribe
                if self._pending_tokens:
                    new_tokens = list(self._pending_tokens)
                    await self._subscribe(ws, new_tokens)
                    self._subscribed_tokens.update(self._pending_tokens)
                    self._pending_tokens.clear()

                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    self._process_message(raw)
                except asyncio.TimeoutError:
                    # No message in 30s — still alive via ping/pong
                    continue

    async def _subscribe(self, ws, token_ids: list[str]):
        """Send subscription message for tokens."""
        msg = {
            "auth": {},
            "type": "market",
            "assets_ids": token_ids,
        }
        await ws.send(json.dumps(msg))
        logger.info(f"CLOB WS subscribed to {len(token_ids)} tokens")

    def _process_message(self, raw: str):
        """Parse CLOB WS message and update cached prices."""
        self._msg_count += 1
        try:
            data = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            return

        # Messages can be a list of events or a single event
        events = data if isinstance(data, list) else [data]

        for event in events:
            if not isinstance(event, dict):
                continue

            asset_id = event.get("asset_id", "")
            event_type = event.get("event_type", "")

            if event_type == "book" and asset_id:
                # Full or incremental book update
                bids = event.get("bids", [])
                asks = event.get("asks", [])

                # Best ask = lowest sell price = what we'd pay to buy
                if asks:
                    try:
                        # Filter to rows with both valid price and positive size.
                        # Previously used .get("price", 999) which could inject a
                        # phantom $999 ask if price was missing.
                        valid_asks = []
                        for a in asks:
                            p_raw = a.get("price")
                            s_raw = a.get("size")
                            if p_raw is None or s_raw is None:
                                continue
                            try:
                                s = float(s_raw)
                                if s > 0:
                                    valid_asks.append(float(p_raw))
                            except (ValueError, TypeError):
                                continue
                        if valid_asks:
                            best_ask = min(valid_asks)
                            self.prices[asset_id] = best_ask
                            self.last_update[asset_id] = time.time()
                            if self._on_price_update:
                                self._on_price_update(asset_id, best_ask)
                    except (ValueError, TypeError):
                        pass

                # Calculate total ask-side liquidity ($ available to buy)
                if asks:
                    try:
                        total_ask_usd = sum(
                            float(a.get("price", 0)) * float(a.get("size", 0))
                            for a in asks if float(a.get("size", 0)) > 0
                        )
                        self.ask_sizes[asset_id] = total_ask_usd
                        # Cache sorted ask levels (top 20) for depth queries
                        sorted_asks = sorted(
                            [(float(a.get("price", 0)), float(a.get("size", 0)))
                             for a in asks if float(a.get("size", 0)) > 0],
                            key=lambda x: x[0]
                        )[:20]
                        self.ask_levels[asset_id] = sorted_asks
                    except (ValueError, TypeError):
                        pass

                # Best bid = highest buy price (for logging/spread calculation)
                if bids:
                    try:
                        best_bid = max(float(b.get("price", 0)) for b in bids if float(b.get("size", 0)) > 0)
                        self.best_bids[asset_id] = best_bid
                    except (ValueError, TypeError):
                        pass

            elif event_type == "last_trade_price" and asset_id:
                # Do NOT overwrite best_ask with last trade price.
                # last_trade_price can be at bid or any filled level,
                # which would cause FAK buy orders to be placed below
                # the actual ask, resulting in zero fills.
                pass

            elif event_type == "price_change" and asset_id:
                # Do NOT overwrite best_ask with price_change events.
                # Only book events provide reliable bid/ask separation.
                pass

    def stop(self):
        self._running = False


# ═══════════════════════════════════════════════════════════════
#  PERFORMANCE TRACKER
# ═══════════════════════════════════════════════════════════════

class PerformanceTracker:
    """Logs detailed metrics for each market window."""

    def __init__(self):
        os.makedirs(LOGS_DIR, exist_ok=True)

    def log_window(self, data: dict):
        """Append a market window record to the performance log."""
        with open(PERFORMANCE_LOG_FILE, "a") as f:
            f.write(json.dumps(data) + "\n")



# ═══════════════════════════════════════════════════════════════
#  ROLLING VOLATILITY CALCULATOR
# ═══════════════════════════════════════════════════════════════

class RollingVolatility:
    """Calculates rolling 1-second price volatility from Binance trade feed."""

    def __init__(self, window_seconds: int = 300):
        self.window = window_seconds
        self._prices: dict[str, list[tuple[float, float]]] = {}  # crypto_key -> [(timestamp, price), ...]
        self._vol_cache: dict[str, float] = {}  # crypto_key -> current vol

    def add_price(self, crypto_key: str, price: float, timestamp: float):
        """Called on every Binance trade tick."""
        if crypto_key not in self._prices:
            self._prices[crypto_key] = []
        buf = self._prices[crypto_key]
        buf.append((timestamp, price))
        # Trim to window
        cutoff = timestamp - self.window
        while buf and buf[0][0] < cutoff:
            buf.pop(0)
        # Recalculate vol if enough data (at least 30 samples)
        if len(buf) >= 30:
            self._recalculate(crypto_key)

    def _recalculate(self, crypto_key: str):
        """Calculate standard deviation of 1-second returns."""
        import math
        buf = self._prices[crypto_key]
        # Bucket into 1-second intervals, take last price per second
        second_prices = {}
        for ts, price in buf:
            sec = int(ts)
            second_prices[sec] = price
        sorted_secs = sorted(second_prices.keys())
        if len(sorted_secs) < 10:
            return
        returns = []
        for i in range(1, len(sorted_secs)):
            if sorted_secs[i] - sorted_secs[i-1] == 1:  # consecutive seconds only
                ret = second_prices[sorted_secs[i]] - second_prices[sorted_secs[i-1]]
                returns.append(ret)
        if len(returns) >= 10:
            mean = sum(returns) / len(returns)
            variance = sum((r - mean) ** 2 for r in returns) / len(returns)
            vol = math.sqrt(variance)
            self._vol_cache[crypto_key] = max(vol, 0.0001)  # floor to prevent division by zero

    def get_vol(self, crypto_key: str, fallback: float = 2.5) -> float:
        """Get current rolling volatility. Falls back to config default if insufficient data."""
        return self._vol_cache.get(crypto_key, fallback)


# ═══════════════════════════════════════════════════════════════
#  FREQUENCY-OPTIMIZED PAPER TRADER
# ═══════════════════════════════════════════════════════════════

class FrequencyOptimizedTrader:
    """V2.1 Paper Trader — maximum trade frequency through predictive
    discovery, WebSocket monitoring, and staged entry."""

    def __init__(self, params: dict | None = None, bankroll: float = PAPER_INITIAL_BANKROLL):
        self.params = {**DEFAULT_STRATEGY_PARAMS, **(params or {})}
        self.bankroll = bankroll
        self.initial_bankroll = bankroll
        self._ny_session_pnl: float = 0.0
        self._ny_session_date: str = ''
        self._ny_session_halted: bool = False
        self.trades: list[dict] = []
        self.is_running = False
        self.mode = TRADING_MODE  # "paper", "shadow", "live"

        # Real trading sub-systems
        self.trader = PolymarketTrader()
        self.safety = SafetyManager(trader=self.trader)
        self.wallet_balance: float = 0.0
        self.real_pnl_total: float = 0.0
        self.real_trade_count: int = 0

        # Live positions (for shadow/live mode)
        self._live_positions: dict = {}  # slug -> live position info

        # Fix 2: Per-market locks to prevent double-entry race conditions
        self._market_locks: dict[str, asyncio.Lock] = {}

        # Fix 3: Rolling volatility calculator
        self.rolling_vol = RollingVolatility(window_seconds=300)


        # Sub-systems
        self.discovery = PredictiveDiscovery()
        self.clob_ws = CLOBWebSocket(on_price_update=self._on_ws_price)
        self.perf = PerformanceTracker()

        # ── State ──
        self.active_markets: dict[str, dict] = {}   # slug → market info
        self.settled_slugs: set[str] = set()
        self._btc_price: float = 0.0                # backward compat
        self._crypto_prices: dict[str, float] = {}   # crypto_key -> price
        self._chainlink_prices: dict[str, float] = {}
        self._minute_open_prices: dict[str, dict[int, float]] = {}  # crypto_key -> {minute_ts: open_price}
        self._chainlink_minute_open_prices: dict[str, dict[int, float]] = {}
        self._last_btc_update: float = 0.0
        self.binance_feed: BinanceMultiFeed | None = None
        self.chainlink_feed: ChainlinkPriceFeed | None = None
        self._ws_eval_queue: asyncio.Queue | None = None  # token_id triggers
        self._token_to_slug: dict[str, str] = {}    # token_id → slug

        # ── Stats ──
        self._markets_discovered = 0
        self._markets_traded = 0
        self._markets_qualified = 0
        self._total_evals = 0
        self._ws_evals = 0
        self._rest_evals = 0
        self._started_at = 0
        self._loop_heartbeats: dict[str, float] = {}
        self._loop_restarts: dict[str, float] = {}
        # Map of loop_name -> asyncio.Task. Used by the watchdog so it can
        # cancel a stalled task before launching its replacement; without
        # this the recovery path leaks duplicate loops.
        self._loop_tasks: dict[str, asyncio.Task] = {}

        self._load_trades()
        self._load_v21_state()

    # ── Persistence ───────────────────────────────────────────

    def _load_trades(self):
        path = Path(PAPER_TRADES_FILE)
        if path.exists():
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.trades.append(json.loads(line))
            if self.trades:
                self.bankroll = self.trades[-1].get("bankroll_after", self.bankroll)
                logger.info(f"Loaded {len(self.trades)} trades, bankroll=${self.bankroll:.2f}")

    def _save_trade(self, trade: dict):
        os.makedirs(LOGS_DIR, exist_ok=True)
        with open(PAPER_TRADES_FILE, "a") as f:
            f.write(json.dumps(trade) + "\n")

    def _rewrite_trades_file(self):
        """Atomically rewrite paper_trades.jsonl from self.trades.

        Called when a previously logged early-exit row needs its
        actual_outcome backfilled at settlement. Writes via tmp + rename so
        a crash mid-write cannot truncate the file.
        """
        try:
            os.makedirs(LOGS_DIR, exist_ok=True)
            tmp_path = PAPER_TRADES_FILE + ".tmp"
            with open(tmp_path, "w") as f:
                for t in self.trades:
                    f.write(json.dumps(t) + "\n")
            os.replace(tmp_path, PAPER_TRADES_FILE)
        except Exception as e:
            logger.error("Failed to rewrite trades file: %s", e)

    # ── WS Price Callback ─────────────────────────────────────

    def _on_ws_price(self, token_id: str, price: float):
        """Called by CLOB WS on each price tick — queue evaluation."""
        if self._ws_eval_queue and not self._ws_eval_queue.full():
            try:
                self._ws_eval_queue.put_nowait(token_id)
            except asyncio.QueueFull:
                pass

    def _beat(self, loop_name: str):
        self._loop_heartbeats[loop_name] = time.time()

    def _spawn_loop_recovery(self, loop_name: str):
        now = time.time()
        last_restart = self._loop_restarts.get(loop_name, 0)
        if now - last_restart < 60:
            return
        self._loop_restarts[loop_name] = now

        # Cancel the stalled task before spawning a replacement. Without
        # this, the watchdog accumulates parallel loops (discovery,
        # rest-eval) that race on self.active_markets and produce
        # duplicate trade attempts.
        old_task = self._loop_tasks.get(loop_name)
        if old_task is not None and not old_task.done():
            logger.warning(
                "Watchdog cancelling stalled task '%s' before spawning replacement",
                loop_name,
            )
            old_task.cancel()

        logger.warning("Watchdog restarting stalled loop: %s", loop_name)
        if loop_name == "discovery":
            new_task = asyncio.create_task(
                self._discovery_loop(), name=f"discovery-recovery-{int(now)}"
            )
        elif loop_name == "settlement":
            new_task = asyncio.create_task(
                self._settlement_loop(), name=f"settlement-recovery-{int(now)}"
            )
        elif loop_name == "rest-eval":
            new_task = asyncio.create_task(
                self._rest_evaluation_loop(), name=f"rest-eval-recovery-{int(now)}"
            )
        elif loop_name == "btc-price":
            new_task = asyncio.create_task(
                self._btc_price_loop(), name=f"btc-price-recovery-{int(now)}"
            )
        else:
            return
        self._loop_tasks[loop_name] = new_task

    async def _watchdog_loop(self):
        """Restart critical loops if their heartbeats stop advancing."""
        thresholds = {
            "discovery": 120,
            "settlement": 120,
            "rest-eval": 120,
            "btc-price": 15,
        }
        while self.is_running:
            await asyncio.sleep(15)
            if self._get_crypto_price("btc") <= 0:
                continue
            now = time.time()
            for loop_name, threshold in thresholds.items():
                last = self._loop_heartbeats.get(loop_name, 0)
                if last and now - last > threshold:
                    self._spawn_loop_recovery(loop_name)


    def _on_binance_price(self, crypto_key: str, price: float, ts_ms: int):
        """Called by BinanceMultiFeed on each price tick."""
        self._crypto_prices[crypto_key] = price
        ts_sec = int(ts_ms / 1000)
        minute_ts = ts_sec - (ts_sec % 60)
        opens = self._minute_open_prices.setdefault(crypto_key, {})
        opens.setdefault(minute_ts, price)
        cutoff = minute_ts - 3600
        stale_minutes = [m for m in opens if m < cutoff]
        for m in stale_minutes:
            del opens[m]
        # Feed rolling volatility calculator
        self.rolling_vol.add_price(crypto_key, price, time.time())
        if crypto_key == "btc":
            self._btc_price = price           # backward compat
            self._last_btc_update = time.time()

    def _on_chainlink_price(self, crypto_key: str, price: float, ts_ms: int):
        """Called by ChainlinkPriceFeed on each price tick."""
        self._chainlink_prices[crypto_key] = price
        ts_sec = int(ts_ms / 1000)
        minute_ts = ts_sec - (ts_sec % 60)
        opens = self._chainlink_minute_open_prices.setdefault(crypto_key, {})
        opens.setdefault(minute_ts, price)
        cutoff = minute_ts - 3600
        stale_minutes = [m for m in opens if m < cutoff]
        for m in stale_minutes:
            del opens[m]

    def _get_crypto_price(self, crypto_key: str) -> float:
        """Get current price for a crypto asset."""
        return self._crypto_prices.get(crypto_key, 0.0)

    def _get_cached_minute_open(self, crypto_key: str, start_ts: int) -> float | None:
        """Return cached Binance minute open captured from the live trade feed."""
        if start_ts <= 0:
            return None
        return self._minute_open_prices.get(crypto_key, {}).get(start_ts)

    def _get_cached_chainlink_minute_open(self, crypto_key: str, start_ts: int) -> float | None:
        """Return cached Chainlink minute open captured from the RTDS feed."""
        if start_ts <= 0:
            return None
        return self._chainlink_minute_open_prices.get(crypto_key, {}).get(start_ts)

    def _get_market_lock(self, slug: str) -> asyncio.Lock:
        """Get or create per-market lock to prevent double-entry race condition."""
        if slug not in self._market_locks:
            self._market_locks[slug] = asyncio.Lock()
        return self._market_locks[slug]

    # ── Data Fetchers ─────────────────────────────────────────

    async def _get_btc_price(self, client: httpx.AsyncClient) -> float:
        try:
            resp = await client.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": "BTCUSDT"},
            )
            resp.raise_for_status()
            price = float(resp.json()["price"])
            self._btc_price = price
            self._last_btc_update = time.time()
            return price
        except Exception as e:
            logger.debug(f"BTC price error: {e}")
            return self._btc_price

    async def _get_historical_price(
        self, client: httpx.AsyncClient, crypto_key: str, target_ts: int
    ) -> float | None:
        """Fetch the Binance trade price at or nearest to target_ts (unix seconds).

        Used for settlement-time outcome inference when Polymarket resolution is
        delayed. Previously we fell back to the CURRENT price, which for 5m
        markets could drift meaningfully from the actual close price and produce
        paper P&L that disagrees with real resolution.

        Returns None on failure; caller falls back to live price as last resort.
        """
        cfg = CRYPTO_CONFIGS.get(crypto_key)
        if not cfg:
            return None
        symbol = cfg.get("binance_symbol")
        if not symbol:
            return None
        start_ms = target_ts * 1000
        end_ms = start_ms + 60_000   # 1-minute window starting at target
        try:
            resp = await client.get(
                "https://api.binance.com/api/v3/klines",
                params={
                    "symbol": symbol,
                    "interval": "1m",
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": 1,
                },
                timeout=5,
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return None
            # Kline format: [open_ts, open, high, low, close, volume, close_ts, ...]
            # Use the OPEN of the minute containing target_ts as the best
            # approximation of the close price at market resolution.
            kline = data[0]
            return float(kline[1])
        except Exception as e:
            logger.debug(f"Historical price fetch failed for {crypto_key}@{target_ts}: {e}")
            return None

    async def _get_share_prices_rest(
        self, client: httpx.AsyncClient, up_token: str, down_token: str
    ) -> tuple[float, float]:
        """REST fallback for share prices (uses best ask via side=buy)."""
        up_price, down_price = 0.5, 0.5
        for tid, is_up in [(up_token, True), (down_token, False)]:
            if not tid:
                continue
            try:
                resp = await client.get(
                    f"{CLOB_API}/price",
                    params={"token_id": tid, "side": "buy"},
                )
                resp.raise_for_status()
                p = float(resp.json().get("price", 0.5))
                if is_up:
                    up_price = p
                else:
                    down_price = p
            except Exception:
                pass
        return up_price, down_price

    def _get_share_prices_ws(self, up_token: str, down_token: str) -> tuple[float | None, float | None]:
        """Get prices from WS cache. Returns None if stale/missing."""
        up = self.clob_ws.get_price(up_token) if up_token else None
        down = self.clob_ws.get_price(down_token) if down_token else None
        # Consider stale if > 10 seconds old
        if up_token and self.clob_ws.price_age_ms(up_token) > 10_000:
            up = None
        if down_token and self.clob_ws.price_age_ms(down_token) > 10_000:
            down = None
        return up, down

    def _extract_price_to_beat_from_gamma(self, payload, crypto_key: str = "btc") -> float | None:
        """Extract authoritative strike metadata from Gamma event/market payloads."""
        cfg = CRYPTO_CONFIGS.get(crypto_key, CRYPTO_CONFIGS["btc"])
        min_ptb = cfg.get("min_ptb", 1)
        keys = {"pricetobeat", "price_to_beat", "strikeprice", "strike", "targetprice"}
        stack = [_parse_jsonish(payload)]

        while stack:
            current = _parse_jsonish(stack.pop())
            if isinstance(current, dict):
                for key, value in current.items():
                    key_norm = str(key).replace("-", "").replace("_", "").lower()
                    if key_norm in keys:
                        coerced = _coerce_float(value)
                        if coerced and coerced > min_ptb:
                            return coerced
                    parsed_value = _parse_jsonish(value)
                    if isinstance(parsed_value, (dict, list)):
                        stack.append(parsed_value)
            elif isinstance(current, list):
                for item in current:
                    parsed_item = _parse_jsonish(item)
                    if isinstance(parsed_item, (dict, list)):
                        stack.append(parsed_item)

        return None

    async def _fetch_gamma_event(self, client: httpx.AsyncClient, slug: str) -> dict | None:
        """Fetch the Gamma event for a market slug."""
        try:
            resp = await client.get(f"{GAMMA_API}/events", params={"slug": slug})
            resp.raise_for_status()
            events = resp.json()
            if not events or not isinstance(events, list):
                return None
            return events[0]
        except Exception:
            return None

    async def _get_price_to_beat(
        self, client: httpx.AsyncClient, slug: str,
        question: str = "", crypto_key: str = "btc",
        start_ts: int = 0,
        market_details: dict | None = None,
    ) -> tuple[float, str]:
        """Get the strike price for a crypto up/down market.

        Priority: 1) Question text, 2) REST API, 3) Gamma metadata,
        4) Chainlink RTDS minute open, 5) Binance minute open,
        6) Binance historical open. Never use live price as strike.
        """
        cfg = CRYPTO_CONFIGS.get(crypto_key, CRYPTO_CONFIGS["btc"])
        min_ptb = cfg.get("min_ptb", 1)

        # 1) Parse from question text (most reliable)
        if question:
            parsed = _extract_price_from_question(question)
            if parsed and parsed > min_ptb:
                return parsed, "question"

        # 2) Try REST endpoint
        try:
            resp = await client.get(
                f"https://polymarket.com/api/equity/price-to-beat/{slug}",
            )
            resp.raise_for_status()
            ptb = float(resp.json().get("price", 0))
            if ptb > min_ptb:
                return ptb, "rest_api"
        except Exception:
            pass

        # 3) Try authoritative Gamma metadata if available.
        gamma_ptb = None
        if market_details:
            market_ptb_source = market_details.get("price_to_beat_source", "")
            if market_ptb_source == "gamma_metadata":
                gamma_ptb = _coerce_float(market_details.get("price_to_beat"))
                if gamma_ptb and gamma_ptb > min_ptb:
                    return gamma_ptb, market_ptb_source

        if gamma_ptb is None:
            event = await self._fetch_gamma_event(client, slug)
            if event:
                gamma_ptb = self._extract_price_to_beat_from_gamma(event, crypto_key)
                if gamma_ptb and gamma_ptb > min_ptb:
                    return gamma_ptb, "gamma_metadata"

        # 4) Prefer the cached Chainlink minute-open from Polymarket RTDS.
        cached_chainlink_open = self._get_cached_chainlink_minute_open(crypto_key, start_ts)
        if cached_chainlink_open and cached_chainlink_open > min_ptb:
            logger.info(
                f"  [{slug}] Using cached Chainlink minute open ${cached_chainlink_open:,.2f} "
                f"at ts={start_ts} as price_to_beat for {cfg['name']}"
            )
            return cached_chainlink_open, "chainlink_rtds_open"

        # 5) For "Up or Down" markets, fall back to the Binance minute open,
        #    then REST klines if the opening tick wasn't captured live.
        cached_open = self._get_cached_minute_open(crypto_key, start_ts)
        if cached_open and cached_open > min_ptb:
            logger.info(
                f"  [{slug}] Using cached Binance minute open ${cached_open:,.2f} "
                f"at ts={start_ts} as price_to_beat for {cfg['name']}"
            )
            return cached_open, "binance_minute_open"

        if start_ts > 0:
            hist = await self._get_historical_price(client, crypto_key, start_ts)
            if hist and hist > min_ptb:
                logger.info(
                    f"  [{slug}] Using Binance historical open ${hist:,.2f} "
                    f"at ts={start_ts} as price_to_beat for {cfg['name']}"
                )
                return hist, "binance_open"

        # 6) All sources failed — return 0 to signal "unknown strike".
        return 0, "unavailable"

    async def _check_resolution(self, client: httpx.AsyncClient, slug: str) -> str | None:
        try:
            event = await self._fetch_gamma_event(client, slug)
            if not event:
                return None
            markets = event.get("markets", [])
            if not markets:
                return None
            mkt = markets[0]
            outcome_prices_raw = mkt.get("outcomePrices", "[]")
            outcomes_raw = mkt.get("outcomes", "[]")
            outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            for i, p in enumerate(outcome_prices):
                if float(p) >= 0.99 and i < len(outcomes):
                    return outcomes[i]
            return None
        except Exception:
            return None

    async def _fetch_market_details(
        self, client: httpx.AsyncClient, slug: str, crypto_key: str = "btc"
    ) -> dict | None:
        """Fetch market details (token IDs, condition_id) from Gamma API."""
        fetch_start = time.time()
        try:
            event = await self._fetch_gamma_event(client, slug)
            if event is None:
                return None
            markets_list = event.get("markets", [])
            if not markets_list:
                return None

            mkt = markets_list[0]
            if mkt.get("closed") is True or str(mkt.get("closed", "")).lower() == "true":
                return None

            outcomes = json.loads(mkt.get("outcomes", "[]")) if isinstance(mkt.get("outcomes"), str) else mkt.get("outcomes", [])
            clob_tokens = json.loads(mkt.get("clobTokenIds", "[]")) if isinstance(mkt.get("clobTokenIds"), str) else mkt.get("clobTokenIds", [])

            up_idx, down_idx = 0, 1
            for i, o in enumerate(outcomes):
                if o.lower() in ("up", "yes"):
                    up_idx = i
                elif o.lower() in ("down", "no"):
                    down_idx = i

            up_token = clob_tokens[up_idx] if len(clob_tokens) > up_idx else None
            down_token = clob_tokens[down_idx] if len(clob_tokens) > down_idx else None

            fetch_ms = (time.time() - fetch_start) * 1000
            price_to_beat = self._extract_price_to_beat_from_gamma(event, crypto_key=crypto_key)
            return {
                "question": mkt.get("question", event.get("title", "")),
                "condition_id": mkt.get("conditionId", ""),
                "up_token_id": up_token,
                "down_token_id": down_token,
                "price_to_beat": price_to_beat,
                "price_to_beat_source": "gamma_metadata" if price_to_beat else "unknown",
                "fetch_ms": fetch_ms,
            }
        except Exception as e:
            logger.debug(f"Fetch failed for {slug}: {e}")
            return None

    # ── Main Run ──────────────────────────────────────────────


    async def _redeem_loop(self):
        """Periodically redeem resolved positions to recycle capital."""
        from config import (
            PRIVATE_KEY, FUNDER_ADDRESS, SIGNATURE_TYPE,
            BUILDER_API_KEY, BUILDER_SECRET, BUILDER_PASSPHRASE,
        )
        # Wait 30s on startup before first check
        await asyncio.sleep(30)

        while self.is_running:
            # Only run in shadow or live mode
            if self.mode not in ("shadow", "live"):
                await asyncio.sleep(120)
                continue

            # Skip if Builder keys not configured
            if not BUILDER_API_KEY or not BUILDER_SECRET or not BUILDER_PASSPHRASE:
                logger.debug("Builder API keys not configured, skipping redeem")
                await asyncio.sleep(300)
                continue

            try:
                result = await asyncio.to_thread(
                    run_redeem_cycle,
                    private_key=PRIVATE_KEY,
                    funder_address=FUNDER_ADDRESS,
                    signature_type=SIGNATURE_TYPE,
                    builder_api_key=BUILDER_API_KEY,
                    builder_secret=BUILDER_SECRET,
                    builder_passphrase=BUILDER_PASSPHRASE,
                )
                count = result.get("redeemed", 0)
                if count > 0:
                    logger.info("Auto-redeemed %d positions", count)
                    # Force balance refresh — _balance_cache is a (balance, timestamp) tuple
                    if hasattr(self, "trader") and self.trader:
                        self.trader._balance_cache = (0.0, 0)
            except Exception as e:
                logger.error("Redeem loop error: %s", e)

            await asyncio.sleep(120)  # Check every 2 minutes

    async def run(self):
        # Singleton lock: refuse to start if another process is already trading.
        # Two FrequencyOptimizedTrader instances on the same wallet produced
        # the duplicate orders observed on 2026-04-28.
        self._lock_fh = _acquire_singleton_lock(self.mode)

        self.is_running = True
        self._started_at = time.time()
        self._initial_bankroll = self.bankroll  # V6: track for drawdown calc
        self._ws_eval_queue = asyncio.Queue(maxsize=1000)

        logger.info(f"V6 Kelly+Hedge Trader starting | MODE={self.mode.upper()} | sizing={POSITION_SIZE_MODE} | exit={ENABLE_DYNAMIC_EXIT} | hedge={HEDGE_INSTEAD_OF_STOP}")
        logger.info(f"  Exit guards: MIN_POSITION_AGE={MIN_POSITION_AGE_FOR_EXIT}s CONSECUTIVE_SCANS={STOP_LOSS_CONSECUTIVE_SCANS} SL={STOP_LOSS_PRICE} TP={TAKE_PROFIT_PRICE}")
        logger.info(f"  Entry filters: MIN_EST_PROB={MIN_EST_PROB} MAX_TIME_REMAINING_ENTRY={MAX_TIME_REMAINING_ENTRY}s")
        logger.info(f"  Bankroll: ${self.bankroll:.2f}")
        logger.info(f"  Strategy: {self.params}")
        logger.info(f"  Staged entry: {STAGED_ENTRY}")

        # Reconcile safety state with actual live positions.
        # _load_state resets current_positions to 0 on restart; if we crashed
        # with open positions, safety counter must match reality to avoid
        # exceeding MAX_CONCURRENT_POSITIONS.
        # IMPORTANT: previous version called trader.get_open_orders() which
        # returns RESTING CLOB orders, not open positions. FAK orders that
        # filled disappear from get_orders, so a crash + restart with N
        # filled-but-unsettled positions would reconcile to 0, letting the
        # bot open N more on top. We now query data-api/positions which
        # returns the actual on-chain position set.
        if self.mode != "paper" and self.trader and self.trader.is_ready:
            try:
                from config import FUNDER_ADDRESS
                async with httpx.AsyncClient(timeout=10) as _c:
                    resp = await _c.get(
                        "https://data-api.polymarket.com/positions",
                        params={"user": FUNDER_ADDRESS, "sizeThreshold": 0},
                    )
                    resp.raise_for_status()
                    positions = resp.json() or []
                # Count only un-redeemed positions (size > 0). Resolved
                # markets without redemption still report size>0; that's
                # fine — they'll be redeemed by the auto-redeem loop and
                # cleared from the safety counter when settlement runs.
                open_count = sum(1 for p in positions if float(p.get("size", 0)) > 0)
                await self.safety.reconcile_positions(open_count)
            except Exception as e:
                logger.warning("Safety reconciliation failed: %s — assuming 0 open positions", e)
                await self.safety.reconcile_positions(0)

        # Start Binance multi-crypto feed
        self.binance_feed = BinanceMultiFeed(
            crypto_keys=ACTIVE_CRYPTOS,
            on_price_callback=self._on_binance_price,
        )
        self.chainlink_feed = ChainlinkPriceFeed(
            crypto_keys=ACTIVE_CRYPTOS,
            on_price_callback=self._on_chainlink_price,
        )

        # Track watchdog-managed loops by name so _spawn_loop_recovery can
        # cancel a stalled task before launching its replacement.
        named_tasks = {
            "discovery":        asyncio.create_task(self._discovery_loop(),         name="discovery"),
            "ws-eval":          asyncio.create_task(self._ws_evaluation_loop(),     name="ws-eval"),
            "rest-eval":        asyncio.create_task(self._rest_evaluation_loop(),   name="rest-eval"),
            "settlement":       asyncio.create_task(self._settlement_loop(),        name="settlement"),
            "btc-price":        asyncio.create_task(self._btc_price_loop(),         name="btc-price"),
            "binance-feed":     asyncio.create_task(self.binance_feed.connect(),    name="binance-multi-feed"),
            "chainlink-feed":   asyncio.create_task(self.chainlink_feed.connect(),  name="chainlink-feed"),
            "clob-ws":          asyncio.create_task(self.clob_ws.run(),             name="clob-ws"),
            "status":           asyncio.create_task(self._status_loop(),            name="status"),
            "daily-reset":      asyncio.create_task(self._daily_reset_loop(),       name="daily-reset"),
            "auto-redeem":      asyncio.create_task(self._redeem_loop(),            name="auto-redeem"),
            "position-monitor": asyncio.create_task(self._position_monitor_loop(),  name="position-monitor"),
            "watchdog":         asyncio.create_task(self._watchdog_loop(),          name="watchdog"),
        }
        self._loop_tasks = named_tasks
        tasks = list(named_tasks.values())

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Trader cancelled")
        finally:
            self.clob_ws.stop()
            self.is_running = False

    # ── Discovery Loop ────────────────────────────────────────

    async def _discovery_loop(self):
        """Predictive discovery: compute upcoming slugs and pre-fetch."""
        self._beat("discovery")
        # Wait for BTC price before starting discovery
        logger.info("Discovery waiting for price feeds...")
        while self.is_running and self._get_crypto_price("btc") <= 0:
            await asyncio.sleep(0.5)
            self._beat("discovery")
        logger.info(f"Discovery started, BTC=${self._get_crypto_price('btc'):,.2f}")
        logger.info(f"Active cryptos: {ACTIVE_CRYPTOS}")

        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT) as client:
            while self.is_running:
                self._beat("discovery")
                try:
                    now = int(time.time())
                    candidates = self.discovery.get_upcoming_slugs(now)

                    for cand in candidates:
                        slug = cand["slug"]
                        if slug in self.active_markets or slug in self.settled_slugs:
                            self.discovery.mark_prefetched(slug)
                            continue

                        # Already expired?
                        if cand["end_ts"] <= now:
                            self.discovery.mark_prefetched(slug)
                            continue

                        crypto_key = cand.get("crypto_key", "btc")
                        disc_start = time.time()
                        details = await self._fetch_market_details(
                            client, slug, crypto_key=crypto_key
                        )

                        if details is None:
                            # Market not yet on API — mark as tried, will retry
                            # (don't add to prefetched so we'll retry next cycle)
                            continue

                        disc_ms = (time.time() - disc_start) * 1000
                        self.discovery.record_discovery(disc_ms, cand["source"])

                        # Get price-to-beat from question text or API
                        cfg = CRYPTO_CONFIGS.get(crypto_key, CRYPTO_CONFIGS["btc"])
                        min_ptb = cfg.get("min_ptb", 1)

                        ptb, ptb_source = await self._get_price_to_beat(
                            client, slug, question=details["question"],
                            crypto_key=crypto_key,
                            start_ts=cand["start_ts"],
                            market_details=details,
                        )
                        if ptb <= min_ptb:
                            # Do not mark as prefetched yet. For prefetch windows,
                            # the market can exist before the opening strike is
                            # available. Retrying next cycle lets us pick it up
                            # once Gamma metadata or the Binance open becomes
                            # available at/after the boundary.
                            logger.warning(f"  [{slug}] No valid price_to_beat for {cfg['name']}, will retry")
                            continue

                        self.discovery.mark_prefetched(slug)

                        # Register market. Use setdefault so a concurrent
                        # discovery iteration (or a watchdog-spawned recovery
                        # loop) cannot overwrite an entry that already has
                        # info["position"] set — which would silently re-arm
                        # double-entry. setdefault is atomic at the asyncio
                        # level since dict assignment cannot be interleaved.
                        new_entry = {
                            "slug": slug,
                            "crypto_key": crypto_key,
                            "start_ts": cand["start_ts"],
                            "end_ts": cand["end_ts"],
                            "tf": cand["tf"],
                            "question": details["question"],
                            "condition_id": details["condition_id"],
                            "up_token_id": details["up_token_id"],
                            "down_token_id": details["down_token_id"],
                            "price_to_beat": ptb,
                            "price_to_beat_source": ptb_source,
                            "last_ptb_refresh_at": time.time(),
                            "position": None,
                            "settled": False,
                            "settled_at": 0,
                            "discovered_at": time.time(),
                            "discovery_source": cand["source"],
                            "discovery_delay_ms": disc_ms,
                            "first_qualified_at": None,
                            "best_qualifying_price": None,
                        }
                        existing = self.active_markets.setdefault(slug, new_entry)
                        if existing is not new_entry:
                            logger.debug(
                                "Discovery race resolved for %s: keeping existing entry", slug
                            )
                            continue
                        self._markets_discovered += 1

                        # Subscribe to WS for this market's tokens
                        tokens = []
                        if details["up_token_id"]:
                            tokens.append(details["up_token_id"])
                            self._token_to_slug[details["up_token_id"]] = slug
                        if details["down_token_id"]:
                            tokens.append(details["down_token_id"])
                            self._token_to_slug[details["down_token_id"]] = slug
                        if tokens:
                            self.clob_ws.add_tokens(tokens)

                        remaining = cand["end_ts"] - now
                        logger.info(
                            f"+ DISCOVERED [{cfg['name']}|{cand['tf']}] {slug} | "
                            f"ends in {remaining}s | target=${ptb:,.2f} | "
                            f"source={ptb_source} | discovered_via={cand['source']} ({disc_ms:.0f}ms)"
                        )
                        self._beat("discovery")

                except Exception as e:
                    logger.error(f"Discovery error: {e}", exc_info=True)

                await asyncio.sleep(PREDICTIVE_CHECK_INTERVAL)

    # ── BTC Price Loop ────────────────────────────────────────

    async def _btc_price_loop(self):
        """Keep crypto prices fresh via Binance REST (fallback for WS)."""
        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT) as client:
            while self.is_running:
                self._beat("btc-price")
                # Always fetch BTC first (backward compat)
                await self._get_btc_price(client)
                # Fetch other cryptos via REST as fallback
                for crypto_key in ACTIVE_CRYPTOS:
                    if crypto_key == "btc":
                        continue
                    if self._get_crypto_price(crypto_key) > 0:
                        continue  # BinanceMultiFeed is providing data
                    try:
                        cfg = CRYPTO_CONFIGS[crypto_key]
                        resp = await client.get(
                            "https://api.binance.com/api/v3/ticker/price",
                            params={"symbol": cfg["binance_symbol"]},
                        )
                        resp.raise_for_status()
                        price = float(resp.json()["price"])
                        self._crypto_prices[crypto_key] = price
                    except Exception:
                        pass
                await asyncio.sleep(1)

    # ── WS-Triggered Evaluation ───────────────────────────────

    async def _ws_evaluation_loop(self):
        """Evaluate strategy on each WS price update."""
        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT) as client:
            while self.is_running:
                self._beat("ws-eval")
                try:
                    token_id = await asyncio.wait_for(
                        self._ws_eval_queue.get(), timeout=2
                    )
                    slug = self._token_to_slug.get(token_id)
                    if slug and slug in self.active_markets:
                        info = self.active_markets[slug]
                        if not info["settled"] and info["position"] is None:
                            await self._evaluate_market(client, slug, info, source="ws")
                            self._ws_evals += 1
                            self._beat("ws-eval")
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.debug(f"WS eval error: {e}")

    # ── REST Fallback Evaluation ──────────────────────────────

    async def _rest_evaluation_loop(self):
        """Periodic REST-based evaluation as fallback to WS."""
        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT) as client:
            while self.is_running:
                self._beat("rest-eval")
                try:
                    now = int(time.time())
                    # At least one crypto must have a price
                    if not any(self._crypto_prices.values()):
                        await asyncio.sleep(1)
                        continue

                    for slug, info in list(self.active_markets.items()):
                        if info["settled"] or info["position"] is not None:
                            continue

                        remaining = info["end_ts"] - now
                        if remaining <= 0 or remaining > self.params["MAX_TIME_REMAINING"]:
                            continue

                        # Check if WS has fresh prices — if so, skip REST
                        up_ws, down_ws = self._get_share_prices_ws(
                            info["up_token_id"], info["down_token_id"]
                        )
                        if up_ws is not None and down_ws is not None:
                            # WS is providing data — skip REST for this market
                            continue

                        # REST fallback
                        await self._evaluate_market(client, slug, info, source="rest")
                        self._rest_evals += 1
                        self._beat("rest-eval")

                except Exception as e:
                    logger.error(f"REST eval error: {e}", exc_info=True)

                await asyncio.sleep(WS_FALLBACK_POLL_INTERVAL)

    # ── Market Evaluation ─────────────────────────────────────

    async def _evaluate_market(
        self, client: httpx.AsyncClient, slug: str, info: dict, source: str
    ):
        """Evaluate strategy for one market and potentially enter."""
        async with self._get_market_lock(slug):
            await self._evaluate_market_inner(client, slug, info, source)

    async def _evaluate_market_inner(
        self, client: httpx.AsyncClient, slug: str, info: dict, source: str
    ):
        """Inner evaluation logic (called under per-market lock)."""
        # Double-entry guard: skip if already positioned
        if info["position"] is not None:
            return

        now = int(time.time())
        remaining = info["end_ts"] - now

        if remaining <= 0 or remaining > self.params["MAX_TIME_REMAINING"]:
            return

        crypto_key = info.get("crypto_key", "btc")
        cfg = CRYPTO_CONFIGS.get(crypto_key, CRYPTO_CONFIGS["btc"])
        asset_price = self._get_crypto_price(crypto_key)
        if asset_price <= 0:
            return

        min_ptb = cfg.get("min_ptb", 1)
        # Guard: skip if price_to_beat is invalid
        if info["price_to_beat"] <= min_ptb:
            return

        if (
            info.get("price_to_beat_source") not in TRUSTED_PTB_SOURCES
            and time.time() - info.get("last_ptb_refresh_at", 0) >= 5.0
        ):
            info["last_ptb_refresh_at"] = time.time()
            new_ptb, new_source = await self._get_price_to_beat(
                client,
                slug,
                question=info.get("question", ""),
                crypto_key=crypto_key,
                start_ts=info.get("start_ts", 0),
            )
            if new_ptb > min_ptb and new_source in TRUSTED_PTB_SOURCES:
                logger.info(
                    f"  [{slug}] strike refreshed: {info['price_to_beat_source']} "
                    f"${info['price_to_beat']:,.6g} → {new_source} ${new_ptb:,.6g}"
                )
                info["price_to_beat"] = new_ptb
                info["price_to_beat_source"] = new_source

        # Get prices — prefer WS best ask, fallback to REST
        up_ws, down_ws = self._get_share_prices_ws(
            info["up_token_id"], info["down_token_id"]
        )
        if up_ws is not None and down_ws is not None:
            up_price, down_price = up_ws, down_ws
            # Log order book spread for both sides
            up_bid = self.clob_ws.get_best_bid(info["up_token_id"])
            down_bid = self.clob_ws.get_best_bid(info["down_token_id"])
            if up_bid is not None:
                logger.debug(f"  [{slug}] Up book: best_bid={up_bid:.4f} best_ask={up_price:.4f} spread={up_price-up_bid:.4f}")
            if down_bid is not None:
                logger.debug(f"  [{slug}] Down book: best_bid={down_bid:.4f} best_ask={down_price:.4f} spread={down_price-down_bid:.4f}")
        else:
            up_price, down_price = await self._get_share_prices_rest(
                client, info["up_token_id"], info["down_token_id"]
            )

        self._total_evals += 1

        # Build per-crypto params
        crypto_params = {
            **self.params,
            "MIN_DISTANCE": cfg.get("min_distance", self.params.get("MIN_BTC_DISTANCE", 50.0)),
            "VOLATILITY_1SEC": self.rolling_vol.get_vol(
                crypto_key,
                fallback=cfg.get("volatility_1sec", self.params.get("BTC_1SEC_VOLATILITY", 2.5))
            ),
            "POSITION_SIZE": cfg.get("position_size", self.params.get("POSITION_SIZE", 0.05)),
        }

        ptb_source = info.get("price_to_beat_source", "unknown")
        ptb_trusted = ptb_source in TRUSTED_PTB_SOURCES
        size_factor = 1.0
        edge_bump = 0.0

        if not ptb_trusted:
            if STRIKE_GATE_MODE == "strict":
                logger.debug(
                    f"  [{slug}] strike-gate(strict): skip — source={ptb_source}"
                )
                return
            if STRIKE_GATE_MODE == "reduced":
                size_factor = STRIKE_GATE_SIZE_FACTOR
                edge_bump = STRIKE_GATE_MIN_EDGE_BUMP
                crypto_params = {
                    **crypto_params,
                    "MIN_EDGE": crypto_params.get(
                        "MIN_EDGE", self.params.get("MIN_EDGE", 0.06)
                    ) + edge_bump,
                }

        # Run staged entry strategy
        should, side, share_price, est_prob = should_enter_staged(
            time_remaining_seconds=remaining,
            current_price=asset_price,
            price_to_beat=info["price_to_beat"],
            up_share_price=up_price,
            down_share_price=down_price,
            params=crypto_params,
            staged_thresholds=STAGED_ENTRY,
        )

        if should:
            if not ptb_trusted and STRIKE_GATE_MODE == "reduced":
                logger.info(
                    f"  [{slug}] strike-gate(reduced): non-trusted source={ptb_source} "
                    f"→ size×{size_factor}, edge_req+={edge_bump:.3f}"
                )

            # Track first qualification time
            if info["first_qualified_at"] is None:
                info["first_qualified_at"] = time.time()
                self._markets_qualified += 1

            # Track best qualifying price
            if info["best_qualifying_price"] is None or share_price < info["best_qualifying_price"]:
                info["best_qualifying_price"] = share_price

            # Log order book for the chosen side
            _chosen_tok = info.get("up_token_id", "") if side == "Up" else info.get("down_token_id", "")
            _ob_bid = self.clob_ws.get_best_bid(_chosen_tok) or 0
            _ob_ask = self.clob_ws.get_price(_chosen_tok) or share_price
            logger.info(f"  [{slug}] Order book: best_bid={_ob_bid:.4f} best_ask={_ob_ask:.4f} spread={_ob_ask-_ob_bid:.4f}")

            # V7 session-aware risk controls
            import datetime as _dt
            _utc_now = _dt.datetime.now(_dt.timezone.utc)
            _utc_hour = _utc_now.hour
            _today_str = _utc_now.strftime('%Y-%m-%d')
            _is_ny = NY_SESSION_START_UTC <= _utc_hour < NY_SESSION_END_UTC
            if self._ny_session_date != _today_str:
                self._ny_session_pnl = 0.0
                self._ny_session_halted = False
                self._ny_session_date = _today_str
            if _is_ny and self._ny_session_halted:
                logger.info(f"  [{slug}] NY_HALTED: loss=${self._ny_session_pnl:.2f} today, skipping")
                return
            _eff_min_entry = NY_MIN_ENTRY_PRICE if _is_ny else MIN_ENTRY_PRICE
            _eff_kelly_frac = NY_KELLY_FRACTION  if _is_ny else KELLY_FRACTION
            _eff_max_trade  = NY_MAX_TRADE_SIZE  if _is_ny else MAX_TRADE_SIZE
            if _is_ny:
                logger.info(f"  [{slug}] NY_SESSION: entry>={_eff_min_entry} kelly={_eff_kelly_frac} max=${_eff_max_trade}")

            # Reject below-minimum entries
            if share_price < _eff_min_entry:
                logger.warning(f"  [{slug}] REJECTED price {share_price:.4f} < {_eff_min_entry} ({'NY' if _is_ny else 'normal'})")
                return

            # ── V7 Per-crypto percentage distance filter ─────────────────────────
            # Thresholds from 48h analysis of 3,442 markets across 6 cryptos
            distance = abs(asset_price - info["price_to_beat"])
            ptb = info["price_to_beat"]
            crypto_symbol = crypto_key.upper()  # "btc" -> "BTC"
            threshold_pct = MIN_DISTANCE_PCT.get(crypto_symbol, DEFAULT_MIN_DISTANCE_PCT)
            distance_pct = (distance / ptb * 100) if ptb > 0 else 0.0
            if distance_pct < threshold_pct:
                tf_label = slug.split("-")[2] if slug.count("-") >= 2 else "?"
                logger.info(
                    f"  [{crypto_symbol} {tf_label}m] distance_check: "
                    f"crypto={crypto_symbol}, price_to_beat=${ptb:.6g}, "
                    f"current=${asset_price:.6g}, distance=${distance:.6g} ({distance_pct:.4f}%), "
                    f"threshold={threshold_pct}%, REJECTED (below_min_distance_pct)"
                )
                try:
                    import json as _json
                    os.makedirs("logs_v7", exist_ok=True)
                    with open(os.path.join("logs_v7", "distance_rejections.jsonl"), "a") as _rf:
                        _rf.write(_json.dumps({
                            "ts": int(time.time()), "crypto": crypto_symbol,
                            "slug": slug, "distance_pct": round(distance_pct, 6),
                            "threshold": threshold_pct, "current_price": asset_price,
                            "price_to_beat": ptb, "distance_dollar": round(distance, 6),
                        }) + "\n")
                except Exception:
                    pass
                return

            # ── Dynamic trade sizing based on order book depth ──
            # Cap trade size to what can be filled cleanly within acceptable price.
            # Identical logic for paper and live — paper P&L matches live.
            # distance already computed above

            # ── V6 Kelly-based position sizing ──
            if POSITION_SIZE_MODE == "kelly":
                edge = est_prob - share_price
                win_payout = 1.0 - share_price
                if win_payout > 0:
                    kelly = (est_prob * win_payout - (1 - est_prob) * share_price) / win_payout
                else:
                    kelly = 0.0
                kelly = max(0.0, kelly)
                # V7: Kelly boundary zone — use KELLY_BOUNDARY_FRACTION when near threshold
                tf_label = slug.split("-")[2] if slug.count("-") >= 2 else "?"
                if distance_pct < threshold_pct * KELLY_BOUNDARY_MULTIPLIER:
                    kelly_mult = _eff_kelly_frac * KELLY_BOUNDARY_FRACTION
                    kelly_zone = "boundary"
                elif est_prob < 0.95:
                    kelly_mult = _eff_kelly_frac * 0.5  # eighth-Kelly for lower conviction
                    kelly_zone = "eighth"
                else:
                    kelly_mult = _eff_kelly_frac
                    kelly_zone = "full"
                logger.info(
                    f"  [{crypto_symbol} {tf_label}m] distance_check: "
                    f"crypto={crypto_symbol}, price_to_beat=${ptb:.6g}, "
                    f"current=${asset_price:.6g}, distance=${distance:.6g} ({distance_pct:.4f}%), "
                    f"threshold={threshold_pct}%, PASSED (kelly_zone={kelly_zone})"
                )
                # Apply STRIKE_GATE_SIZE_FACTOR for non-trusted strikes —
                # this multiplier was previously computed at line ~1544 but
                # never actually applied to the bet, leaving a defensive
                # control as dead code.
                desired_size = min(self.bankroll * kelly * kelly_mult * size_factor, _eff_max_trade)
                if desired_size < MIN_BET_SIZE:
                    logger.info(f"  [{slug}] Kelly bet too small: kelly={kelly:.4f} -> ${desired_size:.2f}, skipping")
                    return
                kelly_label = kelly_zone
                logger.info(
                    f"  [{slug}] Kelly sizing ({kelly_label}): edge={edge:.4f} "
                    f"kelly={kelly:.4f} prob={est_prob:.4f} size_factor={size_factor:.2f} "
                    f"bet=${desired_size:.2f}"
                )
            else:
                desired_size = min(self.bankroll * FIXED_POSITION_SIZE * size_factor, _eff_max_trade)

            _chosen_token = info.get("up_token_id", "") if side == "Up" else info.get("down_token_id", "")
            max_acceptable_price = min(share_price + PRICE_BUFFER, 0.99)

            # Try WS cached depth first, then REST fallback
            ws_age = self.clob_ws.price_age_ms(_chosen_token)
            available_shares = 0.0
            available_usd = 0.0
            liq_source = "none"

            if ws_age < 10_000:
                available_shares, available_usd = self.clob_ws.get_available_liquidity(
                    _chosen_token, max_acceptable_price
                )
                if available_usd > 0:
                    liq_source = "WS"

            if available_usd <= 0:
                # REST fallback — query full book
                try:
                    book_resp = await client.get(
                        f"{CLOB_API}/book",
                        params={"token_id": _chosen_token},
                        timeout=3,
                    )
                    if book_resp.status_code == 200:
                        book_data = book_resp.json()
                        rest_asks = book_data.get("asks", [])
                        for a in rest_asks:
                            p = float(a.get("price", 0))
                            s = float(a.get("size", 0))
                            if s > 0 and p <= max_acceptable_price:
                                available_shares += s
                                available_usd += p * s
                        if available_usd > 0:
                            liq_source = "REST"
                        else:
                            # Book returned 200 but no asks at acceptable price
                            # Fall back to desired_size (same as exception case)
                            available_usd = desired_size
                            liq_source = "book-empty-fallback"
                            logger.info(f"  [{slug}] Book empty/no asks at <={max_acceptable_price:.2f}, using fallback (paper mode)")
                except Exception as liq_err:
                    logger.warning(f"  [{slug}] Liquidity check failed ({liq_err}), using desired size")
                    available_usd = desired_size  # allow trade if check fails
                    liq_source = "fallback"

            # Cap trade size to available liquidity
            actual_size = min(desired_size, available_usd)
            liquidity_adjusted = actual_size < desired_size

            if available_usd <= 0:
                logger.info(f"  [{slug}] SKIP: no asks at <={max_acceptable_price:.2f} — would not fill (liq_source={liq_source})")
                return

            if actual_size < MIN_TRADE_SIZE:
                logger.info(
                    f"  [{slug}] Insufficient liquidity: ${available_usd:.2f} available at <={max_acceptable_price:.2f}, "
                    f"need ${MIN_TRADE_SIZE:.2f} min"
                )
                return

            if liquidity_adjusted:
                logger.info(
                    f"  [{slug}] Liquidity-adjusted size: ${desired_size:.2f} -> ${actual_size:.2f} "
                    f"(book has ${available_usd:.2f} at <={max_acceptable_price:.2f}, {liq_source})"
                )
            else:
                logger.info(
                    f"  [{slug}] Liquidity OK ({liq_source}): ${available_usd:.2f} available vs ${actual_size:.2f} needed"
                )

            if size_factor < 1.0:
                actual_size = max(MIN_TRADE_SIZE, actual_size * size_factor)

            bet_amount = actual_size
            ask_liquidity = available_usd

            # Apply simulated slippage for paper mode (makes paper P&L more realistic)
            if self.mode == "paper":
                from config import PAPER_SLIPPAGE_BPS
                slippage_multiplier = 1 + (PAPER_SLIPPAGE_BPS / 10000)
                simulated_price = share_price * slippage_multiplier
                # Don't let slippage push price above 0.99
                simulated_price = min(simulated_price, 0.99)
            else:
                simulated_price = share_price

            shares = bet_amount / simulated_price  # use simulated_price in paper mode

            info["position"] = {
                "side": side,
                "entry_price": simulated_price,
                "raw_market_price": share_price,  # actual market price before slippage
                "token_id": info["up_token_id"] if side == "Up" else info["down_token_id"],
                "other_token_id": info["down_token_id"] if side == "Up" else info["up_token_id"],
                "estimated_prob": est_prob,
                "bet_amount": bet_amount,
                "shares": shares,
                "time_remaining": remaining,
                "crypto_key": crypto_key,
                "crypto_name": cfg["name"],
                "asset_price": asset_price,
                "btc_price": asset_price if crypto_key == "btc" else 0,
                "btc_distance": distance if crypto_key == "btc" else 0,
                "asset_distance": distance,
                "price_to_beat": info["price_to_beat"],
                "entry_time": now,
                "entry_source": source,
                "available_liquidity_usd": round(available_usd, 2),
                "liquidity_adjusted": liquidity_adjusted,
                "original_size": round(desired_size, 2),
                "actual_size": round(actual_size, 2),
                "ptb_source": ptb_source,
                "ptb_trusted_at_entry": ptb_trusted,
                "strike_gate_size_factor": size_factor,
                "entry_delay_ms": (time.time() - info["first_qualified_at"]) * 1000 if info["first_qualified_at"] else 0,
            }

            self._markets_traded += 1
            _liq_str = f"liq=${ask_liquidity:.0f}" if ask_liquidity > 0 else "liq=REST"
            _adj_str = f" [ADJUSTED ${desired_size:.0f}->${actual_size:.0f}]" if liquidity_adjusted else ""
            _vol_raw = self.rolling_vol.get_vol(crypto_key, fallback=cfg.get("volatility_1sec", 2.5))
            _edge = est_prob - share_price
            logger.info(
                f"*** TRADE [{cfg['name']}|{info['tf']}] {slug} | {side} @ {share_price:.3f} | "
                f"Bet=${bet_amount:.2f} | Prob={est_prob:.4f} | Edge={_edge:.4f} | "
                f"Dist=${distance:,.6g} | t-{remaining}s | vol={_vol_raw:.2f}x1.5 | {_liq_str}{_adj_str} | via={source}"
            )

            # Execute real trade if in shadow/live mode
            if self.mode != "paper":
                await self._execute_real_trade(
                    slug=slug, info=info, side=side,
                    share_price=share_price, est_prob=est_prob,
                    time_remaining=remaining, asset_price=asset_price,
                    crypto_key=crypto_key,
                )
        else:
            distance = asset_price - info["price_to_beat"]
            logger.debug(
                f"  [{slug}] t-{remaining}s | Dist={distance:+,.6g} | "
                f"Up={up_price:.3f} Dn={down_price:.3f} | skip ({source})"
            )



    # ── V6 REST book fetch for position monitor ──

    async def _get_book_prices_rest(self, token_id: str) -> tuple[float, float]:
        """Fetch best bid and best ask from CLOB REST API.
        Returns (best_bid, best_ask). Either can be 0 if unavailable."""
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                resp = await client.get(
                    f"{CLOB_API}/book",
                    params={"token_id": token_id},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    best_bid = 0.0
                    best_ask = 0.0
                    if bids:
                        best_bid = max(
                            (float(b.get("price", 0)) for b in bids if float(b.get("size", 0)) > 0),
                            default=0.0
                        )
                    if asks:
                        valid_asks = [float(a.get("price", 999)) for a in asks if float(a.get("size", 0)) > 0]
                        best_ask = min(valid_asks) if valid_asks else 0.0
                    return best_bid, best_ask
        except Exception as e:
            logger.debug("REST book fetch failed for %s: %s", token_id[:20], e)
        return 0.0, 0.0

    # ── V6 Position Monitor (take-profit / stop-loss / hedge) ──

    async def _position_monitor_loop(self):
        """Monitor open positions for take-profit and stop-loss exits."""
        if not ENABLE_DYNAMIC_EXIT:
            logger.info("Dynamic exit disabled, position monitor not running")
            return
        logger.info("Position monitor started (TP=%.2f, SL=%.2f, hedge=%s, interval=%ds)",
                     TAKE_PROFIT_PRICE, STOP_LOSS_PRICE, HEDGE_INSTEAD_OF_STOP,
                     POSITION_MONITOR_INTERVAL)
        heartbeat_counter = 0
        while self.is_running:
            self._beat("position-monitor")
            try:
                heartbeat_counter += 1
                checked = 0
                no_token = 0
                no_bid = 0
                in_range = 0

                for slug, info in list(self.active_markets.items()):
                    pos = info.get("position")
                    if pos is None or pos.get("exited"):
                        continue

                    # Get current best bid for our token (what we'd sell at)
                    token_id = pos.get("token_id")
                    other_token_id = pos.get("other_token_id")
                    if not token_id:
                        no_token += 1
                        continue

                    checked += 1

                    # -- Age guard: skip ALL exit checks if position too young --
                    entry_time = pos.get("entry_time", 0)
                    position_age = time.time() - entry_time if entry_time else 9999
                    if position_age < MIN_POSITION_AGE_FOR_EXIT:
                        in_range += 1  # count as in range for heartbeat
                        continue

                    # Use the new fallback method that estimates from opposite ask
                    best_bid = self.clob_ws.get_best_bid_or_estimate(token_id, other_token_id)
                    bid_source = "ws" if best_bid else None
                    
                    # REST fallback: fetch from CLOB API if WS has no data
                    if best_bid is None or best_bid <= 0:
                        rest_bid, rest_ask = await self._get_book_prices_rest(token_id)
                        if rest_bid > 0:
                            best_bid = rest_bid
                            bid_source = "rest"
                        elif other_token_id:
                            # Try opposite token REST for estimation
                            _, other_rest_ask = await self._get_book_prices_rest(other_token_id)
                            if other_rest_ask and 0 < other_rest_ask < 1:
                                best_bid = round(1.0 - other_rest_ask - 0.005, 4)
                                bid_source = "rest-est"

                    if best_bid is None or best_bid <= 0:
                        no_bid += 1
                        continue

                    entry_price = pos["entry_price"]
                    shares = pos["shares"]
                    bet_amount = pos.get("bet_amount", 0)
                    if shares <= 0:
                        continue

                    # ── Take profit ──
                    if best_bid >= TAKE_PROFIT_PRICE:
                        revenue = shares * best_bid
                        sell_fee = shares * POLY_CRYPTO_FEE_RATE * best_bid * (1 - best_bid)
                        net_profit = revenue - bet_amount - sell_fee

                        self.bankroll += net_profit  # net_profit already includes -bet_amount
                        pos["exited"] = True
                        pos["exit_type"] = "take_profit"
                        pos["exit_price"] = best_bid
                        pos["exit_profit"] = net_profit
                        logger.info(
                            "  [%s] TAKE PROFIT: entry=$%.4f bid=$%.4f shares=%.2f "
                            "revenue=$%.4f fee=$%.4f net=$%.4f",
                            slug, entry_price, best_bid, shares,
                            revenue, sell_fee, net_profit
                        )
                        self._log_exit(slug, pos, "take_profit", best_bid, net_profit)
                        await self._finalize_live_position_exit(
                            slug, pos, info.get("crypto_key", "btc"), best_bid, "take_profit"
                        )
                        continue

                    # ── Stop loss / Hedge (consecutive scan requirement) ──
                    if best_bid <= STOP_LOSS_PRICE:
                        pos["consecutive_low_scans"] = pos.get("consecutive_low_scans", 0) + 1
                        if pos["consecutive_low_scans"] < STOP_LOSS_CONSECUTIVE_SCANS:
                            logger.info("  [%s] Low bid $%.3f (scan %d/%d, waiting for confirmation)",
                                        slug, best_bid, pos["consecutive_low_scans"], STOP_LOSS_CONSECUTIVE_SCANS)
                            in_range += 1
                            continue
                        # 3 consecutive scans below threshold — trigger exit
                        if HEDGE_INSTEAD_OF_STOP:
                            # Try to hedge by buying opposite side
                            if other_token_id:
                                other_ask = self.clob_ws.get_price(other_token_id)  # best ask
                                if not other_ask or other_ask <= 0:
                                    # REST fallback for opposite token ask
                                    _, rest_other_ask = await self._get_book_prices_rest(other_token_id)
                                    if rest_other_ask > 0:
                                        other_ask = rest_other_ask
                                if other_ask and other_ask > 0:
                                    total_cost = entry_price + other_ask
                                    if total_cost <= MAX_HEDGE_TOTAL_COST:
                                        # Hedge: buy opposite side
                                        hedge_bet = shares * other_ask
                                        hedge_fee = shares * POLY_CRYPTO_FEE_RATE * other_ask * (1 - other_ask)
                                        guaranteed_loss = (total_cost - 1.0) * shares + hedge_fee
                                        self.bankroll -= (hedge_bet + hedge_fee)  # pay for hedge
                                        pos["exited"] = True
                                        pos["exit_type"] = "hedge"
                                        pos["hedge_price"] = other_ask
                                        pos["hedge_total_cost"] = total_cost
                                        pos["max_loss"] = guaranteed_loss
                                        pos["exit_profit"] = -guaranteed_loss
                                        logger.info(
                                            "  [%s] HEDGE: entry=$%.4f + opposite@$%.4f = $%.4f/share | "
                                            "hedge_cost=$%.4f fee=$%.4f | max_loss=$%.4f "
                                            "(unhedged would be $%.2f)",
                                            slug, entry_price, other_ask, total_cost,
                                            hedge_bet, hedge_fee, guaranteed_loss, bet_amount
                                        )
                                        self._log_exit(slug, pos, "hedge", other_ask, -guaranteed_loss)
                                        await self._finalize_live_position_exit(
                                            slug, pos, info.get("crypto_key", "btc"), other_ask, "hedge"
                                        )
                                        continue
                            # If hedge not possible, fall through to regular stop-loss

                        # Regular stop-loss: sell at current bid
                        revenue = shares * best_bid
                        sell_fee = shares * POLY_CRYPTO_FEE_RATE * best_bid * (1 - best_bid)
                        net_loss = revenue - bet_amount - sell_fee

                        self.bankroll += net_loss  # net_loss already includes -bet_amount
                        pos["exited"] = True
                        pos["exit_type"] = "stop_loss"
                        pos["exit_price"] = best_bid
                        pos["exit_profit"] = net_loss
                        logger.info(
                            "  [%s] STOP LOSS: entry=$%.4f bid=$%.4f shares=%.2f "
                            "revenue=$%.4f fee=$%.4f net=$%.4f",
                            slug, entry_price, best_bid, shares,
                            revenue, sell_fee, net_loss
                        )
                        self._log_exit(slug, pos, "stop_loss", best_bid, net_loss)
                        await self._finalize_live_position_exit(
                            slug, pos, info.get("crypto_key", "btc"), best_bid, "stop_loss"
                        )
                        continue

                    # Position checked but between SL and TP thresholds — reset consecutive counter
                    pos["consecutive_low_scans"] = 0
                    in_range += 1

                # Heartbeat every 30 cycles (~60 seconds)
                if heartbeat_counter % 30 == 0 and (checked > 0 or no_bid > 0 or no_token > 0):
                    ws_bids = len(self.clob_ws.best_bids)
                    ws_asks = len(self.clob_ws.prices)
                    logger.info(
                        "Position monitor heartbeat: checked=%d no_token=%d no_bid=%d in_range=%d "
                        "| WS bids=%d asks=%d",
                        checked, no_token, no_bid, in_range, ws_bids, ws_asks
                    )
                    # Sample one position's bid for debugging
                    if checked > 0:
                        for slug, info in list(self.active_markets.items()):
                            pos = info.get("position")
                            if pos and not pos.get("exited") and pos.get("token_id"):
                                tid = pos["token_id"]
                                oid = pos.get("other_token_id", "")
                                ws_bid = self.clob_ws.best_bids.get(tid)
                                ws_ask = self.clob_ws.prices.get(tid)
                                other_ask = self.clob_ws.prices.get(oid) if oid else None
                                est_bid = (1.0 - other_ask - 0.005) if other_ask and 0 < other_ask < 1 else None
                                logger.info(
                                    "  Monitor sample [%s]: ws_bid=%s ws_ask=%s other_ask=%s est_bid=%s entry=%.3f (REST fallback active)",
                                    slug[:30],
                                    f"${ws_bid:.3f}" if ws_bid else "None",
                                    f"${ws_ask:.3f}" if ws_ask else "None",
                                    f"${other_ask:.3f}" if other_ask else "None",
                                    f"${est_bid:.3f}" if est_bid else "None",
                                    pos["entry_price"]
                                )
                                break

            except Exception as e:
                logger.error("Position monitor error: %s", e, exc_info=True)

            now_cleanup = time.time()
            stale = [
                s for s in list(self._live_positions.keys())
                if s not in self.active_markets
                or self.active_markets.get(s, {}).get("settled")
                or self.active_markets.get(s, {}).get("end_ts", 0) + 300 < now_cleanup
            ]
            for s in stale:
                del self._live_positions[s]
                self.safety.current_positions = max(0, self.safety.current_positions - 1)
                logger.warning(
                    "STALE CLEANUP: removed %s from live_positions, safety count now=%d",
                    s, self.safety.current_positions
                )
            if stale:
                self.safety._save_state()
                self._save_v21_state()

            await asyncio.sleep(POSITION_MONITOR_INTERVAL)

    def _get_live_fill_details(self, slug: str, fallback_entry_price: float) -> tuple[dict | None, dict, float, float]:
        """Return live position, order result, real bet, and filled shares for shadow/live trades."""
        live_pos = self._live_positions.get(slug)
        if not live_pos:
            return None, {}, 0.0, 0.0

        order_result = live_pos.get("order_result", {})
        real_bet = float(live_pos.get("trade_size_usd", 0) or 0)
        real_shares = float(order_result.get("taking_amount", 0) or 0)
        if real_shares <= 0:
            raw = str(order_result.get("raw_result", ""))
            taking_match = re.search(r"'takingAmount':\s*'([\d.]+)'", raw)
            real_shares = (
                float(taking_match.group(1)) if taking_match
                else (real_bet / fallback_entry_price if fallback_entry_price > 0 else 0.0)
            )
        return live_pos, order_result, real_bet, real_shares

    async def _finalize_live_position_exit(self, slug: str, pos: dict, crypto_key: str,
                                           exit_price: float, exit_type: str):
        """Immediately clear shadow/live position state when an early exit occurs."""
        if self.mode == "paper":
            return

        live_pos, order_result, real_bet, real_shares = self._get_live_fill_details(
            slug, pos.get("entry_price", 0)
        )
        if not live_pos:
            return

        if exit_type == "hedge":
            hedge_cost = real_shares * exit_price
            hedge_fee = real_shares * POLY_CRYPTO_FEE_RATE * exit_price * (1 - exit_price)
            payout = real_shares
            actual_pnl = payout - real_bet - hedge_cost - hedge_fee
            won = False
        else:
            payout = real_shares * exit_price
            exit_fee = real_shares * POLY_CRYPTO_FEE_RATE * exit_price * (1 - exit_price)
            actual_pnl = payout - real_bet - exit_fee
            won = actual_pnl > 0

        await self.safety.record_result(won, actual_pnl)
        self.real_pnl_total += actual_pnl
        self.real_trade_count += 1
        append_jsonl(TRADE_LOG, {
            "timestamp": time.time(),
            "type": "trade_settled",
            "market_slug": slug,
            "crypto_key": crypto_key,
            "side": pos.get("side", ""),
            "outcome": "",
            "won": won,
            "ptb_source": pos.get("ptb_source", "unknown"),
            "ptb_trusted_at_entry": pos.get("ptb_trusted_at_entry", False),
            "strike_gate_size_factor": pos.get("strike_gate_size_factor", 1.0),
            "bet_amount": real_bet,
            "payout": round(payout, 6),
            "net_profit": round(actual_pnl, 6),
            "real_pnl_total": round(self.real_pnl_total, 6),
            "order_id": order_result.get("order_id", ""),
            "exit_type": exit_type,
            "exit_price": round(exit_price, 6),
        })
        del self._live_positions[slug]
        self._save_v21_state()
        logger.info("  [%s] Live position cleaned up (%s)", slug, exit_type)

    def _log_exit(self, slug: str, pos: dict, exit_type: str, exit_price: float, net_profit: float):
        """Log an early exit (take-profit, stop-loss, or hedge) to trades file."""
        trade = {
            "market_slug": slug,
            "side": pos["side"],
            "actual_outcome": "",
            "entry_price": round(pos["entry_price"], 4),
            "exit_price": round(exit_price, 4),
            "exit_type": exit_type,
            "estimated_prob": round(pos.get("estimated_prob", 0), 4),
            "won": exit_type == "take_profit",
            "bet_amount": round(pos["bet_amount"], 4),
            "shares": round(pos["shares"], 4),
            "net_profit": round(net_profit, 4),
            "bankroll_after": round(self.bankroll, 4),
            "time_remaining": pos.get("time_remaining", 0),
            "crypto_key": pos.get("crypto_key", "btc"),
            "crypto_name": pos.get("crypto_name", "Bitcoin"),
            "asset_price": round(pos.get("asset_price", 0), 6),
            "timestamp": int(time.time()),
            "entry_source": pos.get("entry_source", "unknown"),
            "ptb_source": pos.get("ptb_source", "unknown"),
            "ptb_trusted_at_entry": pos.get("ptb_trusted_at_entry", False),
            "strike_gate_size_factor": pos.get("strike_gate_size_factor", 1.0),
            "mode": pos.get("mode", self.mode),
        }
        if exit_type == "hedge":
            trade["hedge_price"] = round(pos.get("hedge_price", 0), 4)
            trade["hedge_total_cost"] = round(pos.get("hedge_total_cost", 0), 4)
            trade["max_loss"] = round(pos.get("max_loss", 0), 4)
        # V7: NY session P&L tracking
        import datetime as _dtx
        _hx = _dtx.datetime.now(_dtx.timezone.utc).hour
        _dx = _dtx.datetime.now(_dtx.timezone.utc).strftime('%Y-%m-%d')
        if self._ny_session_date != _dx:
            self._ny_session_pnl = 0.0; self._ny_session_halted = False; self._ny_session_date = _dx
        if NY_SESSION_START_UTC <= _hx < NY_SESSION_END_UTC:
            self._ny_session_pnl += trade.get('net_profit', 0.0)
            if self._ny_session_pnl <= NY_SESSION_LOSS_LIMIT and not self._ny_session_halted:
                self._ny_session_halted = True
                logger.warning(f'NY_SESSION_HALT: pnl=${self._ny_session_pnl:.2f}')
        self.trades.append(trade)
        self._save_trade(trade)

    # ── Real Trade Execution (shadow/live mode) ───────────────

    async def _execute_real_trade(self, slug: str, info: dict, side: str,
                                  share_price: float, est_prob: float,
                                  time_remaining: float, asset_price: float,
                                  crypto_key: str):
        """Execute a real trade on Polymarket CLOB (shadow or live mode)."""
        if self.mode == "paper":
            return

        # Re-entry guard. The per-market asyncio.Lock acquired in
        # _evaluate_market is the primary defense; this synchronous
        # placeholder is a second layer in case the lock dict gets reset
        # (e.g. settlement clearing the lock between evaluator hops). The
        # check + set is two consecutive synchronous statements so no
        # context switch can interleave them within a single event loop.
        if slug in self._live_positions:
            logger.warning("[%s] _execute_real_trade called but live position already exists, skipping",
                           slug)
            return
        self._live_positions[slug] = {
            "status": "pending_execution",
            "side": side,
            "timestamp": time.time(),
        }

        cfg = CRYPTO_CONFIGS.get(crypto_key, CRYPTO_CONFIGS["btc"])

        # Safety check
        can_trade = await self.safety.can_trade()
        if not can_trade:
            logger.info("[%s] Safety check failed: %s — real trade skipped",
                        cfg["name"], self.safety.last_reason)
            append_jsonl(TRADE_LOG, {
                "timestamp": time.time(),
                "type": "safety_block",
                "crypto": crypto_key,
                "reason": self.safety.last_reason,
                "slug": slug,
                "mode": self.mode,
            })
            self._live_positions.pop(slug, None)
            return

        # Determine trade size. Use the Kelly-derived bet that paper
        # evaluation already computed (info["position"]["bet_amount"]),
        # capped by the per-trade wallet ceiling (5% of balance) and the
        # global MAX_TRADE_SIZE. Previously this used balance*0.05 only,
        # which discarded Kelly + STRIKE_GATE_SIZE_FACTOR scaling and
        # caused live size to disagree with paper for non-trivial reasons.
        balance = 0.0
        if self.mode == "shadow":
            trade_size = SHADOW_TRADE_SIZE
        elif self.mode == "live":
            balance = await self.trader.get_balance()
            paper_bet = float(
                info.get("position", {}).get("bet_amount", 0)
                or info.get("position", {}).get("actual_size", 0)
                or 0
            )
            wallet_cap = balance * 0.05
            trade_size = min(paper_bet, wallet_cap, MAX_TRADE_SIZE)
            if trade_size <= 0:
                trade_size = min(wallet_cap, MAX_TRADE_SIZE)
        else:
            self._live_positions.pop(slug, None)
            return

        if trade_size < 1.0 or (self.mode == "live" and trade_size > balance):
            logger.info("[%s] Trade size $%.2f not viable (balance=$%.2f), skipping",
                        cfg["name"], trade_size, balance if self.mode == "live" else 0)
            self._live_positions.pop(slug, None)
            return

        # Token ID
        if side == "Up":
            token_id = info.get("up_token_id", "")
        else:
            token_id = info.get("down_token_id", "")

        if not token_id:
            logger.error("[%s] No token_id for side=%s in %s",
                         cfg["name"], side, slug)
            self._live_positions.pop(slug, None)
            return

        # Log order book state at execution time
        _up_bid = self.clob_ws.get_best_bid(info.get("up_token_id", ""))
        _up_ask = self.clob_ws.get_price(info.get("up_token_id", ""))
        _dn_bid = self.clob_ws.get_best_bid(info.get("down_token_id", ""))
        _dn_ask = self.clob_ws.get_price(info.get("down_token_id", ""))
        logger.info(
            "[%s] Order book at execution: Up bid=%.4f ask=%.4f | Down bid=%.4f ask=%.4f",
            cfg["name"],
            _up_bid or 0, _up_ask or 0,
            _dn_bid or 0, _dn_ask or 0,
        )

        logger.info(
            "[%s] EXECUTING %s TRADE: %s @ %.4f | size=$%.2f | market=%s",
            cfg["name"], self.mode.upper(), side, share_price,
            trade_size, slug
        )

        try:
            result = await self.trader.place_order(
                token_id=token_id,
                side="BUY",
                size_usd=trade_size,
                price=share_price,
                market_slug=slug,
                estimated_prob=est_prob,
            )

            # Check fill ratio
            fill_ratio = result.get("fill_ratio", 1.0)
            if fill_ratio < 0.5:
                logger.warning("[%s] Low fill ratio %.1f%% — order book may be thin",
                               cfg["name"], fill_ratio * 100)

            # Only proceed if something actually filled
            order_success = result.get("success", False)
            actual_filled = result.get("filled_size", 0)
            if not order_success or actual_filled <= 0:
                logger.warning("[%s] Zero fill — no position recorded (success=%s filled=$%.2f)",
                               cfg["name"], order_success, actual_filled)
                self._live_positions.pop(slug, None)
                return
        except Exception as e:
            logger.error("[%s] Order execution failed: %s", cfg["name"], e)
            append_jsonl(TRADE_LOG, {
                "timestamp": time.time(),
                "type": "order_exception",
                "crypto": crypto_key,
                "error": str(e),
                "slug": slug,
                "mode": self.mode,
            })
            self._live_positions.pop(slug, None)
            return

        # Record live position
        actual_trade_size = result.get("filled_size", trade_size) if result.get("fill_ratio", 0) > 0 else trade_size
        self._live_positions[slug] = {
            "crypto_key": crypto_key,
            "crypto_name": cfg["name"],
            "slug": slug,
            "side": side,
            "entry_price": share_price,
            "estimated_prob": est_prob,
            "trade_size_usd": actual_trade_size,
            "token_id": token_id,
            "order_result": result,
            "mode": self.mode,
            "asset_price": asset_price,
            "time_remaining": time_remaining,
            "entry_time": int(time.time()),
        }

        # Only call safety.record_trade() if something actually filled
        if result.get("success", False) and result.get("fill_ratio", 0) > 0:
            await self.safety.record_trade(self._live_positions[slug])
        self._save_v21_state()

        logger.info(
            "[%s] %s TRADE PLACED: %s @ %.4f | $%.2f | order_id=%s | success=%s",
            cfg["name"], self.mode.upper(), side, share_price,
            trade_size, result.get("order_id", "?"), result.get("success", False)
        )

    def _save_v21_state(self):
        """Save trader state for persistence across restarts."""
        import json as _json
        state = {
            "mode": self.mode,
            "real_pnl_total": self.real_pnl_total,
            "real_trade_count": self.real_trade_count,
            "bankroll": self.bankroll,
            "timestamp": time.time(),
        }
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            with open(STATE_FILE, "w") as f:
                _json.dump(state, f, indent=2)
        except Exception as e:
            logger.warning("Failed to save state: %s", e)

    def _load_v21_state(self):
        """Load saved state."""
        import json as _json
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE) as f:
                    state = _json.load(f)
                self.mode = state.get("mode", self.mode)
                self.real_pnl_total = state.get("real_pnl_total", 0.0)
                self.real_trade_count = state.get("real_trade_count", 0)
                logger.info("V2.1 state loaded: mode=%s, real_pnl=$%.2f",
                            self.mode, self.real_pnl_total)
            except Exception as e:
                logger.warning("Failed to load state: %s", e)

    # ── Settlement Loop ───────────────────────────────────────

    async def _settlement_loop(self):
        """Check and settle expired markets."""
        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT) as client:
            while self.is_running:
                self._beat("settlement")
                try:
                    now = int(time.time())
                    for slug, info in list(self.active_markets.items()):
                        if info["settled"]:
                            # Cleanup old settled entries
                            if now - info["settled_at"] > 120:
                                del self.active_markets[slug]
                            continue

                        remaining = info["end_ts"] - now
                        if remaining > 0:
                            continue

                        # Market expired — settle
                        await self._settle_market(client, slug, info)

                except Exception as e:
                    logger.error(f"Settlement error: {e}", exc_info=True)

                await asyncio.sleep(2)

    async def _settle_market(self, client: httpx.AsyncClient, slug: str, info: dict):
        """Settle one expired market."""
        now = int(time.time())
        expired_for = now - info["end_ts"]
        crypto_key = info.get("crypto_key", "btc")
        cfg = CRYPTO_CONFIGS.get(crypto_key, CRYPTO_CONFIGS["btc"])
        asset_price = self._get_crypto_price(crypto_key)

        outcome = await self._check_resolution(client, slug)

        if outcome is None:
            if expired_for < 35:
                return  # Retry next cycle
            if info.get("price_to_beat_source") not in TRUSTED_PTB_SOURCES:
                refreshed_ptb, refreshed_source = await self._get_price_to_beat(
                    client,
                    slug,
                    question=info.get("question", ""),
                    crypto_key=crypto_key,
                    start_ts=info["start_ts"],
                )
                if refreshed_ptb > cfg.get("min_ptb", 1):
                    if (
                        refreshed_source != info.get("price_to_beat_source")
                        or abs(refreshed_ptb - info["price_to_beat"]) > 1e-9
                    ):
                        logger.info(
                            f"  [{cfg['name']}] {slug} → refreshed strike ${refreshed_ptb:,.6g} "
                            f"from {refreshed_source} (was ${info['price_to_beat']:,.6g} from "
                            f"{info.get('price_to_beat_source', 'unknown')})"
                        )
                    info["price_to_beat"] = refreshed_ptb
                    info["price_to_beat_source"] = refreshed_source

            if info.get("price_to_beat_source") not in TRUSTED_PTB_SOURCES:
                logger.warning(
                    f"  [{cfg['name']}] {slug} → waiting for authoritative strike; "
                    f"current source={info.get('price_to_beat_source', 'unknown')}"
                )
                return

            # Prefer historical price AT market close over current live price.
            # For 5m markets especially, 35+ seconds of drift between end_ts and
            # "now" can flip the inferred outcome vs what Polymarket will
            # eventually resolve to. Fall back to live price only if hist fetch
            # fails entirely.
            close_price = await self._get_historical_price(client, crypto_key, info["end_ts"])
            if close_price is None:
                close_price = asset_price
                logger.warning(
                    f"  [{cfg['name']}] {slug} → historical fetch failed, "
                    f"using live price ${close_price:,.6g} for inference"
                )
            outcome = "Up" if close_price >= info["price_to_beat"] else "Down"
            logger.info(
                f"  [{cfg['name']}] {slug} → inferred {outcome} "
                f"(close=${close_price:,.6g} vs strike=${info['price_to_beat']:,.6g})"
            )
        else:
            logger.info(f"  {slug} → resolved {outcome}")

        await self._log_chainlink_strike_discrepancy(client, slug, info, crypto_key)

        info["settled"] = True
        info["settled_at"] = now
        self.settled_slugs.add(slug)

        # Per-market lock retention: previously this deleted the lock at
        # settlement to "prevent a memory leak". The leak was overblown
        # (_prune trims state), but the deletion creates a real race —
        # if a concurrent evaluator was awaiting on this lock object and
        # the dict entry is deleted, a fresh _get_market_lock() call
        # creates a *different* Lock instance, breaking serialization.
        # We now keep the locks; _prune handles long-term growth.

        # Log performance data for this window
        perf_data = {
            "market_slug": slug,
            "discovered_at": info.get("discovered_at", 0),
            "discovery_source": info.get("discovery_source", "unknown"),
            "discovery_delay_ms": info.get("discovery_delay_ms", 0),
            "market_start": info["start_ts"],
            "market_end": info["end_ts"],
            "tf": info["tf"],
            "qualified": info["first_qualified_at"] is not None,
            "qualified_at": info.get("first_qualified_at"),
            "best_entry_price": info.get("best_qualifying_price"),
            "traded": info["position"] is not None,
            "ptb_source": info.get("price_to_beat_source", "unknown"),
            "actual_outcome": outcome,
            "timestamp": now,
        }

        if info["position"] is None:
            # No trade taken
            missed_reason = "no_qualifying_price"
            if info["first_qualified_at"] is not None:
                missed_reason = "qualified_but_not_entered"  # shouldn't happen
            perf_data["missed_reason"] = missed_reason
            perf_data["actual_entry_price"] = None
            perf_data["entry_delay_ms"] = None
            self.perf.log_window(perf_data)
            logger.info(f"  {slug}: no position ({missed_reason})")
            return

        # Calculate P&L
        pos = info["position"]

        # ── V6: Handle positions already exited via take-profit/stop-loss/hedge ──
        if pos.get("exited"):
            exit_type = pos.get("exit_type", "unknown")
            if exit_type == "hedge":
                # Hedge settlement: we own both YES and NO → guaranteed $1.00/share payout
                payout = pos["shares"] * 1.0
                self.bankroll += payout - pos["bet_amount"]  # payout minus original bet (never deducted at entry)
                net = -pos.get("max_loss", 0)
                logger.info(
                    f"  [{cfg['name']}] {slug}: HEDGE settled | payout=${payout:.4f} | "
                    f"net=${net:.4f} (max_loss=${pos.get('max_loss', 0):.4f})"
                )
            else:
                net = pos.get("exit_profit", 0)
                logger.info(
                    f"  [{cfg['name']}] {slug}: already exited via {exit_type} | "
                    f"recorded P&L=${net:.4f}"
                )
            # Update trade record with actual outcome (was "" at exit time)
            for t in reversed(self.trades):
                if t.get("market_slug") == slug and t.get("exit_type"):
                    t["actual_outcome"] = outcome
                    break
            # Persist updated trades to file so outcome survives restarts
            self._rewrite_trades_file()

            # Log perf data
            perf_data["actual_entry_price"] = pos["entry_price"]
            perf_data["entry_delay_ms"] = pos.get("entry_delay_ms", 0)
            perf_data["won"] = net > 0
            perf_data["net_profit"] = net
            perf_data["exit_type"] = exit_type
            perf_data["missed_reason"] = None
            self.perf.log_window(perf_data)

            # If a real shadow/live order was placed for this market, settlement still
            # needs to clear the live position and update safety counters even though
            # the paper position exited earlier via TP/SL/hedge.
            if self.mode != "paper" and slug in self._live_positions:
                live_pos = self._live_positions[slug]
                order_result = live_pos.get("order_result", {})
                real_bet = live_pos.get("trade_size_usd", 0)
                real_shares = float(order_result.get("taking_amount", 0) or 0)
                if real_shares <= 0:
                    raw = str(order_result.get("raw_result", ""))
                    taking_match = re.search(r"'takingAmount':\s*'([\d.]+)'", raw)
                    real_shares = (
                        float(taking_match.group(1)) if taking_match
                        else (real_bet / pos["entry_price"] if pos["entry_price"] > 0 else 0)
                    )
                won_real = pos["side"] == outcome
                real_payout = real_shares * 1.0 if won_real else 0.0
                actual_pnl = real_payout - real_bet

                await self.safety.record_result(won_real, actual_pnl)
                self.real_pnl_total += actual_pnl
                self.real_trade_count += 1
                append_jsonl(TRADE_LOG, {
                    "timestamp": time.time(),
                    "type": "trade_settled",
                    "market_slug": slug,
                    "crypto_key": crypto_key,
                    "side": pos["side"],
                    "outcome": outcome,
                    "won": won_real,
                    "ptb_source": pos.get("ptb_source", "unknown"),
                    "ptb_trusted_at_entry": pos.get("ptb_trusted_at_entry", False),
                    "strike_gate_size_factor": pos.get("strike_gate_size_factor", 1.0),
                    "bet_amount": real_bet,
                    "payout": real_payout,
                    "net_profit": round(actual_pnl, 6),
                    "real_pnl_total": round(self.real_pnl_total, 6),
                    "order_id": order_result.get("order_id", ""),
                })
                del self._live_positions[slug]
                self._save_v21_state()
            return

        won = pos["side"] == outcome

        # Check if we have a real CLOB trade for this market
        live_pos = self._live_positions.get(slug)
        if live_pos and self.mode != "paper":
            # Use ACTUAL CLOB amounts (not paper-sized)
            real_bet = live_pos.get("trade_size_usd", 0)
            order_result = live_pos.get("order_result", {})
            # Prefer clean taking_amount from order_result (structured).
            # Fall back to legacy regex on raw_result for positions opened
            # before the schema change, then to bet/entry_price ratio.
            real_shares = float(order_result.get("taking_amount", 0) or 0)
            if real_shares <= 0:
                raw = str(order_result.get("raw_result", ""))
                taking_match = re.search(r"'takingAmount':\s*'([\d.]+)'", raw)
                real_shares = (
                    float(taking_match.group(1)) if taking_match
                    else (real_bet / pos["entry_price"] if pos["entry_price"] > 0 else 0)
                )
            real_payout = real_shares * 1.0 if won else 0.0
            # For CLOB trades, fee is already embedded in the fill price
            real_net = real_payout - real_bet
            net_profit_real = real_net
            logger.info(f"  [{cfg['name']}] CLOB P&L: bet=${real_bet:.4f} shares={real_shares:.4f} payout=${real_payout:.4f} net=${real_net:+.4f} {'WIN' if won else 'LOSS'}")
        else:
            net_profit_real = None  # paper trade, no real amounts
            real_shares = 0.0

        payout = calculate_payout(pos["shares"], won)
        fee = calculate_fee(pos["bet_amount"], pos["entry_price"], POLY_CRYPTO_FEE_RATE)
        net_profit = (payout - pos["bet_amount"] - fee) if won else (-pos["bet_amount"] - fee)
        self.bankroll += net_profit

        trade = {
            "market_slug": slug,
            "side": pos["side"],
            "entry_price": round(pos["entry_price"], 4),
            "estimated_prob": round(pos["estimated_prob"], 4),
            "actual_outcome": outcome,
            "won": won,
            "bet_amount": round(pos["bet_amount"], 4),
            "payout": round(payout, 4),
            "fee": round(fee, 4),
            "net_profit": round(net_profit, 4),
            "bankroll_after": round(self.bankroll, 4),
            "time_remaining": pos["time_remaining"],
            "crypto_key": pos.get("crypto_key", "btc"),
            "crypto_name": pos.get("crypto_name", "Bitcoin"),
            "asset_price": round(pos.get("asset_price", pos.get("btc_price", 0)), 6),
            "asset_distance": round(pos.get("asset_distance", pos.get("btc_distance", 0)), 6),
            "btc_price": round(pos.get("btc_price", pos.get("asset_price", 0)), 2),
            "btc_distance": round(pos.get("btc_distance", pos.get("asset_distance", 0)), 2),
            "timestamp": pos["entry_time"],
            "entry_source": pos.get("entry_source", "unknown"),
            "ptb_source": pos.get("ptb_source", "unknown"),
            "ptb_trusted_at_entry": pos.get("ptb_trusted_at_entry", False),
            "strike_gate_size_factor": pos.get("strike_gate_size_factor", 1.0),
            "mode": pos.get("mode", self.mode),
            "order_id": pos.get("order_result", {}).get("order_id", ""),
            "available_liquidity_usd": pos.get("available_liquidity_usd", 0),
            "liquidity_adjusted": pos.get("liquidity_adjusted", False),
            "original_size": pos.get("original_size", 0),
            "actual_size": pos.get("actual_size", 0),
        }
        # V7: NY session P&L tracking
        import datetime as _dty
        _hy = _dty.datetime.now(_dty.timezone.utc).hour
        _dy = _dty.datetime.now(_dty.timezone.utc).strftime('%Y-%m-%d')
        if self._ny_session_date != _dy:
            self._ny_session_pnl = 0.0; self._ny_session_halted = False; self._ny_session_date = _dy
        if NY_SESSION_START_UTC <= _hy < NY_SESSION_END_UTC:
            self._ny_session_pnl += trade.get('net_profit', 0.0)
            if self._ny_session_pnl <= NY_SESSION_LOSS_LIMIT and not self._ny_session_halted:
                self._ny_session_halted = True
                logger.warning(f'NY_SESSION_HALT: pnl=${self._ny_session_pnl:.2f}')
        self.trades.append(trade)
        self._save_trade(trade)

        # Performance log
        perf_data["actual_entry_price"] = pos["entry_price"]
        perf_data["entry_delay_ms"] = pos.get("entry_delay_ms", 0)
        perf_data["won"] = won
        perf_data["net_profit"] = net_profit
        perf_data["missed_reason"] = None
        self.perf.log_window(perf_data)

        result = "WIN" if won else "LOSS"
        logger.info(
            f"  *** {result} [{cfg['name']}|{info['tf']}] {slug} {pos['side']} @ "
            f"{pos['entry_price']:.3f} | P&L=${net_profit:+.4f} | "
            f"BR=${self.bankroll:.2f}"
        )

        # Update safety manager for real trades
        if self.mode != "paper" and slug in self._live_positions:
            # Use actual CLOB P&L if available, not paper-sized P&L
            actual_pnl = net_profit_real if net_profit_real is not None else net_profit
            await self.safety.record_result(won, actual_pnl)
            self.real_pnl_total += actual_pnl
            self.real_trade_count += 1
            # Log settlement to trades.jsonl for CLOB Order Log display
            live_pos = self._live_positions[slug]
            append_jsonl(TRADE_LOG, {
                "timestamp": time.time(),
                "type": "trade_settled",
                "market_slug": slug,
                "crypto_key": crypto_key,
                "side": pos["side"],
                "outcome": outcome,
                "won": won,
                "ptb_source": pos.get("ptb_source", "unknown"),
                "ptb_trusted_at_entry": pos.get("ptb_trusted_at_entry", False),
                "strike_gate_size_factor": pos.get("strike_gate_size_factor", 1.0),
                "bet_amount": live_pos.get("trade_size_usd", 0),
                "payout": (real_shares if won else 0.0),
                "net_profit": round(actual_pnl, 6),
                "real_pnl_total": round(self.real_pnl_total, 6),
                "order_id": live_pos.get("order_result", {}).get("order_id", ""),
            })
            del self._live_positions[slug]
            self._save_v21_state()

    async def _log_chainlink_strike_discrepancy(
        self, client: httpx.AsyncClient, slug: str, info: dict, crypto_key: str
    ):
        """Log Chainlink-vs-authoritative strike differences for audit purposes."""
        cfg = CRYPTO_CONFIGS.get(crypto_key, CRYPTO_CONFIGS["btc"])
        min_ptb = cfg.get("min_ptb", 1)
        chainlink_open = self._get_cached_chainlink_minute_open(
            crypto_key, info.get("start_ts", 0)
        )
        if not chainlink_open or chainlink_open <= min_ptb:
            return

        authoritative_ptb, authoritative_source = await self._get_price_to_beat(
            client,
            slug,
            question=info.get("question", ""),
            crypto_key=crypto_key,
        )
        if authoritative_source not in {"question", "rest_api", "gamma_metadata"}:
            return
        if authoritative_ptb <= min_ptb:
            return

        diff_abs = abs(chainlink_open - authoritative_ptb)
        diff_bps = (diff_abs / authoritative_ptb) * 10_000 if authoritative_ptb else 0.0
        logger.info(
            f"  [{cfg['name']}] {slug} → chainlink strike check: "
            f"chainlink=${chainlink_open:,.6g} vs {authoritative_source}=${authoritative_ptb:,.6g} "
            f"(Δ=${diff_abs:,.6g}, {diff_bps:.2f}bps)"
        )

    # ── Status Loop ───────────────────────────────────────────

    async def _daily_reset_loop(self):
        """Reset safety manager daily at midnight UTC."""
        while self.is_running:
            now = datetime.now(timezone.utc)
            # Calculate seconds until next midnight UTC
            tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0)
            if tomorrow <= now:
                from datetime import timedelta
                tomorrow += timedelta(days=1)
            seconds_until_reset = (tomorrow - now).total_seconds()
            logger.info("Daily reset scheduled in %.0f seconds (midnight UTC)", seconds_until_reset)
            await asyncio.sleep(seconds_until_reset)
            if self.is_running:
                await self.safety.daily_reset()
                logger.info("Daily safety reset completed")

    async def _status_loop(self):
        """Periodic status logging."""
        while self.is_running:
            await asyncio.sleep(30)
            # Trim long-lived state — settled_slugs in particular grows
            # unbounded across days (a few thousand markets per day) and
            # _prune was previously defined but never called.
            self._prune()
            # Refresh wallet balance if in live/shadow mode
            if self.mode != "paper" and self.trader and self.trader.is_ready:
                try:
                    self.wallet_balance = await self.trader.get_balance()
                except Exception:
                    pass
            active = sum(1 for i in self.active_markets.values() if not i["settled"])
            positions = sum(1 for i in self.active_markets.values() if i["position"] and not i["settled"])
            hours = (time.time() - self._started_at) / 3600 if self._started_at else 0
            rate = len(self.trades) / hours if hours > 0 else 0

            # Build price summary
            price_parts = []
            for ck in ACTIVE_CRYPTOS:
                p = self._get_crypto_price(ck)
                if p > 0:
                    name = CRYPTO_CONFIGS[ck]["name"][:3].upper()
                    if p > 100:
                        price_parts.append(f"{name}=${p:,.0f}")
                    else:
                        price_parts.append(f"{name}=${p:.4f}")
            price_summary = " ".join(price_parts) if price_parts else "no prices"

            logger.info(
                f"[STATUS] active={active} positions={positions} "
                f"trades={len(self.trades)} bankroll=${self.bankroll:.2f} "
                f"{price_summary} | "
                f"rate={rate:.1f}/hr | ws_evals={self._ws_evals} rest_evals={self._rest_evals} | "
                f"discovered={self._markets_discovered} qualified={self._markets_qualified} "
                f"traded={self._markets_traded} | "
                f"WS={'ON' if self.clob_ws._connected else 'OFF'}"
            )

    # ── Prune memory ──────────────────────────────────────────

    def _prune(self):
        if len(self.settled_slugs) > 2000:
            self.settled_slugs = set(list(self.settled_slugs)[-1000:])
        # Drop per-market locks for slugs that are no longer active.
        # We retain locks for slugs in active_markets (even settled-but-
        # not-yet-cleaned ones) so any in-flight evaluator continues to
        # serialize correctly. Stale entries are safe to drop.
        if len(self._market_locks) > 500:
            stale_locks = [
                s for s in list(self._market_locks.keys())
                if s not in self.active_markets
            ]
            for s in stale_locks:
                # Don't drop locks that are currently held — that would let
                # a fresh _get_market_lock() call hand out a different Lock.
                lock = self._market_locks[s]
                if not lock.locked():
                    del self._market_locks[s]


    def _get_crypto_pnl(self) -> dict:
        """Compute per-crypto P&L from trades."""
        pnl = {}
        for crypto_key in ACTIVE_CRYPTOS:
            cfg = CRYPTO_CONFIGS[crypto_key]
            trades = [t for t in self.trades if t.get("crypto_key", "btc") == crypto_key]
            total_pnl = sum(t.get("net_profit", 0) for t in trades)
            wins = sum(1 for t in trades if t.get("won"))
            pnl[crypto_key] = {
                "name": cfg["name"],
                "trades": len(trades),
                "wins": wins,
                "losses": len(trades) - wins,
                "pnl": round(total_pnl, 4),
                "win_rate": round(wins / len(trades) * 100, 2) if trades else 0.0,
            }
        return pnl

    # ── Dashboard State ───────────────────────────────────────

    def get_state(self) -> dict:
        wins = sum(1 for t in self.trades if t["won"])
        losses = len(self.trades) - wins
        hours = (time.time() - self._started_at) / 3600 if self._started_at else 0

        active_positions = []
        for slug, info in self.active_markets.items():
            if info["position"] and not info["settled"]:
                pos = info["position"]
                remaining = max(0, info["end_ts"] - int(time.time()))
                active_positions.append({
                    "slug": slug,
                    "tf": info["tf"],
                    "side": pos["side"],
                    "entry_price": pos["entry_price"],
                    "bet_amount": round(pos["bet_amount"], 2),
                    "time_remaining": remaining,
                    "entry_source": pos.get("entry_source", ""),
                })

        # Active tracking list
        tracking = []
        for slug, info in self.active_markets.items():
            if not info["settled"]:
                remaining = max(0, info["end_ts"] - int(time.time()))
                tracking.append({
                    "slug": slug,
                    "tf": info["tf"],
                    "time_remaining": remaining,
                    "has_position": info["position"] is not None,
                    "discovery_source": info.get("discovery_source", ""),
                })

        # Discovery stats
        disc = self.discovery
        avg_disc_ms = sum(disc.discovery_times) / len(disc.discovery_times) if disc.discovery_times else 0

        return {
            "bankroll": round(self.bankroll, 4),
            "initial_bankroll": self.initial_bankroll,
            "total_trades": len(self.trades),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / len(self.trades) * 100, 2) if self.trades else 0.0,
            "total_profit": round(self.bankroll - self.initial_bankroll, 4),
            "is_running": self.is_running,
            "btc_price": self._btc_price,
            "recent_trades": list(self.trades) if self.trades else [],
            "version": VERSION,
            "active_cryptos": ACTIVE_CRYPTOS,
            # V2.1 specific
            "active_positions": active_positions,
            "tracking": tracking,
            "tracked_markets": len([i for i in self.active_markets.values() if not i["settled"]]),
            "hours_running": round(hours, 2),
            "trade_rate_per_hour": round(len(self.trades) / hours, 2) if hours > 0.01 else 0,
            # Discovery stats
            "discovery": {
                "total_discovered": self._markets_discovered,
                "predicted": disc.predicted_count,
                "fallback": disc.fallback_count,
                "avg_discovery_ms": round(avg_disc_ms, 1),
            },
            # Evaluation stats
            "evaluations": {
                "total": self._total_evals,
                "ws_triggered": self._ws_evals,
                "rest_fallback": self._rest_evals,
            },
            # Coverage stats
            "coverage": {
                "markets_discovered": self._markets_discovered,
                "markets_qualified": self._markets_qualified,
                "markets_traded": self._markets_traded,
                "coverage_pct": round(self._markets_traded / self._markets_qualified * 100, 1) if self._markets_qualified > 0 else 0,
            },
            # Entry quality
            "entry_quality": {
                "avg_entry_price": round(
                    sum(t["entry_price"] for t in self.trades) / len(self.trades), 4
                ) if self.trades else 0,
                "best_entry_price": round(
                    min(t["entry_price"] for t in self.trades), 4
                ) if self.trades else 0,
                "entries_below_070": sum(1 for t in self.trades if t["entry_price"] < 0.70),
                "entries_below_080": sum(1 for t in self.trades if t["entry_price"] < 0.80),
            },
            # WS status
            "ws_connected": self.clob_ws._connected,
            "ws_messages": self.clob_ws._msg_count,
            # Multi-crypto prices
            "crypto_prices": {k: round(v, 6) for k, v in self._crypto_prices.items()},
            # Per-crypto P&L
            "crypto_pnl": self._get_crypto_pnl(),
            # Binance multi-feed status
            "binance_multi_feed": {
                "connected": self.binance_feed._connected if self.binance_feed else False,
                "messages": self.binance_feed._msg_count if self.binance_feed else 0,
            },
            # Trading mode
            "mode": self.mode,
            "trader_ready": self.trader.is_ready if self.trader else False,
            "wallet_balance": self.wallet_balance,
            "real_pnl_total": round(self.real_pnl_total, 4),
            "real_trade_count": self.real_trade_count,
            "live_positions": len(self._live_positions),
            "safety": self.safety.get_state() if self.safety else {},
        }


# ═══════════════════════════════════════════════════════════════
#  CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════

async def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Polymarket Frequency-Optimized Trader (paper / shadow / live).",
    )
    parser.add_argument(
        "--mode", choices=["paper", "shadow", "live"], default=None,
        help="Override TRADING_MODE from config.py.",
    )
    parser.add_argument(
        "--port", type=int, default=None,
        help="Accepted for compatibility with restart scripts; the dashboard "
             "listens on DASHBOARD_PORT from config. The trader itself does "
             "not bind a port.",
    )
    args = parser.parse_args()

    trader = FrequencyOptimizedTrader()
    if args.mode:
        trader.mode = args.mode
        logger.info("Trade mode overridden via --mode: %s", args.mode)
    if args.port is not None:
        logger.info("--port=%d ignored by trader (set DASHBOARD_PORT in config)", args.port)
    await trader.run()


if __name__ == "__main__":
    asyncio.run(main())
