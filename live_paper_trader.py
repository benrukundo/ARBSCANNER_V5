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
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from redeem import run_redeem_cycle, get_redeem_status

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
    WS_CLOB_URL, WS_RECONNECT_DELAY, WS_FALLBACK_POLL_INTERVAL,
    SLUG_DURATION_MAP,
    CRYPTO_CONFIGS, ACTIVE_CRYPTOS, TRADING_MODE, SHADOW_TRADE_SIZE, MAX_TRADE_SIZE, MIN_TRADE_SIZE, TRADE_LOG, STATE_FILE,
    )
from strategy import should_enter_staged, calculate_fee, calculate_payout
from trading_client import PolymarketTrader, append_jsonl
from safety import SafetyManager

logger = logging.getLogger("v21.trader")
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
                        best_ask = min(float(a.get("price", 999)) for a in asks if float(a.get("size", 0)) > 0)
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
        self._last_btc_update: float = 0.0
        self.binance_feed: BinanceMultiFeed | None = None
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

    # ── WS Price Callback ─────────────────────────────────────

    def _on_ws_price(self, token_id: str, price: float):
        """Called by CLOB WS on each price tick — queue evaluation."""
        if self._ws_eval_queue and not self._ws_eval_queue.full():
            try:
                self._ws_eval_queue.put_nowait(token_id)
            except asyncio.QueueFull:
                pass


    def _on_binance_price(self, crypto_key: str, price: float, ts_ms: int):
        """Called by BinanceMultiFeed on each price tick."""
        self._crypto_prices[crypto_key] = price
        # Feed rolling volatility calculator
        self.rolling_vol.add_price(crypto_key, price, time.time())
        if crypto_key == "btc":
            self._btc_price = price           # backward compat
            self._last_btc_update = time.time()

    def _get_crypto_price(self, crypto_key: str) -> float:
        """Get current price for a crypto asset."""
        return self._crypto_prices.get(crypto_key, 0.0)

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

    async def _get_price_to_beat(
        self, client: httpx.AsyncClient, slug: str,
        question: str = "", crypto_key: str = "btc",
    ) -> float:
        """Get the strike price for a crypto up/down market.

        Priority: 1) Parse from question text, 2) REST API, 3) current price fallback.
        """
        cfg = CRYPTO_CONFIGS.get(crypto_key, CRYPTO_CONFIGS["btc"])
        min_ptb = cfg.get("min_ptb", 1)

        # 1) Parse from question text (most reliable)
        if question:
            parsed = _extract_price_from_question(question)
            if parsed and parsed > min_ptb:
                return parsed

        # 2) Try REST endpoint
        try:
            resp = await client.get(
                f"https://polymarket.com/api/equity/price-to-beat/{slug}",
            )
            resp.raise_for_status()
            ptb = float(resp.json().get("price", 0))
            if ptb > min_ptb:
                return ptb
        except Exception:
            pass

        # 3) Fallback: use current crypto price (if available)
        price = self._get_crypto_price(crypto_key)
        if price > min_ptb:
            return price
        return 0  # Caller must handle 0 = unknown

    async def _check_resolution(self, client: httpx.AsyncClient, slug: str) -> str | None:
        try:
            resp = await client.get(f"{GAMMA_API}/events", params={"slug": slug})
            resp.raise_for_status()
            events = resp.json()
            if not events:
                return None
            markets = events[0].get("markets", [])
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

    async def _fetch_market_details(self, client: httpx.AsyncClient, slug: str) -> dict | None:
        """Fetch market details (token IDs, condition_id) from Gamma API."""
        fetch_start = time.time()
        try:
            resp = await client.get(f"{GAMMA_API}/events", params={"slug": slug})
            if resp.status_code != 200:
                return None
            events = resp.json()
            if not events or not isinstance(events, list):
                return None

            event = events[0]
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
            return {
                "question": mkt.get("question", event.get("title", "")),
                "condition_id": mkt.get("conditionId", ""),
                "up_token_id": up_token,
                "down_token_id": down_token,
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
                    # Force balance refresh
                    if hasattr(self, "trader") and self.trader:
                        self.trader._balance_cache_time = 0
            except Exception as e:
                logger.error("Redeem loop error: %s", e)

            await asyncio.sleep(120)  # Check every 2 minutes

    async def run(self):
        self.is_running = True
        self._started_at = time.time()
        self._ws_eval_queue = asyncio.Queue(maxsize=1000)

        logger.info(f"V2.1 Frequency-Optimized Trader starting | MODE={self.mode.upper()}")
        logger.info(f"  Bankroll: ${self.bankroll:.2f}")
        logger.info(f"  Strategy: {self.params}")
        logger.info(f"  Staged entry: {STAGED_ENTRY}")

        # Start Binance multi-crypto feed
        self.binance_feed = BinanceMultiFeed(
            crypto_keys=ACTIVE_CRYPTOS,
            on_price_callback=self._on_binance_price,
        )

        tasks = [
            asyncio.create_task(self._discovery_loop(), name="discovery"),
            asyncio.create_task(self._ws_evaluation_loop(), name="ws-eval"),
            asyncio.create_task(self._rest_evaluation_loop(), name="rest-eval"),
            asyncio.create_task(self._settlement_loop(), name="settlement"),
            asyncio.create_task(self._btc_price_loop(), name="btc-price"),
            asyncio.create_task(self.binance_feed.connect(), name="binance-multi-feed"),
            asyncio.create_task(self.clob_ws.run(), name="clob-ws"),
            asyncio.create_task(self._status_loop(), name="status"),
            asyncio.create_task(self._daily_reset_loop(), name="daily-reset"),
            asyncio.create_task(self._redeem_loop(), name="auto-redeem"),
        ]

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
        # Wait for BTC price before starting discovery
        logger.info("Discovery waiting for price feeds...")
        while self.is_running and self._get_crypto_price("btc") <= 0:
            await asyncio.sleep(0.5)
        logger.info(f"Discovery started, BTC=${self._get_crypto_price('btc'):,.2f}")
        logger.info(f"Active cryptos: {ACTIVE_CRYPTOS}")

        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT) as client:
            while self.is_running:
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

                        disc_start = time.time()
                        details = await self._fetch_market_details(client, slug)

                        if details is None:
                            # Market not yet on API — mark as tried, will retry
                            # (don't add to prefetched so we'll retry next cycle)
                            continue

                        self.discovery.mark_prefetched(slug)
                        disc_ms = (time.time() - disc_start) * 1000
                        self.discovery.record_discovery(disc_ms, cand["source"])

                        # Get price-to-beat from question text or API
                        crypto_key = cand.get("crypto_key", "btc")
                        cfg = CRYPTO_CONFIGS.get(crypto_key, CRYPTO_CONFIGS["btc"])
                        min_ptb = cfg.get("min_ptb", 1)

                        ptb = await self._get_price_to_beat(
                            client, slug, question=details["question"],
                            crypto_key=crypto_key,
                        )
                        if ptb <= min_ptb:
                            # Still couldn't get a valid price, skip
                            logger.warning(f"  [{slug}] No valid price_to_beat for {cfg['name']}, skipping")
                            continue

                        # Register market
                        self.active_markets[slug] = {
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
                            "position": None,
                            "settled": False,
                            "settled_at": 0,
                            "discovered_at": time.time(),
                            "discovery_source": cand["source"],
                            "discovery_delay_ms": disc_ms,
                            "first_qualified_at": None,
                            "best_qualifying_price": None,
                        }
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
                            f"source={cand['source']} ({disc_ms:.0f}ms)"
                        )

                except Exception as e:
                    logger.error(f"Discovery error: {e}", exc_info=True)

                await asyncio.sleep(PREDICTIVE_CHECK_INTERVAL)

    # ── BTC Price Loop ────────────────────────────────────────

    async def _btc_price_loop(self):
        """Keep crypto prices fresh via Binance REST (fallback for WS)."""
        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT) as client:
            while self.is_running:
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
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.debug(f"WS eval error: {e}")

    # ── REST Fallback Evaluation ──────────────────────────────

    async def _rest_evaluation_loop(self):
        """Periodic REST-based evaluation as fallback to WS."""
        async with httpx.AsyncClient(timeout=FETCH_TIMEOUT) as client:
            while self.is_running:
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

            # Reject below-minimum entries (MIN_ENTRY_PRICE=0.80 filters coin-flip bets)
            if share_price < MIN_ENTRY_PRICE:
                logger.warning(f"  [{slug}] REJECTED price {share_price:.4f} < MIN_ENTRY_PRICE={MIN_ENTRY_PRICE}")
                return

            # ── Dynamic trade sizing based on order book depth ──
            # Cap trade size to what can be filled cleanly within acceptable price.
            # Identical logic for paper and live — paper P&L matches live.
            distance = abs(asset_price - info["price_to_beat"])
            desired_size = min(self.bankroll * crypto_params["POSITION_SIZE"], MAX_TRADE_SIZE)

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
                        liq_source = "REST"
                except Exception as liq_err:
                    logger.warning(f"  [{slug}] Liquidity check failed ({liq_err}), using desired size")
                    available_usd = desired_size  # allow trade if check fails
                    liq_source = "fallback"

            # Cap trade size to available liquidity
            actual_size = min(desired_size, available_usd)
            liquidity_adjusted = actual_size < desired_size

            if available_usd <= 0:
                logger.info(f"  [{slug}] SKIP: no asks at <={max_acceptable_price:.2f} — would not fill")
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


    # ── Real Trade Execution (shadow/live mode) ───────────────

    async def _execute_real_trade(self, slug: str, info: dict, side: str,
                                  share_price: float, est_prob: float,
                                  time_remaining: float, asset_price: float,
                                  crypto_key: str):
        """Execute a real trade on Polymarket CLOB (shadow or live mode)."""
        if self.mode == "paper":
            return

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
            return

        # Determine trade size (respects liquidity cap from paper evaluation)
        if self.mode == "shadow":
            trade_size = SHADOW_TRADE_SIZE
        elif self.mode == "live":
            balance = await self.trader.get_balance()
            # Use liquidity-capped size from position info, then apply live caps
            liq_capped = info.get("position", {}).get("actual_size", MAX_TRADE_SIZE)
            trade_size = min(balance * 0.05, liq_capped, MAX_TRADE_SIZE)
        else:
            return

        if trade_size < 1.0 or (self.mode == "live" and trade_size > balance):
            logger.info("[%s] Trade size $%.2f not viable (balance=$%.2f), skipping",
                        cfg["name"], trade_size, await self.trader.get_balance() if self.mode == "live" else 0)
            return

        # Token ID
        if side == "Up":
            token_id = info.get("up_token_id", "")
        else:
            token_id = info.get("down_token_id", "")

        if not token_id:
            logger.error("[%s] No token_id for side=%s in %s",
                         cfg["name"], side, slug)
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
            outcome = "Up" if asset_price >= info["price_to_beat"] else "Down"
            logger.info(f"  [{cfg['name']}] {slug} → inferred {outcome}")
        else:
            logger.info(f"  {slug} → resolved {outcome}")

        info["settled"] = True
        info["settled_at"] = now
        self.settled_slugs.add(slug)

        # Clean up per-market lock to prevent memory leak
        if slug in self._market_locks:
            del self._market_locks[slug]

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
        won = pos["side"] == outcome

        # Check if we have a real CLOB trade for this market
        live_pos = self._live_positions.get(slug)
        if live_pos and self.mode != "paper":
            # Use ACTUAL CLOB amounts (not paper-sized)
            real_bet = live_pos.get("trade_size_usd", 0)
            # Get real shares from order result's takingAmount
            raw = str(live_pos.get("order_result", {}).get("raw_result", ""))
            taking_match = re.search(r"'takingAmount':\s*'([\d.]+)'", raw)
            real_shares = float(taking_match.group(1)) if taking_match else (real_bet / pos["entry_price"] if pos["entry_price"] > 0 else 0)
            real_payout = real_shares * 1.0 if won else 0.0
            # For CLOB trades, fee is already embedded in the fill price
            real_net = real_payout - real_bet
            net_profit_real = real_net
            logger.info(f"  [{cfg['name']}] CLOB P&L: bet=${real_bet:.4f} shares={real_shares:.4f} payout=${real_payout:.4f} net=${real_net:+.4f} {'WIN' if won else 'LOSS'}")
        else:
            net_profit_real = None  # paper trade, no real amounts

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
            "mode": pos.get("mode", self.mode),
            "order_id": pos.get("order_result", {}).get("order_id", ""),
            "available_liquidity_usd": pos.get("available_liquidity_usd", 0),
            "liquidity_adjusted": pos.get("liquidity_adjusted", False),
            "original_size": pos.get("original_size", 0),
            "actual_size": pos.get("actual_size", 0),
        }
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
                "bet_amount": live_pos.get("trade_size_usd", 0),
                "payout": (float(taking_match.group(1)) if taking_match else 0) if won else 0,
                "net_profit": round(actual_pnl, 6),
                "real_pnl_total": round(self.real_pnl_total, 6),
                "order_id": live_pos.get("order_result", {}).get("order_id", ""),
            })
            del self._live_positions[slug]
            self._save_v21_state()

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
            "recent_trades": self.trades[-30:] if self.trades else [],
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
    trader = FrequencyOptimizedTrader()
    await trader.run()


if __name__ == "__main__":
    asyncio.run(main())
