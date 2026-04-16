"""Trading Client for V2.1 — wraps py-clob-client for real order execution."""
import json
import logging
import os
import time

from config import (
    CLOB_HOST, CHAIN_ID,
    PRIVATE_KEY, FUNDER_ADDRESS, SIGNATURE_TYPE,
    API_KEY, API_SECRET, API_PASSPHRASE,
    TRADE_LOG, ERROR_LOG, DATA_DIR,
)

logger = logging.getLogger("v21.trading_client")


def append_jsonl(filepath: str, record: dict):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "a") as f:
        f.write(json.dumps(record) + "\n")


def log_error(message: str, exc: Exception | None = None):
    record = {"timestamp": time.time(), "error": message}
    if exc:
        record["exception"] = str(exc)
    append_jsonl(ERROR_LOG, record)
    logger.error(message, exc_info=exc)


class PolymarketTrader:
    """Wraps py-clob-client ClobClient for Polymarket CLOB order execution."""

    def __init__(self):
        self.client = None
        self._initialized = False
        self._balance_cache = (0.0, 0)  # (balance, timestamp)

        if not PRIVATE_KEY or not FUNDER_ADDRESS:
            logger.warning("No wallet credentials — trading client disabled (paper-only)")
            return

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            # Try loading cached API creds
            creds_file = os.path.join(DATA_DIR, "api_creds.json")
            api_key = API_KEY
            api_secret = API_SECRET
            api_passphrase = API_PASSPHRASE

            if not api_key:
                # Try loading from cache
                if os.path.exists(creds_file):
                    with open(creds_file) as f:
                        cached = json.load(f)
                    api_key = cached.get("apiKey", "")
                    api_secret = cached.get("secret", "")
                    api_passphrase = cached.get("passphrase", "")
                    logger.info("Loaded cached API credentials")

            if not api_key:
                # Derive new API creds
                logger.info("Deriving new API credentials...")
                temp_client = ClobClient(
                    CLOB_HOST, key=PRIVATE_KEY,
                    chain_id=CHAIN_ID,
                    funder=FUNDER_ADDRESS,
                    signature_type=SIGNATURE_TYPE,
                )
                creds = temp_client.derive_api_key()
                api_key = creds.get("apiKey", "")
                api_secret = creds.get("secret", "")
                api_passphrase = creds.get("passphrase", "")

                # Cache for next restart
                os.makedirs(DATA_DIR, exist_ok=True)
                with open(creds_file, "w") as f:
                    json.dump(creds, f, indent=2)
                logger.info("API credentials derived and cached")

            # Create authenticated client
            api_creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            )
            self.client = ClobClient(
                CLOB_HOST, key=PRIVATE_KEY,
                chain_id=CHAIN_ID,
                signature_type=SIGNATURE_TYPE,
                funder=FUNDER_ADDRESS,
                creds=api_creds,
            )
            self._initialized = True

            # ── Fee support check (CRITICAL for crypto fee-enabled markets) ──
            # Polymarket crypto markets require feeRateBps in signed orders.
            # If py-clob-client is too old, orders WILL be rejected.
            self._fee_supported = True
            try:
                import py_clob_client
                clob_version = getattr(py_clob_client, "__version__", "0.0.0")
                from packaging.version import Version
                if Version(clob_version) < Version("0.15.0"):
                    self._fee_supported = False
                    logger.critical(
                        "py-clob-client version %s is too old — feeRateBps not supported. "
                        "Orders WILL be rejected on fee-enabled markets. "
                        "Please upgrade: pip install --upgrade py-clob-client",
                        clob_version
                    )
                else:
                    logger.info("py-clob-client version %s — fee support OK", clob_version)
            except ImportError:
                logger.info("packaging not installed — skipping version check, assuming fee support OK")
            except Exception as ve:
                logger.warning("Fee version check failed: %s — assuming OK", ve)

            logger.info("PolymarketTrader initialized (CLOB client ready)")

        except Exception as e:
            log_error(f"Failed to initialize trading client: {e}", e)

    @property
    def is_ready(self) -> bool:
        return self._initialized and self.client is not None

    async def get_balance(self) -> float:
        """Get USDC balance (cached for 30s)."""
        now = time.time()
        if now - self._balance_cache[1] < 30:
            return self._balance_cache[0]
        try:
            raw = self.client.get_balance_allowance()
            balance = float(raw.get("balance", 0)) / 1e6  # USDC 6 decimals
            self._balance_cache = (balance, now)
            logger.info("Wallet balance: $%.2f", balance)
            return balance
        except Exception as e:
            log_error(f"Balance check failed: {e}", e)
            return self._balance_cache[0]

    async def place_order(self, token_id: str, side: str, size_usd: float,
                          price: float, market_slug: str = "") -> dict:
        """Place a FAK order on the CLOB."""
        from py_clob_client.clob_types import OrderArgs, OrderType


        # ── CRITICAL: Verify fee support before placing any order ──
        if hasattr(self, "_fee_supported") and not self._fee_supported:
            logger.error("REFUSING to place order — py-clob-client too old for feeRateBps")
            return {
                "success": False,
                "order_id": None,
                "status": "fee_unsupported",
                "error": "py-clob-client version too old for fee support",
                "filled_size": 0,
                "intended_size": size_usd,
                "fill_ratio": 0,
                "slippage": 0,
            }

        # Query current fee rate from CLOB API
        try:
            import httpx
            fee_resp = httpx.get(
                f"https://clob.polymarket.com/fee-rate?token_id={token_id}",
                timeout=5,
            )
            fee_data = fee_resp.json()
            logger.info("Fee rate for %s: %s", token_id[:16], fee_data)
        except Exception as fe:
            logger.warning("Fee rate query failed: %s — proceeding with order", fe)

        append_jsonl(TRADE_LOG, {
            "timestamp": time.time(),
            "type": "order_intent",
            "token_id": token_id,
            "side": side,
            "size_usd": size_usd,
            "price": price,
            "market_slug": market_slug,
        })

        try:
            num_shares = size_usd / price
            logger.info("Placing order: %s %.4f shares @ %.4f ($%.2f) | %s",
                        side, num_shares, price, size_usd, market_slug)

            # Get tick size for rounding
            tick_size = "0.01"
            try:
                market_info = self.client.get_market(token_id)
                if market_info and "minimum_tick_size" in market_info:
                    tick_size = str(market_info["minimum_tick_size"])
            except Exception:
                pass

            # Round price to tick size
            tick = float(tick_size)
            rounded_price = round(round(price / tick) * tick, 4)

            order_args = OrderArgs(
                token_id=token_id,
                price=rounded_price,
                size=round(num_shares, 2),
                side=side,
            )

            signed_order = self.client.create_order(order_args)
            result = self.client.post_order(signed_order, order_type=OrderType.FAK)


            order_id = result.get("orderID", result.get("id", "unknown"))
            status = result.get("status", "unknown")

            # ── Track actual fill amounts from FAK orders ──
            filled_amount = float(result.get("filled_size", result.get("matchedAmount", 0)))
            intended_amount = size_usd
            fill_ratio = filled_amount / intended_amount if intended_amount > 0 else 0
            slippage = abs(rounded_price - price) if filled_amount > 0 else 0

            logger.info("Fill ratio: %.1f%% (filled $%.2f of $%.2f)",
                        fill_ratio * 100, filled_amount, intended_amount)

            append_jsonl(TRADE_LOG, {
                "timestamp": time.time(),
                "type": "order_result",
                "order_id": order_id,
                "status": status,
                "token_id": token_id,
                "side": side,
                "size_usd": size_usd,
                "price": rounded_price,
                "market_slug": market_slug,
                "filled_size": filled_amount,
                "intended_size": intended_amount,
                "fill_ratio": fill_ratio,
                "raw_result": str(result)[:500],
            })

            logger.info("Order result: id=%s status=%s", order_id, status)
            return {
                "success": status not in ("error", "failed"),
                "order_id": order_id,
                "status": status,
                "filled_size": filled_amount,
                "intended_size": intended_amount,
                "fill_ratio": fill_ratio,
                "slippage": slippage,
                "avg_price": rounded_price,
            }

        except Exception as e:
            log_error(f"Order placement failed: {e}", e)
            append_jsonl(TRADE_LOG, {
                "timestamp": time.time(),
                "type": "order_error",
                "error": str(e),
                "token_id": token_id,
                "side": side,
                "size_usd": size_usd,
            })
            return {
                "success": False,
                "order_id": None,
                "status": "exception",
                "error": str(e),
            }

    async def get_open_orders(self) -> list:
        """Get open/live orders."""
        try:
            orders = self.client.get_orders()
            return [o for o in orders if o.get("status") in ("open", "live")]
        except Exception as e:
            log_error(f"Failed to get open orders: {e}", e)
            return []

    async def cancel_all_orders(self):
        """Emergency cancel all orders."""
        try:
            result = self.client.cancel_all()
            logger.info("All orders cancelled: %s", result)
            return result
        except Exception as e:
            log_error(f"Cancel all failed: {e}", e)

    async def send_heartbeat(self):
        """Refresh API creds / keep connection alive."""
        try:
            if self.client:
                self.client.derive_api_key()
                logger.debug("Heartbeat sent")
        except Exception as e:
            log_error(f"Heartbeat failed: {e}", e)
