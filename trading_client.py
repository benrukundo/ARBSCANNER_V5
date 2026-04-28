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
    PRICE_BUFFER, MIN_ENTRY_PRICE,
    MAX_ENTRY_PRICE, MAX_ENTRY_PRICE_ABS, MIN_EDGE_POST_BUFFER,
    POLY_CRYPTO_FEE_RATE,
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
                # derive_api_key() may return ApiCreds object or dict
                if hasattr(creds, 'api_key'):
                    # ApiCreds object
                    api_key = creds.api_key
                    api_secret = creds.api_secret
                    api_passphrase = creds.api_passphrase
                elif isinstance(creds, dict):
                    api_key = creds.get("apiKey", "")
                    api_secret = creds.get("secret", "")
                    api_passphrase = creds.get("passphrase", "")
                else:
                    raise ValueError(f"Unexpected creds type: {type(creds)}")

                # Cache for next restart
                os.makedirs(DATA_DIR, exist_ok=True)
                creds_dict = {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}
                with open(creds_file, "w") as f:
                    json.dump(creds_dict, f, indent=2)
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
                try:
                    from importlib.metadata import version as _get_version
                    clob_version = _get_version("py-clob-client")
                except Exception:
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
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=SIGNATURE_TYPE)
            raw = self.client.get_balance_allowance(params)
            logger.info("RAW balance response: %s", raw)
            balance = float(raw.get("balance", 0)) / 1e6  # USDC 6 decimals
            self._balance_cache = (balance, now)
            logger.info("Wallet balance: $%.2f", balance)
            return balance
        except Exception as e:
            log_error(f"Balance check failed: {e}", e)
            return self._balance_cache[0]

    async def place_order(self, token_id: str, side: str, size_usd: float,
                          price: float, market_slug: str = "",
                          estimated_prob: float = None) -> dict:
        """Place a FAK order on the CLOB using MarketOrderArgs (USDC-based).

        Uses create_market_order instead of create_order to avoid CLOB API
        'invalid amounts' errors. MarketOrderArgs takes USDC amount directly,
        ensuring maker_amount (USDC) stays at ≤2 decimal precision.
        """
        from py_clob_client.clob_types import MarketOrderArgs, OrderType

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

        # Query current fee rate from CLOB API (non-blocking)
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5) as _client:
                fee_resp = await _client.get(
                    f"https://clob.polymarket.com/fee-rate?token_id={token_id}",
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
            # Round USDC amount to 2 decimals (cents) — ensures maker precision
            amount = round(size_usd, 2)

            # ── Apply PRICE_BUFFER for reliable FAK fills ──
            # MAX_ENTRY_PRICE, MAX_ENTRY_PRICE_ABS, MIN_EDGE_POST_BUFFER imported
            # from config. NOTE: the post-buffer edge check below uses
            # MIN_EDGE_POST_BUFFER (a lenient safety net, not MIN_EDGE). The
            # strategy entry gate in strategy.py already enforced MIN_EDGE on
            # the raw ask; here we only verify the buffer didn't eat ALL the
            # edge. Applying MIN_EDGE again here would charge the buffer cost
            # twice against the edge budget and reject profitable trades.

            market_ask = round(price, 2)
            buffered_price = round(min(market_ask + PRICE_BUFFER, MAX_ENTRY_PRICE), 2)
            logger.info("Price buffer: market_ask=%.4f -> order=%.4f (buffer=%.2f)",
                        market_ask, buffered_price, PRICE_BUFFER)

            # Safety check: cap at MAX_ENTRY_PRICE
            if buffered_price > MAX_ENTRY_PRICE:
                logger.warning("Buffered price %.4f > MAX_ENTRY_PRICE %.2f — capping",
                               buffered_price, MAX_ENTRY_PRICE)
                buffered_price = MAX_ENTRY_PRICE

            # Safety check: reject above MAX_ENTRY_PRICE_ABS
            if buffered_price > MAX_ENTRY_PRICE_ABS:
                logger.warning("Buffered price %.4f > MAX_ENTRY_PRICE_ABS %.2f — rejecting",
                               buffered_price, MAX_ENTRY_PRICE_ABS)
                return {
                    "success": False, "order_id": None, "status": "price_too_high",
                    "filled_size": 0, "intended_size": size_usd, "fill_ratio": 0, "slippage": 0,
                }

            # Recalculate edge after buffer — circuit-breaker, not quality filter.
            # See comment above re: why this uses MIN_EDGE_POST_BUFFER not MIN_EDGE.
            if estimated_prob is not None:
                edge_after_buffer = estimated_prob - buffered_price
                logger.info(
                    "Edge after buffer: est_prob=%.4f - buffered=%.4f = edge=%.4f (min_post_buffer=%.2f)",
                    estimated_prob, buffered_price, edge_after_buffer, MIN_EDGE_POST_BUFFER,
                )
                if edge_after_buffer < MIN_EDGE_POST_BUFFER:
                    logger.info(
                        "SKIPPED: edge_after_buffer=%.4f < MIN_EDGE_POST_BUFFER=%.2f — buffer eats the edge",
                        edge_after_buffer, MIN_EDGE_POST_BUFFER,
                    )
                    return {
                        "success": False, "order_id": None, "status": "edge_too_thin_after_buffer",
                        "filled_size": 0, "intended_size": size_usd, "fill_ratio": 0, "slippage": 0,
                        "edge_after_buffer": edge_after_buffer,
                    }

            # Recalculate fee using buffered price (informational logging only).
            fee = (
                (amount / buffered_price) * POLY_CRYPTO_FEE_RATE * buffered_price * (1 - buffered_price)
                if buffered_price > 0 else 0
            )
            logger.info("Fee at buffered price: $%.4f (%.2f%%)", fee, (fee / amount * 100) if amount > 0 else 0)

            rounded_price = buffered_price
            logger.info("Placing market order: %s $%.2f @ %.4f (ask=%.4f + buffer) | %s",
                        side, amount, rounded_price, market_ask, market_slug)

            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                price=rounded_price,
                side=side,
            )

            signed_order = self.client.create_market_order(order_args)
            result = self.client.post_order(signed_order, orderType=OrderType.FAK)

            order_id = result.get("orderID", result.get("id", "unknown"))
            status = result.get("status", "unknown")

            # ── Track actual fill amounts from FAK orders ──
            # CLOB response keys: makingAmount (USDC paid), takingAmount (shares),
            # status ("matched" = filled), success (True/False)
            intended_amount = size_usd
            is_success = result.get("success", False)
            matched = result.get("status", "") == "matched"

            if is_success and matched:
                # Order filled — makingAmount = USDC paid (for BUY)
                making = result.get("makingAmount", "0")
                filled_amount = float(making) if making else 0.0
                if filled_amount <= 0:
                    filled_amount = intended_amount  # fallback — success=True means it filled
                logger.info("Order FILLED: makingAmount=%s takingAmount=%s txns=%s",
                            making, result.get("takingAmount", "0"),
                            result.get("transactionsHashes", []))
            else:
                # Not matched — try legacy keys as fallback
                filled_amount = float(result.get("filled_size", result.get("matchedAmount", 0)))

            fill_ratio = filled_amount / intended_amount if intended_amount > 0 else 0

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
                "market_ask": market_ask,
                "buffered_price": buffered_price,
                "price_buffer": PRICE_BUFFER,
                "market_slug": market_slug,
                "filled_size": filled_amount,
                "intended_size": intended_amount,
                "fill_ratio": fill_ratio,
                "estimated_prob": estimated_prob,
                "raw_result": str(result)[:500],
            })

            logger.info("Order result: id=%s status=%s", order_id, status)
            # Pull takingAmount (shares filled) structurally — avoids the
            # fragile regex parse on truncated str(result) in settlement.
            try:
                taking_amount = float(result.get("takingAmount", 0) or 0)
            except (ValueError, TypeError):
                taking_amount = 0.0
            return {
                "success": status not in ("error", "failed"),
                "order_id": order_id,
                "status": status,
                "filled_size": filled_amount,
                "intended_size": intended_amount,
                "fill_ratio": fill_ratio,
                "slippage": 0,
                "avg_price": price,
                "taking_amount": taking_amount,
            }

        except Exception as e:
            error_str = str(e)
            # FAK/GTC no-fill: order was valid but no matching liquidity
            # Extract orderID from error if present (order WAS accepted)
            import re as _re
            oid_match = _re.search(r'orderID.*?(0x[a-fA-F0-9]{20,})', error_str)
            extracted_oid = oid_match.group(1) if oid_match else None
            if "no orders found to match" in error_str:
                # FAK order had no matching liquidity at this price — this is
                # expected behaviour, NOT an error.  Log and move on.
                logger.info(
                    "FAK order not filled — no liquidity at %.4f for %s (orderID=%s). Moving on.",
                    price, market_slug, extracted_oid or "unknown",
                )
                append_jsonl(TRADE_LOG, {
                    "timestamp": time.time(),
                    "type": "order_no_fill",
                    "order_id": extracted_oid,
                    "token_id": token_id,
                    "side": side,
                    "size_usd": size_usd,
                    "price": price,
                    "market_slug": market_slug,
                    "reason": "FAK_no_liquidity",
                })
                return {
                    "success": False,
                    "order_id": extracted_oid,
                    "status": "no_fill",
                    "filled_size": 0,
                    "intended_size": size_usd,
                    "fill_ratio": 0,
                    "slippage": 0,
                    "avg_price": price,
                }
            log_error(f"Order placement failed: {e}", e)
            append_jsonl(TRADE_LOG, {
                "timestamp": time.time(),
                "type": "order_error",
                "error": error_str,
                "token_id": token_id,
                "side": side,
                "size_usd": size_usd,
            })
            return {
                "success": False,
                "order_id": extracted_oid,
                "status": "exception",
                "error": error_str,
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
