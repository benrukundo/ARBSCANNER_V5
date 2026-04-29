"""Trading Client — wraps py-clob-client-v2 for real order execution.

Migrated from py-clob-client (V1) to py-clob-client-v2 on 2026-04-28
because Polymarket cut over to CTF Exchange V2 at ~11:00 UTC. After the
cutover, V1-signed orders are rejected with HTTP 400
{"error": "order_version_mismatch"}. The V2 SDK uses the new exchange
contracts, the new EIP-712 domain version "2", removes feeRateBps from
the order struct, and treats pUSD as collateral instead of USDC.e.
"""
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
            # V2 SDK — required after Polymarket's CTF Exchange V2 cutover
            # on 2026-04-28. The V1 SDK (py_clob_client) signs against the
            # old EIP-712 domain version "1" and the old exchange addresses,
            # producing "order_version_mismatch" 400s post-cutover.
            from py_clob_client_v2 import ClobClient, ApiCreds

            # Try loading cached API creds. The L2 API creds (apiKey,
            # secret, passphrase) survive the V1→V2 cutover; only the
            # exchange contract and order struct changed.
            creds_file = os.path.join(DATA_DIR, "api_creds.json")
            api_key = API_KEY
            api_secret = API_SECRET
            api_passphrase = API_PASSPHRASE

            if not api_key:
                if os.path.exists(creds_file):
                    with open(creds_file) as f:
                        cached = json.load(f)
                    api_key = cached.get("apiKey", "")
                    api_secret = cached.get("secret", "")
                    api_passphrase = cached.get("passphrase", "")
                    logger.info("Loaded cached API credentials")

            if not api_key:
                logger.info("Deriving new API credentials via V2 client...")
                temp_client = ClobClient(
                    CLOB_HOST,
                    key=PRIVATE_KEY,
                    chain_id=CHAIN_ID,
                    funder=FUNDER_ADDRESS,
                    signature_type=SIGNATURE_TYPE,
                )
                creds = temp_client.derive_api_key()
                if hasattr(creds, "api_key"):
                    api_key = creds.api_key
                    api_secret = creds.api_secret
                    api_passphrase = creds.api_passphrase
                elif isinstance(creds, dict):
                    api_key = creds.get("apiKey", "")
                    api_secret = creds.get("secret", "")
                    api_passphrase = creds.get("passphrase", "")
                else:
                    raise ValueError(f"Unexpected creds type: {type(creds)}")

                os.makedirs(DATA_DIR, exist_ok=True)
                creds_dict = {
                    "apiKey": api_key,
                    "secret": api_secret,
                    "passphrase": api_passphrase,
                }
                with open(creds_file, "w") as f:
                    json.dump(creds_dict, f, indent=2)
                logger.info("API credentials derived and cached")

            api_creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            )
            self.client = ClobClient(
                CLOB_HOST,
                key=PRIVATE_KEY,
                chain_id=CHAIN_ID,
                signature_type=SIGNATURE_TYPE,
                funder=FUNDER_ADDRESS,
                creds=api_creds,
            )
            self._initialized = True

            # Sanity-log SDK version so post-incident logs show what was running.
            try:
                from importlib.metadata import version as _get_version
                logger.info(
                    "PolymarketTrader initialized — py_clob_client_v2 %s (CTF Exchange V2)",
                    _get_version("py-clob-client-v2"),
                )
            except Exception:
                logger.info("PolymarketTrader initialized (V2 SDK)")

        except Exception as e:
            log_error(f"Failed to initialize trading client: {e}", e)

    @property
    def is_ready(self) -> bool:
        return self._initialized and self.client is not None

    async def get_balance(self) -> float:
        """Get pUSD collateral balance (cached for 30s).

        Post-V2 cutover, the collateral token is pUSD (still 6 decimals,
        backed by USDC). The V2 SDK abstracts this — AssetType.COLLATERAL
        returns the pUSD balance.
        """
        now = time.time()
        if now - self._balance_cache[1] < 30:
            return self._balance_cache[0]
        try:
            from py_clob_client_v2 import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=SIGNATURE_TYPE,
            )
            raw = self.client.get_balance_allowance(params)
            logger.debug("RAW balance response: %s", raw)
            balance = float(raw.get("balance", 0)) / 1e6  # pUSD has 6 decimals
            self._balance_cache = (balance, now)
            logger.info("Wallet balance (pUSD): $%.2f", balance)
            return balance
        except Exception as e:
            log_error(f"Balance check failed: {e}", e)
            return self._balance_cache[0]

    async def place_order(self, token_id: str, side: str,
                          size_usd: float = 0.0, shares: float = 0.0,
                          price: float = 0.0, market_slug: str = "",
                          estimated_prob: float = None) -> dict:
        """Place a FAK BUY or SELL order on the CLOB via V2 SDK.

        BUY:  pass `size_usd` (USDC amount to spend); shares received = size_usd/price.
              Applies PRICE_BUFFER to the limit price for reliable fills.
        SELL: pass `shares` (number of CTF tokens to sell); USDC received ≈ shares*price.
              No buffer applied — the limit is the bid we accept; we do NOT
              concede price to fill, the position monitor decides the bid
              threshold and accepts only fills at that bid or better.

        V2 differences from V1:
        - feeRateBps no longer goes in the order struct; the V2 server
          resolves the fee schedule for the token at submission time.
        - The order is signed against EIP-712 domain version "2" and
          submitted to the CTF Exchange V2 contract; signing handled by
          the V2 SDK.
        - FAK = Fill-And-Kill at the limit price — anything not matched
          immediately is cancelled (no resting order on the book).
        """
        from py_clob_client_v2 import MarketOrderArgs, OrderType

        side_norm = side.upper()
        if side_norm not in ("BUY", "SELL"):
            return {"success": False, "status": "invalid_side", "filled_size": 0,
                    "intended_size": size_usd if side_norm == "BUY" else shares,
                    "fill_ratio": 0}

        append_jsonl(TRADE_LOG, {
            "timestamp": time.time(),
            "type": "order_intent",
            "token_id": token_id,
            "side": side_norm,
            "size_usd": size_usd if side_norm == "BUY" else shares * price,
            "shares": shares if side_norm == "SELL" else 0,
            "price": price,
            "market_slug": market_slug,
        })

        try:
            if side_norm == "BUY":
                # Round USDC amount to 2 decimals (cents) — ensures maker precision
                amount = round(size_usd, 2)

                # ── Apply PRICE_BUFFER for reliable FAK fills ──
                # MAX_ENTRY_PRICE, MAX_ENTRY_PRICE_ABS, MIN_EDGE_POST_BUFFER
                # from config. The post-buffer edge check below uses
                # MIN_EDGE_POST_BUFFER (a lenient safety net, not MIN_EDGE).
                # The strategy entry gate already enforced MIN_EDGE on the
                # raw ask; here we only verify the buffer didn't eat ALL
                # the edge. Applying MIN_EDGE again would double-charge.
                market_ask = round(price, 2)
                buffered_price = round(min(market_ask + PRICE_BUFFER, MAX_ENTRY_PRICE), 2)
                logger.info("Price buffer: market_ask=%.4f -> order=%.4f (buffer=%.2f)",
                            market_ask, buffered_price, PRICE_BUFFER)

                if buffered_price > MAX_ENTRY_PRICE:
                    logger.warning("Buffered price %.4f > MAX_ENTRY_PRICE %.2f — capping",
                                   buffered_price, MAX_ENTRY_PRICE)
                    buffered_price = MAX_ENTRY_PRICE

                if buffered_price > MAX_ENTRY_PRICE_ABS:
                    logger.warning("Buffered price %.4f > MAX_ENTRY_PRICE_ABS %.2f — rejecting",
                                   buffered_price, MAX_ENTRY_PRICE_ABS)
                    return {
                        "success": False, "order_id": None, "status": "price_too_high",
                        "filled_size": 0, "intended_size": size_usd, "fill_ratio": 0, "slippage": 0,
                    }

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

                fee = (
                    (amount / buffered_price) * POLY_CRYPTO_FEE_RATE * buffered_price * (1 - buffered_price)
                    if buffered_price > 0 else 0
                )
                logger.info("Fee at buffered price: $%.4f (%.2f%%)",
                            fee, (fee / amount * 100) if amount > 0 else 0)

                rounded_price = buffered_price
                intended_amount = size_usd
                logger.info("Placing market order: BUY $%.2f @ %.4f (ask=%.4f + buffer) | %s",
                            amount, rounded_price, market_ask, market_slug)

            else:  # SELL
                # For SELLs: amount is shares; price is the bid we'll accept.
                # We round shares down (round_down semantics) to avoid
                # quoting more shares than we own due to floating-point.
                # No price buffer — the position monitor already chose this
                # bid level; degrading further would silently book worse fills.
                amount = round(shares, 2)
                if amount <= 0:
                    return {"success": False, "status": "zero_shares",
                            "filled_size": 0, "intended_size": shares, "fill_ratio": 0}
                rounded_price = round(price, 2)
                # Bound the limit price to the legal tick range.
                if rounded_price <= 0 or rounded_price >= 1:
                    return {"success": False, "status": "invalid_price",
                            "filled_size": 0, "intended_size": shares, "fill_ratio": 0}
                intended_amount = shares
                logger.info("Placing market order: SELL %.4f shares @ %.4f | %s",
                            amount, rounded_price, market_slug)

            # V2 MarketOrderArgs: feeRateBps removed; server resolves the
            # fee schedule. Side accepts "BUY"/"SELL" string or Side enum.
            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                price=rounded_price,
                side=side_norm,
                order_type=OrderType.FAK,
            )

            signed_order = self.client.create_market_order(order_args)
            # V2 renamed the kwarg orderType -> order_type.
            result = self.client.post_order(signed_order, order_type=OrderType.FAK)

            order_id = result.get("orderID", result.get("id", "unknown"))
            status = result.get("status", "unknown")

            # ── Track actual fill amounts ──
            # CLOB response keys: makingAmount, takingAmount, status, success.
            # For BUY: maker = USDC spent, taker = shares received.
            # For SELL: maker = shares sold, taker = USDC received.
            is_success = result.get("success", False)
            matched = result.get("status", "") == "matched"

            try:
                making = float(result.get("makingAmount", 0) or 0)
                taking = float(result.get("takingAmount", 0) or 0)
            except (ValueError, TypeError):
                making = taking = 0.0

            if side_norm == "BUY":
                # filled_amount expressed in USDC (what we spent)
                filled_amount = making if making > 0 else (intended_amount if (is_success and matched) else 0.0)
                # taking is shares received
                taking_amount = taking
            else:  # SELL
                # filled_amount expressed in shares (what we sold)
                filled_amount = making if making > 0 else (intended_amount if (is_success and matched) else 0.0)
                # taking is USDC received
                taking_amount = taking

            if is_success and matched:
                logger.info("Order FILLED: side=%s makingAmount=%s takingAmount=%s txns=%s",
                            side_norm, making, taking,
                            result.get("transactionsHashes", []))
            else:
                # Not matched — try legacy keys as fallback
                filled_amount = float(result.get("filled_size", result.get("matchedAmount", 0)))

            fill_ratio = filled_amount / intended_amount if intended_amount > 0 else 0

            unit = "USDC" if side_norm == "BUY" else "shares"
            logger.info("Fill ratio: %.1f%% (filled %.4f of %.4f %s)",
                        fill_ratio * 100, filled_amount, intended_amount, unit)

            append_jsonl(TRADE_LOG, {
                "timestamp": time.time(),
                "type": "order_result",
                "order_id": order_id,
                "status": status,
                "token_id": token_id,
                "side": side_norm,
                # USDC field carries the USDC leg regardless of side
                # (BUY: USDC spent, SELL: USDC received).
                "size_usd": (filled_amount if side_norm == "BUY" else taking_amount),
                "shares": (taking_amount if side_norm == "BUY" else filled_amount),
                "market_slug": market_slug,
                "filled_size": filled_amount,
                "intended_size": intended_amount,
                "fill_ratio": fill_ratio,
                "estimated_prob": estimated_prob,
                "raw_result": str(result)[:500],
            })

            logger.info("Order result: id=%s status=%s", order_id, status)
            return {
                "success": status not in ("error", "failed") and is_success and matched,
                "order_id": order_id,
                "status": status,
                "filled_size": filled_amount,
                "intended_size": intended_amount,
                "fill_ratio": fill_ratio,
                "slippage": 0,
                "avg_price": rounded_price,
                "taking_amount": taking_amount,
                "side": side_norm,
                # Convenience: USDC leg of the trade (positive for BUY = spent,
                # positive for SELL = received) so callers can always read
                # one consistent field.
                "filled_usdc": filled_amount if side_norm == "BUY" else taking_amount,
                "filled_shares": taking_amount if side_norm == "BUY" else filled_amount,
            }

        except Exception as e:
            error_str = str(e)
            # FAK/GTC no-fill: order was valid but no matching liquidity
            # Extract orderID from error if present (order WAS accepted)
            import re as _re
            oid_match = _re.search(r'orderID.*?(0x[a-fA-F0-9]{20,})', error_str)
            extracted_oid = oid_match.group(1) if oid_match else None
            if "no orders found to match" in error_str:
                logger.info(
                    "FAK order not filled — no liquidity at %.4f for %s (orderID=%s). Moving on.",
                    price, market_slug, extracted_oid or "unknown",
                )
                append_jsonl(TRADE_LOG, {
                    "timestamp": time.time(),
                    "type": "order_no_fill",
                    "order_id": extracted_oid,
                    "token_id": token_id,
                    "side": side_norm,
                    "size_usd": size_usd if side_norm == "BUY" else 0,
                    "shares": shares if side_norm == "SELL" else 0,
                    "price": price,
                    "market_slug": market_slug,
                    "reason": "FAK_no_liquidity",
                })
                return {
                    "success": False,
                    "order_id": extracted_oid,
                    "status": "no_fill",
                    "filled_size": 0,
                    "intended_size": (size_usd if side_norm == "BUY" else shares),
                    "fill_ratio": 0,
                    "slippage": 0,
                    "avg_price": price,
                    "side": side_norm,
                }
            log_error(f"Order placement failed: {e}", e)
            append_jsonl(TRADE_LOG, {
                "timestamp": time.time(),
                "type": "order_error",
                "error": error_str,
                "token_id": token_id,
                "side": side_norm,
                "size_usd": size_usd if side_norm == "BUY" else 0,
                "shares": shares if side_norm == "SELL" else 0,
            })
            return {
                "success": False,
                "order_id": extracted_oid,
                "status": "exception",
                "error": error_str,
                "side": side_norm,
            }

    # ── On-chain reconciliation (Priority 3) ──────────────────────────
    async def fetch_realized_pnl(self) -> dict:
        """Pull realized P&L straight from Polymarket's data-api.

        The bot's internal `real_pnl_total` previously summed the bot's
        OWN settlement inferences, which double-counted TP fantasies and
        mis-inferred close prices (see 2026-04-29 03:05 incident). This
        method instead asks the data-api for the wallet's actual Buys
        and Redeems and returns the on-chain realized P&L. Caller is
        expected to overwrite `self.real_pnl_total` with this value.

        Returns dict with: realized_pnl, total_buys_usd, total_redeems_usd,
        total_sells_usd, n_buys, n_redeems, n_sells, n_deposits.
        """
        from config import FUNDER_ADDRESS
        import httpx as _httpx

        url = "https://data-api.polymarket.com/activity"
        params = {
            "user": FUNDER_ADDRESS,
            # data-api currently caps `limit` at ~500. The bot has
            # 100-200 trades/day so a 1000-row pull covers ~5 days, well
            # over the daily-reset window. If you ever need more, add
            # pagination via `offset`.
            "limit": 1000,
        }
        try:
            async with _httpx.AsyncClient(timeout=15) as c:
                r = await c.get(url, params=params)
                r.raise_for_status()
                events = r.json()
        except Exception as e:
            log_error(f"data-api activity fetch failed: {e}", e)
            return {"error": str(e)}

        total_buys = 0.0
        total_redeems = 0.0
        total_sells = 0.0
        n_buys = n_redeems = n_sells = n_deposits = 0
        for ev in events:
            etype = (ev.get("type") or ev.get("eventType") or "").upper()
            usdc = float(ev.get("usdcSize", ev.get("usdcAmount", 0) or 0))
            if etype in ("TRADE", "BUY"):
                # Field name across Polymarket APIs: side may be "BUY"/"SELL"
                side = (ev.get("side") or "").upper()
                if side == "BUY":
                    total_buys += usdc
                    n_buys += 1
                elif side == "SELL":
                    total_sells += usdc
                    n_sells += 1
            elif etype == "REDEEM":
                total_redeems += usdc
                n_redeems += 1
            elif etype == "DEPOSIT":
                n_deposits += 1
        realized = total_redeems + total_sells - total_buys
        return {
            "realized_pnl": realized,
            "total_buys_usd": total_buys,
            "total_redeems_usd": total_redeems,
            "total_sells_usd": total_sells,
            "n_buys": n_buys,
            "n_redeems": n_redeems,
            "n_sells": n_sells,
            "n_deposits": n_deposits,
            "events_seen": len(events),
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
