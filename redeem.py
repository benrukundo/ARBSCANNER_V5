"""Auto-redeem module — converts winning outcome tokens back to USDC.

Uses Polymarket's relayer (gasless) via py-builder-relayer-client.
Handles both standard CTF markets and neg-risk markets.

Relayer rate limit: 25 req/min.  resp.wait() blocks for on-chain
confirmation which naturally throttles.
"""

import json
import logging
import os
import time
import requests
from datetime import datetime
from pathlib import Path

from eth_abi import encode as eth_encode
from eth_utils import keccak

from py_builder_relayer_client.client import RelayClient
from py_builder_relayer_client.models import (
    RelayerTxType,
    OperationType,
    SafeTransaction,
    Transaction,
)
from py_builder_signing_sdk.config import BuilderConfig, BuilderApiKeyCreds

logger = logging.getLogger("redeem")

# ── Contract addresses (Polygon) ────────────────────────────────────
USDC_ADDRESS   = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_ADDRESS    = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

# ── Function selectors (pre-computed) ────────────────────────────────
REDEEM_SELECTOR = keccak(
    text="redeemPositions(address,bytes32,bytes32,uint256[])"
)[:4]
NEG_RISK_REDEEM_SELECTOR = keccak(
    text="redeemPositions(bytes32,uint256[])"
)[:4]

RELAYER_URL = "https://relayer-v2.polymarket.com"
DATA_API    = "https://data-api.polymarket.com"
CHAIN_ID    = 137

RELAYER_RETRY_WAIT = 60          # seconds to wait on rate-limit
MAX_CONSECUTIVE_FAILURES = 3     # alert threshold

# Redeem log
REDEEMS_LOG = Path("logs/redeems.jsonl")


def _ts():
    return datetime.now().strftime("%H:%M:%S")


def _log_redeem(entry: dict):
    """Append a redeem record to the JSONL log."""
    REDEEMS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(REDEEMS_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def redeem_all(
    private_key: str,
    funder_address: str,
    signature_type: int,
    builder_api_key: str,
    builder_secret: str,
    builder_passphrase: str,
) -> int:
    """Redeem all resolved positions via the Polymarket relayer.

    Returns the number of positions successfully redeemed.
    """

    # ── Client setup ─────────────────────────────────────────────────
    wallet_type = (
        RelayerTxType.PROXY if signature_type == 1 else RelayerTxType.SAFE
    )

    client = RelayClient(
        RELAYER_URL,
        chain_id=CHAIN_ID,
        private_key=private_key,
        builder_config=BuilderConfig(
            local_builder_creds=BuilderApiKeyCreds(
                key=builder_api_key,
                secret=builder_secret,
                passphrase=builder_passphrase,
            )
        ),
        relay_tx_type=wallet_type,
    )

    # ── Find redeemable positions ────────────────────────────────────
    try:
        response = requests.get(
            f"{DATA_API}/positions",
            params={
                "user": funder_address,
                "redeemable": "true",
                "sizeThreshold": 0,
            },
            timeout=15,
        )
        if response.status_code in (429, 1015):
            logger.warning(f"{_ts()} Data API rate-limited, waiting {RELAYER_RETRY_WAIT}s")
            time.sleep(RELAYER_RETRY_WAIT)
            response = requests.get(
                f"{DATA_API}/positions",
                params={
                    "user": funder_address,
                    "redeemable": "true",
                    "sizeThreshold": 0,
                },
                timeout=15,
            )
        positions = response.json()
    except Exception as e:
        logger.error(f"{_ts()} Failed to fetch positions: {e}")
        return 0

    # Filter out zero-size (already redeemed)
    positions = [p for p in positions if float(p.get("size", 0)) > 0]

    if not positions:
        logger.debug(f"{_ts()} No positions to redeem")
        return 0

    logger.info(f"{_ts()} Found {len(positions)} redeemable position(s)")

    # ── Redeem each position ─────────────────────────────────────────
    redeemed = 0
    for pos in positions:
        cid = pos.get("conditionId", pos.get("condition_id", ""))
        if not cid:
            continue
        if not cid.startswith("0x"):
            cid = "0x" + cid

        market = pos.get("title", cid[:12])

        try:
            condition_bytes = bytes.fromhex(cid[2:])
            neg_risk = pos.get("negativeRisk")

            if neg_risk is True:
                # ── Neg-risk markets ─────────────────────────────────
                size_raw = int(float(pos.get("size", 0)) * 1e6)
                outcome_index = int(pos.get("outcomeIndex", 0))
                amounts = [0, 0]
                amounts[outcome_index] = size_raw
                args = eth_encode(
                    ["bytes32", "uint256[]"],
                    [condition_bytes, amounts],
                )
                txn = SafeTransaction(
                    to=NEG_RISK_ADAPTER,
                    operation=OperationType.Call,
                    data="0x" + (NEG_RISK_REDEEM_SELECTOR + args).hex(),
                    value="0",
                )

            elif neg_risk is False:
                # ── Standard CTF markets ─────────────────────────────
                args = eth_encode(
                    ["address", "bytes32", "bytes32", "uint256[]"],
                    [USDC_ADDRESS, b"\x00" * 32, condition_bytes, [1, 2]],
                )
                txn = SafeTransaction(
                    to=CTF_ADDRESS,
                    operation=OperationType.Call,
                    data="0x" + (REDEEM_SELECTOR + args).hex(),
                    value="0",
                )

            else:
                logger.warning(
                    f"{_ts()} Skipping {market}: unsupported market type "
                    f"(negativeRisk={neg_risk!r})"
                )
                continue

            # ── Execute via relayer ──────────────────────────────────
            # client.execute expects list[Transaction] not SafeTransaction
            # but internally it wraps them — we pass SafeTransaction list
            # directly via the private methods. Actually looking at the
            # source, execute() takes list[Transaction] and wraps into
            # SafeTransaction/ProxyTransaction. So we need Transaction:
            plain_txn = Transaction(
                to=txn.to,
                data=txn.data,
                value=txn.value,
            )

            try:
                resp = client.execute([plain_txn], f"redeem {cid[:12]}")
                result = resp.wait()
            except Exception as relay_err:
                status = getattr(relay_err, "status_code", None)
                if status in (429, 1015):
                    logger.warning(
                        f"{_ts()} Relayer rate-limited (HTTP {status}), "
                        f"waiting {RELAYER_RETRY_WAIT}s"
                    )
                    time.sleep(RELAYER_RETRY_WAIT)
                    resp = client.execute([plain_txn], f"redeem {cid[:12]}")
                    result = resp.wait()
                else:
                    raise

            redeemed += 1
            size_val = pos.get("size", "?")
            current_val = pos.get("currentValue", "?")
            logger.info(f"{_ts()} Redeemed: {market} (size={size_val}, value=${current_val})")

            _log_redeem({
                "timestamp": datetime.utcnow().isoformat(),
                "market": market,
                "conditionId": cid,
                "size": size_val,
                "currentValue": current_val,
                "negativeRisk": neg_risk,
                "transactionId": resp.transaction_id if resp else None,
                "status": "success",
            })

        except Exception as e:
            logger.error(f"{_ts()} Failed to redeem {market}: {e}")
            _log_redeem({
                "timestamp": datetime.utcnow().isoformat(),
                "market": market,
                "conditionId": cid,
                "status": "failed",
                "error": str(e),
            })

    logger.info(f"{_ts()} Redeemed {redeemed}/{len(positions)} positions")
    return redeemed


# Tracking state for the async loop
_last_redeem_time: float = 0.0
_last_redeem_count: int = 0
_last_redeem_error: str = ""
_consecutive_failures: int = 0


def get_redeem_status() -> dict:
    """Return current redeem loop status for dashboard."""
    return {
        "last_redeem_time": _last_redeem_time,
        "last_redeem_count": _last_redeem_count,
        "last_redeem_error": _last_redeem_error,
        "consecutive_failures": _consecutive_failures,
    }


def run_redeem_cycle(
    private_key: str,
    funder_address: str,
    signature_type: int,
    builder_api_key: str,
    builder_secret: str,
    builder_passphrase: str,
) -> dict:
    """Run a single redeem cycle, updating global status.

    Returns status dict.
    """
    global _last_redeem_time, _last_redeem_count
    global _last_redeem_error, _consecutive_failures

    try:
        count = redeem_all(
            private_key=private_key,
            funder_address=funder_address,
            signature_type=signature_type,
            builder_api_key=builder_api_key,
            builder_secret=builder_secret,
            builder_passphrase=builder_passphrase,
        )
        _last_redeem_time = time.time()
        _last_redeem_count = count
        _last_redeem_error = ""
        _consecutive_failures = 0
        return {"status": "ok", "redeemed": count}

    except Exception as e:
        _consecutive_failures += 1
        _last_redeem_error = str(e)
        _last_redeem_time = time.time()
        logger.error(f"Redeem cycle failed ({_consecutive_failures}x): {e}")

        if _consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            logger.critical(
                f"ALERT: Redeem has failed {_consecutive_failures} times consecutively!"
            )
        return {"status": "error", "error": str(e)}
