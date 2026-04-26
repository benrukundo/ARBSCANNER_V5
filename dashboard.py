"""Dashboard — V6 Kelly + Dynamic Exit + Hedge Paper Trader."""

import asyncio
import json
import logging
import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from redeem import get_redeem_status, run_redeem_cycle
from config import (
    DASHBOARD_PORT, PAPER_INITIAL_BANKROLL, VERSION,
    PERFORMANCE_LOG_FILE, PAPER_TRADES_FILE,
    ACTIVE_CRYPTOS, CRYPTO_CONFIGS, TRADING_MODE,
    POSITION_SIZE_MODE, KELLY_FRACTION,
    ENABLE_DYNAMIC_EXIT, TAKE_PROFIT_PRICE, STOP_LOSS_PRICE,
    HEDGE_INSTEAD_OF_STOP,
)

CLOB_TRADE_LOG = Path("logs_v7/trades.jsonl")
from safety import SafetyManager
from live_paper_trader import FrequencyOptimizedTrader

logger = logging.getLogger("v6.dashboard")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

app = FastAPI(title="V6 Kelly + Dynamic Exit + Hedge Paper Trader")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ── Global state ──────────────────────────────────────────────
_trader: FrequencyOptimizedTrader | None = None
_trader_task: asyncio.Task | None = None


def _load_clob_trade_map() -> dict[str, dict]:
    """Return latest real CLOB execution info keyed by market slug."""
    if not CLOB_TRADE_LOG.exists():
        return {}

    by_slug: dict[str, dict] = {}
    with open(CLOB_TRADE_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            slug = entry.get("market_slug") or entry.get("slug")
            if not slug:
                continue

            rec = by_slug.setdefault(slug, {
                "real_status": "not_attempted",
                "real_order_id": "",
                "real_bet": 0.0,
                "real_pnl": None,
                "real_outcome": "",
                "real_fill_ratio": None,
                "real_tx": "",
            })

            etype = entry.get("type", "")
            if etype == "order_result":
                rec["real_status"] = "matched"
                rec["real_order_id"] = entry.get("order_id", "")
                rec["real_bet"] = float(entry.get("filled_size", 0) or entry.get("size_usd", 0) or 0)
                rec["real_fill_ratio"] = entry.get("fill_ratio")
                raw = str(entry.get("raw_result", ""))
                tx_start = raw.find("transactionsHashes")
                if tx_start >= 0:
                    rec["real_tx"] = raw[tx_start:]
            elif etype == "order_no_fill" and rec["real_status"] != "matched":
                rec["real_status"] = "no_fill"
                rec["real_order_id"] = entry.get("order_id", "")
            elif etype in ("order_error", "order_exception") and rec["real_status"] not in ("matched", "no_fill"):
                rec["real_status"] = "error"
                rec["real_order_id"] = entry.get("order_id", "")
            elif etype == "safety_block" and rec["real_status"] == "not_attempted":
                rec["real_status"] = "blocked"
            elif etype == "trade_settled":
                rec["real_pnl"] = entry.get("net_profit")
                rec["real_outcome"] = entry.get("outcome", "")
                rec["real_bet"] = float(entry.get("bet_amount", rec["real_bet"]) or 0)
                rec["real_order_id"] = entry.get("order_id", rec["real_order_id"])

    return by_slug


def _merge_shadow_trades(trades: list[dict]) -> list[dict]:
    """Collapse duplicate shadow ledger rows for the same market slug."""
    merged: dict[str, dict] = {}
    for trade in trades:
        slug = trade.get("market_slug")
        if not slug:
            continue

        current = merged.get(slug)
        if not current:
            merged[slug] = dict(trade)
            continue

        # Keep the latest timestamp as the base row.
        if trade.get("timestamp", 0) >= current.get("timestamp", 0):
            base = dict(trade)
            other = current
        else:
            base = current
            other = trade

        for key in (
            "actual_outcome", "exit_type", "order_id", "available_liquidity_usd",
            "liquidity_adjusted", "original_size", "actual_size", "entry_source",
        ):
            if (not base.get(key)) and other.get(key):
                base[key] = other.get(key)

        # Prefer richer row values when duplicates exist.
        if not base.get("actual_outcome") and other.get("actual_outcome"):
            base["actual_outcome"] = other.get("actual_outcome")
        if not base.get("bankroll_after") and other.get("bankroll_after") is not None:
            base["bankroll_after"] = other.get("bankroll_after")

        merged[slug] = base

    return sorted(merged.values(), key=lambda t: t.get("timestamp", 0), reverse=True)


# ── Dashboard ─────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


# ── Paper Trader API ──────────────────────────────────────────

@app.get("/api/paper/state")
async def paper_state():
    if not _trader:
        return {"running": False, "message": "Paper trader not started"}
    return _trader.get_state()


@app.post("/api/paper/start")
async def paper_start():
    global _trader, _trader_task
    if _trader and _trader.is_running:
        return {"status": "already_running"}
    _trader = FrequencyOptimizedTrader(bankroll=PAPER_INITIAL_BANKROLL)
    _trader_task = asyncio.create_task(_trader.run())
    return {"status": "started", "version": VERSION}


@app.post("/api/paper/stop")
async def paper_stop():
    global _trader, _trader_task
    if _trader_task:
        _trader_task.cancel()
        try:
            await _trader_task
        except asyncio.CancelledError:
            pass
    if _trader:
        _trader.is_running = False
    return {"status": "stopped"}


# ── Trade History ─────────────────────────────────────────────

@app.get("/api/paper/trades")
async def paper_trades(limit: int = 0):
    if not _trader:
        return []
    trades = list(reversed(_trader.trades)) if _trader.trades else []
    if limit > 0:
        return trades[:limit]
    return trades


@app.get("/api/trade-history")
async def reconciled_trade_history(limit: int = 200):
    """Return one dashboard row per market enriched with real CLOB execution status."""
    if not _trader:
        return []

    shadow_rows = _merge_shadow_trades(list(_trader.trades or []))
    clob_map = _load_clob_trade_map()

    rows = []
    for trade in shadow_rows:
        row = dict(trade)
        real = clob_map.get(trade.get("market_slug", ""), {})
        row["real_status"] = real.get("real_status", "not_attempted")
        row["real_order_id"] = real.get("real_order_id", "")
        row["real_bet"] = real.get("real_bet", 0.0)
        row["real_pnl"] = real.get("real_pnl")
        row["real_outcome"] = real.get("real_outcome", "")
        row["real_fill_ratio"] = real.get("real_fill_ratio")
        rows.append(row)

    if limit > 0:
        return rows[:limit]
    return rows


# ── Equity Curve ──────────────────────────────────────────────

@app.get("/api/paper/equity-curve")
async def equity_curve():
    if not _trader or not _trader.trades:
        return {"labels": [], "data": []}
    labels = list(range(len(_trader.trades) + 1))
    data = [_trader.initial_bankroll]
    for t in _trader.trades:
        data.append(t["bankroll_after"])
    return {"labels": labels, "data": data}


# ── Performance Logs ──────────────────────────────────────────

@app.get("/api/performance")
async def performance_data():
    """Return performance log entries."""
    path = Path(PERFORMANCE_LOG_FILE)
    if not path.exists():
        return []
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries[-200:]  # Last 200


# ── V1 Comparison Data ───────────────────────────────────────

@app.get("/api/comparison")
async def comparison():
    """Compare V2.1 metrics against V1 (port 8081) live data."""
    v21_state = _trader.get_state() if _trader else {}

    # Try to fetch V1 state
    v1_state = {}
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get("http://localhost:8081/api/paper/state")
            if resp.status_code == 200:
                v1_state = resp.json()
    except Exception:
        pass

    return {
        "v21": {
            "trades": v21_state.get("total_trades", 0),
            "win_rate": v21_state.get("win_rate", 0),
            "bankroll": v21_state.get("bankroll", 0),
            "profit": v21_state.get("total_profit", 0),
            "trade_rate": v21_state.get("trade_rate_per_hour", 0),
            "avg_entry": v21_state.get("entry_quality", {}).get("avg_entry_price", 0),
            "hours": v21_state.get("hours_running", 0),
        },
        "v1": {
            "trades": v1_state.get("total_trades", 0),
            "win_rate": v1_state.get("win_rate", 0),
            "bankroll": v1_state.get("bankroll", 0),
            "profit": v1_state.get("total_profit", 0),
            "hours": 0,  # V1 doesn't track this
        },
    }



@app.get("/api/crypto/pnl")
async def crypto_pnl():
    """Return per-crypto P&L breakdown."""
    if not _trader:
        return {}
    state = _trader.get_state()
    return {
        "crypto_pnl": state.get("crypto_pnl", {}),
        "crypto_prices": state.get("crypto_prices", {}),
        "active_cryptos": state.get("active_cryptos", []),
    }



# ── Trading Mode API ──────────────────────────────────────────

@app.post("/api/mode/switch")
async def switch_mode(request: Request):
    global _trader
    body = await request.json()
    new_mode = body.get("mode", "paper")
    confirm = body.get("confirm", "")

    if new_mode not in ("paper", "shadow", "live"):
        return JSONResponse({"error": "Invalid mode. Use: paper, shadow, live"}, status_code=400)

    if new_mode == "live" and confirm != "CONFIRM":
        return JSONResponse(
            {"error": "Type CONFIRM to switch to live mode"},
            status_code=400,
        )

    if _trader:
        old_mode = _trader.mode
        _trader.mode = new_mode
        _trader._save_v21_state()
        logger.info("Mode switched: %s -> %s", old_mode, new_mode)
        return {"status": "ok", "old_mode": old_mode, "new_mode": new_mode}

    return {"status": "ok", "message": "No active trader, mode will apply on next start"}


# ── Kill Switch API ───────────────────────────────────────────

@app.post("/api/kill-switch/activate")
async def activate_kill_switch():
    global _trader
    SafetyManager.activate_kill_switch(reason="dashboard_button")

    if _trader and _trader.trader and _trader.trader.is_ready:
        try:
            await _trader.trader.cancel_all_orders()
        except Exception as e:
            logger.error("Failed to cancel orders on kill switch: %s", e)

    if _trader:
        _trader.is_running = False

    return {"status": "kill_switch_activated"}


@app.post("/api/kill-switch/deactivate")
async def deactivate_kill_switch():
    SafetyManager.deactivate_kill_switch()
    return {"status": "kill_switch_deactivated"}


# ── Safety API ────────────────────────────────────────────────

@app.get("/api/safety/state")
async def get_safety_state():
    if _trader:
        return _trader.safety.get_state()
    return SafetyManager().get_state()


@app.post("/api/safety/unhalt")
async def unhalt_bot():
    if _trader:
        _trader.safety.force_unhalt()
        return {"status": "unhalted"}
    return {"status": "no_active_trader"}


@app.get("/api/safety/alerts")
async def get_alerts():
    if _trader:
        return {"alerts": _trader.safety.get_recent_alerts(20)}
    return {"alerts": []}


# ── Live CLOB Orders API ───────────────────────────────────

@app.get("/api/live/orders")
async def live_orders(limit: int = 200):
    """Return CLOB order history from trades.jsonl."""
    if not CLOB_TRADE_LOG.exists():
        return []
    entries = []
    settlements = {}  # market_slug -> settlement data
    with open(CLOB_TRADE_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                t = entry.get("type", "")
                if t in ("order_intent", "order_result",
                         "order_no_fill", "order_error"):
                    entries.append(entry)
                elif t == "trade_settled":
                    settlements[entry.get("market_slug", "")] = entry
            except json.JSONDecodeError:
                continue
    # Merge settlement outcome into order_result entries
    for e in entries:
        if e.get("type") == "order_result" and e.get("market_slug"):
            settle = settlements.get(e["market_slug"])
            if settle:
                e["won"] = settle.get("won")
                e["outcome"] = settle.get("outcome", "")
                e["real_pnl"] = settle.get("net_profit", 0)
                e["real_payout"] = settle.get("payout", 0)
                e["real_bet"] = settle.get("bet_amount", 0)
    return entries[-limit:]


@app.get("/api/live/positions")
async def live_positions():
    """Return current live/shadow positions tracked by the trader."""
    if not _trader:
        return {"positions": [], "mode": "unknown"}
    positions = []
    for slug, pos in _trader._live_positions.items():
        positions.append({
            "slug": slug,
            "crypto": pos.get("crypto_name", "?"),
            "side": pos.get("side", "?"),
            "entry_price": pos.get("entry_price", 0),
            "trade_size": pos.get("trade_size_usd", 0),
            "mode": pos.get("mode", "?"),
            "order_id": pos.get("order_result", {}).get("order_id", "?"),
            "entry_time": pos.get("entry_time", 0),
            "token_id": pos.get("token_id", "")[:16] + "...",
        })
    return {
        "positions": positions,
        "mode": _trader.mode,
        "real_pnl_total": _trader.real_pnl_total,
        "real_trade_count": _trader.real_trade_count,
        "wallet_balance": getattr(_trader, '_last_wallet_balance', 0),
    }


@app.get("/api/live/summary")
async def live_summary():
    """Summary stats for live CLOB trading."""
    if not CLOB_TRADE_LOG.exists():
        return {"total_orders": 0}

    intents = 0
    fills = 0
    no_fills = 0
    errors = 0
    total_usd_sent = 0.0
    total_usd_filled = 0.0

    with open(CLOB_TRADE_LOG) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
                t = e.get("type", "")
                if t == "order_intent":
                    intents += 1
                elif t == "order_result":
                    fills += 1
                    total_usd_sent += float(e.get("size_usd", 0))
                    total_usd_filled += float(e.get("filled_size", 0))
                elif t == "order_no_fill":
                    no_fills += 1
                elif t == "order_error":
                    errors += 1
            except (json.JSONDecodeError, ValueError):
                continue


    # Count wins/losses from settlement log
    wins = losses = 0
    if CLOB_TRADE_LOG.exists():
        with open(CLOB_TRADE_LOG) as sf:
            for sline in sf:
                sline = sline.strip()
                if not sline:
                    continue
                try:
                    se = json.loads(sline)
                    if se.get("type") == "trade_settled":
                        if se.get("won"):
                            wins += 1
                        elif se.get("won") is False:
                            losses += 1
                except (json.JSONDecodeError, ValueError):
                    continue

    return {
        "total_orders": intents,
        "fills": fills,
        "no_fills": no_fills,
        "errors": errors,
        "total_usd_sent": round(total_usd_sent, 2),
        "total_usd_filled": round(total_usd_filled, 2),
        "fill_rate_pct": round(fills / intents * 100, 1) if intents > 0 else 0,
        "mode": _trader.mode if _trader else TRADING_MODE,
        "real_pnl_total": round(_trader.real_pnl_total, 6) if _trader else 0,
        "real_trade_count": _trader.real_trade_count if _trader else 0,
        "wins": wins,
        "losses": losses,
    }



# ── Redeem ────────────────────────────────────────────────────

@app.get("/api/redeem/status")
async def redeem_status():
    """Return auto-redeem loop status."""
    return get_redeem_status()


@app.post("/api/redeem/trigger")
async def redeem_trigger():
    """Manually trigger a redeem cycle."""
    import asyncio
    from config import (
        PRIVATE_KEY, FUNDER_ADDRESS, SIGNATURE_TYPE,
        BUILDER_API_KEY, BUILDER_SECRET, BUILDER_PASSPHRASE,
    )
    if not BUILDER_API_KEY or not BUILDER_SECRET:
        return {"status": "error", "error": "Builder API keys not configured"}
    result = await asyncio.to_thread(
        run_redeem_cycle,
        private_key=PRIVATE_KEY,
        funder_address=FUNDER_ADDRESS,
        signature_type=SIGNATURE_TYPE,
        builder_api_key=BUILDER_API_KEY,
        builder_secret=BUILDER_SECRET,
        builder_passphrase=BUILDER_PASSPHRASE,
    )
    return result



# ── Health ────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {
        "status": "healthy",
        "version": VERSION,
        "paper_running": _trader.is_running if _trader else False,
        "bankroll": _trader.bankroll if _trader else 0,
        "mode": _trader.mode if _trader else TRADING_MODE,
        "trader_ready": _trader.trader.is_ready if _trader and _trader.trader else False,
    }



# ── V6 Strategy Stats ────────────────────────────────────────

@app.get("/api/v6/strategy-stats")
async def v6_strategy_stats():
    """Return V6 strategy performance metrics."""
    if not _trader:
        return {"running": False, "message": "Trader not started"}

    trades = _trader.trades
    total = len(trades)
    wins = sum(1 for t in trades if t.get("won"))
    losses = total - wins

    take_profits = sum(1 for t in trades if t.get("exit_type") == "take_profit")
    stop_losses = sum(1 for t in trades if t.get("exit_type") == "stop_loss")
    hedges = sum(1 for t in trades if t.get("exit_type") == "hedge")
    held_to_resolution = total - take_profits - stop_losses - hedges

    profits = [t.get("net_profit", 0) for t in trades]
    avg_kelly = sum(t.get("bet_amount", 0) for t in trades) / max(total, 1)

    # Max drawdown
    peak = _trader._initial_bankroll if hasattr(_trader, '_initial_bankroll') else PAPER_INITIAL_BANKROLL
    max_dd = 0
    running = peak
    for p in profits:
        running += p
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    # Sharpe ratio (daily approx from per-trade returns)
    import statistics
    if len(profits) >= 2:
        mean_r = statistics.mean(profits)
        std_r = statistics.stdev(profits)
        sharpe = (mean_r / std_r) * (252 ** 0.5) if std_r > 0 else 0
    else:
        sharpe = 0

    return {
        "version": VERSION,
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "take_profits": take_profits,
        "stop_losses": stop_losses,
        "hedges": hedges,
        "held_to_resolution": held_to_resolution,
        "avg_kelly_bet": round(avg_kelly, 2),
        "max_drawdown": round(max_dd, 4),
        "sharpe_ratio": round(sharpe, 4),
        "win_rate": round(wins / max(total, 1) * 100, 2),
        "bankroll": round(_trader.bankroll, 4),
        "position_size_mode": POSITION_SIZE_MODE,
        "kelly_fraction": KELLY_FRACTION,
        "dynamic_exit": ENABLE_DYNAMIC_EXIT,
        "take_profit_price": TAKE_PROFIT_PRICE,
        "stop_loss_price": STOP_LOSS_PRICE,
        "hedge_enabled": HEDGE_INSTEAD_OF_STOP,
    }

# ── Startup ──────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup():
    global _trader, _trader_task
    logger.info(f"Starting V6 Kelly+Hedge Paper Trader dashboard on port {DASHBOARD_PORT}")
    _trader = FrequencyOptimizedTrader(bankroll=PAPER_INITIAL_BANKROLL)
    _trader_task = asyncio.create_task(_trader.run())
    logger.info("V6 Paper Trader started")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=DASHBOARD_PORT)
