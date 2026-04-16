"""Dashboard — V2.1 Frequency Optimized Paper Trader."""

import asyncio
import json
import logging
import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from config import (
    DASHBOARD_PORT, PAPER_INITIAL_BANKROLL, VERSION,
    PERFORMANCE_LOG_FILE, PAPER_TRADES_FILE,
    ACTIVE_CRYPTOS, CRYPTO_CONFIGS, TRADING_MODE,
)
from safety import SafetyManager
from live_paper_trader import FrequencyOptimizedTrader

logger = logging.getLogger("v21.dashboard")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

app = FastAPI(title="V2.1 Frequency Optimized Paper Trader")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ── Global state ──────────────────────────────────────────────
_trader: FrequencyOptimizedTrader | None = None
_trader_task: asyncio.Task | None = None


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
async def paper_trades(limit: int = 100):
    if not _trader:
        return []
    return _trader.trades[-limit:]


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


# ── Startup ──────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup():
    global _trader, _trader_task
    logger.info(f"Starting V2.1 Frequency Optimized Paper Trader dashboard on port {DASHBOARD_PORT}")
    _trader = FrequencyOptimizedTrader(bankroll=PAPER_INITIAL_BANKROLL)
    _trader_task = asyncio.create_task(_trader.run())
    logger.info("V2.1 Paper Trader started")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=DASHBOARD_PORT)
