"""Safety Manager for V2.1 — enforces risk limits and kill switch."""
import json
import logging
import os
import time
from pathlib import Path

from config import (
    DAILY_LOSS_LIMIT, MAX_CONSECUTIVE_LOSSES, MAX_CONCURRENT_POSITIONS,
    MIN_BALANCE_TO_TRADE, TRADING_MODE,
    KILL_SWITCH_FILE, ALERTS_LOG, LOGS_DIR, DATA_DIR,
)

logger = logging.getLogger("v21.safety")


def append_jsonl(filepath: str, record: dict):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "a") as f:
        f.write(json.dumps(record) + "\n")


class SafetyManager:
    """Enforces risk limits — all checks must pass before any trade is placed."""

    STATE_FILE = os.path.join(DATA_DIR, "safety_state.json")

    def __init__(self, trader=None):
        self.trader = trader  # PolymarketTrader reference for balance checks
        self.daily_pnl: float = 0.0
        self.daily_trades: int = 0
        self.consecutive_losses: int = 0
        self.current_positions: int = 0
        self.is_halted: bool = False
        self.halt_reason: str = ""
        self.last_reason: str = ""
        self.alerts: list[dict] = []
        self._load_state()

    # ── Persistence ───────────────────────────────────────────

    def _load_state(self):
        """Load safety state from disk (survives restarts)."""
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE) as f:
                    state = json.load(f)
                self.daily_pnl = state.get("daily_pnl", 0.0)
                self.daily_trades = state.get("daily_trades", 0)
                self.consecutive_losses = state.get("consecutive_losses", 0)
                self.current_positions = 0  # Always reset on startup — _live_positions is empty after restart
                # (persisted value is stale; positions don't survive restarts)
                self.is_halted = state.get("is_halted", False)
                self.halt_reason = state.get("halt_reason", "")
                logger.info(
                    "Safety state loaded: daily_pnl=$%.2f, consec_losses=%d, halted=%s",
                    self.daily_pnl, self.consecutive_losses, self.is_halted
                )
            except Exception as e:
                logger.warning("Failed to load safety state: %s", e)

    def _save_state(self):
        """Persist safety state to disk."""
        os.makedirs(DATA_DIR, exist_ok=True)
        state = {
            "daily_pnl": self.daily_pnl,
            "daily_trades": self.daily_trades,
            "consecutive_losses": self.consecutive_losses,
            "current_positions": self.current_positions,
            "is_halted": self.is_halted,
            "halt_reason": self.halt_reason,
            "last_updated": time.time(),
        }
        with open(self.STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)

    # ── Kill Switch (file-based) ──────────────────────────────

    @staticmethod
    def is_kill_switch_active() -> bool:
        return os.path.exists(KILL_SWITCH_FILE)

    @staticmethod
    def activate_kill_switch(reason: str = "manual"):
        os.makedirs(os.path.dirname(KILL_SWITCH_FILE), exist_ok=True)
        with open(KILL_SWITCH_FILE, "w") as f:
            f.write(json.dumps({
                "activated_at": time.time(),
                "reason": reason,
            }))
        logger.warning("KILL SWITCH ACTIVATED: %s", reason)

    @staticmethod
    def deactivate_kill_switch():
        if os.path.exists(KILL_SWITCH_FILE):
            os.remove(KILL_SWITCH_FILE)
            logger.info("Kill switch deactivated")

    # ── Core Safety Check ─────────────────────────────────────

    async def can_trade(self) -> bool:
        """Master safety check — ALL must pass."""
        if self.is_kill_switch_active():
            self.last_reason = "KILL SWITCH ACTIVE"
            return False

        if self.is_halted:
            self.last_reason = f"BOT HALTED: {self.halt_reason}"
            return False

        if self.daily_pnl <= -DAILY_LOSS_LIMIT:
            self.last_reason = f"Daily loss limit reached: ${self.daily_pnl:.2f}"
            self.is_halted = True
            self.halt_reason = "daily_loss_limit"
            self._save_state()
            await self.send_alert(f"DAILY LOSS LIMIT: Lost ${abs(self.daily_pnl):.2f} today")
            return False

        if self.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            self.last_reason = f"Max consecutive losses: {self.consecutive_losses}"
            self.is_halted = True
            self.halt_reason = "consecutive_losses"
            self._save_state()
            await self.send_alert(f"CONSECUTIVE LOSSES: {self.consecutive_losses} in a row")
            return False

        if self.current_positions >= MAX_CONCURRENT_POSITIONS:
            self.last_reason = "Max concurrent positions reached"
            return False

        if TRADING_MODE != "paper" and self.trader is not None:
            try:
                balance = await self.trader.get_balance()
                if balance < MIN_BALANCE_TO_TRADE:
                    self.last_reason = f"Balance too low: ${balance:.2f}"
                    self.is_halted = True
                    self.halt_reason = "low_balance"
                    self._save_state()
                    await self.send_alert(f"LOW BALANCE: ${balance:.2f}")
                    return False
            except Exception as e:
                logger.warning("Balance check failed: %s", e)

        return True

    # ── Trade Recording ───────────────────────────────────────

    async def record_trade(self, trade_record: dict):
        self.current_positions += 1
        self.daily_trades += 1
        self._save_state()

    async def record_result(self, won: bool, pnl: float):
        if won:
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1

        self.daily_pnl += pnl
        self.current_positions = max(0, self.current_positions - 1)
        self._save_state()

        logger.info(
            "Safety update: %s pnl=$%.4f | daily=$%.2f | consec_losses=%d",
            "WIN" if won else "LOSS", pnl, self.daily_pnl, self.consecutive_losses
        )

    # ── Alerts ────────────────────────────────────────────────

    async def send_alert(self, message: str):
        alert = {"timestamp": time.time(), "alert": message}
        self.alerts.append(alert)
        if len(self.alerts) > 100:
            self.alerts = self.alerts[-100:]
        append_jsonl(ALERTS_LOG, alert)
        logger.warning("ALERT: %s", message)

    def get_recent_alerts(self, count: int = 20) -> list[dict]:
        if self.alerts:
            return self.alerts[-count:]
        try:
            if os.path.exists(ALERTS_LOG):
                alerts = []
                with open(ALERTS_LOG) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            alerts.append(json.loads(line))
                self.alerts = alerts
                return alerts[-count:]
        except Exception:
            pass
        return []

    # ── Daily Reset ───────────────────────────────────────────

    async def daily_reset(self):
        old_pnl = self.daily_pnl
        self.daily_pnl = 0.0
        self.daily_trades = 0
        if self.halt_reason == "daily_loss_limit":
            self.is_halted = False
            self.halt_reason = ""
            await self.send_alert(f"Daily reset: previous P&L was ${old_pnl:.2f}. Bot un-halted.")
        else:
            await self.send_alert(f"Daily reset: previous P&L was ${old_pnl:.2f}.")
        self._save_state()
        logger.info("Daily reset complete. Previous P&L: $%.2f", old_pnl)

    # ── Manual Controls ───────────────────────────────────────

    def force_unhalt(self):
        self.is_halted = False
        self.halt_reason = ""
        self._save_state()
        logger.info("Bot manually un-halted")

    # ── Dashboard State ───────────────────────────────────────

    def get_state(self) -> dict:
        return {
            "daily_pnl": round(self.daily_pnl, 4),
            "daily_loss_limit": DAILY_LOSS_LIMIT,
            "daily_trades": self.daily_trades,
            "consecutive_losses": self.consecutive_losses,
            "max_consecutive_losses": MAX_CONSECUTIVE_LOSSES,
            "current_positions": self.current_positions,
            "max_concurrent_positions": MAX_CONCURRENT_POSITIONS,
            "is_halted": self.is_halted,
            "halt_reason": self.halt_reason,
            "kill_switch_active": self.is_kill_switch_active(),
            "last_reason": self.last_reason,
        }
