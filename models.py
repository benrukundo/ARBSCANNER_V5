"""Data models for V2.1 Paper Trader."""

from pydantic import BaseModel
from typing import Optional


class PricePoint(BaseModel):
    t: int
    price: float


class BTCKline(BaseModel):
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class Trade(BaseModel):
    market_slug: str
    side: str
    entry_price: float
    estimated_prob: float
    actual_outcome: str
    won: bool
    bet_amount: float
    payout: float = 0.0
    fee: float = 0.0
    net_profit: float
    bankroll_after: float
    time_remaining: int
    btc_price: float
    btc_distance: float
    timestamp: Optional[int] = None
