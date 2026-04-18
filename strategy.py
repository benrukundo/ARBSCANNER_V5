"""Trading strategy — V2.1 with Staged Entry for earlier, cheaper entries."""

import math
import logging

logger = logging.getLogger("v21.strategy")

MAX_ENTRY_PRICE_ABS = 0.99


def _norm_cdf(z: float) -> float:
    """Approximate the standard normal CDF (Abramowitz & Stegun)."""
    if z < -8.0:
        return 0.0
    if z > 8.0:
        return 1.0
    sign = 1.0
    if z < 0:
        sign = -1.0
        z = -z
    t = 1.0 / (1.0 + 0.2316419 * z)
    d = 0.3989422804014327  # 1/sqrt(2*pi)
    poly = (
        0.319381530 * t
        - 0.356563782 * t**2
        + 1.781477937 * t**3
        - 1.821255978 * t**4
        + 1.330274429 * t**5
    )
    cdf = 1.0 - d * math.exp(-0.5 * z * z) * poly
    if sign < 0:
        cdf = 1.0 - cdf
    return cdf


def _core_evaluate(
    time_remaining_seconds: int,
    current_price: float,
    price_to_beat: float,
    up_share_price: float,
    down_share_price: float,
    params: dict,
) -> tuple[bool, str | None, float | None, float | None]:
    """Core strategy evaluation — determines side, share price, probability.

    Returns (passes_basic_checks, side, share_price, estimated_prob).
    Does NOT apply staged price thresholds — caller handles that.
    """
    min_distance = params.get("MIN_DISTANCE", params.get("MIN_BTC_DISTANCE", 50.0))
    min_edge = params["MIN_EDGE"]
    vol_per_sec_raw = params.get("VOLATILITY_1SEC", params.get("BTC_1SEC_VOLATILITY", 2.5))

    # Apply safety multiplier — rolling vol underestimates true vol during calm periods
    from config import VOL_SAFETY_MULTIPLIER
    vol_per_sec = vol_per_sec_raw * VOL_SAFETY_MULTIPLIER

    if time_remaining_seconds < 5:
        return False, None, None, None

    distance = current_price - price_to_beat
    abs_distance = abs(distance)

    if abs_distance < min_distance:
        return False, None, None, None

    remaining_volatility = vol_per_sec * math.sqrt(time_remaining_seconds)
    if remaining_volatility <= 0:
        return False, None, None, None

    z_score = abs_distance / remaining_volatility
    estimated_prob = _norm_cdf(z_score)

    if distance > 0:
        side = "Up"
        share_price = up_share_price
    else:
        side = "Down"
        share_price = down_share_price

    if share_price <= 0 or share_price > MAX_ENTRY_PRICE_ABS:
        return False, None, None, None

    # Enforce minimum entry price — reject cheap/coin-flip bets
    from config import MIN_ENTRY_PRICE
    if share_price < MIN_ENTRY_PRICE:
        logger.info("Rejected: share_price=%.4f < MIN_ENTRY_PRICE=%.2f", share_price, MIN_ENTRY_PRICE)
        return False, None, None, None

    edge = estimated_prob - share_price
    logger.debug(
        "  eval: dist=$%.2f t=%ds vol_raw=%.2f vol_adj=%.2f z=%.3f prob=%.4f price=%.3f edge=%.4f min_edge=%.3f",
        abs_distance, time_remaining_seconds, vol_per_sec_raw, vol_per_sec,
        z_score, estimated_prob, share_price, edge, min_edge,
    )
    if edge < min_edge:
        return False, None, None, None

    return True, side, share_price, estimated_prob


def should_enter(
    time_remaining_seconds: int,
    current_price: float,
    price_to_beat: float,
    up_share_price: float,
    down_share_price: float,
    params: dict,
) -> tuple[bool, str | None, float | None, float | None]:
    """Standard entry check (V1-compatible)."""
    max_time = params["MAX_TIME_REMAINING"]
    max_entry = params["MAX_ENTRY_PRICE"]

    if time_remaining_seconds > max_time:
        return False, None, None, None

    ok, side, share_price, est_prob = _core_evaluate(
        time_remaining_seconds, current_price, price_to_beat,
        up_share_price, down_share_price, params,
    )
    if not ok:
        return False, None, None, None

    if share_price > max_entry:
        return False, None, None, None

    return True, side, share_price, est_prob


def should_enter_staged(
    time_remaining_seconds: int,
    current_price: float,
    price_to_beat: float,
    up_share_price: float,
    down_share_price: float,
    params: dict,
    staged_thresholds: list[dict],
) -> tuple[bool, str | None, float | None, float | None]:
    """Staged entry: apply different max_price thresholds by time window.

    staged_thresholds example:
        [
            {"min_time": 180, "max_time": 300, "max_price": 0.60},
            {"min_time": 120, "max_time": 180, "max_price": 0.80},
            {"min_time": 5,   "max_time": 120, "max_price": 0.95},
        ]

    Enter at the first stage whose time window matches AND price is acceptable.
    """
    max_time = params["MAX_TIME_REMAINING"]

    if time_remaining_seconds > max_time:
        return False, None, None, None

    ok, side, share_price, est_prob = _core_evaluate(
        time_remaining_seconds, current_price, price_to_beat,
        up_share_price, down_share_price, params,
    )
    if not ok:
        return False, None, None, None

    # Find the matching staged threshold
    for stage in staged_thresholds:
        if stage["min_time"] <= time_remaining_seconds <= stage["max_time"]:
            if share_price <= stage["max_price"]:
                return True, side, share_price, est_prob
            else:
                # Price too high for this time window — don't enter yet
                return False, None, None, None

    # Fallback: if no stage matched (shouldn't happen with good config)
    if share_price <= params["MAX_ENTRY_PRICE"]:
        return True, side, share_price, est_prob

    return False, None, None, None


def calculate_fee(
    bet_amount: float, share_price: float, fee_rate: float = 0.072
) -> float:
    """Calculate Polymarket taker fee."""
    if share_price <= 0 or share_price >= 1:
        return 0.0
    num_shares = bet_amount / share_price
    return num_shares * fee_rate * share_price * (1.0 - share_price)


def calculate_payout(shares: float, won: bool) -> float:
    """Winner gets $1/share, loser gets $0."""
    return shares * 1.0 if won else 0.0
