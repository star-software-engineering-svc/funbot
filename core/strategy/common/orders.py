"""Order sizing helpers shared by the strategies (polybot's core/strategy/common/orders.py). In common/: out of the
strategy registry."""
from __future__ import annotations

import math

MIN_NOTIONAL = 1.0           # USDT: the smallest order worth sending (predict.fun's own minimum: see core/pfclient.py)
SIZE_TOL_SH = 0.1            # an order filled to within this many shares of its size is complete


def min_notional_shares(price: float) -> float:
    """Shares (0.01 resolution) needed so that shares * price is above MIN_NOTIONAL."""
    if price <= 0:
        return math.inf
    shares = math.ceil(round(MIN_NOTIONAL / price, 6) * 100 - 1e-6) / 100
    if shares * price <= MIN_NOTIONAL + 1e-9:
        shares = round(shares + 0.01, 2)
    return round(shares, 2)
