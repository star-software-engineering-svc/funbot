"""Level-2 order book for one outcome of a predict.fun market.

predict.fun quotes one book per market, in the outcome whose indexSet is 1 (Up). The Down book is its mirror:
Down asks = 1 - Up bids, Down bids = 1 - Up asks, at the market's decimalPrecision (core/feeds.py PredictFeed builds both
from every message). Every websocket / REST message carries the whole book, so a book is always set from a snapshot."""
from __future__ import annotations

import time


class Book:
    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.best_bid: float | None = None
        self.best_ask: float | None = None
        self.updated_at: float = 0.0
        self.ready = False

    def snapshot(self, bids, asks) -> None:
        """bids / asks: [[price, shares], ...] (any order)."""
        self.bids = {float(p): float(s) for p, s in bids if float(s) > 0}
        self.asks = {float(p): float(s) for p, s in asks if float(s) > 0}
        self.best_bid = max(self.bids) if self.bids else None
        self.best_ask = min(self.asks) if self.asks else None
        self.updated_at = time.time()
        self.ready = True

    @property
    def mid(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    def ask_depth_upto(self, limit_price: float) -> list[tuple[float, float]]:
        """(price, size) ask levels at or below limit_price, best first."""
        return sorted((p, s) for p, s in self.asks.items() if p <= limit_price + 1e-9)

    def bid_depth_downto(self, floor_price: float) -> list[tuple[float, float]]:
        """(price, size) bid levels at or above floor_price, best first."""
        return sorted(((p, s) for p, s in self.bids.items() if p >= floor_price - 1e-9), reverse=True)


def mirror(bids, asks, decimals: int = 2) -> tuple[list, list]:
    """The other outcome's (bids, asks) from one outcome's: its bids are 1 - our asks, its asks 1 - our bids, rounded to
    the market's decimalPrecision so the two prices sum to exactly 1."""
    f = lambda lv: [[round(1 - float(p), decimals), float(s)] for p, s in lv]
    return f(asks), f(bids)
