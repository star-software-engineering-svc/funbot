"""Orders, positions, PnL and CSV logging (polybot's core/state.py, adapted to predict.fun: a market has an id, the
two outcomes' on-chain token ids and its own fee rate; one book is quoted, the other mirrored)."""
from __future__ import annotations

import asyncio
import csv
import itertools
import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from .book import Book

load_dotenv()                                   # MARKET_PREFIX may come from .env (no override of the process environment)


def _window_s() -> int:
    """Window length in seconds: WINDOW_S if set, else from the MARKET_PREFIX suffix (btc-updown-5m -> 300), else 300."""
    v = os.getenv("WINDOW_S")
    if v:
        return int(v)
    m = re.search(r"-(\d+)m$", os.getenv("MARKET_PREFIX", "btc-updown-5m"))
    return int(m.group(1)) * 60 if m else 300


WINDOW_S = _window_s()


def now() -> float:
    return time.time()


# ------------------------------------------------------------------ logging --
class CsvLog:
    """Append-only CSV logs in <base>/<YYYYMMDD>/ (UTC date). The tick logs (LAZY) are flushed at most once a second
    (main.csv_flush_loop); every other file at each row."""
    LAZY = frozenset({"binance.csv", "coinbase.csv", "chainlink.csv", "prices.csv"})
    LAZY_FLUSH_S = 1.0
    TS_DECIMALS = {"binance.csv": 6, "coinbase.csv": 6, "chainlink.csv": 6}
    FIELDS = {
        "orders.csv": ["ts", "market", "strategy", "elapsed", "kind", "side", "token", "price", "size", "status",
                       "filled", "avg_price", "fee", "order_id", "note", "ref"],
        "fills.csv": ["ts", "market", "strategy", "elapsed", "kind", "side", "price", "size", "fee", "order_id",
                      "trade_id", "up_shares", "down_shares", "up_avg", "down_avg", "mtm_pnl"],
        "markets.csv": ["ts", "market", "market_id", "outcome", "source", "beat", "end_price", "up_shares", "down_shares",
                        "cost", "fees", "payout", "pnl", "trades", "day_pnl"],
        "signals.csv": ["ts", "market", "elapsed", "signal", "side", "btc", "beat", "delta", "p_up", "sigma", "age_ms",
                        "up_bid", "up_ask", "down_bid", "down_ask", "action", "reason", "why"],
        "binance.csv": ["ts", "event_ts", "bid", "ask", "mid"],                 # the resolution proxy: Binance spot bookTicker
        "coinbase.csv": ["ts", "event_ts", "bid", "ask", "mid"],                # comparison feed
        "chainlink.csv": ["ts", "obs_ts", "price", "bid", "ask", "binance"],    # Chainlink Data Streams report (the resolution source)
        "prices.csv": ["ts", "market", "elapsed", "up_bid", "up_ask", "down_bid", "down_ask"],   # every best bid / ask change
        "opens.csv": ["ts", "market", "start", "source", "price", "binance", "chainlink", "api"],  # the price to beat as pinned
    }

    def __init__(self, base: Path) -> None:
        self.base = base
        self.day = ""
        self.dir = base
        self._files: dict[str, tuple] = {}
        self._day_span = (0.0, 0.0)
        self._dirty: set[str] = set()
        self._flushed_at = 0.0
        self._roll(now())

    @staticmethod
    def day_of(ts: float) -> str:
        return time.strftime("%Y%m%d", time.gmtime(ts))

    def _roll(self, ts: float) -> None:
        if self._day_span[0] <= ts < self._day_span[1]:
            return
        day = self.day_of(ts)
        start = ts - ts % 86400
        self._day_span = (start, start + 86400)
        if day == self.day:
            return
        for f, _ in self._files.values():
            f.close()
        self._files = {}
        self._dirty.clear()
        self.day = day
        self.dir = self.base / day
        self.dir.mkdir(parents=True, exist_ok=True)

    def row(self, name: str, **kv) -> None:
        ts = float(kv["ts"]) if kv.get("ts") not in (None, "") else now()
        kv["ts"] = f"{ts:.{self.TS_DECIMALS.get(name, 3)}f}"
        self._roll(ts)
        if name not in self._files:
            path = self.dir / name
            new = not path.exists() or path.stat().st_size == 0
            if not new:                                       # schema changed since the file was started: keep the old one aside
                with open(path, newline="") as g:
                    header = g.readline().strip().split(",")
                if header != self.FIELDS[name]:
                    path.rename(path.with_name(f"{path.stem}.old-{time.strftime('%H%M%S', time.gmtime())}{path.suffix}"))
                    new = True
            f = open(path, "a", newline="")
            w = csv.writer(f)
            if new:
                w.writerow(self.FIELDS[name])
            self._files[name] = (f, w)
        f, w = self._files[name]
        w.writerow([kv.get(k, "") for k in self.FIELDS[name]])
        if name in self.LAZY:
            self._dirty.add(name)
            if ts - self._flushed_at >= self.LAZY_FLUSH_S:
                self.flush_lazy(ts)
        else:
            f.flush()

    def flush_lazy(self, ts: float | None = None) -> None:
        for name in self._dirty:
            if name in self._files:
                self._files[name][0].flush()
        self._dirty.clear()
        self._flushed_at = now() if ts is None else ts

    def close(self) -> None:
        self.flush_lazy()
        for f, _ in self._files.values():
            f.close()
        self._files = {}


# ------------------------------------------------------------------- orders --
_ids = itertools.count(1)


@dataclass
class Order:
    market: "MarketState"
    token: str
    side: str            # UP / DOWN (the outcome; kind "sell" sells it, every other kind buys it)
    price: float
    size: float
    kind: str            # taker | sell
    market_order: bool = False
    strategy: str = ""
    order_id: str = ""   # predict.fun's order id (live) / sim-<n> (dry run)
    order_hash: str = "" # the signed order's hash (live): what the cancel and the fills name
    status: str = "new"  # new | live | filled | cancelled | rejected
    filled: float = 0.0
    cost: float = 0.0
    fee: float = 0.0
    created_at: float = field(default_factory=now)
    first_fill_at: float | None = None
    local_id: int = field(default_factory=lambda: next(_ids))
    trade_ids: set = field(default_factory=set)
    ref: str = ""
    error: str = ""
    done_evt: asyncio.Event = field(default_factory=asyncio.Event, repr=False, compare=False)

    def __setattr__(self, name, value) -> None:
        object.__setattr__(self, name, value)
        if name == "status" and value in ("live", "cancelled", "filled", "rejected"):
            self.done_evt.set()

    @property
    def remaining(self) -> float:
        return max(self.size - self.filled, 0.0)

    @property
    def avg_price(self) -> float:
        return self.cost / self.filled if self.filled else 0.0

    @property
    def done(self) -> bool:
        return self.status in ("cancelled", "filled", "rejected")


@dataclass
class Position:
    shares: float = 0.0
    cost: float = 0.0
    fee: float = 0.0

    @property
    def avg(self) -> float:
        return self.cost / self.shares if self.shares else 0.0


# ------------------------------------------------------------------- market --
@dataclass
class MarketState:
    slug: str                            # btc-updown-5m-<window start, unix s>
    start: int
    market_id: int                       # predict.fun's market id (books, orders)
    condition_id: str
    up_token: str                        # outcome onChainId, indexSet 1 (Up)
    down_token: str                      # indexSet 2 (Down)
    fee_rate_bps: int = 200
    decimals: int = 2                    # decimalPrecision: price tick 10^-decimals
    neg_risk: bool = False
    yield_bearing: bool = False
    price_feed_id: str = ""              # the Chainlink Data Streams feed the market resolves on
    min_size: float = 1.0
    up_book: Book = field(default_factory=Book)
    down_book: Book = field(default_factory=Book)
    orders: list[Order] = field(default_factory=list)
    pos: dict[str, Position] = field(default_factory=lambda: {"UP": Position(), "DOWN": Position()})
    beat: float | None = None            # the price at the window start (the resolution's start price)
    beat_src: str = ""                   # chainlink | binance | api
    taker_count: int = 0
    last_entry: float = 0.0
    settled: bool = False
    outcome: str | None = None
    pnl: float | None = None
    restored: bool = False

    @property
    def end(self) -> int:
        return self.start + WINDOW_S

    @property
    def tick(self) -> float:
        return 10 ** -self.decimals

    def elapsed(self) -> float:
        return now() - self.start

    def token(self, side: str) -> str:
        return self.up_token if side == "UP" else self.down_token

    def book(self, side: str) -> Book:
        return self.up_book if side == "UP" else self.down_book

    @property
    def books_ready(self) -> bool:
        return self.up_book.ready and self.down_book.ready

    def open_orders(self, kind: str | None = None) -> list[Order]:
        return [o for o in self.orders if not o.done and (kind is None or o.kind == kind)]

    @property
    def cost(self) -> float:
        return sum(p.cost for p in self.pos.values())

    @property
    def fees(self) -> float:
        return sum(p.fee for p in self.pos.values())

    def mtm(self) -> float:
        v = 0.0
        for side, p in self.pos.items():
            mid = self.book(side).mid
            if p.shares and mid is not None:
                v += p.shares * mid
        return v - self.cost - self.fees

    def pnl_if(self, side: str) -> float:
        return self.pos[side].shares - self.cost - self.fees


# ---------------------------------------------------------------- portfolio --
class Portfolio:
    """Positions and PnL of every market seen. Positions are mirrored to <log base>/positions.json after every fill and
    settlement; a market discovered after a restart is restored from it."""

    def __init__(self, log: CsvLog) -> None:
        self.log = log
        self.markets: dict[str, MarketState] = {}
        self.day = time.strftime("%Y-%m-%d", time.gmtime())
        self.day_pnl = 0.0
        self.total_pnl = 0.0
        self.n_settled = 0
        self.n_wins = 0
        self.state_path = log.base / "positions.json"
        self._saved: dict[str, dict] = self._load_state()

    def _load_state(self) -> dict[str, dict]:
        try:
            with open(self.state_path) as f:
                d = json.load(f)
            cutoff = now() - 6 * 3600
            return {k: v for k, v in d.items() if v.get("start", 0) > cutoff}
        except (FileNotFoundError, ValueError):
            return {}

    @staticmethod
    def _record(m: MarketState) -> dict:
        return dict(start=m.start, market_id=m.market_id, taker_count=m.taker_count, settled=m.settled, outcome=m.outcome,
                    pnl=m.pnl, beat=m.beat, beat_src=m.beat_src, pos={s: [p.shares, p.cost, p.fee] for s, p in m.pos.items()})

    def save_state(self) -> None:
        d = dict(self._saved)
        for m in self.markets.values():
            if m.cost == 0 and m.slug not in d:
                continue
            d[m.slug] = self._record(m)
        try:
            tmp = self.state_path.with_suffix(".tmp")
            tmp.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp, "w") as f:
                json.dump(d, f)
            tmp.replace(self.state_path)
        except OSError as e:
            print(f"[state] could not save {self.state_path}: {e!r}", flush=True)

    def prune(self, keep: int = 30) -> None:
        by_start = sorted(self.markets.values(), key=lambda m: m.start, reverse=True)
        for m in [m for m in by_start[keep:] if m.settled]:
            if m.cost > 0:
                self._saved[m.slug] = self._record(m)
            del self.markets[m.slug]

    def restore(self, m: MarketState) -> bool:
        s = self._saved.get(m.slug)
        if not s or m.restored:
            return False
        for side, (sh, cost, fee) in s["pos"].items():
            m.pos[side] = Position(shares=sh, cost=cost, fee=fee)
        m.taker_count = int(s.get("taker_count", 0))
        m.beat, m.beat_src = s.get("beat"), s.get("beat_src", "")
        m.settled, m.outcome, m.pnl = bool(s.get("settled")), s.get("outcome"), s.get("pnl")
        m.restored = True
        return m.cost > 0

    def unrestored(self) -> list[tuple[str, dict]]:
        return [(k, v) for k, v in self._saved.items() if k not in self.markets and v.get("pos") and not v.get("settled")]

    def _roll_day(self) -> None:
        d = time.strftime("%Y-%m-%d", time.gmtime())
        if d != self.day:
            self.day, self.day_pnl = d, 0.0

    def _fill_row(self, order: Order, kind: str, price: float, size: float, fee: float, trade_id: str) -> None:
        m = order.market
        self.log.row("fills.csv", market=m.slug, strategy=order.strategy, elapsed=f"{m.elapsed():.1f}", kind=kind,
                     side=order.side, price=price, size=size, fee=f"{fee:.5f}", order_id=order.order_id,
                     trade_id=trade_id, up_shares=m.pos["UP"].shares, down_shares=m.pos["DOWN"].shares,
                     up_avg=f"{m.pos['UP'].avg:.4f}", down_avg=f"{m.pos['DOWN'].avg:.4f}", mtm_pnl=f"{m.mtm():.4f}")

    def _take(self, order: Order, size: float, price: float, fee: float, trade_id: str) -> float:
        """The part of a fill not booked yet (0: a repeated trade id or nothing left), booked on the order."""
        if trade_id and trade_id in order.trade_ids:
            return 0.0
        if trade_id:
            order.trade_ids.add(trade_id)
        size = min(size, order.remaining + 1e-9)
        if size <= 0:
            return 0.0
        if order.first_fill_at is None:
            order.first_fill_at = now()
        order.filled += size
        order.cost += size * price
        order.fee += fee
        if order.remaining <= 1e-6:
            order.status = "filled"
        return size

    def record_fill(self, order: Order, size: float, price: float, fee: float, trade_id: str = "", fee_shares: float = 0.0) -> None:
        """A BUY fill of `size` shares at `price`. predict.fun takes a buy's taker fee in shares (fee_shares: received =
        size - fee_shares, the cost is the full size x price); `fee` = a fee in USDT, if any."""
        size = self._take(order, size, price, fee, trade_id)
        if size <= 0:
            return
        p = order.market.pos[order.side]
        p.shares += size - fee_shares
        p.cost += size * price
        p.fee += fee
        self._roll_day()
        self.save_state()
        self._fill_row(order, order.kind, price, size, fee + fee_shares * price, trade_id)

    def record_sell(self, order: Order, size: float, price: float, fee: float, trade_id: str = "") -> None:
        """A SELL fill: the shares leave the position and the proceeds reduce its cost basis."""
        size = self._take(order, size, price, fee, trade_id)
        if size <= 0:
            return
        p = order.market.pos[order.side]
        p.shares = max(0.0, round(p.shares - size, 6))
        p.cost -= size * price
        p.fee += fee
        self._roll_day()
        self.save_state()
        self._fill_row(order, "sell", price, -size, fee, trade_id)

    def log_order(self, o: Order, note: str = "") -> None:
        self.log.row("orders.csv", market=o.market.slug, strategy=o.strategy, elapsed=f"{o.market.elapsed():.1f}", kind=o.kind,
                     side=o.side, token=o.token[:12], price=o.price, size=o.size, status=o.status,
                     filled=o.filled, avg_price=f"{o.avg_price:.4f}", fee=f"{o.fee:.5f}",
                     order_id=o.order_id, note=note, ref=o.ref)

    def settle(self, m: MarketState, outcome: str, end_price: float | None, source: str) -> float:
        """Realise PnL: winning shares pay 1 USDT each; a tie ("TIE", predict.fun resolves 50-50) pays 0.5 per share."""
        payout = 0.5 * (m.pos["UP"].shares + m.pos["DOWN"].shares) if outcome == "TIE" else m.pos[outcome].shares
        pnl = payout - m.cost - m.fees
        m.settled, m.outcome, m.pnl = True, outcome, pnl
        self._roll_day()
        self.day_pnl += pnl
        self.total_pnl += pnl
        self.n_settled += 1
        self.n_wins += pnl > 0
        self.save_state()
        self.log.row("markets.csv", market=m.slug, market_id=m.market_id, outcome=outcome, source=source, beat=m.beat,
                     end_price=end_price, up_shares=m.pos["UP"].shares, down_shares=m.pos["DOWN"].shares,
                     cost=f"{m.cost:.4f}", fees=f"{m.fees:.5f}", payout=payout, pnl=f"{pnl:.4f}", trades=m.taker_count,
                     day_pnl=f"{self.day_pnl:.4f}")
        return pnl
