"""FairV1Strategy: buys on the fair signal (core/strategy/signals/fair.py), held to settlement. Parameters:
config/strategy/fairv1.py (FAIRV1_*). Dry run only until FAIRV1_LIVE = True.

At every new resolution price (core/prices.py RefPrice: the Chainlink BTC/USDT report, or the Binance spot mid standing
in) and every LOOP_S between them, the current window's P(UP) = Phi((price - beat) / (sigma x sqrt(s left))) is
checked against the asks: a move of at least EDGE toward a side whose ask P still covers buys SHARE_SIZE x 1 / 2 / 3.
A buy is a limit at ask + BUY_SLIP, raised to the $1 minimum; what does not fill at once rests TAKER_TTL_S, then its
remainder is cancelled. No new buy once the window's cost reaches MAX_COST.

Every signal is a signals.csv row."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from ..config import Config
from ..execution import BaseExecutor
from ..logutil import log
from ..prices import RefPrice
from ..state import CsvLog, MarketState, Order, Portfolio, WINDOW_S
from .common.orders import MIN_NOTIONAL, SIZE_TOL_SH, min_notional_shares
from .signals import fair as _fair

LOOP_S = 0.05


@dataclass
class _Local:
    fair: _fair.Fair = field(default_factory=_fair.Fair)
    ttl_cancelled: set = field(default_factory=set)
    no_beat_logged: bool = False


class FairV1Strategy:
    name = "fairv1"
    run_flag = "fairv1_run"
    live_flag = "fairv1_live"

    def __init__(self, cfg: Config, pf: Portfolio, ref: RefPrice, ex: BaseExecutor, csv: CsvLog) -> None:
        self.cfg, self.pf, self.ref, self.ex, self.csv = cfg, pf, ref, ex, csv
        ex.strategies[self.name] = self
        self._local: dict[str, _Local] = {}
        self.feed = _fair.FairFeed()
        ref.hooks.append(self._on_price)

    # ------------------------------------------------------------- helpers --
    def local(self, m: MarketState) -> _Local:
        st = self._local.get(m.slug)
        if st is None:
            st = self._local[m.slug] = _Local()
            for old in [k for k in self._local if k != m.slug]:
                del self._local[old]
        return st

    def current(self) -> MarketState | None:
        return self.pf.markets.get(f"{self.cfg.market_prefix}-{int(time.time()) // WINDOW_S * WINDOW_S}")

    def _row(self, m: MarketState, now: float, signal: str, side: str, action: str, reason: str, why: str) -> None:
        st = self.local(m)
        px = self.ref.last
        num = lambda v, d=2: "" if v is None else f"{v:.{d}f}"
        age = self.ref.age_s
        self.csv.row("signals.csv", ts=now, market=m.slug, elapsed=f"{now - m.start:.2f}", signal=signal, side=side,
                     btc=num(px), beat=num(m.beat), delta=num(None if px is None or m.beat is None else px - m.beat),
                     p_up=num(st.fair.p_up, 3), sigma=num(st.fair.sigma, 3), age_ms=num(None if age is None else age * 1000, 0),
                     up_bid=m.up_book.best_bid, up_ask=m.up_book.best_ask, down_bid=m.down_book.best_bid,
                     down_ask=m.down_book.best_ask, action=action, reason=reason, why=why)

    # -------------------------------------------------------------- signal --
    def _on_price(self, ts: float, px: float) -> None:
        self.feed.on_price(ts, px)
        try:
            self._check(time.time())
        except Exception as e:                               # never let the strategy break the price feed
            log(f"[fairv1] signal check failed: {e!r}")

    def _check(self, now: float) -> None:
        c = self.cfg
        if not c.fairv1_run or not c.fairv1_signal:
            return
        m = self.current()
        if m is None or not m.books_ready or not m.start <= now < m.end:
            return
        st = self.local(m)
        if m.beat is None:
            if not st.no_beat_logged and now - m.start >= c.fairv1_start_s:
                st.no_beat_logged = True
                log(f"[fairv1] {m.slug} | no price to beat pinned (no price within 2 s of the start): no signal in this window")
            return
        q = st.fair.check(self.feed, now, m.start, m.end, m.beat, {"UP": m.up_book.best_ask, "DOWN": m.down_book.best_ask},
                          start_s=c.fairv1_start_s, end_s=c.fairv1_end_s, score_s=c.fairv1_score_s, edge=c.fairv1_edge,
                          edge2=c.fairv1_edge2, edge3=c.fairv1_edge3, share_size=c.fairv1_share_size, ask_min=c.fairv1_ask_min,
                          ask_max=c.fairv1_ask_max, gap_s=c.fairv1_gap_s, max_imb=c.fairv1_max_imbalance,
                          smooth_s=c.fairv1_smooth_s, max_age_s=c.fairv1_max_age_ms / 1000, min_sigma=c.fairv1_min_sigma,
                          slip=c.fairv1_slip)
        if q is None:
            return
        if m.cost >= c.fairv1_max_cost:
            self._row(m, now, "fair", q["side"], "skip", f"window cost {m.cost:.2f} >= FAIRV1_MAX_COST {c.fairv1_max_cost:g}", q["text"])
            st.fair.bought(q, now, 0.0)
            return
        side, ask = q["side"], q["ask"]
        limit = round(min(ask + c.fairv1_buy_slip, 1 - m.tick), m.decimals)
        size = round(max(q["size"], m.min_size, min_notional_shares(ask) if ask * q["size"] < MIN_NOTIONAL else 0.0), 2)
        st.fair.bought(q, now, size)
        m.taker_count += 1
        m.last_entry = now
        asyncio.create_task(self._buy(m, side, limit, size))
        log(f"[fairv1] {m.slug} | {q['text']} | BUY {side} {size:g} sh limit @ {limit:.2f} ({self.ref.using()})")
        self._row(m, now, "fair", side, f"BUY {side} {size:g} up to {limit:.2f}", "", q["text"])

    async def _buy(self, m: MarketState, side: str, limit: float, size: float) -> Order | None:
        try:
            o = await self.ex.buy(m, side, limit, size, "taker", strategy=self.name)
        except Exception as e:
            log(f"[fairv1] {m.slug} | BUY {side} {size:g} @ {limit:.2f} failed: {e!r}")
            m.taker_count = max(0, m.taker_count - 1)
            return None
        if o.status == "rejected":
            m.taker_count = max(0, m.taker_count - 1)
        return o

    # ---------------------------------------------------------------- loop --
    async def loop(self) -> None:
        c = self.cfg
        log(f"[fairv1] P(UP) of the resolution price ({self.ref.source}) moved >= {c.fairv1_edge:g} in {c.fairv1_score_s:g} s and "
            f">= the ask - {c.fairv1_slip:g}, {c.fairv1_start_s:g} .. {c.fairv1_end_s:g} s, {c.fairv1_share_size:g} sh x 1 / 2 / 3, "
            f"ask in [{c.fairv1_ask_min:g}, {c.fairv1_ask_max:g}], limit at ask + {c.fairv1_buy_slip:g}, TTL {c.fairv1_taker_ttl_s:g} s, "
            f"max cost {c.fairv1_max_cost:g} per window" + ("" if c.fairv1_signal else " | SIGNAL OFF: feeds and books logged only"))
        running = None
        while True:
            try:
                on = bool(getattr(c, self.run_flag))
                if on != running:
                    log(f"[strategy] FairV1Strategy {'running' if on else 'paused'} ({self.run_flag.upper()} {'on' if on else 'off'})")
                    running = on
                if on:
                    await self.step()
                    self._check(time.time())                 # between price changes too: time to the close shrinks the spread
            except Exception as e:
                log(f"[strategy] FairV1Strategy step failed: {e!r}")
            await asyncio.sleep(LOOP_S)

    async def step(self) -> None:
        """The current market's TAKER_TTL_S cancels."""
        m = self.current()
        if m is None:
            return
        c, now = self.cfg, time.time()
        if now >= m.end or c.fairv1_taker_ttl_s <= 0:
            return
        st = self.local(m)
        for o in [o for o in m.orders if o.kind == "taker" and o.strategy == self.name and not o.done
                  and now - o.created_at >= c.fairv1_taker_ttl_s and o.local_id not in st.ttl_cancelled]:
            st.ttl_cancelled.add(o.local_id)
            log(f"[fairv1] {m.slug} | buy {o.order_id[:12] or '(posting)'} BUY {o.side} filled {o.filled:g}/{o.size:g} after "
                f"{now - o.created_at:.1f} s: cancelled (FAIRV1_TAKER_TTL_S {c.fairv1_taker_ttl_s:g})")
            await self.ex.cancel(o, f"FAIRV1_TAKER_TTL_S {c.fairv1_taker_ttl_s:g}")
            if o.filled <= SIZE_TOL_SH:
                m.taker_count = max(0, m.taker_count - 1)

    # -------------------------------------------------------------- events --
    async def on_fill(self, o: Order) -> None:
        if o.status == "filled":
            m = o.market
            log(f"[taker] {m.slug} | fairv1 | {o.order_id[:12]} BUY {o.side} filled {o.filled:g} sh @ {o.avg_price:.3f} | "
                f"pos UP {m.pos['UP'].shares:g} DOWN {m.pos['DOWN'].shares:g} | if UP {m.pnl_if('UP'):+.2f} / DOWN {m.pnl_if('DOWN'):+.2f}")

    async def on_settle(self, m: MarketState, outcome: str, pnl: float) -> str:
        return ""
