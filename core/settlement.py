"""Settlement of every market of the process (polybot's core/settlement.py, for predict.fun). Every SETTLE_EVERY_S:

* the price to beat: from 0.5 s after a window's start, the resolution price at the start (core/prices.py
  RefPrice.price_at: the Chainlink report in force at the start, else the Binance mid) is pinned as m.beat (opens.csv);
* from the window's end on, every order still open in the market is cancelled;
* from 5 s after the end, a market with a position is resolved: our own end price against the beat when it is at least
  SETTLE_BAND_USD away (Up when above, Down when below), else predict.fun's resolution (GET /v1/markets/{id}: status
  RESOLVED, resolution.indexSet 1 = Up, 2 = Down; both outcomes resolved = the 50-50 tie), asked from 60 s after the end
  every API_EVERY_S; 30 min after the end an unresolved market is left unsettled.
A resolved market is booked with Portfolio.settle and handed to every strategy's on_settle."""
from __future__ import annotations

import asyncio
import time

from .config import Config
from .execution import BaseExecutor
from .logutil import log
from .pfclient import PredictClient
from .prices import RefPrice
from .state import CsvLog, MarketState, Portfolio

SETTLE_EVERY_S = 1.0
API_EVERY_S = 20.0


def api_outcome(mk: dict) -> str | None:
    """predict.fun's result of a market: UP | DOWN | TIE, None while unresolved."""
    if (mk.get("status") or "").upper() != "RESOLVED":
        return None
    res = mk.get("resolution") or {}
    won = [o.get("indexSet") for o in mk.get("outcomes") or [] if (o.get("status") or "").upper() == "WON"]
    if len(won) == 2:
        return "TIE"
    idx = res.get("indexSet") if isinstance(res, dict) else None
    idx = idx or (won[0] if won else None)
    return {1: "UP", 2: "DOWN"}.get(idx)


class Settler:
    def __init__(self, cfg: Config, pf: Portfolio, ref: RefPrice, ex: BaseExecutor, client: PredictClient, csv: CsvLog) -> None:
        self.cfg, self.pf, self.ref, self.ex, self.client, self.csv = cfg, pf, ref, ex, client, csv
        self._asked: dict[str, float] = {}

    async def loop(self) -> None:
        while True:
            try:
                await self.settle_once()
            except Exception as e:
                log(f"[settle] settlement pass failed: {e!r}")
            await asyncio.sleep(SETTLE_EVERY_S)

    def _pin_beat(self, m: MarketState, now: float) -> None:
        if m.beat is not None or now < m.start + 0.5:
            return
        r = self.ref.price_at(m.start)
        if r is None:
            return
        m.beat, m.beat_src = r
        b, c = self.ref.binance.at(m.start, 2.0), self.ref.chainlink.at(m.start, 2.0)
        self.csv.row("opens.csv", market=m.slug, start=m.start, source=m.beat_src, price=f"{m.beat:.3f}",
                     binance="" if b is None else f"{b[1]:.3f}", chainlink="" if c is None else f"{c[1]:.3f}", api="")
        log(f"[open] {m.slug} | price to beat {m.beat:.2f} ({m.beat_src})")

    async def settle_once(self) -> None:
        now = time.time()
        for m in list(self.pf.markets.values()):
            if m.start <= now < m.start + 30:
                self._pin_beat(m, now)
            if now >= m.end:
                for o in m.open_orders():
                    await self.ex.cancel(o, "window ended")
            if m.settled or now < m.end + 5:
                continue
            if m.cost == 0:
                m.settled = True
                continue
            outcome, src, end_px = None, "", None
            r = self.ref.price_at(m.end)
            if r is not None:
                end_px = r[0]
                if m.beat is not None and abs(end_px - m.beat) >= self.cfg.settle_band_usd:
                    outcome, src = ("UP" if end_px > m.beat else "DOWN"), f"{m.beat_src}/{r[1]}"
            if outcome is None and now >= m.end + 60 and now - self._asked.get(m.slug, 0) >= API_EVERY_S:
                self._asked[m.slug] = now
                try:
                    mk = await self.client.market(m.market_id)
                    outcome = api_outcome(mk or {})
                    if outcome:
                        vd = (mk or {}).get("variantData") or {}
                        src = f"predict.fun (start {vd.get('startPrice')}, end {vd.get('endPrice')})"
                except Exception as e:
                    log(f"[settle] {m.slug} | market lookup failed: {e!r}")
            if outcome is None:
                if now > m.end + 1800:
                    log(f"[settle] {m.slug} | no resolution 30 min after the window | leaving unsettled")
                    m.settled = True
                continue
            pnl = self.pf.settle(m, outcome, end_px, src)
            notes = []
            for s in list(self.ex.strategies.values()):
                try:
                    note = await s.on_settle(m, outcome, pnl)
                    if note:
                        notes.append(note)
                except Exception as e:
                    log(f"[settle] {m.slug} | {type(s).__name__}.on_settle failed: {e!r}")
            px = lambda v: f"{v:.2f}" if v is not None else "?"
            log(f"[settle] {m.slug} | resolved {outcome} via {src} | price to beat {px(m.beat)} -> end {px(end_px)} "
                f"| position UP {m.pos['UP'].shares:g} DOWN {m.pos['DOWN'].shares:g} | cost {m.cost:.2f} "
                f"| pnl {pnl:+.3f} | day {self.pf.day_pnl:+.2f}, total {self.pf.total_pnl:+.2f} "
                f"({self.pf.n_wins} won of {self.pf.n_settled})" + (" | restored after restart" if m.restored else "")
                + "".join(f" | {n}" for n in notes))
