"""Order execution (polybot's core/execution.py, for predict.fun): a dry-run simulator driven by the live book, and the
live executor (signed LIMIT orders through core/pfclient.py, fills from the wallet events websocket).

Fees (predict.fun): takers pay feeRateBps/10000 x min(p, 1-p) per share, makers nothing. A buy's fee is taken in shares
(fewer shares received), a sell's in USDT (less received)."""
from __future__ import annotations

import asyncio
import random
import time

from .config import Config
from .logutil import log
from .state import MarketState, Order, Portfolio

WEI = 10 ** 18
SELL_BELOW_BID = 0.05        # a sell is a limit this far under the best bid (it takes what the bids pay down to it)


def taker_fee_usdt(rate_bps: int, price: float, size: float) -> float:
    return rate_bps / 10000 * min(price, 1 - price) * size


def r2(x: float) -> float:
    return round(x + 1e-9, 2)


class BaseExecutor:
    """Every order carries the name of the strategy that placed it; after each fill _fill awaits that strategy's on_fill."""

    def __init__(self, cfg: Config, pf: Portfolio) -> None:
        self.cfg, self.pf = cfg, pf
        self.strategies: dict = {}

    async def start(self) -> None: ...

    async def stop(self) -> None:
        self.pf.save_state()

    async def buy(self, m: MarketState, side: str, price: float, size: float, kind: str, *, strategy: str, ref: str = "") -> Order:
        raise NotImplementedError

    async def sell(self, m: MarketState, side: str, size: float, *, strategy: str) -> Order:
        raise NotImplementedError

    async def cancel(self, o: Order, reason: str = "") -> None:
        raise NotImplementedError

    async def on_book(self, m: MarketState) -> None: ...

    async def _fill(self, o: Order, size: float, price: float, fee: float = 0.0, trade_id: str = "", fee_shares: float = 0.0) -> None:
        size = r2(size)
        if size <= 0:
            return
        if o.kind == "sell":
            self.pf.record_sell(o, size, price, fee, trade_id)
        else:
            self.pf.record_fill(o, size, price, fee, trade_id, fee_shares)
        m = o.market
        log(f"[fill] {m.slug} | {o.strategy} {o.kind} {'SELL' if o.kind == 'sell' else 'BUY'} {o.side} {size:g} sh @ {price:.3f}"
            + (f", fee {fee_shares:.4f} sh" if fee_shares else f", fee {fee:.4f}" if fee else "")
            + f" | order {o.filled:g}/{o.size:g} | position UP {m.pos['UP'].shares:g}@{m.pos['UP'].avg:.3f} "
            f"DOWN {m.pos['DOWN'].shares:g}@{m.pos['DOWN'].avg:.3f} | mtm {m.mtm():+.3f}")
        s = self.strategies.get(o.strategy)
        if s is None:
            return
        try:
            await s.on_fill(o)
        except Exception as e:
            log(f"[fill] {m.slug} | {type(s).__name__}.on_fill failed: {e!r}")


# ---------------------------------------------------------------- dry run --
class DryRunExecutor(BaseExecutor):
    """Fills against the live book after a simulated latency (DRY_RUN_LATENCY_MS). A marketable buy sweeps the asks up
    to its limit as a taker (fee in shares); its remainder rests and fills, as a maker (no fee), once the ask comes down
    to it. A sell sweeps the bids down to its floor."""

    def __init__(self, cfg, pf) -> None:
        super().__init__(cfg, pf)
        self._in_flight: set[int] = set()

    def _new(self, m, side, price, size, kind, strategy, ref="") -> Order:
        o = Order(market=m, token=m.token(side), side=side, price=round(price, m.decimals), size=r2(size), kind=kind,
                  strategy=strategy, ref=ref)
        o.order_id = f"sim-{o.local_id}"
        o.status = "live"
        m.orders.append(o)
        return o

    async def buy(self, m, side, price, size, kind, *, strategy: str, ref: str = "") -> Order:
        o = self._new(m, side, price, size, kind, strategy, ref)
        self.pf.log_order(o, "placed(sim)")
        self._in_flight.add(o.local_id)
        asyncio.create_task(self._arrive(o))
        return o

    async def _arrive(self, o: Order) -> None:
        await asyncio.sleep(self.cfg.dry_latency_ms / 1000)
        self._in_flight.discard(o.local_id)
        if o.done:
            return
        want = o.remaining * random.uniform(0.95, 1.0)
        for p, s in o.market.book(o.side).ask_depth_upto(o.price):
            if want <= 0.005:
                break
            take = min(s, want)
            fee_sh = taker_fee_usdt(o.market.fee_rate_bps, p, take) / p
            await self._fill(o, take, p, 0.0, fee_shares=fee_sh)
            want -= take

    async def sell(self, m, side, size, *, strategy: str) -> Order:
        bid = m.book(side).best_bid or 0.0
        o = self._new(m, side, max(bid - SELL_BELOW_BID, m.tick), size, "sell", strategy)
        self.pf.log_order(o, "placed(sim) SELL")
        asyncio.create_task(self._arrive_sell(o))
        return o

    async def _arrive_sell(self, o: Order) -> None:
        await asyncio.sleep(self.cfg.dry_latency_ms / 1000)
        if o.done:
            return
        want = o.remaining
        for p, s in o.market.book(o.side).bid_depth_downto(o.price):
            if want <= 0.005:
                break
            take = min(s, want)
            await self._fill(o, take, p, taker_fee_usdt(o.market.fee_rate_bps, p, take))
            want -= take
        if not o.done:
            o.status = "cancelled" if o.filled <= 0 else "filled"
            self.pf.log_order(o, "sell remainder cancelled(sim)")

    async def cancel(self, o: Order, reason: str = "") -> None:
        if o.done:
            return
        o.status = "cancelled"
        self.pf.log_order(o, "cancelled(sim)" + (f": {reason}" if reason else ""))

    async def on_book(self, m) -> None:
        for side in ("UP", "DOWN"):
            book = m.book(side)
            ask = book.best_ask
            if ask is None:
                continue
            for o in [o for o in m.orders if o.kind != "sell" and o.side == side and ask <= o.price + 1e-9 and not o.done
                      and o.remaining > 0 and o.local_id not in self._in_flight]:
                avail = sum(s for _, s in book.ask_depth_upto(o.price))
                await self._fill(o, min(o.remaining, avail), o.price, 0.0)   # resting bid hit: maker, no fee


# ------------------------------------------------------------------- live --
class LiveExecutor(BaseExecutor):
    """Signed LIMIT orders (core/pfclient.py); fills from the wallet events (core/feeds.py WalletFeed, by orderHash).
    On stop every open order of this process is removed."""

    def __init__(self, cfg: Config, pf: Portfolio, client) -> None:
        super().__init__(cfg, pf)
        self.client = client
        self.by_hash: dict[str, Order] = {}
        self.wallet = None

    async def start(self) -> None:
        from .feeds import WalletFeed
        await self.client.login()
        try:
            bal = await self.client.usdt_balance()
            log(f"[start] USDT balance {bal:.2f} ({self.client.address})")
        except Exception as e:
            log(f"[start] could not read the USDT balance: {e!r}")
        log("[start] approvals: run `venv/bin/python tools/approve.py` once per wallet (USDT allowance + ERC1155 approval, BNB gas)")
        self.wallet = WalletFeed(self.cfg, self.client, self.on_event)
        self._wallet_task = asyncio.create_task(self.wallet.run())

    async def stop(self) -> None:
        mine = [o for o in self.by_hash.values() if not o.done]
        for o in mine:
            try:
                await self.cancel(o, "process exit")
            except Exception as e:
                log(f"[stop] could not remove order {o.order_id}: {e!r}")
        log(f"[stop] {len(mine)} open order(s) removed")
        if getattr(self, "_wallet_task", None):
            self._wallet_task.cancel()
        self.pf.save_state()

    async def _place(self, o: Order, side_sdk: str) -> Order:
        m = o.market
        m.orders.append(o)
        t0 = time.time()
        try:
            d = await self.client.place_limit(m, side_sdk, o.token, o.price, o.size)
        except Exception as e:
            o.error = str(e)
            o.status = "rejected"
            self.pf.log_order(o, f"rejected: {e}"[:300])
            log(f"[order] {m.slug} | {side_sdk} {o.side} {o.size:g} sh @ {o.price:.2f} REJECTED: {e}")
            return o
        o.order_id, o.order_hash = str(d.get("orderId") or ""), str(d.get("orderHash") or "")
        self.by_hash[o.order_hash.lower()] = o
        o.status = "live"
        lock = d.get("removalLockedUntil")
        self.pf.log_order(o, f"placed in {(time.time() - t0) * 1000:.0f} ms" + (f", removal locked until {lock}" if lock else ""))
        return o

    async def buy(self, m, side, price, size, kind, *, strategy: str, ref: str = "") -> Order:
        o = Order(market=m, token=m.token(side), side=side, price=round(price, m.decimals), size=r2(size), kind=kind,
                  strategy=strategy, ref=ref)
        return await self._place(o, "BUY")

    async def sell(self, m, side, size, *, strategy: str) -> Order:
        bid = m.book(side).best_bid or 0.0
        o = Order(market=m, token=m.token(side), side=side, price=round(max(bid - SELL_BELOW_BID, m.tick), m.decimals),
                  size=r2(size), kind="sell", strategy=strategy)
        return await self._place(o, "SELL")

    async def cancel(self, o: Order, reason: str = "") -> None:
        if o.done or not o.order_id:
            return
        r = await self.client.remove([o.order_id])
        removed = [str(x) for x in (r.get("removed") or [])]
        if o.order_id in removed or str(o.order_id) in [str(x) for x in (r.get("noop") or [])]:
            o.status = "filled" if o.remaining <= 1e-6 else "cancelled"
            self.pf.log_order(o, "removed" + (f": {reason}" if reason else ""))
        else:
            log(f"[order] {o.market.slug} | remove {o.order_id} not done ({reason}): {r}")
        asyncio.create_task(self._reconcile(o))

    async def _reconcile(self, o: Order) -> None:
        """After a remove: what the API says filled, in case a wallet event was missed (logged; the events book fills)."""
        await asyncio.sleep(3)
        try:
            d = await self.client.order(o.order_hash)
        except Exception as e:
            log(f"[order] reconcile {o.order_id} failed: {e!r}")
            return
        if d is None:
            return
        api_filled = int(d.get("amountFilled") or 0) / WEI
        if abs(api_filled - o.filled) > 0.01:
            log(f"[order] {o.market.slug} | {o.order_id}: API says {api_filled:g} filled ({d.get('status')}), booked {o.filled:g} "
                f"(amountFilled may be in USDT for a buy: check)")

    async def on_event(self, ev: dict) -> None:
        """A wallet event (core/feeds.py WalletFeed)."""
        o = self.by_hash.get(str(ev.get("orderHash") or "").lower())
        kind = ev.get("type")
        if o is None:
            return                                           # another process's / a manual order
        if kind == "orderTransactionSuccess":
            f = ev.get("fill") or {}
            size = int(f.get("executedSizeWei") or 0) / WEI
            price = int(f.get("executedPriceWei") or 0) / WEI or o.price
            fee = ev.get("fee") or {}
            fee_amt = int(fee.get("amountWei") or 0) / WEI
            if o.kind == "sell":
                await self._fill(o, size, price, fee_amt if fee.get("type") == "COLLATERAL" else 0.0, str(ev.get("settlementId") or ""))
            else:
                await self._fill(o, size, price, 0.0, str(ev.get("settlementId") or ""),
                                 fee_shares=fee_amt if fee.get("type") == "SHARES" else 0.0)
        elif kind in ("orderCancelled", "orderExpired", "orderNotAccepted"):
            if not o.done:
                o.status = "rejected" if kind == "orderNotAccepted" else "cancelled"
                self.pf.log_order(o, f"{kind}" + (f": {ev.get('reason')}" if ev.get("reason") else ""))
        elif kind == "orderTransactionFailed":
            log(f"[order] {o.market.slug} | {o.order_id}: a settlement failed on-chain ({ev.get('settlementId')})")
        elif kind == "orderMarketMatchPlan" and ev.get("remainderCancelled") not in (None, "0", 0):
            log(f"[order] {o.market.slug} | {o.order_id}: match plan, remainder cancelled {ev.get('remainderCancelled')}")
