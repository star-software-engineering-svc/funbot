"""Price feeds: the resolution price of predict.fun's BTC Up/Down markets and its comparison feeds.

predict.fun's BTC 5m markets resolve on the Chainlink Data Streams BTC/USDT stream (topofbook-datalink, feed
0x000366e0..., schema v3), whose price is the mid of Binance's BTCUSDT top of book. So:

  * binance_feed: Binance SPOT bookTicker BTCUSDT -> its mid. The stream's own input, no key needed; always on.
  * chainlink_feed: the Data Streams websocket (needs CHAINLINK_API_KEY / _SECRET), its decoded v3 reports -> the
    benchmark price, bid and ask. The resolution source itself; optional.
  * coinbase_feed: Coinbase BTC-USDT level2 mid. Comparison only (coinbase.csv, status line), nothing trades on it.

RefPrice merges them into the one price the strategy and the settler read (PRICE_SOURCE: chainlink | binance | auto =
chainlink while its last report is under CHAINLINK_STALE_S old, else binance) and keeps a short history of both for the
price at a window's start / end (price_at)."""
from __future__ import annotations

import asyncio
import bisect
import hashlib
import hmac
import json
import time
from collections import deque
from datetime import datetime
from itertools import islice

import websockets

from .logutil import log
from .state import CsvLog

BINANCE_SPOT = "wss://stream.binance.com:9443/ws/{sym}@bookTicker"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
BINANCE_STALE_S = 10.0
COINBASE_STALE_S = 30.0
CHAINLINK_STALE_S = 5.0          # auto: a Chainlink report older than this hands the price to Binance
TICK_HEARTBEAT_S = 0.25          # tick logs: a row with an unchanged price too once the last row is this old (polybot's)


def tick_row_due(now: float, last_row: float) -> bool:
    return int(now) != int(last_row) or now - last_row >= TICK_HEARTBEAT_S


# ----------------------------------------------------------- price series --
class PriceSeries:
    """The last KEEP seconds of (ts, price) of one source; ts = the source's own time when it has one."""

    def __init__(self, name: str, keep_s: float = 1200) -> None:
        self.name = name
        self.ts: deque[float] = deque()
        self.px: deque[float] = deque()
        self.keep = keep_s
        self.msgs = 0
        self.recv_ts: float | None = None                    # when the newest sample arrived

    def push(self, ts: float, price: float) -> None:
        self.recv_ts = time.time()
        self.msgs += 1
        if self.ts and ts < self.ts[-1]:
            return                                           # out of order (a reconnect replays): ignored
        self.ts.append(ts); self.px.append(price)
        while self.ts and ts - self.ts[0] > self.keep:
            self.ts.popleft(); self.px.popleft()

    @property
    def last(self) -> float | None:
        return self.px[-1] if self.px else None

    @property
    def last_ts(self) -> float | None:
        return self.ts[-1] if self.ts else None

    def at(self, t: float, max_before_s: float = 5.0) -> tuple[float, float] | None:
        """(ts, price) of the sample in force at t: the last one at or before t, at most max_before_s old."""
        i = bisect.bisect_right(self.ts, t) - 1
        if i >= 0 and t - self.ts[i] <= max_before_s:
            return self.ts[i], self.px[i]
        return None

    def window(self, t0: float, t1: float) -> list[tuple[float, float]]:
        i0, i1 = bisect.bisect_left(self.ts, t0), bisect.bisect_right(self.ts, t1)
        return list(zip(islice(self.ts, i0, i1), islice(self.px, i0, i1)))


class RefPrice:
    """The resolution price as the bot sees it (see the module docstring). on_price(ts, price) hooks (the strategy's
    signal feed) run at every new value of the chosen source."""

    def __init__(self, source: str) -> None:
        self.source = source                                 # chainlink | binance | auto
        self.binance = PriceSeries("binance")
        self.chainlink = PriceSeries("chainlink")
        self.coinbase = PriceSeries("coinbase")
        self.hooks: list = []

    def using(self) -> str:
        """The source in force now."""
        if self.source != "auto":
            return self.source
        r = self.chainlink.recv_ts
        return "chainlink" if r is not None and time.time() - r <= CHAINLINK_STALE_S else "binance"

    @property
    def last(self) -> float | None:
        return (self.chainlink if self.using() == "chainlink" else self.binance).last

    @property
    def age_s(self) -> float | None:
        s = self.chainlink if self.using() == "chainlink" else self.binance
        return None if s.recv_ts is None else time.time() - s.recv_ts

    def _emit(self, src: str, ts: float, price: float) -> None:
        if self.using() != src:
            return
        for h in self.hooks:
            try:
                h(ts, price)
            except Exception as e:
                log(f"[price] hook failed: {e!r}")

    def on_binance(self, ts: float, mid: float) -> None:
        self.binance.push(ts, mid)
        self._emit("binance", ts, mid)

    def on_chainlink(self, ts: float, price: float) -> None:
        self.chainlink.push(ts, price)
        self._emit("chainlink", ts, price)

    def price_at(self, t: float) -> tuple[float, str] | None:
        """The resolution price at time t (a window's start or end) and where it came from: the Chainlink report in force
        at t (observation time), else the Binance mid in force at t."""
        c = self.chainlink.at(t, 2.0)
        if c is not None:
            return c[1], "chainlink"
        b = self.binance.at(t, 2.0)
        if b is not None:
            return b[1], "binance"
        return None


class _Watchdog:
    """Closes a websocket that delivered nothing for stale_s (polybot's): the reader's loop ends and the caller reconnects."""

    def __init__(self, ws, stale_s: float) -> None:
        self.ws, self.stale_s = ws, stale_s
        self.last = time.time()
        self.stale = False
        self.task = asyncio.create_task(self._run())

    def seen(self) -> None:
        self.last = time.time()

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            if time.time() - self.last >= self.stale_s:
                self.stale = True
                await self.ws.close()
                return

    def stop(self) -> None:
        self.task.cancel()


# ---------------------------------------------------------------- binance --
async def binance_feed(symbol: str, ref: RefPrice, csv: CsvLog | None = None) -> None:
    """Binance spot bookTicker -> ref.on_binance(receive ts, mid). Spot bookTicker carries no event time, so the receive
    time stands in. A binance.csv row at every mid change (and heartbeats). Reconnects with backoff."""
    url = BINANCE_SPOT.format(sym=symbol)
    backoff, last_mid, last_row = 1, None, 0.0
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, max_queue=None, open_timeout=10) as ws:
                log(f"[binance] spot bookTicker {symbol} connected")
                backoff = 1
                dog = _Watchdog(ws, BINANCE_STALE_S)
                try:
                    async for raw in ws:
                        dog.seen()
                        m = json.loads(raw)
                        now, bid, ask = time.time(), float(m["b"]), float(m["a"])
                        mid = (bid + ask) / 2
                        if mid != last_mid:
                            ref.on_binance(now, mid)
                        if csv is not None and (mid != last_mid or tick_row_due(now, last_row)):
                            last_row = now
                            csv.row("binance.csv", ts=now, event_ts="", bid=bid, ask=ask, mid=f"{mid:.3f}")
                        last_mid = mid
                finally:
                    dog.stop()
                raise RuntimeError(f"no message for {BINANCE_STALE_S:g}s" if dog.stale else "closed by the server")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(f"[binance] websocket dropped: {e!r} | reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15)


# --------------------------------------------------------------- coinbase --
def _iso_ts(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


async def coinbase_feed(product: str, ref: RefPrice, csv: CsvLog | None = None) -> None:
    """Coinbase ticker channel (best bid / ask at every trade) -> ref.coinbase and coinbase.csv. Comparison only."""
    backoff, last_mid, last_row = 1, None, 0.0
    while True:
        try:
            async with websockets.connect(COINBASE_WS, ping_interval=20, max_queue=None, max_size=None, open_timeout=10) as ws:
                await ws.send(json.dumps({"type": "subscribe", "product_ids": [product], "channels": ["ticker"]}))
                log(f"[coinbase] {product} ticker connected")
                backoff = 1
                dog = _Watchdog(ws, COINBASE_STALE_S)
                try:
                    async for raw in ws:
                        dog.seen()
                        m = json.loads(raw)
                        if m.get("type") == "error":
                            raise RuntimeError(f"coinbase error: {m.get('message')} {m.get('reason') or ''}")
                        if m.get("type") != "ticker" or not m.get("best_bid") or not m.get("best_ask"):
                            continue
                        now = time.time()
                        bid, ask = float(m["best_bid"]), float(m["best_ask"])
                        mid = (bid + ask) / 2
                        ev = _iso_ts(m.get("time"))
                        if mid != last_mid:
                            ref.coinbase.push(now, mid)
                        if csv is not None and (mid != last_mid or tick_row_due(now, last_row)):
                            last_row = now
                            csv.row("coinbase.csv", ts=now, event_ts="" if ev is None else f"{ev:.3f}", bid=bid, ask=ask, mid=f"{mid:.3f}")
                        last_mid = mid
                finally:
                    dog.stop()
                raise RuntimeError(f"no message for {COINBASE_STALE_S:g}s" if dog.stale else "closed by the server")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(f"[coinbase] websocket dropped: {e!r} | reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# -------------------------------------------------------------- chainlink --
def _hmac_headers(method: str, path: str, key: str, secret: str, body: bytes = b"") -> dict:
    """Chainlink Data Streams HMAC auth: HMAC-SHA256(secret, "<METHOD> <path?query> <sha256(body) hex> <key> <ts ms>")."""
    ts = str(int(time.time() * 1000))
    msg = f"{method} {path} {hashlib.sha256(body).hexdigest()} {key} {ts}"
    sig = hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return {"Authorization": key, "X-Authorization-Timestamp": ts, "X-Authorization-Signature-SHA256": sig}


def decode_v3(full_report_hex: str) -> dict:
    """A Data Streams fullReport (schema v3, crypto streams) -> dict(feed_id, obs_ts, price, bid, ask). Prices carry 18
    decimals."""
    from eth_abi import decode
    raw = bytes.fromhex(full_report_hex[2:] if full_report_hex.startswith("0x") else full_report_hex)
    _ctx, blob, _rs, _ss, _vs = decode(["bytes32[3]", "bytes", "bytes32[]", "bytes32[]", "bytes32"], raw)
    fid, _valid_from, obs_ts, _nfee, _lfee, _exp, price, bid, ask = decode(
        ["bytes32", "uint32", "uint32", "uint192", "uint192", "uint32", "int192", "int192", "int192"], blob)
    return dict(feed_id="0x" + fid.hex(), obs_ts=float(obs_ts), price=price / 1e18, bid=bid / 1e18, ask=ask / 1e18)


async def chainlink_feed(url: str, feed_id: str, key: str, secret: str, ref: RefPrice, csv: CsvLog | None = None) -> None:
    """Chainlink Data Streams websocket (/api/v1/ws?feedIDs=<id>) -> ref.on_chainlink(observation ts, benchmark price) and
    chainlink.csv. Reconnects with backoff; never raises into the caller."""
    path = f"/api/v1/ws?feedIDs={feed_id}"
    backoff, last_obs = 1, None
    while True:
        try:
            async with websockets.connect(url.rstrip("/") + path, additional_headers=_hmac_headers("GET", path, key, secret),
                                          ping_interval=20, max_queue=None, max_size=None, open_timeout=10) as ws:
                log(f"[chainlink] Data Streams {feed_id[:10]}.. connected")
                backoff = 1
                dog = _Watchdog(ws, 15.0)
                try:
                    async for raw in ws:
                        dog.seen()
                        m = json.loads(raw)
                        rep = m.get("report") or {}
                        if not rep.get("fullReport"):
                            continue
                        r = decode_v3(rep["fullReport"])
                        if r["obs_ts"] == last_obs:
                            continue                                 # the same observation from another DON node
                        last_obs = r["obs_ts"]
                        ref.on_chainlink(r["obs_ts"], r["price"])
                        if csv is not None:
                            csv.row("chainlink.csv", ts=time.time(), obs_ts=f"{r['obs_ts']:.0f}", price=f"{r['price']:.4f}",
                                    bid=f"{r['bid']:.4f}", ask=f"{r['ask']:.4f}",
                                    binance="" if ref.binance.last is None else f"{ref.binance.last:.3f}")
                finally:
                    dog.stop()
                raise RuntimeError("no message for 15s" if dog.stale else "closed by the server")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log(f"[chainlink] websocket dropped: {e!r} | reconnecting in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
