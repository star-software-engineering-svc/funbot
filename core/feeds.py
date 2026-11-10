"""predict.fun market data (polybot's ClobFeed / Gamma discovery / UserFeed, for predict.fun):

  * Discovery: every DISCOVERY_EVERY_S the current and the next round are looked up by slug, GET
    /v1/categories/<prefix>-<window start> (btc-updown-5m-1794287100), and become MarketStates.
  * PredictFeed: one websocket (wss://ws.predict.fun/ws, ws-testnet on testnet; the API key at the handshake) subscribed
    to predictOrderbook/<marketId> of every live round. Each message carries the whole Up book; the Down book is its
    mirror. The server's heartbeat (every 15 s) must be echoed or it closes the socket. Nothing is replayed after a
    reconnect: every topic is subscribed again.
  * WalletFeed (live): predictWalletEvents/<jwt> on the same kind of socket: the bot's own order events; fills
    (orderTransactionSuccess) are booked through the executor."""
from __future__ import annotations

import asyncio
import json
import time

import websockets

from .book import mirror
from .config import Config
from .logutil import log
from .pfclient import PredictClient
from .state import CsvLog, MarketState, Portfolio, WINDOW_S

WS_URL = {"mainnet": "wss://ws.predict.fun/ws", "testnet": "wss://ws-testnet.predict.fun/ws"}
BOOK_STALE_S = 40.0                  # heartbeats come every 15 s: this long without any message = a dead socket


def ws_connect(cfg: Config):
    url = WS_URL[cfg.predict_env]
    headers = {"x-api-key": cfg.predict_api_key} if cfg.predict_env == "mainnet" and cfg.predict_api_key else None
    return websockets.connect(url, additional_headers=headers, ping_interval=None, max_queue=None, max_size=None, open_timeout=10)


def market_from_category(cfg: Config, cat: dict) -> MarketState | None:
    """A round (category) -> MarketState, None when it is not a two-outcome up/down market."""
    mk = (cat.get("markets") or [None])[0]
    if not mk:
        return None
    outs = {o.get("indexSet"): o for o in mk.get("outcomes") or []}
    if 1 not in outs or 2 not in outs:
        return None
    start = int(cat["slug"].rsplit("-", 1)[1])
    vd = cat.get("variantData") or mk.get("variantData") or {}
    return MarketState(slug=cat["slug"], start=start, market_id=int(mk["id"]), condition_id=mk.get("conditionId", ""),
                       up_token=str(outs[1]["onChainId"]), down_token=str(outs[2]["onChainId"]),
                       fee_rate_bps=int(mk.get("feeRateBps") or 200), decimals=int(mk.get("decimalPrecision") or 2),
                       neg_risk=bool(mk.get("isNegRisk")), yield_bearing=bool(mk.get("isYieldBearing")),
                       price_feed_id=str(vd.get("priceFeedId") or ""))


class PredictFeed:
    """Discovery + the order books of the live rounds. on_book(m) (the dry-run executor) runs after every book."""

    def __init__(self, cfg: Config, pf: Portfolio, client: PredictClient, on_book=None, csv: CsvLog | None = None) -> None:
        self.cfg, self.pf, self.client, self.on_book, self.csv = cfg, pf, client, on_book, csv
        self.by_id: dict[int, MarketState] = {}
        self.msgs = 0
        self.connected = False
        self._ws = None
        self._subscribed: set[int] = set()
        self._req = 0
        self.on_new_market = None                            # (m) when a round is first seen (core/main.py: logging)

    # ------------------------------------------------------------- discovery --
    async def discovery_loop(self) -> None:
        while True:
            try:
                await self.discover()
            except Exception as e:
                log(f"[predict] discovery failed: {e!r}")
            await asyncio.sleep(self.cfg.discovery_every_s)

    async def discover(self) -> None:
        w = int(time.time()) // WINDOW_S * WINDOW_S
        for start in (w, w + WINDOW_S):
            slug = f"{self.cfg.market_prefix}-{start}"
            if slug in self.pf.markets:
                continue
            cat = await self.client.category(slug)
            if not cat:
                continue
            m = market_from_category(self.cfg, cat)
            if m is None:
                log(f"[predict] {slug}: not a two-outcome market, skipped")
                continue
            self.pf.markets[slug] = m
            self.by_id[m.market_id] = m
            restored = self.pf.restore(m)
            log(f"[predict] {slug} | market {m.market_id} | fee {m.fee_rate_bps} bps | {m.decimals} decimals"
                + (" | neg risk" if m.neg_risk else "") + (" | yield bearing" if m.yield_bearing else "")
                + (f" | position restored UP {m.pos['UP'].shares:g} DOWN {m.pos['DOWN'].shares:g}" if restored else ""))
            if self.on_new_market:
                self.on_new_market(m)
        self.pf.prune()
        for mid in [i for i, m in self.by_id.items() if m.slug not in self.pf.markets]:
            del self.by_id[mid]
        await self._sync_subs()

    def _live(self) -> list[MarketState]:
        t = time.time()
        return [m for m in self.pf.markets.values() if t < m.end + 5]

    async def _sync_subs(self) -> None:
        ws = self._ws
        if ws is None:
            return
        want = {m.market_id for m in self._live()}
        for mid in want - self._subscribed:
            await self._send(ws, "subscribe", f"predictOrderbook/{mid}")
            self._subscribed.add(mid)
            asyncio.create_task(self._rest_snapshot(mid))
        for mid in self._subscribed - want:
            await self._send(ws, "unsubscribe", f"predictOrderbook/{mid}")
            self._subscribed.discard(mid)

    async def _rest_snapshot(self, mid: int) -> None:
        """The book over REST once at subscribe: an empty book sends no websocket message, and this marks it known."""
        try:
            d = await self.client.orderbook(mid)
        except Exception as e:
            log(f"[predict] orderbook {mid} over REST failed: {e!r}")
            return
        m = self.by_id.get(mid)
        if d is not None and m is not None and not m.up_book.ready:
            self.apply_book(m, d.get("bids") or [], d.get("asks") or [])

    async def _send(self, ws, method: str, topic: str) -> None:
        self._req += 1
        await ws.send(json.dumps({"method": method, "requestId": self._req, "params": [topic]}))

    # ----------------------------------------------------------------- books --
    async def run(self) -> None:
        backoff = 1
        while True:
            try:
                async with ws_connect(self.cfg) as ws:
                    self._ws, self._subscribed, self.connected = ws, set(), True
                    log(f"[predict] book websocket connected ({self.cfg.predict_env})")
                    backoff = 1
                    await self._sync_subs()
                    last = time.time()
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=BOOK_STALE_S - (time.time() - last))
                        except asyncio.TimeoutError:
                            raise RuntimeError(f"no message for {BOOK_STALE_S:g}s")
                        last = time.time()
                        await self._handle(ws, json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(f"[predict] book websocket dropped: {e!r} | reconnecting in {backoff}s")
            finally:
                self._ws, self.connected = None, False
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _handle(self, ws, msg: dict) -> None:
        self.msgs += 1
        if msg.get("type") == "R":
            if not msg.get("success"):
                log(f"[predict] request {msg.get('requestId')} refused: {msg.get('error')}")
            return
        topic = msg.get("topic") or ""
        if topic == "heartbeat":
            await ws.send(json.dumps({"method": "heartbeat", "data": msg.get("data")}))
            return
        if topic == "system":
            log(f"[predict] system: {msg.get('data')}")
            return
        if topic.startswith("predictOrderbook/"):
            d = msg.get("data") or {}
            m = self.by_id.get(int(d.get("marketId") or topic.split("/")[1]))
            if m is not None:
                self.apply_book(m, d.get("bids") or [], d.get("asks") or [])

    def apply_book(self, m: MarketState, bids: list, asks: list) -> None:
        """The Up book from a message, the Down book its mirror; a prices.csv row when a best price moved."""
        before = (m.up_book.best_bid, m.up_book.best_ask)
        m.up_book.snapshot(bids, asks)
        db, da = mirror(bids, asks, m.decimals)
        m.down_book.snapshot(db, da)
        if self.csv is not None and (m.up_book.best_bid, m.up_book.best_ask) != before:
            self.csv.row("prices.csv", market=m.slug, elapsed=f"{m.elapsed():.2f}", up_bid=m.up_book.best_bid,
                         up_ask=m.up_book.best_ask, down_bid=m.down_book.best_bid, down_ask=m.down_book.best_ask)
        if self.on_book is not None:
            asyncio.create_task(self.on_book(m))


class WalletFeed:
    """Live only: the bot's own order events (predictWalletEvents/<jwt>). on_event(event dict) gets every event; the
    executor matches them by orderHash. The JWT is renewed and the topic subscribed again at every reconnect."""

    def __init__(self, cfg: Config, client: PredictClient, on_event) -> None:
        self.cfg, self.client, self.on_event = cfg, client, on_event
        self.msgs = 0
        self.connected = False

    async def run(self) -> None:
        backoff = 1
        while True:
            try:
                await self.client.ensure_jwt()
                async with ws_connect(self.cfg) as ws:
                    await ws.send(json.dumps({"method": "subscribe", "requestId": 1, "params": [f"predictWalletEvents/{self.client.jwt}"]}))
                    self.connected = True
                    log("[wallet] wallet events websocket connected")
                    backoff = 1
                    async for raw in ws:
                        msg = json.loads(raw)
                        self.msgs += 1
                        if msg.get("type") == "R":
                            if not msg.get("success"):
                                raise RuntimeError(f"wallet events subscribe refused: {msg.get('error')}")
                            continue
                        topic = msg.get("topic") or ""
                        if topic == "heartbeat":
                            await ws.send(json.dumps({"method": "heartbeat", "data": msg.get("data")}))
                        elif topic.startswith("predictWalletEvents/"):
                            try:
                                await self.on_event(msg.get("data") or {})
                            except Exception as e:
                                log(f"[wallet] event handling failed: {e!r} | {str(msg)[:300]}")
                    raise RuntimeError("closed by the server")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log(f"[wallet] websocket dropped: {e!r} | reconnecting in {backoff}s")
            finally:
                self.connected = False
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
