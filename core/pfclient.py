"""predict.fun REST client (polybot's core/pmclient.py's place): the API key, the JWT login, the market lookups and,
for live trading, order signing (predict-sdk OrderBuilder) and sending / removing.

  * mainnet https://api.predict.fun (x-api-key on every call), testnet https://api-testnet.predict.fun (no key);
  * JWT: GET /v1/auth/message -> personal_sign -> POST /v1/auth {signer, message, signature} -> Bearer token (24 h);
    a Predict smart account signs with OrderBuilder.sign_predict_account_message and logs in as the account address;
  * orders: POST /v1/orders {data: {order: {..typed data message, hash, signature}, pricePerShare, strategy}} -> orderId,
    orderHash; POST /v1/orders/remove {data: {ids}}; GET /v1/orders/{hash} -> status, amountFilled (wei).
Amounts are wei strings (1e18 = 1 USDT = 1 share). Prices carry at most the market's decimalPrecision decimals."""
from __future__ import annotations

import asyncio
import time

import aiohttp

from .config import Config
from .logutil import log

WEI = 10 ** 18
JWT_REFRESH_S = 20 * 3600            # the JWT lasts 24 h: renewed well before


class ApiError(RuntimeError):
    def __init__(self, status: int, body) -> None:
        self.status, self.body = status, body
        msg = body.get("message") or body.get("error") if isinstance(body, dict) else body
        super().__init__(f"HTTP {status}: {msg}")


class PredictClient:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.base = cfg.api_base
        self.session: aiohttp.ClientSession | None = None
        self.jwt: str = ""
        self.jwt_at = 0.0
        self.builder = None                                  # predict_sdk.OrderBuilder (live only)
        self.address = ""                                    # the wallet orders are made for (the account, or the EOA)

    async def start(self) -> None:
        h = {"x-api-key": self.cfg.predict_api_key} if self.cfg.predict_api_key and self.cfg.predict_env == "mainnet" else {}
        self.session = aiohttp.ClientSession(headers=h, timeout=aiohttp.ClientTimeout(total=10))

    async def close(self) -> None:
        if self.session:
            await self.session.close()

    # ------------------------------------------------------------- plumbing --
    async def _req(self, method: str, path: str, *, auth: bool = False, params=None, body=None, ok404: bool = False):
        headers = {}
        if auth:
            await self.ensure_jwt()
            headers["Authorization"] = f"Bearer {self.jwt}"
        for attempt in range(3):
            async with self.session.request(method, self.base + path, params=params, json=body, headers=headers) as r:
                try:
                    data = await r.json(content_type=None)
                except Exception:
                    data = {"message": (await r.text())[:300]}
                if r.status == 429 and attempt < 2:                  # rate limited: wait for the window's reset
                    wait = float(r.headers.get("RateLimit-Reset") or 1)
                    log(f"[predict] 429 on {path}: waiting {wait:g}s")
                    await asyncio.sleep(min(wait, 10))
                    continue
                if r.status == 404 and ok404:
                    return None
                if r.status >= 400:
                    raise ApiError(r.status, data)
                return data
        raise ApiError(429, {"message": "rate limited"})

    # --------------------------------------------------------------- market --
    async def category(self, slug: str) -> dict | None:
        """GET /v1/categories/{slug}: one round (btc-updown-5m-<start>) with its market(s); None when unknown."""
        r = await self._req("GET", f"/v1/categories/{slug}", ok404=True)
        return None if r is None else r.get("data")

    async def market(self, market_id: int) -> dict | None:
        r = await self._req("GET", f"/v1/markets/{market_id}", ok404=True)
        return None if r is None else r.get("data")

    async def orderbook(self, market_id: int) -> dict | None:
        r = await self._req("GET", f"/v1/markets/{market_id}/orderbook", ok404=True)
        return None if r is None else r.get("data")

    # ----------------------------------------------------------------- auth --
    def _make_builder(self):
        from predict_sdk import ChainId, OrderBuilder, OrderBuilderOptions
        chain = ChainId.BNB_TESTNET if self.cfg.predict_env == "testnet" else ChainId.BNB_MAINNET
        opts = OrderBuilderOptions(predict_account=self.cfg.predict_account) if self.cfg.predict_account else None
        return OrderBuilder.make(chain, self.cfg.private_key, opts)

    async def login(self) -> None:
        """Make the order builder (live) and get a JWT for the wallet."""
        if self.builder is None:
            self.builder = await asyncio.to_thread(self._make_builder)
            from eth_account import Account
            self.address = self.cfg.predict_account or Account.from_key(self.cfg.private_key).address
        msg = (await self._req("GET", "/v1/auth/message"))["data"]["message"]
        if self.cfg.predict_account:
            sig = self.builder.sign_predict_account_message(msg)
        else:
            from eth_account import Account
            from eth_account.messages import encode_defunct
            sig = Account.from_key(self.cfg.private_key).sign_message(encode_defunct(text=msg)).signature.hex()
        sig = sig if sig.startswith("0x") else "0x" + sig
        r = await self._req("POST", "/v1/auth", body={"signer": self.address, "message": msg, "signature": sig})
        self.jwt, self.jwt_at = r["data"]["token"], time.time()
        log(f"[predict] logged in as {self.address} ({self.cfg.predict_env})")

    async def ensure_jwt(self) -> None:
        if not self.jwt or time.time() - self.jwt_at > JWT_REFRESH_S:
            await self.login()

    # --------------------------------------------------------------- orders --
    def _sign(self, m, side_sdk, token_id: str, price: float, shares: float) -> tuple[dict, str, str]:
        """A signed LIMIT order: (the POST body, the order hash, the price per share in wei). Runs in a thread (sync SDK)."""
        from predict_sdk import BuildOrderInput, LimitHelperInput
        b = self.builder
        price_wei = int(round(price, m.decimals) * WEI)
        qty_wei = int(round(shares, 2) * 100) * (WEI // 100)
        a = b.get_limit_order_amounts(LimitHelperInput(side=side_sdk, price_per_share_wei=price_wei, quantity_wei=qty_wei))
        order = b.build_order("LIMIT", BuildOrderInput(side=side_sdk, token_id=token_id, maker_amount=a.maker_amount,
                                                       taker_amount=a.taker_amount, fee_rate_bps=m.fee_rate_bps))
        td = b.build_typed_data(order, is_neg_risk=m.neg_risk, is_yield_bearing=m.yield_bearing)
        signed = b.sign_typed_data_order(td)
        h = b.build_typed_data_hash(td)
        sig = signed.signature if str(signed.signature).startswith("0x") else "0x" + str(signed.signature)
        msg = {k: (v if k in ("side", "signatureType") else str(v)) for k, v in td.message.items()}
        msg["side"], msg["signatureType"] = int(msg["side"]), int(msg["signatureType"])
        body = {"data": {"order": {**msg, "hash": h, "signature": sig}, "pricePerShare": str(a.price_per_share), "strategy": "LIMIT"}}
        return body, h, str(a.price_per_share)

    async def place_limit(self, m, side: str, token_id: str, price: float, shares: float) -> dict:
        """A LIMIT BUY / SELL (side "BUY" | "SELL") of `shares` at `price`. Returns {orderId, orderHash, removalLockedUntil}."""
        from predict_sdk import Side
        body, h, _ = await asyncio.to_thread(self._sign, m, Side.BUY if side == "BUY" else Side.SELL, token_id, price, shares)
        r = await self._req("POST", "/v1/orders", auth=True, body=body)
        d = r.get("data") or {}
        d.setdefault("orderHash", h)
        return d

    async def remove(self, order_ids: list[str]) -> dict:
        """POST /v1/orders/remove: off-chain, free. {removed, noop, rejected} (top level)."""
        return await self._req("POST", "/v1/orders/remove", auth=True, body={"data": {"ids": list(order_ids)}})

    async def order(self, order_hash: str) -> dict | None:
        r = await self._req("GET", f"/v1/orders/{order_hash}", auth=True, ok404=True)
        return None if r is None else r.get("data")

    async def usdt_balance(self) -> float:
        return await asyncio.to_thread(lambda: self.builder.balance_of("USDT", self.address) / WEI)
