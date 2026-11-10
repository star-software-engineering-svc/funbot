# funbot — predict.fun BTC 5m Up/Down bot, FairV1Strategy

polybot's layout for predict.fun (BNB Chain, USDT). One strategy is in this tree: **FairV1Strategy** (`fairv1`, the
default `STRATEGY`, dry run only until `FAIRV1_LIVE = True`; `core/strategy/fairv1.py`, parameters in
`config/strategy/fairv1.py`).

## What the market resolves on

predict.fun's `btc-updown-5m-<start>` rounds resolve **Up** when the **Chainlink Data Streams BTC/USDT** stream
(`btc-usdt-topofbook-datalink`, feed `0x000366e0…a295a5`) is higher at the round's end than at its start, **Down** when
lower, **50-50** on a tie. That stream's price is the **mid of Binance's BTCUSDT top of book**. So funbot trades on that
price, not on polybot's Chainlink BTC/USD TWAP or Coinbase:

| feed | role | needs |
|---|---|---|
| Chainlink Data Streams BTC/USDT | the resolution source itself (`chainlink.csv`) | `CHAINLINK_API_KEY` / `_SECRET` |
| Binance **spot** bookTicker BTCUSDT | the stream's own input; stands in for Chainlink without keys (`binance.csv`) | nothing |
| Coinbase BTC-USD ticker | comparison only (`coinbase.csv`, status line) | nothing |

`PRICE_SOURCE=auto` trades on Chainlink while its reports are fresh, else on the Binance mid.

## The signal (core/strategy/signals/fair.py)

polybot's `nam`, rebased: the close is one price (no TWAP) and there is no venue basis, so
P(UP) = Φ((price − beat) / (σ·√(seconds left))), σ the blend of the 1 s changes over 300 / 60 / 30 s. A move of
P(UP) ≥ `EDGE` within `SCORE_S` toward a side whose ask P covers (− `SLIP`) buys `SHARE_SIZE` × 1 / 2 / 3, limit at
ask + `BUY_SLIP`, the rest cancelled after `TAKER_TTL_S`; held to settlement. Thresholds are polybot's starting values:
**tune them on funbot's own logs before trading live.**

```
funbot/
  run.py             entry point            venv/bin/python run.py
  .env               account, strategy name, feeds, plumbing, secrets; .env.example = the same blank
  config/strategy/   fairv1.py: the strategy's parameters (Python constants, the defaults of Config)
  tools/             approve.py (one-time on-chain approvals for live trading)
  output/            logs/<strategy>/<day>/, pm2/ (not in git)
  core/
    config.py        .env + config/strategy/*.py -> Config
    prices.py        Binance spot bookTicker, Chainlink Data Streams (v3 reports), Coinbase; RefPrice (the resolution price)
    feeds.py         round discovery (GET /v1/categories/<slug>), the order book websocket (predictOrderbook/<id>), wallet events
    pfclient.py      predict.fun REST: API key, JWT login, markets, signed LIMIT orders (predict-sdk), remove
    book.py          the Up book; the Down book is its mirror (1 - price)
    state.py         orders, positions, PnL, CSV logs, positions.json (survives restarts)
    execution.py     DryRunExecutor (fills against the live book) / LiveExecutor (signed orders, fills from wallet events)
    settlement.py    pins the price to beat, cancels at the end, resolves (own price, else predict.fun's result)
    strategy/        fairv1.py (FairV1Strategy; the registry in __init__.py)
      signals/       fair.py (P(UP)), shared with a future backtest
      common/        orders.py (sizing helpers)
    main.py          wiring + status line
```

## Run

```bash
cd /root/b2/funbot
python3 -m venv venv && venv/bin/pip install -r requirements.txt
cp .env.example .env                                   # then fill PREDICT_API_KEY (mainnet)
venv/bin/python run.py                                 # dry run on mainnet books
PREDICT_ENV=testnet venv/bin/python run.py             # testnet: no key (its BTC 5m books are empty)
pm2 start ecosystem.config.js --only fun-fairv1        # dry run under pm2
```

Logs: `output/logs/fairv1/<YYYYMMDD>/` — `<day>.log`, `signals.csv`, `orders.csv`, `fills.csv`, `markets.csv`,
`opens.csv` (each round's price to beat and its source), `prices.csv` (best bid / ask changes), the tick logs.

**Live** needs `DRY_RUN=false`, `FAIRV1_LIVE = True` (or the `fun-fairv1-live` pm2 app, which sets it in its env),
`PREDICT_API_KEY`, `PREDICT_PRIVATE_KEY` (+ `PREDICT_ACCOUNT` for a Predict smart wallet), USDT and a little BNB in the
wallet, and `venv/bin/python tools/approve.py` once. Never run `fun-fairv1` and `fun-fairv1-live` together (one log tree).

## predict.fun facts used (dev.predict.fun)

- REST `https://api.predict.fun/v1` (`x-api-key`), testnet `api-testnet.predict.fun` (no key); WS `wss://ws.predict.fun/ws`
  (key at the handshake); the server's 15 s heartbeat must be echoed.
- The book is quoted in the Up outcome (indexSet 1), `[price, shares]`, whole book per message; 2 decimals.
- Orders: EIP-712 signed by predict-sdk, amounts in wei (1e18), BUY ≥ 1 USDT; remove via `POST /v1/orders/remove`
  (off-chain, free). Some up/down markets lock removal until `removalLockedUntil`.
- Taker fee = feeRateBps/10000 × min(p, 1−p) per share (200 bps), makers 0; a buy's fee is taken **in shares**.

## Not done yet / unverified

- Live order flow and wallet events are written from the docs but **untested** (needs a funded wallet).
- The Chainlink Data Streams feed is untested (needs Data Streams credentials).
- No web UI, report or backtest tools yet (polybot's `webui/`, `tools/report.py`, `tools/backtest.py`).
