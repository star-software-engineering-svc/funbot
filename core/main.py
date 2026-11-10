"""Wiring + status line (polybot's core/main.py, for predict.fun)."""
from __future__ import annotations

import asyncio
import shutil
import sys
import time
from pathlib import Path

from .config import Config
from .execution import DryRunExecutor, LiveExecutor
from .feeds import PredictFeed
from .logutil import configure as configure_log, log
from .pfclient import PredictClient
from .prices import RefPrice, binance_feed, chainlink_feed, coinbase_feed
from .settlement import Settler
from .state import CsvLog, Portfolio, WINDOW_S
from .strategy import load_strategy

LOG_RETENTION_EVERY_S = 600


async def status_loop(cfg: Config, pf: Portfolio, strat, ref: RefPrice, feed: PredictFeed, ex) -> None:
    while True:
        await asyncio.sleep(cfg.status_every_s)
        t = time.time()
        m = strat.current()
        parts = [time.strftime("%H:%M:%S"), f"rem={int(WINDOW_S - t % WINDOW_S):3d}s",
                 f"{strat.name}={'on' if getattr(cfg, strat.run_flag) else 'OFF'}{' dry' if cfg.dry_run else ''}"]
        if ref.binance.last:
            parts.append(f"bn={ref.binance.last:.2f}")
        if ref.chainlink.last:
            parts.append(f"cl={ref.chainlink.last:.2f}")
        if ref.coinbase.last:
            parts.append(f"cb={ref.coinbase.last:.2f}")
        parts.append(f"src={ref.using()}")
        if m:
            if m.beat is not None and ref.last is not None:
                parts.append(f"beat={m.beat:.2f} d={ref.last - m.beat:+.2f}")
            st = strat.local(m) if hasattr(strat, "local") else None
            if st is not None and getattr(st, "fair", None) is not None and st.fair.p_up is not None:
                parts.append(f"P(UP)={st.fair.p_up:.3f}")
            parts.append(f"UP {m.up_book.best_bid}/{m.up_book.best_ask} DN {m.down_book.best_bid}/{m.down_book.best_ask}")
            if m.cost:
                parts.append(f"pos U{m.pos['UP'].shares:g}@{m.pos['UP'].avg:.2f} D{m.pos['DOWN'].shares:g}@{m.pos['DOWN'].avg:.2f} "
                             f"mtm={m.mtm():+.2f} if UP {m.pnl_if('UP'):+.2f} / DOWN {m.pnl_if('DOWN'):+.2f}")
            parts.append(f"trades={m.taker_count}")
        else:
            parts.append("no market")
        parts.append(f"day={pf.day_pnl:+.2f} total={pf.total_pnl:+.2f}")
        w = getattr(ex, "wallet", None)
        parts.append(f"msgs b{ref.binance.msgs} c{ref.chainlink.msgs} k{ref.coinbase.msgs} p{feed.msgs}{'' if feed.connected else '!'}"
                     + (f" w{w.msgs}{'' if w.connected else '!'}" if w else ""))
        sys.stdout.write("  ".join(parts) + "\n")
        sys.stdout.flush()


async def csv_flush_loop(csv: CsvLog) -> None:
    while True:
        await asyncio.sleep(CsvLog.LAZY_FLUSH_S)
        csv.flush_lazy()


async def log_retention_loop(cfg: Config) -> None:
    """MAX_LOG_PERIOD: keep the last N day folders of this bot's tree (polybot's)."""
    while True:
        try:
            days = int(cfg.max_log_period)
            if days > 0 and cfg.log_dir.is_dir():
                keep_from = time.strftime("%Y%m%d", time.gmtime(time.time() - (days - 1) * 86400))
                old = sorted(d for d in cfg.log_dir.iterdir() if d.is_dir() and len(d.name) == 8 and d.name.isdigit() and d.name < keep_from)
                for d in old:
                    shutil.rmtree(d, ignore_errors=True)
                if old:
                    log(f"[logs] MAX_LOG_PERIOD {days} days: deleted {len(old)} day folder(s) before {keep_from}")
        except Exception as e:
            log(f"[logs] retention pass failed: {e!r}")
        await asyncio.sleep(LOG_RETENTION_EVERY_S)


async def main() -> None:
    cfg = Config()
    cfg.validate()
    cls = load_strategy(cfg.strategy)
    forced_dry = bool(getattr(cls, "live_flag", "")) and not cfg.dry_run and not getattr(cfg, cls.live_flag)
    if forced_dry:
        object.__setattr__(cfg, "dry_run", True)
    log_dir = Path(cfg.log_dir)
    configure_log(log_dir)
    if forced_dry:
        log(f"[start] {cfg.strategy}: {cls.live_flag.upper()} is off, so this process runs DRY although DRY_RUN=false "
            f"(set {cls.live_flag.upper()} = True in config/strategy/{cfg.strategy}.py to trade live)")
    has_cl = bool(cfg.chainlink_api_key and cfg.chainlink_api_secret)
    log(f"[start] funbot {cfg.strategy} {'DRY RUN' if cfg.dry_run else 'LIVE'} | predict.fun {cfg.predict_env} {cfg.market_prefix} "
        f"| resolution price: {'Chainlink Data Streams ' + cfg.chainlink_feed_id[:10] + '..' if has_cl else 'no Chainlink keys'}"
        f" + Binance spot {cfg.symbol} (PRICE_SOURCE {cfg.price_source}) | logs in {log_dir}")
    csv = CsvLog(log_dir)
    pf = Portfolio(csv)
    ref = RefPrice(cfg.price_source if has_cl else "binance")
    client = PredictClient(cfg)
    await client.start()
    ex = DryRunExecutor(cfg, pf) if cfg.dry_run else LiveExecutor(cfg, pf, client)
    tick_csv = csv if getattr(cfg, f"{cfg.strategy}_tick_log", False) else None
    feed = PredictFeed(cfg, pf, client, ex.on_book, tick_csv)
    strat = cls(cfg, pf, ref, ex, csv)
    settler = Settler(cfg, pf, ref, ex, client, csv)
    if pf.unrestored():
        log(f"[state] {len(pf.unrestored())} unsettled market(s) left by a previous run: " + ", ".join(k for k, _ in pf.unrestored()))

    await ex.start()
    tasks = [
        asyncio.create_task(binance_feed(cfg.symbol, ref, tick_csv)),
        asyncio.create_task(feed.discovery_loop()),
        asyncio.create_task(feed.run()),
        asyncio.create_task(strat.loop()),
        asyncio.create_task(settler.loop()),
        asyncio.create_task(status_loop(cfg, pf, strat, ref, feed, ex)),
        asyncio.create_task(csv_flush_loop(csv)),
        asyncio.create_task(log_retention_loop(cfg)),
    ]
    if has_cl:
        tasks.append(asyncio.create_task(chainlink_feed(cfg.chainlink_ws_url, cfg.chainlink_feed_id, cfg.chainlink_api_key,
                                                        cfg.chainlink_api_secret, ref, tick_csv)))
    if cfg.coinbase_feed:
        tasks.append(asyncio.create_task(coinbase_feed(cfg.coinbase_product, ref, tick_csv)))
    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()
        await ex.stop()
        await client.close()
        csv.close()


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("[stop] funbot stopped by Ctrl-C")
