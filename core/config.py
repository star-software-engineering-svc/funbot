"""Configuration: .env (see .env.example) for the account, feeds, plumbing and secrets; the strategy's parameters from
config/strategy/<name>.py, whose keys carry the strategy's prefix (FAIRV1_EDGE from fairv1.py); a shell variable of the
same name still overrides one run. One strategy runs per process (core/main.py, STRATEGY); its <NAME>_RUN gates its
step(). Same scheme as polybot's core/config.py."""
from __future__ import annotations

import importlib
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _strategy_modules() -> dict:
    """{prefix: module} for every config/strategy/<name>.py: {"FAIRV1_": config.strategy.fairv1}."""
    import pkgutil
    import config.strategy as pkg
    return {m.name.upper() + "_": importlib.import_module(f"config.strategy.{m.name}")
            for m in pkgutil.iter_modules(pkg.__path__) if not m.name.startswith("_")}


_SP = _strategy_modules()


def _p(name: str, fallback):
    mod = _SP.get(name.split("_", 1)[0] + "_")
    return getattr(mod, name, fallback) if mod is not None else fallback


def _cli(name: str) -> str | None:
    """A process switch given on the command line; the lowercase name wins (`dry_run=true strategy=fairv1 run.py`)."""
    v = os.getenv(name.lower())
    return v if v not in (None, "") else os.getenv(name)


def _strategy() -> str:
    return (_cli("STRATEGY") or "fairv1").strip().lower()


def _log_dir() -> Path:
    return Path(os.getenv("LOG_DIR", "output/logs")) / _strategy()


def _b(name: str, default: bool) -> bool:
    v = os.getenv(name)
    return default if v in (None, "") else v.strip().lower() in {"1", "true", "yes", "on"}


def _f(name: str, default: float) -> float:
    v = os.getenv(name)
    return float(default) if v in (None, "") else float(v)


def _i(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(default) if v in (None, "") else int(v)


def _s(name: str, default: str) -> str:
    v = os.getenv(name)
    return default if v in (None, "") else v.strip()


def _pf(name: str, fallback: float) -> float:
    return _f(name, _p(name, fallback))


def _pb(name: str, fallback: bool) -> bool:
    return _b(name, _p(name, fallback))


@dataclass(frozen=True)
class Config:
    # --- account -----------------------------------------------------------
    dry_run: bool = (_cli("DRY_RUN") or "true").strip().lower() in {"1", "true", "yes", "on"}   # the whole process
    strategy: str = _strategy()
    predict_env: str = _s("PREDICT_ENV", "mainnet").lower()        # mainnet | testnet
    predict_api_key: str = _s("PREDICT_API_KEY", "")                # x-api-key (mainnet; testnet needs none)
    private_key: str = _s("PREDICT_PRIVATE_KEY", "")               # the signer (an EOA, or a Predict account's privy wallet key)
    predict_account: str = _s("PREDICT_ACCOUNT", "")               # the Predict smart-wallet address, when trading from one ("" = EOA)

    # --- the strategy's switches -------------------------------------------
    fairv1_run: bool = _b("FAIRV1_RUN", True)
    fairv1_live: bool = _pb("FAIRV1_LIVE", False)                  # False = the process is forced to DRY_RUN (core/main.py)
    fairv1_tick_log: bool = _pb("FAIRV1_TICK_LOG", True)

    # --- market / price feeds ----------------------------------------------
    market_prefix: str = _s("MARKET_PREFIX", "btc-updown-5m")       # predict.fun category slug prefix: <prefix>-<window start>
    symbol: str = _s("BINANCE_SYMBOL", "btcusdt").lower()           # Binance SPOT bookTicker: what the Chainlink stream is made of
    price_source: str = _s("PRICE_SOURCE", "auto").lower()          # the resolution price the bot trades on: chainlink | binance |
                                                                    # auto (chainlink while its reports are fresh, else binance)
    chainlink_feed_id: str = _s("CHAINLINK_FEED_ID",                # Chainlink Data Streams BTC/USDT (topofbook-datalink): the one
                                "0x000366e0ccc605d3b7f096c33d6ecf1cd27ebd7fdc4f94e22b577e44e0a295a5")   # predict.fun names
    chainlink_api_key: str = _s("CHAINLINK_API_KEY", "")            # Data Streams credentials (empty = no Chainlink feed: Binance stands in)
    chainlink_api_secret: str = _s("CHAINLINK_API_SECRET", "")
    chainlink_ws_url: str = _s("CHAINLINK_WS_URL", "wss://ws.dataengine.chain.link")
    coinbase_feed: bool = _b("COINBASE_FEED", True)                 # comparison feed only (coinbase.csv, status line): nothing trades on it
    coinbase_product: str = _s("COINBASE_PRODUCT", "BTC-USD")

    # --- FairV1Strategy (config/strategy/fairv1.py) ------------------------
    fairv1_signal: bool = _pb("FAIRV1_SIGNAL", True)
    fairv1_start_s: float = _pf("FAIRV1_START_S", 10.0)
    fairv1_end_s: float = _pf("FAIRV1_END_S", 285.0)
    fairv1_score_s: float = _pf("FAIRV1_SCORE_S", 0.5)
    fairv1_edge: float = _pf("FAIRV1_EDGE", 0.05)
    fairv1_edge2: float = _pf("FAIRV1_EDGE2", 0.12)
    fairv1_edge3: float = _pf("FAIRV1_EDGE3", 0.20)
    fairv1_share_size: float = _pf("FAIRV1_SHARE_SIZE", 5)
    fairv1_ask_min: float = _pf("FAIRV1_ASK_MIN", 0.12)
    fairv1_ask_max: float = _pf("FAIRV1_ASK_MAX", 0.85)
    fairv1_gap_s: float = _pf("FAIRV1_GAP_S", 3.0)
    fairv1_max_imbalance: float = _pf("FAIRV1_MAX_IMBALANCE", 20.0)
    fairv1_smooth_s: float = _pf("FAIRV1_SMOOTH_S", 0.0)
    fairv1_min_sigma: float = _pf("FAIRV1_MIN_SIGMA", 1.0)
    fairv1_slip: float = _pf("FAIRV1_SLIP", 0.02)
    fairv1_max_age_ms: float = _pf("FAIRV1_MAX_AGE_MS", 1500)
    fairv1_buy_slip: float = _pf("FAIRV1_BUY_SLIP", 0.03)
    fairv1_taker_ttl_s: float = _pf("FAIRV1_TAKER_TTL_S", 5.0)
    fairv1_max_cost: float = _pf("FAIRV1_MAX_COST", 50.0)

    # --- resolution ---------------------------------------------------------
    settle_band_usd: float = _f("SETTLE_BAND_USD", 5.0)             # our end price this close to the beat: wait for predict.fun's result

    # --- dry-run simulator --------------------------------------------------
    dry_latency_ms: int = _i("DRY_RUN_LATENCY_MS", 300)

    # --- logs / plumbing ----------------------------------------------------
    log_dir: Path = field(default_factory=_log_dir)
    max_log_period: int = _i("MAX_LOG_PERIOD", 100)                 # days of day folders kept (0 = all)
    status_every_s: float = _f("STATUS_EVERY_S", 1.0)
    discovery_every_s: float = _f("DISCOVERY_EVERY_S", 20.0)        # s: how often the next windows are looked up

    @property
    def api_base(self) -> str:
        return "https://api-testnet.predict.fun" if self.predict_env == "testnet" else "https://api.predict.fun"

    def validate(self) -> None:
        if self.predict_env not in ("mainnet", "testnet"):
            raise ValueError("PREDICT_ENV must be mainnet or testnet")
        if self.price_source not in ("auto", "chainlink", "binance"):
            raise ValueError("PRICE_SOURCE must be auto, chainlink or binance")
        if self.price_source == "chainlink" and not (self.chainlink_api_key and self.chainlink_api_secret):
            raise ValueError("PRICE_SOURCE=chainlink needs CHAINLINK_API_KEY and CHAINLINK_API_SECRET")
        from .strategy import available_strategies
        if self.strategy not in available_strategies():
            raise ValueError(f"STRATEGY must be one of {', '.join(available_strategies())}, not {self.strategy!r}")
        if self.predict_env == "mainnet" and not self.predict_api_key:
            raise ValueError("PREDICT_API_KEY is required on mainnet (developers.predict.fun)")
        if not self.dry_run and not self.private_key:
            raise ValueError("PREDICT_PRIVATE_KEY is required when DRY_RUN=false")
