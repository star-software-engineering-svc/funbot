"""Strategy registry: core/strategy/<name>.py (polybot's). A bot process runs one strategy, chosen by STRATEGY (default
fairv1; e.g. `dry_run=true strategy=fairv1 venv/bin/python run.py`, core/main.py), its step() gated by <NAME>_RUN
(Config.<name>_run). Every strategy logs to its own tree LOG_DIR/<name>/.

Every strategy module exposes one class whose name ends in ``Strategy`` (FairV1Strategy in fairv1.py; the subpackages
signals/ and common/ are not strategies, nor is a module with a leading underscore) with a ``name`` equal to the module
name, the constructor (cfg, pf, ref, ex, csv), a ``loop`` coroutine that calls its ``step()``, and the coroutines
``on_fill(order)`` and ``on_settle(market, outcome, pnl)`` (returns a note for the [settle] line, "" for none). The
constructor registers the strategy in ``ex.strategies[name]`` and every order it places passes ``strategy=self.name``:
the executor hands each fill to the on_fill of the strategy that placed the order. A strategy's parameters live in
config/strategy/<name>.py.
"""
from __future__ import annotations

import importlib
import pkgutil


def available_strategies() -> list[str]:
    """The strategy names: the modules of this package."""
    return sorted(m.name for m in pkgutil.iter_modules(__path__) if not m.ispkg and not m.name.startswith("_"))   # signals/, common/: not strategies


def load_strategies() -> dict:
    """{name: class} for every strategy module, in name order."""
    return {n: load_strategy(n) for n in available_strategies()}


def load_strategy(name: str):
    """Return the strategy class (the one class named *Strategy defined in core/strategy/<name>.py)."""
    name = name.strip().lower()
    if name not in available_strategies():
        raise ValueError(f"unknown strategy {name!r}; available: {', '.join(available_strategies())}")
    mod = importlib.import_module(f".{name}", __name__)
    classes = [c for c in vars(mod).values() if isinstance(c, type) and c.__name__.endswith("Strategy") and c.__module__ == mod.__name__]
    if len(classes) != 1:
        raise ValueError(f"core/strategy/{name}.py must define exactly one *Strategy class (found {[c.__name__ for c in classes]})")
    if getattr(classes[0], "name", None) != name:                          # Order.strategy / BaseExecutor.strategies key
        raise ValueError(f"{classes[0].__name__}.name must be {name!r} (the module name), not {getattr(classes[0], 'name', None)!r}")
    return classes[0]
