"""fair: the probability signal of funbot's FairV1Strategy, ported from polybot's nam (core/strategy/signals/nam.py) and
rebased to how predict.fun settles its BTC 5m markets. Shared by the bot and the backtest, so both decide alike.

predict.fun resolves Up when the Chainlink BTC/USDT Data Stream at the window's end is above its value at the start (Down
below, 50-50 on a tie). That stream is the mid of Binance's BTCUSDT top of book. So, unlike polybot:
  * the price is the resolution price itself (the Chainlink report when the feed is on, else the Binance spot mid): no
    basis between two venues;
  * the close is one price, not a TWAP: its spread with rmt s to go is sigma x sqrt(rmt).

P(UP) = Phi((price - beat) / (sigma x sqrt(rmt))), sigma ($/sqrt s) the blend 0.2 x s300^2 + 0.3 x s60^2 + 0.5 x s30^2 of
the 1 s changes of the price (nam's), floored at FAIR_MIN_SIGMA.
Entry (nam's rules): P(UP) rose at least EDGE above its minimum over the last SCORE_S and P(UP) >= the UP ask - SLIP -> UP;
the mirror image -> DOWN. Size SHARE_SIZE x 1, 2 or 3 (the move >= EDGE2 / EDGE3), cut so the side is at most
MAX_IMBALANCE shares over the other; a side not again within GAP_S; its ask in [ASK_MIN, ASK_MAX]; from START_S to END_S.
A qualifying move clears the history, bought or not."""
from __future__ import annotations

import bisect
import math
from collections import deque

SIGMA_S = (300, 60, 30)                # s: the sigma blend's spans and weights (nam's)
SIGMA_W = (0.2, 0.3, 0.5)
KEEP_S = 420                           # s of 1 s samples kept


def phi(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _sd(xs: list) -> float | None:
    if len(xs) < 3:
        return None
    d = [b - a for a, b in zip(xs, xs[1:])]
    m = sum(d) / len(d)
    return math.sqrt(sum((x - m) ** 2 for x in d) / (len(d) - 1))


class FairFeed:
    """The resolution price as the process sees it: the raw ticks of the last few seconds and one sample a second (the
    price in force at each whole second: sigma)."""

    def __init__(self) -> None:
        self.raw: deque = deque()          # (ts, price), the last few seconds
        self.secs: deque = deque()         # (whole second, the price in force at it)
        self.last: tuple | None = None
        self._sig: tuple | None = None

    def on_price(self, ts: float, px: float) -> None:
        if self.last is not None:
            if ts < self.last[0]:
                return
            k = math.ceil(self.last[0])
            while k <= ts:
                if k < ts:
                    self.secs.append((k, self.last[1]))
                k += 1
            while self.secs and self.secs[0][0] < ts - KEEP_S:
                self.secs.popleft()
        self.last = (ts, px)
        self.raw.append((ts, px))
        while len(self.raw) > 2 and self.raw[1][0] < ts - 5.0:
            self.raw.popleft()

    def smooth(self, now: float, span: float) -> float | None:
        """The time-weighted mean over [now - span, now] (the last price alone when span <= 0)."""
        if self.last is None:
            return None
        if span <= 0:
            return self.last[1]
        t0, acc, prev = now - span, 0.0, None
        for t, p in self.raw:
            if t > now:
                break
            if prev is not None and t > t0:
                acc += prev[1] * (t - max(prev[0], t0))
            prev = (t, p)
        if prev is None:
            return None
        acc += prev[1] * (now - max(prev[0], t0))
        return acc / span if self.raw[0][0] <= t0 else None

    def samples(self, t0: float, t1: float) -> list:
        ks = [k for k, _ in self.secs]
        return [p for _, p in list(self.secs)[bisect.bisect_left(ks, t0):bisect.bisect_left(ks, t1)]]

    def sigma(self) -> float | None:
        if not self.secs:
            return None
        k = self.secs[-1][0]
        if self._sig is None or self._sig[0] != k:
            xs = [p for _, p in self.secs][-(SIGMA_S[0] + 1):]
            s = None
            if len(xs) > SIGMA_S[0]:
                parts = [_sd(xs[-(n + 1):]) for n in SIGMA_S]
                if all(v is not None for v in parts):
                    s = math.sqrt(sum(w * v * v for w, v in zip(SIGMA_W, parts)))
            self._sig = (k, s)
        return self._sig[1]


class Fair:
    """One window's state: the probability's recent history, the last entry per side, the shares sent per side."""
    name = "fair"

    def __init__(self) -> None:
        self.hist: deque = deque()         # (ts, P(UP))
        self.last = {"UP": -1e9, "DOWN": -1e9}
        self.sent = {"UP": 0.0, "DOWN": 0.0}
        self.p_up: float | None = None
        self.sigma: float | None = None

    def prob(self, feed: FairFeed, now: float, end: float, beat: float | None, smooth_s: float, max_age_s: float,
             min_sigma: float) -> tuple | None:
        """(P(UP), price - beat, the spread, sigma) at `now`, None without the data."""
        if beat is None or feed.last is None or now - feed.last[0] > max_age_s:
            return None
        sigma, px = feed.sigma(), feed.smooth(now, smooth_s)
        if sigma is None or px is None:
            return None
        sigma = max(sigma, min_sigma)
        rmt = end - now
        if rmt <= 0:
            return None
        std = sigma * math.sqrt(rmt)
        d = px - beat
        return phi(d / std), d, std, sigma

    def check(self, feed: FairFeed, now: float, start: float, end: float, beat: float | None, asks: dict, *, start_s: float,
              end_s: float, score_s: float, edge: float, edge2: float, edge3: float, share_size: float, ask_min: float,
              ask_max: float, gap_s: float, max_imb: float, smooth_s: float, max_age_s: float, min_sigma: float,
              slip: float) -> dict | None:
        """The buy at `now`, else None: dict(side, size, ask, p, text). The caller sends it and calls bought()."""
        e = now - start
        if e < start_s - 1e-9 or e >= end_s:
            return None
        r = self.prob(feed, now, end, beat, smooth_s, max_age_s, min_sigma)
        if r is None:
            return None
        p, d, std, sigma = r
        self.p_up, self.sigma = p, sigma
        head = f"t={e:.1f}s: P(UP) {p:.3f} (price {d:+.2f} from the beat {beat:.2f}, spread {std:.2f}, sigma {sigma:.2f})"
        while self.hist and self.hist[0][0] < now - score_s:
            self.hist.popleft()
        up_mv = p - min(q for _, q in self.hist) if self.hist else 0.0
        dn_mv = max(q for _, q in self.hist) - p if self.hist else 0.0
        self.hist.append((now, p))
        side = mv = None
        if up_mv >= edge - 1e-9 and asks.get("UP") is not None and p >= asks["UP"] - slip - 1e-9:
            side, mv = "UP", up_mv
        elif dn_mv >= edge - 1e-9 and asks.get("DOWN") is not None and 1 - p >= asks["DOWN"] - slip - 1e-9:
            side, mv = "DOWN", dn_mv
        if side is None:
            return None
        self.hist.clear()
        mult = 3 if mv >= edge3 - 1e-9 else 2 if mv >= edge2 - 1e-9 else 1
        ps = p if side == "UP" else 1 - p
        a = asks[side]
        why = f"P({side}) {ps:.3f} moved {mv:+.3f} in {score_s:g} s (>= EDGE {edge:g}) and >= the {side} ask {a:.2f}" + (f" - {slip:g}" if slip else "")
        other = "DOWN" if side == "UP" else "UP"
        size = min(self.sent[other] + max_imb - self.sent[side], share_size * mult)
        if now - self.last[side] <= gap_s or size <= 0 or not ask_min - 1e-9 <= a <= ask_max + 1e-9:
            return None
        return dict(side=side, size=round(size, 2), ask=a, p=ps, text=head + f" -> {why}: entry {size:g}")

    def bought(self, q: dict, now: float, size: float) -> None:
        self.sent[q["side"]] += size
        self.last[q["side"]] = now
