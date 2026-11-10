"""FairV1Strategy parameters (core/strategy/fairv1.py): buys on the fair signal (core/strategy/signals/fair.py: P(UP) of
the resolution price against the price to beat), held to settlement. A shell variable of the same name still wins for
one run, e.g. `strategy=fairv1 FAIRV1_EDGE=0.06 venv/bin/python run.py`."""

FAIRV1_LIVE = False               # False = the process is forced to DRY_RUN (core/main.py); flip only after a dry-run review
FAIRV1_TICK_LOG = True            # binance.csv / coinbase.csv / chainlink.csv / prices.csv of this tree: the backtest replays them

FAIRV1_SIGNAL = True              # the fair signal on / off (off: the bot only logs feeds and books)
FAIRV1_START_S = 10.0             # s into the window: no entry before (the price to beat must be pinned, books settle)
FAIRV1_END_S = 285.0              # s into the window: no entry after
FAIRV1_SCORE_S = 0.5              # s: P(UP)'s move is scored against its min / max over this long
FAIRV1_EDGE = 0.05                # P(UP) moved at least this -> an entry of SHARE_SIZE
FAIRV1_EDGE2 = 0.12               # ... x 2
FAIRV1_EDGE3 = 0.20               # ... x 3
FAIRV1_SHARE_SIZE = 5             # shares per entry (before the multiplier)
FAIRV1_ASK_MIN = 0.12
FAIRV1_ASK_MAX = 0.85
FAIRV1_GAP_S = 3.0                # s: a side not entered again within this
FAIRV1_MAX_IMBALANCE = 20.0       # shares: a side at most this over the other
FAIRV1_SMOOTH_S = 0.0             # s: the price time-averaged over this (0 = the last one)
FAIRV1_MIN_SIGMA = 1.0            # $/sqrt(s): sigma's floor (a flat minute must not make P(UP) a step function)
FAIRV1_SLIP = 0.02                # P may sit this far under the ask
FAIRV1_MAX_AGE_MS = 1500          # no signal when the newest price is this old

# the buys
FAIRV1_BUY_SLIP = 0.03            # a buy is a limit at ask + this; what does not fill at once rests TAKER_TTL_S
FAIRV1_TAKER_TTL_S = 5.0          # s: a buy not fully filled this long after it was placed has its remainder cancelled (0 = rests)
FAIRV1_MAX_COST = 50.0            # USDT: no new buy once the window's cost reaches this
