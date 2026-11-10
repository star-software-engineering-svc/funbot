"""One log() for the whole bot: console (stderr) + a daily file LOG_DIR/<YYYYMMDD>/<YYYYMMDD>.log.

The day is the UTC date, the same boundary the CSV logs use, so everything for one day lives in one
directory. Console lines carry the local HH:MM:SS as before; file lines carry a full UTC timestamp.
File errors never propagate — a broken disk must not stop the trading loop."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_dir = Path("output/logs")
_file = None
_day = ""


def configure(log_dir: Path | str) -> None:
    """Set the base directory (call once at startup, before anything logs). Reopens the file if needed."""
    global _dir, _file, _day
    _dir = Path(log_dir)
    if _file:
        _file.close()
    _file, _day = None, ""


def _open(day: str):
    global _file, _day
    if _file:
        _file.close()
    d = _dir / day
    d.mkdir(parents=True, exist_ok=True)
    _file = open(d / f"{day}.log", "a", buffering=1)      # line-buffered
    _day = day

def _cur_time_ms() -> str:
    return f"{time.time_ns() // 1_000_000 / 1000:.3f}"

def log(msg: str) -> None:
    now = time.time()
    cur_time_ms = _cur_time_ms()
    sys.stderr.write(f"{cur_time_ms} {msg}\n")          # one write: print() sends the text and the newline separately, which
    sys.stderr.flush()                                   # pm2 (unbuffered pipe, time: true) can log as a line plus an empty line
    try:
        day = time.strftime("%Y%m%d", time.gmtime(now))
        if day != _day or _file is None:
            _open(day)
        _file.write(f"{cur_time_ms} {msg}\n")
    except Exception as e:                                   # never let logging kill the bot
        print(f"[log] file write failed: {e!r}", file=sys.stderr, flush=True)
