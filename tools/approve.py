#!/usr/bin/env python
"""One-time on-chain approvals for live trading (predict-sdk OrderBuilder.set_approvals): the USDT allowance and the
ERC1155 approval of the CTF exchanges for the wallet in .env (PREDICT_PRIVATE_KEY / PREDICT_ACCOUNT). Idempotent; costs
BNB gas.   venv/bin/python tools/approve.py"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.config import Config          # noqa: E402
from core.pfclient import PredictClient  # noqa: E402

if __name__ == "__main__":
    cfg = Config()
    if not cfg.private_key:
        sys.exit("PREDICT_PRIVATE_KEY is not set")
    b = PredictClient(cfg)._make_builder()
    print(b.set_approvals())
