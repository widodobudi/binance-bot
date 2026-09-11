import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime

BINANCE_BASE = "https://api.binance.com"
API_KEY = os.environ.get("BINANCE_API_KEY", "")

HEADERS = {"X-MBX-APIKEY": API_KEY}
OUTPUT_DIR = os.environ.get("DATA_DIR", "/data")

# ── Stoch params ──────────────────────────────────────────────
K_LENGTH   = 14
K_SMOOTH   = 1
D_SMOOTH   = 3
STOCH_LEVEL = 20   # oversold threshold

# ── Backtest period ───────────────────────────────────────────
START_MS = int(datetime(2022, 1, 1).timestamp() * 1000)
END_MS   = int(datetime(2026, 9, 1).timestamp() * 1000)
INTERVAL = "4h"

# ── Exit params ───────────────────────────────────────────────
ATR_LENGTH  = 14
FIXED_PERC_TP = 0.05   # 5%
FIXED_PERC_SL = 0.025  # 2.5%
RR_RATIO     = 2.0     # 1:2

def get_usdt_pairs():
    r = requests.get(f"{BINANCE_BASE}/api/v3/ticker/24hr", timeout=30)
    tickers = r.json()
    pairs = [
        t["symbol"] for t in tickers
        if t["symbol"].endswith("USDT")
        and float(t["quoteVolume"]) > 5_000_000
        and not any(x in t["symbol"] for x in ["UP", "DOWN", "BULL", "BEAR", "TUSD", "BUSD", "USDC", "DAI"])
    ]
    return sorted(pairs)

def fetch_klines(symbol, interval, start_ms, end_ms):
    all_klines = []
    current = start_ms
    while current < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current,
            "endTime": end_ms,
            "limit": 1000
        }
        r = requests.get(f"{BINANCE_BASE}/api/v3/klines", params=params, timeout=30)
        data = r.json()
        if not data or isinstance(data, dict):
            break
        all_klines.extend(data)
        current = data[-1][0] + 1
        if len(data) < 1000:
            break
        time.sleep(0.1)
    return all_klines

def build_df(klines):
