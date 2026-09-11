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

K_LENGTH = 14
K_SMOOTH = 1
D_SMOOTH = 3
STOCH_LEVEL = 20

START_MS = int(datetime(2022, 1, 1).timestamp() * 1000)
END_MS   = int(datetime(2026, 9, 1).timestamp() * 1000)
INTERVAL = "4h"

ATR_LENGTH    = 14
FIXED_PERC_TP = 0.05
FIXED_PERC_SL = 0.025
RR_RATIO      = 2.0

def get_usdt_pairs():
    r = requests.get(f"{BINANCE_BASE}/api/v3/exchangeInfo", timeout=30)
    data = r.json()
    symbols = data.get("symbols", [])
    pairs = [
        s["symbol"] for s in symbols
        if s["symbol"].endswith("USDT")
        and s["status"] == "TRADING"
        and s["quoteAsset"] == "USDT"
        and not any(x in s["symbol"] for x in ["UPUSDT","DOWNUSDT","BULLUSDT","BEARUSDT","TUSDT","BUSDT"])
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
        r = requests.get(f"{BINANCE_BASE}/api/v3/klines", params=params,
                         headers=HEADERS, timeout=30)
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
    df = pd.DataFrame(klines, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","qv","trades","tbbav","tbqav","ignore"
    ])
    for col in ["open","high","low","close","volume"]:
        df[col] = df[col].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df = df.set_index("open_time")
    return df

def calc_stoch(df, k_len=14, k_smooth=1, d_smooth=3):
    low_min  = df["low"].rolling(k_len).min()
    high_max = df["high"].rolling(k_len).max()
    raw_k    = 100 * (df["close"] - low_min) / (high_max - low_min + 1e-10)
    k        = raw_k.rolling(k_smooth).mean()
    d        = k.rolling(d_smooth).mean()
    return k, d

def calc_atr(df, length=14):
    hl  = df["high"] - df["low"]
    hc  = (df["high"] - df["close"].shift(1)).abs()
    lc  = (df["low"]  - df["close"].shift(1)).abs()
    tr  = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/length, adjust=False).mean()
    return atr

def backtest_symbol(symbol):
    klines = fetch_klines(symbol, INTERVAL, START_MS, END_MS)
    if len(klines) < 100:
        return []
    df = build_df(klines)
    k, d = calc_stoch(df, K_LENGTH, K_SMOOTH, D_SMOOTH)
    atr  = calc_atr(df, ATR_LENGTH)
    df["k"]   = k
    df["d"]   = d
    df["atr"] = atr
    df = df.dropna()
    results = []
    arr = df.reset_index()
    for i in range(1, len(arr) - 1):
        k_prev = arr.loc[i-1, "k"]
        d_prev = arr.loc[i-1, "d"]
        k_cur  = arr.loc[i,   "k"]
        d_cur  = arr.loc[i,   "d"]
        cross_up = (k_prev <= d_prev) and (k_cur > d_cur)
        oversold = (k_cur < STOCH_LEVEL) and (d_cur < STOCH_LEVEL)
        if not (cross_up and oversold):
            continue
        if i + 1 >= len(arr):
            continue
        entry_price = arr.loc[i+1, "open"]
        signal_low  = arr.loc[i,   "low"]
        atr_val     = arr.loc[i,   "atr"]
        signal_time = arr.loc[i,   "open_time"]
        sl_a   = signal_low
        dist_a = entry_price - sl_a
        tp_a   = entry_price + RR_RATIO * dist_a if dist_a > 0 else None
        sl_b   = signal_low
        sl_c   = entry_price - 1.5 * atr_val
        tp_c   = entry_price + 3.0 * atr_val
        sl_d   = entry_price * (1 - FIXED_PERC_SL)
        tp_d   = entry_price * (1 + FIXED_PERC_TP)
        pnl_a = pnl_b = pnl_c = pnl_d = None
        for j in range(i+1, min(i+201, len(arr))):
            c_high = arr.loc[j, "high"]
            c_low  = arr.loc[j, "low"]
            k_j    = arr.loc[j,   "k"]
            d_j    = arr.loc[j,   "d"]
            k_pj   = arr.loc[j-1, "k"]
            d_pj   = arr.loc[j-1, "d"]
            if pnl_a is None and dist_a and dist_a > 0:
                if c_low <= sl_a:
                    pnl_a = (sl_a - entry_price) / entry_price
                elif c_high >= tp_a:
                    pnl_a = (tp_a - entry_price) / entry_price
            if pnl_b is None:
                cross_down_b = (k_pj >= d_pj) and (k_j < d_j)
                if c_low <= sl_b:
                    pnl_b = (sl_b - entry_price) / entry_price
                elif cross_down_b:
                    pnl_b = (arr.loc[j, "open"] - entry_price) / entry_price
            if pnl_c is None:
                if c_low <= sl_c:
                    pnl_c = (sl_c - entry_price) / entry_price
                elif c_high >= tp_c:
                    pnl_c = (tp_c - entry_price) / entry_price
            if pnl_d is None:
                if c_low <= sl_d:
                    pnl_d = (sl_d - entry_price) / entry_price
                elif c_high >= tp_d:
                    pnl_d = (tp_d - entry_price) / entry_price
            if all(x is not None for x in [pnl_a, pnl_b, pnl_c, pnl_d]):
                break
        results.append({
            "symbol":       symbol,
            "signal_time":  signal_time,
            "entry_price":  entry_price,
            "k":            round(k_cur, 2),
            "d":            round(d_cur, 2),
            "pnl_a_rr":     round(pnl_a * 100, 3) if pnl_a is not None else None,
            "pnl_b_stoch":  round(pnl_b * 100, 3) if pnl_b is not None else None,
            "pnl_c_atr":    round(pnl_c * 100, 3) if pnl_c is not None else None,
            "pnl_d_fixpct": round(pnl_d * 100, 3) if pnl_d is not None else None,
        })
    return results

def summarize(df_res, col):
    sub = df_res[col].dropna()
    if len(sub) == 0:
        return {}
    wins     = (sub > 0).sum()
    losses   = (sub <= 0).sum()
    total    = len(sub)
    avg_win  = sub[sub > 0].mean() if wins > 0 else 0
    avg_loss = sub[sub <= 0].mean() if losses > 0 else 0
    pf       = abs(sub[sub > 0].sum() / sub[sub <= 0].sum()) if losses > 0 else 999
    return {
        "total_trades":  total,
        "win_rate_%":    round(wins / total * 100, 1),
        "avg_win_%":     round(avg_win, 2),
        "avg_loss_%":    round(avg_loss, 2),
        "profit_factor": round(pf, 2),
        "total_pnl_%":   round(sub.sum(), 2),
    }

def main():
    print(f"[{datetime.now()}] Fetching USDT pairs...")
    pairs = get_usdt_pairs()
    print(f"Total pairs: {len(pairs)}", flush=True)
    all_results = []
    for idx, symbol in enumerate(pairs):
        print(f"[{idx+1}/{len(pairs)}] {symbol}...", flush=True)
        try:
            res = backtest_symbol(symbol)
            all_results.extend(res)
        except Exception as e:
            print(f"  ERROR {symbol}: {e}", flush=True)
        time.sleep(0.15)
    df = pd.DataFrame(all_results)
    out_csv = os.path.join(OUTPUT_DIR, "backtest_stoch_crossup.csv")
    df.to_csv(out_csv, index=False)
    print(f"\nSaved {len(df)} trades to {out_csv}", flush=True)
    print("\n===== SUMMARY =====")
    summary_rows = []
    for col, label in [
        ("pnl_a_rr",     "A_FixedRR_1:2"),
        ("pnl_b_stoch",  "B_StochExit"),
        ("pnl_c_atr",    "C_ATR"),
        ("pnl_d_fixpct", "D_FixedPct_5/2.5"),
    ]:
        s = summarize(df, col)
        s["method"] = label
        summary_rows.append(s)
        print(f"\n{label}: {s}", flush=True)
    pd.DataFrame(summary_rows).to_csv(
        os.path.join(OUTPUT_DIR, "backtest_stoch_summary.csv"), index=False
    )
    print(f"\nDONE — summary saved to {OUTPUT_DIR}/backtest_stoch_summary.csv", flush=True)

if __name__ == "__main__":
    main()
