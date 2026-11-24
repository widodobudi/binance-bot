"""Indicator rules for Auto Sell Asset and Active Deals TP ('ind-rules', 25/11/2026, Mas Budi).

A rule is written as text, e.g.  HARGA > EMA(20,4h)   |   HARGA > BBU(20,2,4h)   |   RSI(14,1h) > 70
and compared on the RUNNING candle (live value, like TradingView). The rule only fires on a NEW crossing:
the condition must first be false, then become true. After a save / restart / rule change the first reading
only records the starting state (never sells because the condition is already true).

Pure functions + an in-memory state table; the bot supplies a `get_df(timeframe)` callable that returns a
DataFrame with open/high/low/close columns (running candle last). No bot imports here, so it can be tested alone.
"""
import re
import threading
import time

import numpy as np
import pandas as pd
import pandas_ta as ta

TIMEFRAMES = ("1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d")
DEFAULT_TF = "4h"

# name -> (description, default params). Param meaning is per name, see compute().
INDICATORS = {
    "EMA":   ("EMA(panjang)", (20,)),
    "SMA":   ("SMA(panjang)", (20,)),
    "BBU":   ("Bollinger Atas(panjang, pengali)", (20, 2.0)),
    "BBM":   ("Bollinger Tengah(panjang)", (20,)),
    "BBL":   ("Bollinger Bawah(panjang, pengali)", (20, 2.0)),
    "BBB":   ("Bollinger %b(panjang, pengali)", (20, 2.0)),
    "ST":    ("Supertrend garis(ATR panjang, pengali)", (10, 3.0)),
    "KCU":   ("Keltner Atas(EMA panjang, ATR panjang, pengali)", (20, 10, 2.0)),
    "KCM":   ("Keltner Tengah(EMA panjang)", (20,)),
    "KCL":   ("Keltner Bawah(EMA panjang, ATR panjang, pengali)", (20, 10, 2.0)),
    "RSI":   ("RSI(panjang)", (14,)),
    "STK":   ("Stochastic %K(K, D, smooth)", (14, 3, 3)),
    "STD":   ("Stochastic %D(K, D, smooth)", (14, 3, 3)),
    "CCI":   ("CCI(panjang)", (20,)),
    "WR":    ("Williams %R(panjang)", (14,)),
    "MACD":  ("MACD garis(cepat, lambat, sinyal)", (12, 26, 9)),
    "MACDS": ("MACD sinyal(cepat, lambat, sinyal)", (12, 26, 9)),
    "MACDH": ("MACD histogram(cepat, lambat, sinyal)", (12, 26, 9)),
    "ATRP":  ("ATR %(panjang)", (14,)),
}
ALIASES = {
    "BB_UPPER": "BBU", "BBUPPER": "BBU", "BB_MID": "BBM", "BB_LOWER": "BBL", "BBLOWER": "BBL",
    "SUPERTREND": "ST", "KELTNER_UPPER": "KCU", "KELTNER_LOWER": "KCL",
    "STOCHK": "STK", "STOCHD": "STD", "WILLIAMS": "WR", "ATR%": "ATRP", "ATR": "ATRP",
    "MACD_SIGNAL": "MACDS", "MACD_HIST": "MACDH",
}
PRICE_WORDS = ("HARGA", "PRICE", "CLOSE")
INT_PARAMS_MAX = 500
MAX_TEXT = 200

_TF_RE = re.compile(r"^\d+[mhd]$", re.I)
_NAME_RE = re.compile(r"^([A-Za-z_%]+)\s*(?:\((.*)\))?$")


def _fmt_num(x) -> str:
    return f"{x:g}"


def _split_top(text: str):
    """Split at the single top-level '>' or '<' (parentheses are respected). Returns (left, op, right) or None."""
    depth = 0
    found = []
    for i, ch in enumerate(text):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and ch in "<>":
            found.append((i, ch))
    if len(found) != 1:
        return None
    i, ch = found[0]
    return text[:i].strip(), ch, text[i + 1:].strip()


def _parse_operand(txt: str):
    t = txt.strip()
    if not t:
        return None, "operand kosong"
    if t.upper() in PRICE_WORDS:
        return {"t": "price"}, ""
    try:
        return {"t": "num", "v": float(t.replace(",", "."))}, ""
    except ValueError:
        pass
    m = _NAME_RE.match(t)
    if not m:
        return None, f"tidak dikenali: '{t}'"
    name = m.group(1).upper()
    name = ALIASES.get(name, name)
    if name not in INDICATORS:
        return None, f"indikator '{m.group(1)}' tidak dikenal (pilihan: HARGA, " + ", ".join(INDICATORS) + ")"
    defaults = INDICATORS[name][1]
    nums, tf = [], DEFAULT_TF
    argtxt = (m.group(2) or "").strip()
    if argtxt:
        for a in argtxt.split(","):
            a = a.strip()
            if not a:
                return None, f"{name}: argumen kosong"
            if _TF_RE.match(a):
                tf = a.lower()
                if tf not in TIMEFRAMES:
                    return None, f"{name}: timeframe '{a}' tidak didukung (pilihan: {', '.join(TIMEFRAMES)})"
            else:
                try:
                    nums.append(float(a.replace(",", ".")))
                except ValueError:
                    return None, f"{name}: argumen '{a}' bukan angka atau timeframe"
    if len(nums) > len(defaults):
        return None, f"{name} hanya punya {len(defaults)} parameter angka (diberi {len(nums)})"
    params = list(defaults)
    for i, v in enumerate(nums):
        params[i] = v
    out = []
    for i, (v, dflt) in enumerate(zip(params, defaults)):
        if isinstance(dflt, int):
            if v != int(v) or not (1 <= int(v) <= INT_PARAMS_MAX):
                return None, f"{name}: parameter ke-{i + 1} harus bilangan bulat 1-{INT_PARAMS_MAX}"
            out.append(int(v))
        else:
            if not (0 < v <= 20):
                return None, f"{name}: pengali harus lebih dari 0 dan maksimal 20"
            out.append(float(v))
    if name in ("MACD", "MACDS", "MACDH") and out[0] >= out[1]:
        return None, f"{name}: periode cepat harus lebih kecil dari periode lambat"
    return {"t": "ind", "n": name, "p": out, "tf": tf}, ""


def operand_text(o: dict) -> str:
    if o["t"] == "price":
        return "HARGA"
    if o["t"] == "num":
        return _fmt_num(o["v"])
    return f"{o['n']}({','.join(_fmt_num(x) for x in o['p'])},{o['tf']})"


def rule_text(rule: dict) -> str:
    return f"{operand_text(rule['l'])} {rule['op']} {operand_text(rule['r'])}"


def parse_rule(text: str):
    """Return (rule, error). rule = {"l": operand, "op": ">"|"<", "r": operand}; error '' when ok."""
    text = (text or "").strip()
    if not text:
        return None, "aturan kosong"
    if len(text) > MAX_TEXT:
        return None, "aturan terlalu panjang"
    parts = _split_top(text)
    if parts is None:
        return None, "tulis dengan satu tanda > atau <, mis. HARGA > EMA(20,4h)"
    ltxt, op, rtxt = parts
    left, err = _parse_operand(ltxt)
    if err:
        return None, "sisi kiri: " + err
    right, err = _parse_operand(rtxt)
    if err:
        return None, "sisi kanan: " + err
    if left["t"] == "num" and right["t"] == "num":
        return None, "salah satu sisi harus HARGA atau indikator"
    return {"l": left, "op": op, "r": right}, ""


# ── indicator math (last value = running candle) ────────────────────────────────────────────────
def _bb(c: pd.Series, n: int, mult: float):
    mid = c.rolling(n).mean()
    sd = c.rolling(n).std(ddof=0)       # TradingView uses the population deviation
    return mid, mid + mult * sd, mid - mult * sd


def compute(name: str, p, df: pd.DataFrame):
    """Last value of indicator `name` with params `p` on df (open/high/low/close). None if not computable."""
    c, h, l = df["close"].astype(float), df["high"].astype(float), df["low"].astype(float)
    if name == "EMA":
        s = ta.ema(c, length=p[0])
    elif name == "SMA":
        s = c.rolling(p[0]).mean()
    elif name in ("BBU", "BBM", "BBL", "BBB"):
        mid, up, lo = _bb(c, p[0], p[1] if len(p) > 1 else 2.0)
        s = {"BBU": up, "BBM": mid, "BBL": lo, "BBB": (c - lo) / (up - lo)}[name]
    elif name == "ST":
        st = ta.supertrend(h, l, c, length=p[0], multiplier=p[1])
        col = next((x for x in st.columns if x.startswith("SUPERT_")), None)
        s = st[col] if col else None
    elif name in ("KCU", "KCM", "KCL"):
        mid = ta.ema(c, length=p[0])
        if name == "KCM":
            s = mid
        else:
            atr = ta.atr(h, l, c, length=p[1])
            s = mid + p[2] * atr if name == "KCU" else mid - p[2] * atr
    elif name == "RSI":
        s = ta.rsi(c, length=p[0])
    elif name in ("STK", "STD"):
        st = ta.stoch(h, l, c, k=p[0], d=p[1], smooth_k=p[2])
        pref = "STOCHk" if name == "STK" else "STOCHd"
        col = next((x for x in st.columns if x.startswith(pref)), None)
        s = st[col] if col else None
    elif name == "CCI":
        s = ta.cci(h, l, c, length=p[0])
    elif name == "WR":
        s = ta.willr(h, l, c, length=p[0])
    elif name in ("MACD", "MACDS", "MACDH"):
        m = ta.macd(c, fast=p[0], slow=p[1], signal=p[2])
        pref = {"MACD": "MACD_", "MACDS": "MACDs_", "MACDH": "MACDh_"}[name]
        col = next((x for x in m.columns if x.startswith(pref)), None)
        s = m[col] if col else None
    elif name == "ATRP":
        s = ta.atr(h, l, c, length=p[0]) / c * 100
    else:
        return None
    if s is None or len(s) == 0:
        return None
    v = s.iloc[-1]
    return None if pd.isna(v) or not np.isfinite(v) else float(v)


def _with_live_price(df: pd.DataFrame, price: float) -> pd.DataFrame:
    """Make the running candle reflect the live price (close = price, high/low widened)."""
    if price is None or price <= 0 or df is None or len(df) == 0:
        return df
    d = df.copy()
    i = d.index[-1]
    d.loc[i, "close"] = price
    d.loc[i, "high"] = max(float(d.loc[i, "high"]), price)
    d.loc[i, "low"] = min(float(d.loc[i, "low"]), price)
    return d


def operand_value(o: dict, price: float, get_df):
    """Value of one operand now, or (None, reason)."""
    if o["t"] == "price":
        return (price, "") if price and price > 0 else (None, "harga tidak tersedia")
    if o["t"] == "num":
        return o["v"], ""
    df = get_df(o["tf"])
    if df is None or len(df) < 3:
        return None, f"data candle {o['tf']} tidak tersedia"
    v = compute(o["n"], o["p"], _with_live_price(df, price))
    if v is None:
        return None, f"{operand_text(o)}: data belum cukup untuk dihitung"
    return v, ""


def evaluate(rule: dict, price: float, get_df) -> dict:
    """Current values and whether the condition holds right now (no state)."""
    lv, e1 = operand_value(rule["l"], price, get_df)
    rv, e2 = operand_value(rule["r"], price, get_df)
    if lv is None or rv is None:
        return {"ok": False, "met": False, "left": lv, "right": rv, "err": e1 or e2}
    met = lv > rv if rule["op"] == ">" else lv < rv
    return {"ok": True, "met": bool(met), "left": lv, "right": rv, "err": ""}


# ── crossing state ──────────────────────────────────────────────────────────────────────────────
_state: dict = {}
_lock = threading.Lock()


def reset_state(key: str) -> None:
    with _lock:
        _state.pop(key, None)


def state_info(key: str) -> dict:
    with _lock:
        return dict(_state.get(key, {}))


def check_armed(key: str, rule: dict, price: float, get_df) -> dict:
    """Evaluate `rule` and advance the crossing state for `key`.

    armed=True means: a NEW crossing was seen (false -> true) and the condition is still true.
    It stays true while the condition holds, and drops the moment the condition is false.
    First reading after save/restart/rule change only records the start state (armed stays False even when met)."""
    ev = evaluate(rule, price, get_df)
    sig = rule_text(rule)
    out = dict(ev, armed=False, text=sig, new_cross=False, waiting_initial=False)
    if not ev["ok"]:
        return out                      # keep the previous state, skip this cycle
    with _lock:
        st = _state.get(key)
        if st is None or st.get("sig") != sig:
            _state[key] = {"sig": sig, "prev": ev["met"], "armed": False, "since": None, "start_met": ev["met"]}
            out["waiting_initial"] = True
            return out
        if not ev["met"]:
            st["armed"] = False
            st["since"] = None
        elif not st["prev"]:
            st["armed"] = True
            st["since"] = time.time()
            out["new_cross"] = True
        st["prev"] = ev["met"]
        out["armed"] = bool(st["armed"])
        out["since"] = st["since"]
    return out


def describe(res: dict) -> str:
    """Human text of one evaluation, e.g. 'HARGA 1.362 > EMA(20,4h) 1.355'."""
    def f(x):
        return "-" if x is None else f"{x:.8g}"
    if not res.get("ok", False):
        return f"{res.get('text', '')} (tidak bisa dihitung: {res.get('err', '')})"
    return f"{res['text']}  | kiri {f(res['left'])}, kanan {f(res['right'])}"
