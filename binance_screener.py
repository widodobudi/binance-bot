"""
=============================================================
  BINANCE SCREENER -> 3COMMAS + TELEGRAM
  STRATEGI: MOMENTUM BREAKOUT HARIAN (brkX2)  -- forward-test
=============================================================
Lihat SPEC_strategi_momentum_harian.md untuk dasar keputusan.

T = THREAD (proses paralel). Ada 2 thread:
  T1 = Screener + Open Long  (evaluasi candle HARIAN, mode (a): setelah candle tutup)
  T2 = Monitor + Close (trailing adaptif)  (tiap 15 detik)
  (Tidak ada add fund otomatis -- manual oleh user.)

OPEN LONG -- syarat entry pada candle HARIAN (1D):
  1. Supertrend uptrend (length 10, mult 3.0)
  2. close > EMA20
  3. EMA20 > EMA50
  4. close > tertinggi 10 candle harian sebelumnya (breakout)
  5. volume >= 2x MA20(volume)        (brkX2)
  6. RSI(14) < 75
  7. Stoch %K < 70 (opsional via STOCH_MAX; None = matikan)
  + slot deal tersedia (max 1) + skip pair yg sudah di active_deals
  -> OPEN LONG via 3Commas (base $6)

EXIT -- trailing adaptif (jaring pengaman, T2):
  - lacak puncak sejak entry; setelah profit >= +2% pasang trailing
  - jarak adaptif per ATR%: <1->0.5 |1-2->1.0 |2-4->1.5 |4-7->2.0 |>7->2.5
  - close saat turun dari puncak sejauh jarak trailing
  - batas 5 hari -> tutup di harga saat itu
  - user bebas close manual lebih awal

FILTER BTC (Lapis1&2): OFF (toggle). ADD FUND otomatis: OFF.
=============================================================
"""
import requests, pandas as pd, pandas_ta as ta, numpy as np
import time, sys, json, threading, os, csv
from datetime import datetime, timedelta, timezone

# ===================== KONFIGURASI =====================
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN",    "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID",  "")
COMMAS_BOT_ID      = int(os.environ.get("COMMAS_BOT_ID", "0"))
COMMAS_EMAIL_TOKEN = os.environ.get("COMMAS_EMAIL_TOKEN", "")
COMMAS_DELAY_SEC   = 0
# Kredensial WAJIB lewat environment variable (jangan hardcode di kode—repo publik!).
# Set di Railway > Variables: TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, COMMAS_BOT_ID, COMMAS_EMAIL_TOKEN
if not all([TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, COMMAS_EMAIL_TOKEN]) or COMMAS_BOT_ID == 0:
    print("FATAL: env var kredensial belum lengkap "
          "(TELEGRAM_TOKEN/TELEGRAM_CHAT_ID/COMMAS_BOT_ID/COMMAS_EMAIL_TOKEN). "
          "Set di Railway > Variables. Bot berhenti.")
    sys.exit(1)

TIMEFRAME         = "12h"
SUPERTREND_LENGTH = 10
SUPERTREND_MULT   = 3.0
EMA_FAST          = 20
EMA_SLOW          = 50
BREAKOUT_LOOKBACK = 10
VOLUME_MULT       = 1.2
VOLUME_MA_PERIOD  = 20
RSI_LENGTH        = 14
RSI_MAX           = 75
STOCH_MAX         = 70      # syarat ke-7: Stoch %K < 70 (hindari entry terlalu overbought). None = matikan.
MIN_VOLUME_USD    = 1_000_000

TRAIL_ARM_PCT     = 2.0
MAX_HOLD_DAYS     = 5
# detik per candle sesuai timeframe (utk batas hold yg benar di TF apa pun).
# 1d=86400, 12h=43200, 6h=21600, 4h=14400. Batas hold = MAX_HOLD_DAYS candle.
_TF_SECONDS = {"1d":86400, "12h":43200, "8h":28800, "6h":21600, "4h":14400, "1h":3600}
SECONDS_PER_CANDLE = _TF_SECONDS.get(TIMEFRAME, 86400)

BASE_ORDER_VOLUME       = 6
COMMAS_MAX_ACTIVE_DEALS = 4      # total pool deal (brkX2 + reversal). Ubah jg di 3Commas ke 4.
MAX_DEALS_BRKX2         = 2      # slot maksimal strategi brkX2
MAX_DEALS_REVERSAL      = 2      # slot maksimal strategi reversal
ADD_FUND_AUTO           = False
BTC_FILTER_ENABLED      = False

# ---- STRATEGI 2: REVERSAL DOJI + HEIKIN ASHI (8h) ----
REVERSAL_ENABLED      = True
REVERSAL_TIMEFRAME    = "8h"
REVERSAL_EMA_FAST     = 20
REVERSAL_EMA_SLOW     = 50
REVERSAL_DOJI_MAX     = 0.20     # badan doji < 20% range
REVERSAL_SECONDS_PER_CANDLE = _TF_SECONDS.get(REVERSAL_TIMEFRAME, 28800)
REVERSAL_MAX_HOLD_CANDLES   = 30 # batas aman hold (8h*30=10 hari) supaya tdk gantung
# add fund reversal OFF dulu (forward-test slippage; sesuai keputusan)
REVERSAL_ADD_FUND     = False

T1_SCAN_INTERVAL_SEC = 600
T2_MONITOR_INTERVAL  = 15
T2_FAST_INTERVAL     = 2     # polling cepat saat trailing armed & harga bergerak cepat
T2_FAST_TRIGGER_PCT  = 0.5   # ambang "harga bergerak cepat" (% sejak cek terakhir)
HEARTBEAT_INTERVAL_SEC = 6 * 3600   # notif "tidak ada coin lolos" tiap 6 jam (4x/hari)
FWDTEST_CHECK_TRADES   = 12         # cek awal: deteksi masalah dini (sanity check, BUKAN keputusan final)
FWDTEST_TARGET_TRADES  = 25         # evaluasi FINAL: keputusan boleh naik modal

BTC_CHG_1D_MAX = -3.0
BTC_EMA20_MULT = 0.98
BTC_RSI_MIN    = 45

EXCLUDED_BASE_ASSETS = {
    'USDC','USDE','FDUSD','TUSD','DAI','USDP','BUSD','UST','USTC','USD1','U',
    'USDD','PYUSD','FRAX','GUSD','LUSD','USDJ','USDN','USD0','USDY',
    'USDS','SUSD','CRVUSD','GHO','USDX','USDL','RLUSD','XUSD',
    'EUR','EURI','EURS','AEUR','EURT','CEUR','EURC','EURQ',
    'GBP','GBPT','CHF','TRY','TRYB','BRL','BRZ','ARS','ZAR',
    'IDRT','JPY','JPYC','AUD','MXN','NGN','COP','UAH',
    # Komoditas (emas/perak) — bergerak ikut harga komoditas, bukan kripto:
    'PAXG','XAUT','XAU','XAUM','KAU','TGOLD','XAGT','XAG','KAG',
}

BASE = "https://data-api.binance.vision"
DATA_DIR = os.environ.get("DATA_DIR", r"D:\tradingview")
ACTIVE_DEALS_FILE = os.path.join(DATA_DIR, "active_deals.json")
TRADES_CSV = os.path.join(DATA_DIR, "trades_forwardtest.csv")
trades_csv_lock = threading.Lock()

# Kolom CSV log forward-test (1 baris per trade; ditulis saat OPEN, dilengkapi saat CLOSE)
CSV_FIELDS = [
    'open_time_wib','symbol','signal_price','entry_price','slip_pct','atr_pct',
    'trail_dist_pct','base_usd',
    'close_time_wib','exit_price','profit_pct','exit_reason','status'
]

def _csv_ensure_header():
    """Buat file + header kalau belum ada."""
    if not os.path.exists(TRADES_CSV):
        os.makedirs(os.path.dirname(TRADES_CSV) or '.', exist_ok=True)
        with open(TRADES_CSV, 'w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

def csv_log_open(row: dict):
    """Tulis 1 baris saat OPEN (status=OPEN, kolom exit kosong)."""
    try:
        with trades_csv_lock:
            _csv_ensure_header()
            full = {k: row.get(k, '') for k in CSV_FIELDS}
            full['status'] = 'OPEN'
            with open(TRADES_CSV, 'a', newline='', encoding='utf-8') as f:
                csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(full)
        log(f"   [CSV] OPEN dicatat: {row.get('symbol')}")
    except Exception as e:
        log(f"   [CSV] gagal tulis OPEN: {e}")

def csv_log_close(symbol: str, close_time_wib: str, exit_price, profit_pct, exit_reason: str):
    """Lengkapi baris OPEN terakhir untuk symbol ini dengan data exit (rewrite seluruh file)."""
    try:
        with trades_csv_lock:
            if not os.path.exists(TRADES_CSV):
                log(f"   [CSV] file belum ada saat CLOSE {symbol}"); return
            with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
            # cari baris OPEN paling akhir utk symbol ini yg belum punya exit
            target = None
            for r in reversed(rows):
                if r.get('symbol') == symbol and r.get('status') == 'OPEN':
                    target = r; break
            if target is None:
                log(f"   [CSV] tidak ketemu baris OPEN utk {symbol} saat CLOSE"); return
            target['close_time_wib'] = close_time_wib
            target['exit_price']     = f"{exit_price:.6g}" if isinstance(exit_price,(int,float)) else exit_price
            target['profit_pct']     = f"{profit_pct:.2f}" if isinstance(profit_pct,(int,float)) else profit_pct
            target['exit_reason']    = exit_reason
            target['status']         = 'CLOSED'
            with open(TRADES_CSV, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=CSV_FIELDS); w.writeheader(); w.writerows(rows)
        log(f"   [CSV] CLOSE dicatat: {symbol}")
    except Exception as e:
        log(f"   [CSV] gagal tulis CLOSE: {e}")

def csv_progress():
    """Baca CSV, hitung trade SELESAI (CLOSED), berapa menang/kalah, total profit%.
    Return dict atau None kalau CSV belum ada / error."""
    try:
        if not os.path.exists(TRADES_CSV):
            return None
        with trades_csv_lock:
            with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
        closed = [r for r in rows if r.get('status') == 'CLOSED']
        n = len(closed)
        if n == 0:
            return {'n': 0, 'win': 0, 'loss': 0, 'total_pct': 0.0}
        win = 0; total = 0.0
        for r in closed:
            try:
                p = float(r.get('profit_pct', '') or 0)
                total += p
                if p > 0: win += 1
            except (ValueError, TypeError):
                pass
        return {'n': n, 'win': win, 'loss': n-win, 'total_pct': total}
    except Exception as e:
        log(f"   [CSV] gagal baca progress: {e}")
        return None

active_deals_lock = threading.Lock()
active_deals      = {}
last_processed_candle_ts = 0
# heartbeat state: kapan periode "tidak ada lolos" dimulai & kapan terakhir lapor
heartbeat_window_start = None   # datetime WIB awal periode berjalan
heartbeat_last_sent    = 0.0    # epoch detik notif terakhir

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

# ===================== UTIL =====================
def now_wib():
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=7))).replace(tzinfo=None)

def log(msg):
    print(f"[{now_wib().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        # Tanpa parse_mode: teks polos, supaya '<' dan '>' (mis. "RSI<75",
        # "vol>=2x") tidak ditafsirkan Telegram sebagai tag HTML.
        resp = session.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "text": message,
            "disable_notification": False
        }, timeout=10)
        if resp.status_code != 200:
            log(f"WARN Telegram error: {resp.text}")
    except Exception as e:
        log(f"WARN gagal kirim Telegram: {e}")

def to_commas_pair(symbol: str) -> str:
    return f"USDT_{symbol.replace('USDT','')}"

def to_display_pair(symbol: str) -> str:
    return f"{symbol.replace('USDT','')}/USDT"

# ===================== PERSISTENSI =====================
def _convert(obj):
    if isinstance(obj, (np.integer,)): return int(obj)
    if isinstance(obj, (np.floating,)): return float(obj)
    raise TypeError(f"Not serializable: {type(obj)}")

def load_active_deals():
    global active_deals
    if not os.path.exists(ACTIVE_DEALS_FILE):
        log("   active_deals.json tidak ada, mulai kosong."); return
    try:
        with open(ACTIVE_DEALS_FILE,'r') as f: data=json.load(f)
        with active_deals_lock: active_deals=data
        log(f"   Loaded active_deals: {list(active_deals.keys())}")
    except Exception as e:
        log(f"WARN gagal baca active_deals.json: {e}")

def save_active_deals():
    try:
        with active_deals_lock: data=dict(active_deals)
        with open(ACTIVE_DEALS_FILE,'w') as f: json.dump(data,f,indent=2,default=_convert)
    except Exception as e:
        log(f"WARN gagal simpan active_deals.json: {e}")

def add_to_active_deals(symbol: str, data: dict):
    with active_deals_lock:
        active_deals[symbol] = {**data, 'opened_at': now_wib().strftime('%Y-%m-%d %H:%M:%S')}
    save_active_deals()
    log(f"   {symbol} ditambah ke active_deals.json")

def remove_from_active_deals(symbol: str):
    with active_deals_lock:
        active_deals.pop(symbol, None)
    save_active_deals()

# ===================== 3COMMAS =====================
def send_3commas(payload: dict, label: str) -> bool:
    try:
        resp = session.post("https://3commas.io/trade_signal/trading_view", json=payload, timeout=10)
        pair = payload.get('pair','')
        if resp.status_code != 200:
            log(f"WARN [3C] {label} HTTP {resp.status_code}: {resp.text}"); return False
        try:
            body=resp.json()
            if isinstance(body,dict):
                if 'error' in body or 'errors' in body:
                    log(f"WARN [3C] {label} ditolak: {body.get('error') or body.get('errors')}"); return False
            log(f"OK [3C] {label} terkirim: {pair}"); return True
        except Exception:
            log(f"OK [3C] {label} terkirim (200): {pair}"); return True
    except Exception as e:
        log(f"WARN [3C] gagal {label}: {e}"); return False

def send_open_long(symbol: str) -> bool:
    return send_3commas({"message_type":"bot","bot_id":COMMAS_BOT_ID,
        "email_token":COMMAS_EMAIL_TOKEN,"delay_seconds":COMMAS_DELAY_SEC,
        "pair":to_commas_pair(symbol)}, "open_long")

def send_close_long(symbol: str) -> bool:
    return send_3commas({"action":"close_at_market_price","message_type":"bot",
        "bot_id":COMMAS_BOT_ID,"email_token":COMMAS_EMAIL_TOKEN,
        "delay_seconds":COMMAS_DELAY_SEC,"pair":to_commas_pair(symbol)}, "close_long")

def send_add_funds(symbol: str, volume) -> bool:
    """Add fund manual (tidak dipanggil otomatis; disediakan utk keperluan manual)."""
    return send_3commas({"action":"add_funds_in_quote","message_type":"bot",
        "bot_id":COMMAS_BOT_ID,"email_token":COMMAS_EMAIL_TOKEN,
        "delay_seconds":COMMAS_DELAY_SEC,"pair":to_commas_pair(symbol),
        "volume":volume}, "add_funds")

def send_start_trailing(symbol: str) -> bool:
    """Aktifkan trailing 3Commas (action start_trailing)."""
    return send_3commas({"action":"start_trailing","message_type":"bot",
        "bot_id":COMMAS_BOT_ID,"email_token":COMMAS_EMAIL_TOKEN,
        "delay_seconds":COMMAS_DELAY_SEC,"pair":to_commas_pair(symbol)}, "start_trailing")

# ===================== DATA =====================
def get_usdt_spot_pairs():
    try:
        info = session.get(f"{BASE}/api/v3/exchangeInfo", timeout=30).json()
        out=[]
        for s in info.get('symbols',[]):
            if s.get('quoteAsset')!='USDT': continue
            if s.get('status')!='TRADING': continue
            if s.get('baseAsset') in EXCLUDED_BASE_ASSETS: continue
            out.append(s['symbol'])
        return out
    except Exception as e:
        log(f"WARN gagal exchangeInfo: {e}"); return []

def get_ticker_24h():
    try:
        return session.get(f"{BASE}/api/v3/ticker/24hr", timeout=30).json()
    except Exception as e:
        log(f"WARN gagal ticker24h: {e}"); return []

def get_ohlcv(symbol: str, interval=TIMEFRAME, limit=120):
    try:
        r = session.get(f"{BASE}/api/v3/klines",
                        params={'symbol':symbol,'interval':interval,'limit':limit}, timeout=15)
        d = r.json()
        if not isinstance(d,list) or len(d)<60: return None
        df = pd.DataFrame(d, columns=['ot','open','high','low','close','vol','ct','qav','nt','tbbav','tbqav','ig'])
        for c in ['open','high','low','close','vol','qav']: df[c]=df[c].astype(float)
        df['ot']=df['ot'].astype('int64'); df['ct']=df['ct'].astype('int64')
        return df
    except Exception:
        return None

def get_price_now(symbol: str) -> float:
    """Harga pasar Binance terkini — dipakai sbg entry_price (opsi a) & monitor."""
    try:
        r = session.get(f"{BASE}/api/v3/ticker/price", params={'symbol':symbol}, timeout=10).json()
        return float(r['price'])
    except Exception:
        return 0.0

# ===================== INDIKATOR & SYARAT =====================
def compute_indicators(df):
    close,high,low = df['close'],df['high'],df['low']
    df['ema_fast']=ta.ema(close,length=EMA_FAST)
    df['ema_slow']=ta.ema(close,length=EMA_SLOW)
    st=ta.supertrend(high,low,close,length=SUPERTREND_LENGTH,multiplier=SUPERTREND_MULT)
    df['st_dir']=st[[c for c in st.columns if 'SUPERTd' in c][0]]
    df['atr_pct']=ta.atr(high,low,close,length=14)/close*100
    df['hh']=high.rolling(BREAKOUT_LOOKBACK).max().shift(1)
    df['vol_ma']=df['vol'].rolling(VOLUME_MA_PERIOD).mean()
    df['rsi']=ta.rsi(close,length=RSI_LENGTH)
    if STOCH_MAX is not None:
        stoch=ta.stoch(high,low,close,k=14,d=3,smooth_k=3)
        kcol=[c for c in stoch.columns if 'STOCHk' in c][0]
        df['stoch_k']=stoch[kcol]
    return df

def check_entry(df) -> bool:
    """Evaluasi pada candle TERTUTUP terakhir (mode a)."""
    row = df.iloc[-1]
    if pd.isna(row['ema_fast']) or pd.isna(row['ema_slow']) or pd.isna(row['hh']) or pd.isna(row['vol_ma']):
        return False
    if row['st_dir'] != 1: return False
    if not (row['close'] > row['ema_fast']): return False
    if not (row['ema_fast'] > row['ema_slow']): return False
    if not (row['close'] > row['hh']): return False
    if row['vol'] < VOLUME_MULT * row['vol_ma']: return False
    if pd.isna(row['rsi']) or row['rsi'] > RSI_MAX: return False
    if STOCH_MAX is not None:
        if 'stoch_k' not in row or pd.isna(row['stoch_k']) or row['stoch_k'] >= STOCH_MAX:
            return False
    return True

# ===================== STRATEGI 2: REVERSAL DOJI + HEIKIN ASHI (8h) =====================
def heikin_ashi_bullish(df):
    """Return Series bool: HA_close > HA_open (HA bullish) tiap candle."""
    ha_close = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    ha_open = [ (df['open'].iloc[0] + df['close'].iloc[0]) / 2 ]
    hc = ha_close.values
    for i in range(1, len(df)):
        ha_open.append((ha_open[i-1] + hc[i-1]) / 2)
    import numpy as _np
    return pd.Series(ha_close.values > _np.array(ha_open), index=df.index)

def compute_indicators_reversal(df):
    """Indikator utk strategi reversal (EMA20/50, ATR%, doji body ratio, HA bullish)."""
    close, high, low = df['close'], df['high'], df['low']
    df['ema_fast'] = ta.ema(close, length=REVERSAL_EMA_FAST)
    df['ema_slow'] = ta.ema(close, length=REVERSAL_EMA_SLOW)
    df['atr_pct']  = ta.atr(high, low, close, length=14) / close * 100
    rng = (high - low).replace(0, float('nan'))
    df['body_ratio'] = (close - df['open']).abs() / rng
    df['ha_bull'] = heikin_ashi_bullish(df)
    return df

def _cross_up(df, idx, ema_col):
    """close transisi dari < EMA ke >= EMA pada idx (vs idx-1)."""
    if idx < 1: return False
    p = df.iloc[idx-1]; cur = df.iloc[idx]
    if pd.isna(p[ema_col]) or pd.isna(cur[ema_col]): return False
    return p['close'] < p[ema_col] and cur['close'] >= cur[ema_col]

def check_entry_reversal(df) -> bool:
    """Setup reversal pada candle TERTUTUP terakhir sbg c+3.
    Pola pd 4 candle terakhir: c0=df[-4], c+1=df[-3], c+2=df[-2], c+3=df[-1].
      - close c0 di BAWAH EMA20 & EMA50
      - c0 DOJI (body_ratio < REVERSAL_DOJI_MAX)
      - c+1 & c+2 HA bullish
      - c+2 ATAU c+3 crossing-up EMA20
    Entry dilakukan di candle c+3 yg baru tutup (mode a)."""
    if len(df) < 4: return False
    n = len(df)
    i0 = n - 4            # c0
    i1, i2, i3 = n-3, n-2, n-1
    c0 = df.iloc[i0]
    if any(pd.isna(c0[x]) for x in ['ema_fast','ema_slow','body_ratio']): return False
    # kondisi awal: c0 di bawah EMA20 & EMA50
    if not (c0['close'] < c0['ema_fast'] and c0['close'] < c0['ema_slow']): return False
    # c0 doji
    if not (c0['body_ratio'] < REVERSAL_DOJI_MAX): return False
    # c+1 & c+2 HA bullish
    if not (bool(df['ha_bull'].iloc[i1]) and bool(df['ha_bull'].iloc[i2])): return False
    # c+2 atau c+3 crossing-up EMA20
    if not (_cross_up(df, i2, 'ema_fast') or _cross_up(df, i3, 'ema_fast')): return False
    return True

def trailing_dist(atr_pct: float) -> float:
    if atr_pct < 1.0: return 0.5
    if atr_pct < 2.0: return 1.0
    if atr_pct < 4.0: return 1.5
    if atr_pct < 7.0: return 2.0
    return 2.5

def btc_filter_ok() -> bool:
    """Lapis 1 & 2 BTC. Hanya dipakai kalau BTC_FILTER_ENABLED True."""
    df = get_ohlcv("BTCUSDT", limit=60)
    if df is None: return True  # gagal ambil -> jangan blokir
    df = compute_indicators(df)
    row = df.iloc[-1]
    chg = (row['close']-row['open'])/row['open']*100
    if chg <= BTC_CHG_1D_MAX: return False                 # Lapis 1
    if not (row['close'] >= row['ema_fast']*BTC_EMA20_MULT): return False  # Lapis 2 harga
    if row['ema_fast'] <= df['ema_fast'].iloc[-2]: return False            # Lapis 2 EMA naik
    if pd.isna(row['rsi']) or row['rsi'] < BTC_RSI_MIN: return False       # Lapis 2 RSI
    return True

# ===================== THREAD 1: SCREENER + OPEN LONG =====================
def active_deal_count() -> int:
    with active_deals_lock:
        return len(active_deals)

def deal_count_by_strategy(strategy: str) -> int:
    """Hitung deal aktif milik strategi tertentu ('brkX2' atau 'reversal').
    Deal tanpa tag strategy dianggap 'brkX2' (kompatibilitas deal lama)."""
    with active_deals_lock:
        return sum(1 for d in active_deals.values()
                   if d.get('strategy', 'brkX2') == strategy)

def heartbeat_tick(status_line: str):
    """Heartbeat tiap HEARTBEAT_INTERVAL_SEC (6 jam). Dipanggil di SETIAP scan
    (apa pun hasilnya) sbg tanda bot hidup. status_line menjelaskan keadaan
    periode itu (mis. tidak ada lolos / ada deal aktif / slot penuh).
    Panggilan PERTAMA langsung kirim (konfirmasi bot hidup saat start),
    selanjutnya mengirim tiap 6 jam terlewat."""
    global heartbeat_window_start, heartbeat_last_sent
    now = time.time()
    now_dt = now_wib()
    if heartbeat_window_start is None:
        heartbeat_window_start = now_dt
    first_time = (heartbeat_last_sent == 0.0)
    if first_time or (now - heartbeat_last_sent >= HEARTBEAT_INTERVAL_SEC):
        if first_time:
            start_str = now_dt.strftime('%d/%m %H:%M')
            header = ("HEARTBEAT (Momentum brkX2) — START\n"
                      f"Mulai memantau: {start_str} WIB\n"
                      "Notif berikutnya tiap 6 jam.")
        else:
            start_str = heartbeat_window_start.strftime('%d/%m %H:%M')
            end_str   = now_dt.strftime('%d/%m %H:%M')
            header = ("HEARTBEAT 6-jam (Momentum brkX2)\n"
                      f"Periode: {start_str} -> {end_str} WIB")
        # progress forward-test (dari CSV) — dua tahap: cek awal 12, final 25
        prog = csv_progress()
        if prog is None:
            prog_line = "Progress forward-test: 0 trade selesai (CSV belum ada)."
        else:
            n = prog['n']
            wl = f"{prog['win']}W/{prog['loss']}L" if n > 0 else "-"
            if n < FWDTEST_CHECK_TRADES:
                tahap = f"menuju cek-awal {FWDTEST_CHECK_TRADES}"
            elif n < FWDTEST_TARGET_TRADES:
                tahap = f"cek-awal {FWDTEST_CHECK_TRADES} LEWAT, menuju final {FWDTEST_TARGET_TRADES}"
            else:
                tahap = f"target final {FWDTEST_TARGET_TRADES} TERCAPAI - waktunya evaluasi!"
            prog_line = (f"Progress forward-test: {n} trade selesai "
                         f"({wl}, total {prog['total_pct']:+.1f}%) | {tahap}.")
        send_telegram(
            f"{header}\n"
            f"{status_line}\n"
            f"Slot deal: {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}\n"
            f"{prog_line}\n"
            f"Bot HIDUP & terus memantau."
        )
        log(f"[T1] Heartbeat terkirim ({'START' if first_time else start_str+' -> '+end_str}): {status_line}")
        heartbeat_last_sent = now
        heartbeat_window_start = now_dt  # mulai periode baru



def thread1_scan():
    global last_processed_candle_ts, heartbeat_window_start, heartbeat_last_sent
    log("[T1] Scan candle (TF tutup)...")
    # ambil ticker utk filter volume + daftar pair
    pairs = get_usdt_spot_pairs()
    if not pairs:
        log("[T1] Tidak ada pair, skip."); return "Gagal ambil daftar pair (cek koneksi Binance)."
    ticker = get_ticker_24h()
    volmap = {}
    for t in ticker:
        try: volmap[t['symbol']] = float(t.get('quoteVolume',0))
        except: pass
    universe = [p for p in pairs if volmap.get(p,0) >= MIN_VOLUME_USD]

    # slot brkX2 penuh ATAU total pool penuh? jangan cari sinyal
    if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2 or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
        log(f"[T1] Slot brkX2 penuh ({deal_count_by_strategy('brkX2')}/{MAX_DEALS_BRKX2}) "
            f"atau total ({active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}), tidak cari entry.")
        with active_deals_lock:
            syms = ", ".join(to_display_pair(s) for s in active_deals.keys()) or "-"
        return f"Slot brkX2/total penuh — deal aktif: {syms}. Tidak cari entry baru."

    # filter BTC (kalau diaktifkan)
    if BTC_FILTER_ENABLED and not btc_filter_ok():
        log("[T1] Filter BTC: kondisi tidak lolos, scan dibatalkan.")
        return "Filter BTC aktif & tidak lolos — scan dibatalkan periode ini."

    candidates = []
    newest_ts = 0
    for sym in universe:
        with active_deals_lock:
            if sym in active_deals: continue
        df = get_ohlcv(sym, limit=120)
        if df is None: continue
        # mode (a): pastikan candle terakhir SUDAH tutup
        # candle tutup saat ct < waktu sekarang (ms)
        if df['ct'].iloc[-1] >= int(time.time()*1000):
            df = df.iloc[:-1]  # buang candle berjalan, pakai yg sudah tutup
            if len(df) < 60: continue
        df = compute_indicators(df)
        newest_ts = max(newest_ts, int(df['ct'].iloc[-1]))
        if check_entry(df):
            candidates.append((sym, float(df['close'].iloc[-1]), float(df['atr_pct'].iloc[-1])))

    if not candidates:
        log(f"[T1] {len(universe)} coin discan, tidak ada yg lolos syarat entry.")
        last_processed_candle_ts = newest_ts
        return f"TIDAK ADA coin lolos 7 syarat entry. ({len(universe)} coin discan)"

    # urutkan kandidat: ATR% terkecil (paling stabil) dulu
    candidates.sort(key=lambda x: x[2])
    log(f"[T1] {len(candidates)} kandidat lolos. Slot terpakai {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}")

    opened_any = False
    for sym, signal_price, atrp in candidates:
        # berhenti kalau slot brkX2 ATAU total sudah penuh
        if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2 or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
            log(f"[T1] Slot brkX2/total penuh, sisa kandidat tidak dibuka.")
            break
        with active_deals_lock:
            if sym in active_deals:
                continue  # sudah punya deal di pair ini
        log(f"[T1] SINYAL: {sym} close_candle={signal_price:.6g} atr%={atrp:.2f}")
        if send_open_long(sym):
            entry_price = get_price_now(sym)
            if entry_price <= 0:
                entry_price = signal_price
            slip_pct = (entry_price/signal_price - 1) * 100 if signal_price > 0 else 0.0
            add_to_active_deals(sym, {
                'entry_price': entry_price, 'peak': entry_price,
                'signal_price': signal_price, 'atr_pct': atrp,
                'opened_candle_ts': int(newest_ts), 'trailing_armed': False,
                'strategy': 'brkX2'
            })
            send_telegram(
                f"OPEN LONG (Momentum Harian)\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (candle close): {signal_price:.6g}\n"
                f"Selisih (lonjakan/slippage): {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot terpakai: {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}"
            )
            csv_log_open({
                'open_time_wib': now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': to_display_pair(sym),
                'signal_price': f"{signal_price:.6g}",
                'entry_price': f"{entry_price:.6g}",
                'slip_pct': f"{slip_pct:+.2f}",
                'atr_pct': f"{atrp:.2f}",
                'trail_dist_pct': f"{trailing_dist(atrp)}",
                'base_usd': BASE_ORDER_VOLUME,
            })
            opened_any = True

    if opened_any:
        # ada trade -> reset window heartbeat
        heartbeat_window_start = now_wib()
        heartbeat_last_sent = time.time()
    last_processed_candle_ts = newest_ts
    return None if opened_any else f"{len(candidates)} kandidat lolos tapi tak ada yg jadi dibuka."

# ===================== THREAD 2: MONITOR + CLOSE (trailing) =====================

# ===================== THREAD 1b: SCAN REVERSAL (8h) + OPEN LONG =====================
def thread1b_scan_reversal():
    """Scan strategi reversal (doji + 2 HA bull + cross EMA20) di timeframe 8h.
    Berbagi pool deal & bot 3Commas dgn brkX2, tapi slot terpisah (MAX_DEALS_REVERSAL)."""
    if not REVERSAL_ENABLED:
        return None
    log("[T1b] Scan REVERSAL candle 8h (TF tutup)...")
    pairs = get_usdt_spot_pairs()
    if not pairs:
        return "Gagal ambil daftar pair (reversal)."
    ticker = get_ticker_24h()
    volmap = {}
    for t in ticker:
        try: volmap[t['symbol']] = float(t.get('quoteVolume',0))
        except: pass
    universe = [p for p in pairs if volmap.get(p,0) >= MIN_VOLUME_USD]

    # slot reversal penuh ATAU total pool penuh?
    if deal_count_by_strategy('reversal') >= MAX_DEALS_REVERSAL or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
        log(f"[T1b] Slot reversal penuh ({deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL}) "
            f"atau total ({active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}).")
        return f"Slot reversal/total penuh. Tidak cari entry reversal."

    candidates = []
    for sym in universe:
        with active_deals_lock:
            if sym in active_deals: continue   # satu coin, satu deal (lintas strategi)
        df = get_ohlcv(sym, interval=REVERSAL_TIMEFRAME, limit=120)
        if df is None or len(df) < 60: continue
        # mode (a): pastikan candle terakhir SUDAH tutup
        if df['ct'].iloc[-1] >= int(time.time()*1000):
            df = df.iloc[:-1]
            if len(df) < 60: continue
        df = compute_indicators_reversal(df)
        if check_entry_reversal(df):
            atrp = float(df['atr_pct'].iloc[-1]) if not pd.isna(df['atr_pct'].iloc[-1]) else 3.0
            candidates.append((sym, float(df['close'].iloc[-1]), atrp, int(df['ct'].iloc[-1])))

    if not candidates:
        log(f"[T1b] {len(universe)} coin discan (reversal), tidak ada yg lolos setup.")
        return f"REVERSAL: tidak ada coin lolos setup. ({len(universe)} discan)"

    # urutkan: ATR% terkecil dulu (paling stabil)
    candidates.sort(key=lambda x: x[2])
    log(f"[T1b] {len(candidates)} kandidat reversal lolos. Slot reversal {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL}")

    opened_any = False
    for sym, signal_price, atrp, cts in candidates:
        if deal_count_by_strategy('reversal') >= MAX_DEALS_REVERSAL or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
            log(f"[T1b] Slot reversal/total penuh, sisa kandidat reversal tidak dibuka.")
            break
        with active_deals_lock:
            if sym in active_deals: continue
        log(f"[T1b] SINYAL REVERSAL: {sym} close_candle={signal_price:.6g} atr%={atrp:.2f}")
        if send_open_long(sym):
            entry_price = get_price_now(sym)
            if entry_price <= 0: entry_price = signal_price
            slip_pct = (entry_price/signal_price - 1) * 100 if signal_price > 0 else 0.0
            add_to_active_deals(sym, {
                'entry_price': entry_price, 'peak': entry_price,
                'signal_price': signal_price, 'atr_pct': atrp,
                'opened_candle_ts': int(cts), 'trailing_armed': False,
                'strategy': 'reversal'
            })
            send_telegram(
                f"OPEN LONG (Reversal Doji+HA 8h)\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (candle close): {signal_price:.6g}\n"
                f"Selisih (slippage): {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} | total {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}"
            )
            csv_log_open({
                'open_time_wib': now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': to_display_pair(sym),
                'signal_price': f"{signal_price:.6g}",
                'entry_price': f"{entry_price:.6g}",
                'slip_pct': f"{slip_pct:+.2f}",
                'atr_pct': f"{atrp:.2f}",
                'trail_dist_pct': f"{trailing_dist(atrp)}",
                'base_usd': BASE_ORDER_VOLUME,
                'strategy': 'reversal',
            })
            opened_any = True
    return None if opened_any else f"{len(candidates)} kandidat reversal lolos tapi tak ada yg dibuka."
def thread2_monitor():
    want_fast = False  # jadi True jika ada deal armed yg harganya bergerak cepat
    with active_deals_lock:
        syms = list(active_deals.keys())
    for sym in syms:
        with active_deals_lock:
            d = dict(active_deals.get(sym, {}))
        if not d: continue
        entry = d.get('entry_price',0); 
        if entry<=0: continue
        price = get_price_now(sym)
        if price<=0: continue

        # update peak
        peak = max(d.get('peak',entry), price)
        prof_from_entry = (price/entry-1)*100
        prof_peak       = (peak/entry-1)*100
        atrp = d.get('atr_pct',3.0)
        tdist = trailing_dist(atrp)
        armed = d.get('trailing_armed', False)

        # arm trailing setelah profit >= +2% (pakai puncak)
        if (not armed) and prof_peak >= TRAIL_ARM_PCT:
            armed = True
            log(f"[T2] {sym} trailing ARMED (peak profit {prof_peak:.2f}%)")

        # deteksi pergerakan cepat (HANYA relevan saat armed) utk polling adaptif
        last_price = d.get('last_price', price)
        if last_price > 0:
            move_pct = abs(price/last_price - 1)*100
            if armed and move_pct >= T2_FAST_TRIGGER_PCT:
                want_fast = True

        do_close=False; reason=""
        if armed:
            stop = peak*(1 - tdist/100)
            if price <= stop:
                do_close=True; reason=f"trailing (turun ke {price:.6g} dari puncak {peak:.6g}, dev {tdist}%)"

        # batas hold sadar-strategi:
        #  brkX2  : MAX_HOLD_DAYS candle 12h (5*12jam=2.5 hari)
        #  reversal: REVERSAL_MAX_HOLD_CANDLES candle 8h
        opened_ts = d.get('opened_candle_ts',0)/1000.0
        if d.get('strategy','brkX2') == 'reversal':
            hold_limit_sec = REVERSAL_MAX_HOLD_CANDLES * REVERSAL_SECONDS_PER_CANDLE
            hold_label = f"batas {REVERSAL_MAX_HOLD_CANDLES} candle 8h"
        else:
            hold_limit_sec = MAX_HOLD_DAYS * SECONDS_PER_CANDLE
            hold_label = f"batas {MAX_HOLD_DAYS} candle"
        if opened_ts>0 and (time.time()-opened_ts) >= hold_limit_sec:
            do_close=True; reason=hold_label+" tercapai"

        # simpan peak/armed/last_price
        with active_deals_lock:
            if sym in active_deals:
                active_deals[sym]['peak']=peak
                active_deals[sym]['trailing_armed']=armed
                active_deals[sym]['last_price']=price
        save_active_deals()

        if do_close:
            log(f"[T2] CLOSE {sym}: {reason} | profit {prof_from_entry:.2f}%")
            if send_close_long(sym):
                send_telegram(
                    f"CLOSE LONG (Momentum Harian)\n"
                    f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                    f"Pair   : {to_display_pair(sym)}\n"
                    f"Alasan : {reason}\n"
                    f"Profit : {prof_from_entry:.2f}% (dari entry)"
                )
                csv_log_close(
                    to_display_pair(sym),
                    now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                    price, prof_from_entry, reason
                )
                remove_from_active_deals(sym)
    return want_fast

# ===================== RUNNERS =====================
def run_thread1():
    while True:
        try:
            status = thread1_scan()
            if status:  # None = ada trade (notif OPEN sudah jalan); selain itu detak heartbeat
                heartbeat_tick(status)
        except Exception as e: log(f"WARN T1 error: {e}")
        # scan reversal (8h) di siklus yg sama; cek candle 8h-nya sendiri di dalam fungsi
        try:
            if REVERSAL_ENABLED:
                thread1b_scan_reversal()
        except Exception as e: log(f"WARN T1b reversal error: {e}")
        time.sleep(T1_SCAN_INTERVAL_SEC)

def run_thread2():
    while True:
        interval = T2_MONITOR_INTERVAL
        try:
            want_fast = thread2_monitor()
            if want_fast:
                interval = T2_FAST_INTERVAL  # armed + harga bergerak cepat -> polling 2 detik
        except Exception as e: log(f"WARN T2 error: {e}")
        time.sleep(interval)

if __name__ == '__main__':
    log("="*55)
    log("  BINANCE SCREENER -> 3COMMAS + TELEGRAM")
    log("  STRATEGI: MOMENTUM BREAKOUT HARIAN (brkX2)")
    log("="*55)
    log(f"  Timeframe        : {TIMEFRAME}")
    log(f"  Entry syarat     : ST-up, >EMA20, EMA20>EMA50, breakout{BREAKOUT_LOOKBACK}, vol>={VOLUME_MULT}xMA, RSI<{RSI_MAX}" + (f", Stoch<{STOCH_MAX}" if STOCH_MAX is not None else ""))
    log(f"  Exit             : trailing adaptif (arm +{TRAIL_ARM_PCT}%), batas {MAX_HOLD_DAYS} hari")
    log(f"  Base order       : ${BASE_ORDER_VOLUME} | Max deal total: {COMMAS_MAX_ACTIVE_DEALS}")
    log(f"  Slot per strategi: brkX2={MAX_DEALS_BRKX2}, reversal={MAX_DEALS_REVERSAL}")
    log(f"  Add fund auto    : {'ON' if ADD_FUND_AUTO else 'OFF (manual)'}")
    log(f"  Filter BTC L1&L2 : {'ON' if BTC_FILTER_ENABLED else 'OFF'}")
    log(f"  Min vol 24h      : ${MIN_VOLUME_USD:,}")
    if REVERSAL_ENABLED:
        log("  " + "-"*51)
        log(f"  STRATEGI 2 REVERSAL: ON | TF {REVERSAL_TIMEFRAME}")
        log(f"  Setup: harga<EMA20&50, doji(<{int(REVERSAL_DOJI_MAX*100)}% body), 2 HA bull, cross-up EMA20")
        log(f"  Exit : trailing adaptif (sama brkX2) | add fund: {'ON' if REVERSAL_ADD_FUND else 'OFF'}")
        log(f"  Hold : maks {REVERSAL_MAX_HOLD_CANDLES} candle 8h")
    log("="*55)

    load_active_deals()
    # Tes telegram + notif startup
    send_telegram(
        "Binance Screener AKTIF (Momentum Harian brkX2)\n"
        f"Entry: ST-up + >EMA20 + EMA20>EMA50 + breakout{BREAKOUT_LOOKBACK} + vol>={VOLUME_MULT}xMA + RSI<{RSI_MAX}" + (f" + Stoch<{STOCH_MAX}" if STOCH_MAX is not None else "") + "\n"
        f"Exit : trailing adaptif (arm +{TRAIL_ARM_PCT}%, jarak per ATR%), batas {MAX_HOLD_DAYS} hari\n"
        f"Base ${BASE_ORDER_VOLUME} | Max deal {COMMAS_MAX_ACTIVE_DEALS} | "
        f"AddFund {'ON' if ADD_FUND_AUTO else 'OFF'} | BTC filter {'ON' if BTC_FILTER_ENABLED else 'OFF'}\n"
        f"Evaluasi: candle {TIMEFRAME} TERTUTUP (mode a)"
    )

    t1 = threading.Thread(target=run_thread1, daemon=True, name="T1-Screener")
    t2 = threading.Thread(target=run_thread2, daemon=True, name="T2-Monitor")
    t1.start(); t2.start()
    log("2 thread aktif. Ctrl+C untuk berhenti.")
    try:
        while True: time.sleep(60)
    except KeyboardInterrupt:
        log("Dihentikan.")
        sys.exit(0)
