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
import time, sys, json, threading, os
from datetime import datetime, timedelta, timezone

# ===================== KONFIGURASI =====================
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN",    "8813981840:AAFv-eDq72btyc5MHiW3aZebJi3bLtbeymM")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID",  "192390919")
COMMAS_BOT_ID      = int(os.environ.get("COMMAS_BOT_ID", "16380123"))
COMMAS_EMAIL_TOKEN = os.environ.get("COMMAS_EMAIL_TOKEN", "f97400b9-e9a4-4058-913e-35eb8372f920")
COMMAS_DELAY_SEC   = 0

TIMEFRAME         = "1d"
SUPERTREND_LENGTH = 10
SUPERTREND_MULT   = 3.0
EMA_FAST          = 20
EMA_SLOW          = 50
BREAKOUT_LOOKBACK = 10
VOLUME_MULT       = 2.0
VOLUME_MA_PERIOD  = 20
RSI_LENGTH        = 14
RSI_MAX           = 75
STOCH_MAX         = 70      # syarat ke-7: Stoch %K < 70 (hindari entry terlalu overbought). None = matikan.
MIN_VOLUME_USD    = 2_000_000

TRAIL_ARM_PCT     = 2.0
MAX_HOLD_DAYS     = 5

BASE_ORDER_VOLUME       = 6
COMMAS_MAX_ACTIVE_DEALS = 1
ADD_FUND_AUTO           = False
BTC_FILTER_ENABLED      = False

T1_SCAN_INTERVAL_SEC = 600
T2_MONITOR_INTERVAL  = 15

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

active_deals_lock = threading.Lock()
active_deals      = {}
last_processed_candle_ts = 0

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

def thread1_scan():
    global last_processed_candle_ts
    log("[T1] Scan candle harian...")
    # ambil ticker utk filter volume + daftar pair
    pairs = get_usdt_spot_pairs()
    if not pairs:
        log("[T1] Tidak ada pair, skip."); return
    ticker = get_ticker_24h()
    volmap = {}
    for t in ticker:
        try: volmap[t['symbol']] = float(t.get('quoteVolume',0))
        except: pass
    universe = [p for p in pairs if volmap.get(p,0) >= MIN_VOLUME_USD]

    # slot penuh? jangan cari sinyal
    if active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
        log(f"[T1] Slot penuh ({active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}), tidak cari entry.")
        return

    # filter BTC (kalau diaktifkan)
    if BTC_FILTER_ENABLED and not btc_filter_ok():
        log("[T1] Filter BTC: kondisi tidak lolos, scan dibatalkan.")
        return

    candidates = []
    newest_ts = 0
    for sym in universe:
        with active_deals_lock:
            if sym in active_deals: continue
        df = get_ohlcv(sym, limit=120)
        if df is None: continue
        # mode (a): pastikan candle terakhir SUDAH tutup
        # candle harian tutup saat ct < waktu sekarang (ms)
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
        return

    # pilih 1 (slot max 1) — pilih ATR% terkecil (paling stabil) sebagai tie-break
    candidates.sort(key=lambda x: x[2])
    sym, signal_price, atrp = candidates[0]
    log(f"[T1] SINYAL: {sym} close_candle={signal_price:.6g} atr%={atrp:.2f} (dari {len(candidates)} kandidat)")

    if send_open_long(sym):
        # OPSI (a): entry_price = harga pasar Binance SAAT kirim sinyal,
        # bukan harga close candle. Ini jauh lebih dekat ke harga eksekusi
        # 3Commas yg sebenarnya. Selisih signal_price vs entry_price = indikasi
        # lonjakan harga sejak candle tutup (slippage yg ingin kita ukur).
        entry_price = get_price_now(sym)
        if entry_price <= 0:
            entry_price = signal_price  # fallback kalau gagal ambil harga
        slip_pct = (entry_price/signal_price - 1) * 100 if signal_price > 0 else 0.0
        add_to_active_deals(sym, {
            'entry_price': entry_price, 'peak': entry_price,
            'signal_price': signal_price, 'atr_pct': atrp,
            'opened_candle_ts': int(newest_ts), 'trailing_armed': False
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
            f"Kandidat lain: {len(candidates)-1}"
        )
    last_processed_candle_ts = newest_ts

# ===================== THREAD 2: MONITOR + CLOSE (trailing) =====================
def thread2_monitor():
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

        do_close=False; reason=""
        if armed:
            stop = peak*(1 - tdist/100)
            if price <= stop:
                do_close=True; reason=f"trailing (turun ke {price:.6g} dari puncak {peak:.6g}, dev {tdist}%)"

        # batas waktu 5 hari (5 candle harian = 5*86400 detik sejak candle entry tutup)
        opened_ts = d.get('opened_candle_ts',0)/1000.0
        if opened_ts>0 and (time.time()-opened_ts) >= MAX_HOLD_DAYS*86400:
            do_close=True; reason=f"batas {MAX_HOLD_DAYS} hari tercapai"

        # simpan peak/armed
        with active_deals_lock:
            if sym in active_deals:
                active_deals[sym]['peak']=peak
                active_deals[sym]['trailing_armed']=armed
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
                remove_from_active_deals(sym)

# ===================== RUNNERS =====================
def run_thread1():
    while True:
        try: thread1_scan()
        except Exception as e: log(f"WARN T1 error: {e}")
        time.sleep(T1_SCAN_INTERVAL_SEC)

def run_thread2():
    while True:
        try: thread2_monitor()
        except Exception as e: log(f"WARN T2 error: {e}")
        time.sleep(T2_MONITOR_INTERVAL)

if __name__ == '__main__':
    log("="*55)
    log("  BINANCE SCREENER -> 3COMMAS + TELEGRAM")
    log("  STRATEGI: MOMENTUM BREAKOUT HARIAN (brkX2)")
    log("="*55)
    log(f"  Timeframe        : {TIMEFRAME}")
    log(f"  Entry syarat     : ST-up, >EMA20, EMA20>EMA50, breakout{BREAKOUT_LOOKBACK}, vol>={VOLUME_MULT}xMA, RSI<{RSI_MAX}" + (f", Stoch<{STOCH_MAX}" if STOCH_MAX is not None else ""))
    log(f"  Exit             : trailing adaptif (arm +{TRAIL_ARM_PCT}%), batas {MAX_HOLD_DAYS} hari")
    log(f"  Base order       : ${BASE_ORDER_VOLUME} | Max deal: {COMMAS_MAX_ACTIVE_DEALS}")
    log(f"  Add fund auto    : {'ON' if ADD_FUND_AUTO else 'OFF (manual)'}")
    log(f"  Filter BTC L1&L2 : {'ON' if BTC_FILTER_ENABLED else 'OFF'}")
    log(f"  Min vol 24h      : ${MIN_VOLUME_USD:,}")
    log("="*55)

    load_active_deals()
    # Tes telegram + notif startup
    send_telegram(
        "Binance Screener AKTIF (Momentum Harian brkX2)\n"
        f"Entry: ST-up + >EMA20 + EMA20>EMA50 + breakout{BREAKOUT_LOOKBACK} + vol>={VOLUME_MULT}xMA + RSI<{RSI_MAX}" + (f" + Stoch<{STOCH_MAX}" if STOCH_MAX is not None else "") + "\n"
        f"Exit : trailing adaptif (arm +{TRAIL_ARM_PCT}%, jarak per ATR%), batas {MAX_HOLD_DAYS} hari\n"
        f"Base ${BASE_ORDER_VOLUME} | Max deal {COMMAS_MAX_ACTIVE_DEALS} | "
        f"AddFund {'ON' if ADD_FUND_AUTO else 'OFF'} | BTC filter {'ON' if BTC_FILTER_ENABLED else 'OFF'}\n"
        f"Evaluasi: candle harian TERTUTUP (mode a)"
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
