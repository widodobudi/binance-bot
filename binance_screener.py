"""
=============================================================
  BINANCE SCREENER -> 3COMMAS + TELEGRAM
  STRATEGI: MOMENTUM BREAKOUT brkX2 (12h)  -- forward-test
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
  - batas 5 candle 12h (2.5 hari) -> tutup di harga saat itu
  - user bebas close manual lebih awal

FILTER BTC (Lapis1&2): OFF (toggle). ADD FUND otomatis: OFF.
=============================================================
"""
import requests, pandas as pd, pandas_ta as ta, numpy as np
import time, sys, json, threading, os, csv
from datetime import datetime, timedelta, timezone
import requests as _requests_mod

# ===================== KONFIGURASI =====================
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN",    "")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID",  "")
COMMAS_BOT_ID      = int(os.environ.get("COMMAS_BOT_ID", "0"))
COMMAS_EMAIL_TOKEN = os.environ.get("COMMAS_EMAIL_TOKEN", "")
# Bot 3Commas TERPISAH untuk reversal (split). Disimpan di env var Railway (sama spt brkX2).
# Set di Railway > Variables: COMMAS_BOT_ID_REVERSAL, COMMAS_EMAIL_TOKEN_REVERSAL
COMMAS_BOT_ID_REVERSAL      = int(os.environ.get("COMMAS_BOT_ID_REVERSAL", "0"))
COMMAS_EMAIL_TOKEN_REVERSAL = os.environ.get("COMMAS_EMAIL_TOKEN_REVERSAL", "")
# Bot 3Commas untuk strategi 4h (brkX2-4h, forward-test 7 deal, 5 slot)
COMMAS_BOT_ID_4H      = int(os.environ.get("COMMAS_BOT_ID_4H", "16935970"))
COMMAS_EMAIL_TOKEN_4H = os.environ.get("COMMAS_EMAIL_TOKEN_4H", "f97400b9-e9a4-4058-913e-35eb8372f920")

def commas_creds(strategy: str):
    """Pilih (bot_id, email_token) sesuai strategi. reversal -> bot baru; 4h -> bot brkX2-4h; lainnya -> bot existing (brkX2)."""
    if strategy == 'reversal':
        return COMMAS_BOT_ID_REVERSAL, COMMAS_EMAIL_TOKEN_REVERSAL
    if strategy == 'brkX2_4h':
        return COMMAS_BOT_ID_4H, COMMAS_EMAIL_TOKEN_4H
    return COMMAS_BOT_ID, COMMAS_EMAIL_TOKEN
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
MACD_FILTER_ENABLED = True    # True=wajib MACD histogram > 0 saat entry

VOLUME_MA_PERIOD  = 20
RSI_LENGTH        = 14
RSI_MAX           = 75
STOCH_MAX         = 70      # syarat ke-7: Stoch %K < 70 (hindari entry terlalu overbought). None = matikan.
MIN_VOLUME_USD    = 3_000_000   # dinaikkan dari 1jt ke 3jt (backtest_entry_filter2)

TRAIL_ARM_PCT     = 2.0
# FAKTOR pengali jarak trailing. 1.0 = jarak tabel ATR% apa adanya; 1.10 = 10% lebih longgar.
# Diturunkan dari 1.10 -> 1.0 (Opsi B, 04/07): backtest_faktor.py simpulkan 1.0 menang telak;
# kasus HOLO/USDT & SOL/USDT (04/07, dev 2.2% dari 1.10) rugi tipis -0.27%/-0.30%, dgn 1.0
# (dev 2.0%, stop lebih dekat puncak) kemungkinan impas/rugi jauh lebih kecil.
TRAILING_FAKTOR   = 1.0
MAX_HOLD_DAYS     = 5
# detik per candle sesuai timeframe (utk batas hold yg benar di TF apa pun).
# 1d=86400, 12h=43200, 6h=21600, 4h=14400. Batas hold = MAX_HOLD_DAYS candle.
_TF_SECONDS = {"1d":86400, "12h":43200, "8h":28800, "6h":21600, "4h":14400, "1h":3600}
SECONDS_PER_CANDLE = _TF_SECONDS.get(TIMEFRAME, 86400)

BASE_ORDER_VOLUME       = 6
COMMAS_MAX_ACTIVE_DEALS = 4      # total kedua bot (brkX2 2 + reversal 2). Tiap bot 3Commas di-set max 2.
MAX_DEALS_BRKX2         = 2      # slot brkX2 (bot existing) — set Max active trades=2 di 3Commas
MAX_DEALS_REVERSAL      = 2      # slot reversal (bot 16921019) — set Max active trades=2 di 3Commas

# ---- STRATEGI 3: brkX2-4h (intrabar 4h, menit ke 5-10) ----
# Hasil backtest: MACD+SUPERTREND+ATR_MIN+VOLUME + HTF 3D PRICE_EMA50+MACD+RSI50
# avg=+3.330% WR=58.4% wf6=OK (backtest_4h_htf.py, 15/07/2026)
STRAT4H_ENABLED         = True
STRAT4H_TIMEFRAME       = "4h"
STRAT4H_SECONDS         = 14400   # 4h dalam detik
STRAT4H_MAX_DEALS       = 5       # slot brkX2-4h — set Max active trades=5 di 3Commas
STRAT4H_MAX_HOLD_CANDLES= 15      # timeout 15 candle 4h = 2.5 hari
STRAT4H_SCAN_INTERVAL   = 180     # scan tiap 3 menit (180 detik)
STRAT4H_ENTRY_MIN_PCT   = 5/240   # menit ke-5 dari 240 menit candle 4h = 2.08%
STRAT4H_ENTRY_MAX_PCT   = 10/240  # menit ke-10 dari 240 menit candle 4h = 4.17%
STRAT4H_FWDTEST_TARGET  = 7       # target forward-test: 7 deal
# Entry conditions 4h
STRAT4H_EMA_FAST        = 9
STRAT4H_EMA_SLOW        = 21
STRAT4H_ST_LENGTH       = 10
STRAT4H_ST_MULT         = 3.0
STRAT4H_MACD_FAST       = 12; STRAT4H_MACD_SLOW = 26; STRAT4H_MACD_SIGNAL = 9
STRAT4H_ATR_MIN_PCT     = 2.0
STRAT4H_VOLUME_MULT     = 1.5
STRAT4H_VOLUME_MA       = 20
STRAT4H_MIN_VOL_USD     = 3_000_000
# HTF 3D filter untuk 4h: PRICE_EMA50 + MACD + RSI50
STRAT4H_HTF_TF          = "3d"
STRAT4H_HTF_EMA_SLOW    = 50
STRAT4H_HTF_MACD_FAST   = 12; STRAT4H_HTF_MACD_SLOW = 26; STRAT4H_HTF_MACD_SIGNAL = 9
STRAT4H_HTF_RSI_LEN     = 14
STRAT4H_HTF_LIMIT       = 120
ADD_FUND_AUTO           = False
BTC_FILTER_ENABLED      = False

# ---- HTF 3D FILTER (backtest_combined.py, 15/07/2026) ----
# Entry 12h hanya boleh kalau di TF 3D: harga > EMA50 DAN MACD hist > 0
# Hasil backtest: avg +2.600% vs baseline +0.770% (+1.830%), WR 61.3%, tona turun 52%
HTF_FILTER_ENABLED  = True
HTF_TIMEFRAME       = "3d"
HTF_EMA_SLOW        = 50       # price > EMA50 3D
HTF_MACD_FAST       = 12
HTF_MACD_SLOW       = 26
HTF_MACD_SIGNAL     = 9
HTF_CANDLE_LIMIT    = 120      # candle 3D yang diambil (~1 tahun)

# ---- STRATEGI 2: REVERSAL DOJI + HEIKIN ASHI (8h) ----
REVERSAL_ENABLED      = True
# Reversal pakai bot 3Commas terpisah (split). Kalau env var-nya belum diset, matikan reversal
# supaya tidak salah kirim sinyal reversal ke bot brkX2.
if REVERSAL_ENABLED and (COMMAS_BOT_ID_REVERSAL == 0 or not COMMAS_EMAIL_TOKEN_REVERSAL):
    print("WARN: REVERSAL aktif tapi COMMAS_BOT_ID_REVERSAL/COMMAS_EMAIL_TOKEN_REVERSAL "
          "belum diset di Railway > Variables. REVERSAL DIMATIKAN sampai env var diisi.")
    REVERSAL_ENABLED = False
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
# ---- INTRABAR SCAN (Thread T3) ----
INTRABAR_ENABLED       = True
INTRABAR_ENTRY_PCT     = 0.60
INTRABAR_WINDOW_END    = 0.75
INTRABAR_SCAN_INTERVAL = 300

# T3-EARLY: window intrabar tambahan di awal candle (5-10% elapsed = menit ke 36-72)
# Hasil backtest_intrabar_early (17/07/2026): avg +9.519%, WR 75.7%, tona 12, wf6 OK
INTRABAR_EARLY_ENABLED   = True
INTRABAR_EARLY_ENTRY_PCT = 0.05    # 5% elapsed = menit ke 36
INTRABAR_EARLY_END_PCT   = 0.10    # 10% elapsed = menit ke 72

# T3-EARLY: window intrabar tambahan di awal candle (5-10% elapsed = menit ke 36-72)
# Hasil backtest_intrabar_early (17/07/2026): avg +9.519%, WR 75.7%, tona 12, wf6 OK
# vs T3-baseline 60-75%: avg +3.332%, WR 61.7%
# vs close candle: avg +0.770%, WR 50.4%
INTRABAR_EARLY_ENABLED   = True
INTRABAR_EARLY_ENTRY_PCT = 0.05    # 5% elapsed = menit ke 36
INTRABAR_EARLY_END_PCT   = 0.10    # 10% elapsed = menit ke 72

# ---- PROGRESSIVE TRAILING ----
PROG_TRAIL_ENABLED   = True
PROG_TRAIL_THRESHOLD = 3.0
PROG_TRAIL_STEP      = 1.0
PROG_TRAIL_REDUCE    = 0.4
PROG_TRAIL_MIN       = 0.4

HEARTBEAT_INTERVAL_SEC = 6 * 3600   # notif "tidak ada coin lolos" tiap 6 jam (4x/hari)
FWDTEST_CHECK_TRADES   = 12         # (lama, gabungan) cek awal: deteksi masalah dini
FWDTEST_TARGET_TRADES  = 25         # (lama, gabungan) evaluasi FINAL
# Target per-strategi utk forward-test berhasil (tiap close update #X/N):
FWDTEST_TARGET_BRKX2    = 15        # target close deal brkX2 utk forward-test berhasil
FWDTEST_TARGET_REVERSAL = 8         # target close deal reversal utk forward-test berhasil
FWDTEST_TARGET_4H       = 7         # target close deal brkX2-4h utk forward-test berhasil

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

# ===================== RETRY / ERROR HANDLING =====================
# Konstanta retry untuk request ke Binance (klines, ticker, price).
# Tidak dipakai untuk 3Commas webhook (kirim sekali, hasil langsung dipakai).
_RETRY_COUNT   = 3          # maksimal percobaan ulang per request
_RETRY_DELAY   = 2.0        # detik jeda antar retry (× backoff)
_RETRY_BACKOFF = 2.0        # kelipatan jeda: percobaan ke-2 = 4s, ke-3 = 8s
_RATE_LIMIT_SLEEP = 10.0    # detik tunggu extra saat terima HTTP 429 (rate limited)

def _binance_get(endpoint: str, params: dict = None, timeout: int = 15):
    """GET ke Binance data API dgn retry otomatis.
    Menangani: timeout, connection error, HTTP 5xx, HTTP 429 (rate limit).
    Return: response object (caller wajib panggil .json()), ATAU None kalau semua retry gagal."""
    url = f"{BASE}{endpoint}"
    delay = _RETRY_DELAY
    for attempt in range(1, _RETRY_COUNT + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                # Rate limited: tunggu extra sebelum retry
                log(f"WARN [Binance] HTTP 429 rate limit di {endpoint} (attempt {attempt})"
                    f" — tunggu {_RATE_LIMIT_SLEEP}s")
                time.sleep(_RATE_LIMIT_SLEEP)
                continue
            if r.status_code >= 500:
                log(f"WARN [Binance] HTTP {r.status_code} server error di {endpoint} (attempt {attempt})")
                if attempt < _RETRY_COUNT:
                    time.sleep(delay); delay *= _RETRY_BACKOFF
                continue
            return r   # status 200 (atau 4xx non-429: kembalikan ke caller utk ditangani lebih lanjut)
        except (_requests_mod.exceptions.ConnectionError,
                _requests_mod.exceptions.Timeout) as e:
            log(f"WARN [Binance] koneksi gagal di {endpoint} (attempt {attempt}): {type(e).__name__}")
            if attempt < _RETRY_COUNT:
                time.sleep(delay); delay *= _RETRY_BACKOFF
        except Exception as e:
            log(f"WARN [Binance] error tak terduga di {endpoint}: {e}")
            break   # error lain (mis. programming error) — jangan retry
    log(f"WARN [Binance] {endpoint} gagal setelah {_RETRY_COUNT} percobaan — data dilewati.")
    return None
ACTIVE_DEALS_FILE = os.path.join(DATA_DIR, "active_deals.json")
TRADES_CSV = os.path.join(DATA_DIR, "trades_forwardtest.csv")

# ===================== COOLDOWN INTERNAL (cegah DEAL HANTU, brkX2) =====================
# Masalah: kalau bot kirim open long tapi 3Commas TOLAK krn cooldown bot (default 28800s/8jam),
# tanpa fitur sinkronisasi bot TETAP catat deal di active_deals (deal hantu, hrs dibersihkan
# manual via RESET_DEAL_SYMBOL). Contoh nyata: EPIC/USDT 04/07 close 14:53, sinyal 7/7 baru
# 19:04 (~4 jam) DITOLAK 3Commas krn cooldown, bot terlanjur catat.
# SOLUSI: mirror cooldown 3Commas di sisi Python SEBELUM kirim sinyal, supaya bot tidak pernah
# mengirim webhook yg pasti ditolak. Statis 28800s (BUKAN unblokir-bersyarat) -- keputusan
# 04/07 setelah backtest_reentry.py: re-entry cepat (sinyal 7/7 <12jam setelah close pair yg
# sama) TIDAK PERNAH terjadi di 150 symbol x ~333 hari data historis (n=0 di semua ambang
# 2/4/6/8 jam) -- kejadian spt EPIC sangat langka, tidak cukup bukti utk logika unblokir yg
# lebih rumit. Kasus langka ditangani MANUAL (RESET_DEAL_SYMBOL) apa adanya.
COOLDOWN_SECONDS = 28800   # 8 jam, samakan dgn setting "Cooldown between trades" bot 3Commas
LAST_CLOSED_FILE = os.path.join(DATA_DIR, "last_closed.json")
last_closed_ts = {}          # symbol -> epoch detik saat close terakhir
last_closed_lock = threading.Lock()

def load_last_closed():
    """Muat riwayat close terakhir per symbol dari file (persisten lintas restart/deploy)."""
    global last_closed_ts
    if not os.path.exists(LAST_CLOSED_FILE):
        log("   last_closed.json tidak ada, mulai kosong (cooldown internal)."); return
    try:
        with open(LAST_CLOSED_FILE, 'r') as f: data = json.load(f)
        with last_closed_lock:
            last_closed_ts = {k: float(v) for k, v in data.items()}
        log(f"   Loaded last_closed_ts: {len(last_closed_ts)} symbol.")
    except Exception as e:
        log(f"WARN gagal baca last_closed.json: {e}")

def save_last_closed():
    try:
        with last_closed_lock: data = dict(last_closed_ts)
        with open(LAST_CLOSED_FILE, 'w') as f: json.dump(data, f, indent=2)
    except Exception as e:
        log(f"WARN gagal simpan last_closed.json: {e}")

def record_closed(symbol: str):
    """Catat waktu close SEKARANG utk symbol ini (dipanggil tiap deal brkX2 ditutup)."""
    with last_closed_lock:
        last_closed_ts[symbol] = time.time()
    save_last_closed()

def cooldown_remaining(symbol: str) -> float:
    """Sisa detik cooldown utk symbol ini. 0 kalau tidak dalam cooldown (atau belum pernah close)."""
    with last_closed_lock:
        ts = last_closed_ts.get(symbol)
    if ts is None: return 0.0
    sisa = COOLDOWN_SECONDS - (time.time() - ts)
    return max(0.0, sisa)

def is_in_cooldown(symbol: str) -> bool:
    return cooldown_remaining(symbol) > 0
trades_csv_lock = threading.Lock()

# Kolom CSV log forward-test (1 baris per trade; ditulis saat OPEN, dilengkapi saat CLOSE)
CSV_FIELDS = [
    'open_time_wib','symbol','strategy','signal_price','entry_price','slip_pct','atr_pct',
    'trail_dist_pct','base_usd','score',
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
            if not full.get('strategy'): full['strategy'] = 'brkX2'
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

# ══════════════════════════════════════════════════════════════════════════════
# DEAL LOG — dokumentasi lengkap semua event (open/addfund/close) + nilai indikator
# Append-only, akumulatif, tidak pernah dihapus.
# ══════════════════════════════════════════════════════════════════════════════
DEAL_LOG_CSV = os.path.join(DATA_DIR, "deal_log.csv")
DEAL_LOG_LOCK = threading.Lock()

DEAL_LOG_FIELDS = [
    # ── Identitas event ──────────────────────────────────────────────────────
    'timestamp_wib',    # waktu event (open/addfund/close)
    'event_type',       # OPEN / ADD_FUND / CLOSE
    'strategy',         # brkX2 / reversal
    'symbol',           # e.g. ALLO/USDT
    'thread',           # T1 / T1b / T1c / T2 (dari mana event berasal)
    # ── Harga & profit ───────────────────────────────────────────────────────
    'signal_price',     # harga candle close saat sinyal
    'entry_price',      # harga eksekusi pasar
    'slip_pct',         # slippage % (entry vs sinyal)
    'exit_price',       # harga close (hanya CLOSE)
    'profit_pct',       # profit % dari entry (hanya CLOSE)
    'exit_reason',      # trail / timeout / batas N candle (hanya CLOSE)
    'trailing_armed',   # True/False saat CLOSE
    'hold_candles',     # berapa candle hold (hanya CLOSE)
    # ── Sizing ───────────────────────────────────────────────────────────────
    'score',            # skor sinyal 0-5
    'base_usd',         # modal base order ($)
    'add_usd',          # add fund amount ($), 0 kalau tidak ada
    'total_usd',        # base + add
    # ── Indikator 12h saat entry ─────────────────────────────────────────────
    'atr_pct',          # ATR% candle sinyal
    'trail_dist_pct',   # jarak trailing berdasar ATR tier
    'ema_fast',         # EMA20 close
    'ema_slow',         # EMA50 close
    'st_dir',           # Supertrend direction (1=up, -1=down)
    'rsi',              # RSI14
    'macd_hist',        # MACD histogram
    'stoch_k',          # Stochastic %K
    'vol_ratio',        # volume / vol_MA20 (berapa x rata-rata)
    'hh10',             # High tertinggi 10 candle terakhir (level breakout)
    'close_price_12h',  # close candle 12h saat sinyal
    # ── HTF 3D saat entry ────────────────────────────────────────────────────
    'htf_tf',           # timeframe HTF (3d)
    'htf_close',        # harga close 3D candle terakhir
    'htf_ema50',        # EMA50 3D
    'htf_macd_hist',    # MACD hist 3D
    'htf_filter_pass',  # True/False: apakah lolos HTF filter
    # ── Intrabar (hanya T1c) ─────────────────────────────────────────────────
    'intrabar_elapsed_pct',  # % elapsed candle 12h saat entry intrabar
    'intrabar_price_live',   # harga live saat entry intrabar
]

def _deal_log_ensure_header():
    """Buat file + header kalau belum ada. Tidak hapus data lama."""
    if not os.path.exists(DEAL_LOG_CSV):
        os.makedirs(os.path.dirname(DEAL_LOG_CSV) or '.', exist_ok=True)
        with open(DEAL_LOG_CSV, 'w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=DEAL_LOG_FIELDS).writeheader()

def deal_log_write(row: dict):
    """Append 1 baris ke deal_log.csv. Kolom yang tidak diisi → string kosong."""
    try:
        with DEAL_LOG_LOCK:
            _deal_log_ensure_header()
            full = {k: row.get(k, '') for k in DEAL_LOG_FIELDS}
            with open(DEAL_LOG_CSV, 'a', newline='', encoding='utf-8') as f:
                csv.DictWriter(f, fieldnames=DEAL_LOG_FIELDS).writerow(full)
        log(f"   [DEALLOG] {row.get('event_type','?')} dicatat: {row.get('symbol','?')}")
    except Exception as e:
        log(f"   [DEALLOG] gagal tulis: {e}")

def _get_htf_values(symbol: str) -> dict:
    """Ambil nilai HTF 3D untuk dokumentasi. Return dict kosong kalau gagal."""
    try:
        df = get_ohlcv_htf(symbol, interval=HTF_TIMEFRAME, limit=HTF_CANDLE_LIMIT)
        if df is None or len(df) < HTF_MACD_SLOW + HTF_MACD_SIGNAL + 5:
            return {}
        df = compute_indicators_htf(df)
        row = df.iloc[-1]
        ema50 = row.get('htf_ema_slow')
        macdh = row.get('htf_macd_hist')
        close = row.get('close')
        passed = (not pd.isna(ema50) and not pd.isna(macdh) and
                  not pd.isna(close) and close > ema50 and macdh > 0)
        return {
            'htf_tf':          HTF_TIMEFRAME,
            'htf_close':       f"{close:.6g}"    if not pd.isna(close)  else '',
            'htf_ema50':       f"{ema50:.6g}"    if not pd.isna(ema50)  else '',
            'htf_macd_hist':   f"{macdh:.6f}"    if not pd.isna(macdh)  else '',
            'htf_filter_pass': str(passed),
        }
    except Exception:
        return {}

def _row_indicators(df_row, vol_ma=None) -> dict:
    """Ekstrak nilai indikator dari 1 baris DataFrame indikator 12h."""
    def _f(v, fmt='.6g'):
        return format(v, fmt) if (v is not None and not pd.isna(v)) else ''
    vol = df_row.get('vol')
    vol_ratio = (float(vol) / float(vol_ma)) if (vol_ma and vol_ma > 0 and vol is not None) else None
    return {
        'ema_fast':       _f(df_row.get('ema_fast')),
        'ema_slow':       _f(df_row.get('ema_slow')),
        'st_dir':         str(int(df_row.get('st_dir', 0))) if not pd.isna(df_row.get('st_dir', float('nan'))) else '',
        'rsi':            _f(df_row.get('rsi'), '.2f'),
        'macd_hist':      _f(df_row.get('macd_hist'), '.6f'),
        'stoch_k':        _f(df_row.get('stoch_k'), '.2f'),
        'vol_ratio':      f"{vol_ratio:.3f}" if vol_ratio is not None else '',
        'hh10':           _f(df_row.get('hh')),
        'close_price_12h':_f(df_row.get('close')),
        'atr_pct':        _f(df_row.get('atr_pct'), '.2f'),
    }

def csv_progress(strategy: str = None):
    """Baca CSV, hitung trade SELESAI (CLOSED), berapa menang/kalah, total profit%.
    Jika strategy diberikan ('brkX2'/'reversal'), hanya hitung trade strategi itu.
    Baris lama tanpa kolom strategy dianggap 'brkX2' (kompatibilitas).
    Return dict atau None kalau CSV belum ada / error."""
    try:
        if not os.path.exists(TRADES_CSV):
            return None
        with trades_csv_lock:
            with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
        closed = [r for r in rows if r.get('status') == 'CLOSED']
        if strategy is not None:
            closed = [r for r in closed if (r.get('strategy') or 'brkX2') == strategy]
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
def _indicator_better_or_equal(current: dict, prev: dict) -> tuple:
    """
    Bandingkan nilai indikator current vs prev (saat open deal sebelumnya).
    Tiap indikator punya arah 'lebih baik' yang berbeda:
      MACD hist   : lebih besar = lebih baik (momentum makin positif)
      RSI         : lebih kecil = lebih baik (makin jauh dari overbought)
      Stoch %K    : lebih kecil = lebih baik (makin jauh dari overbought)
      Vol ratio   : lebih besar = lebih baik (volume makin kuat)
      ATR%        : lebih besar = lebih baik (volatilitas makin potensial)
      EMA gap     : lebih besar = lebih baik (trend makin kuat)
    Return: (is_better_or_equal: bool, detail: str)
    """
    checks = []
    better = []
    worse  = []

    def safe(v):
        try: return float(v)
        except: return None

    # MACD hist: lebih besar = lebih baik
    cm = safe(current.get('macd_hist')); pm = safe(prev.get('macd_hist'))
    if cm is not None and pm is not None:
        checks.append(cm >= pm)
        (better if cm >= pm else worse).append(f"MACD({cm:.5f}>={pm:.5f})")

    # RSI: lebih kecil = lebih baik (arah overbought berlawanan)
    cr = safe(current.get('rsi')); pr = safe(prev.get('rsi'))
    if cr is not None and pr is not None:
        checks.append(cr <= pr)
        (better if cr <= pr else worse).append(f"RSI({cr:.1f}<={pr:.1f})")

    # Stoch %K: lebih kecil = lebih baik
    cs = safe(current.get('stoch_k')); ps = safe(prev.get('stoch_k'))
    if cs is not None and ps is not None:
        checks.append(cs <= ps)
        (better if cs <= ps else worse).append(f"Stoch({cs:.1f}<={ps:.1f})")

    # Vol ratio: lebih besar = lebih baik
    cvr = safe(current.get('vol_ratio')); pvr = safe(prev.get('vol_ratio'))
    if cvr is not None and pvr is not None:
        checks.append(cvr >= pvr)
        (better if cvr >= pvr else worse).append(f"VolRatio({cvr:.2f}>={pvr:.2f})")

    # ATR%: lebih besar = lebih baik
    ca = safe(current.get('atr_pct')); pa = safe(prev.get('atr_pct'))
    if ca is not None and pa is not None:
        checks.append(ca >= pa)
        (better if ca >= pa else worse).append(f"ATR({ca:.2f}>={pa:.2f})")

    # EMA gap (close/ema_fast - 1)*100: lebih besar = trend makin kuat
    cef = safe(current.get('ema_fast')); pef = safe(prev.get('ema_fast'))
    ccp = safe(current.get('close_price_12h')); pcp = safe(prev.get('close_price_12h'))
    if all(v is not None and v > 0 for v in [cef, pef, ccp, pcp]):
        cgap = (ccp/cef - 1)*100; pgap = (pcp/pef - 1)*100
        checks.append(cgap >= pgap)
        (better if cgap >= pgap else worse).append(f"EMAGap({cgap:.2f}>={pgap:.2f})")

    if not checks:
        return True, "tidak ada data indikator sebelumnya → diizinkan"

    # Semua indikator harus sama atau lebih baik
    all_ok = all(checks)
    detail = "OK: " + " | ".join(better) if all_ok else (
        "LEBIH BAIK: " + " | ".join(better) + " | LEBIH JELEK: " + " | ".join(worse)
    )
    return all_ok, detail

# Simpan indikator terakhir per symbol (saat open deal) untuk perbandingan re-entry
last_open_indicators = {}   # sym -> dict indikator saat open deal terakhir
last_open_ind_lock   = threading.Lock()

def save_open_indicators(sym: str, ind: dict):
    with last_open_ind_lock:
        last_open_indicators[sym] = ind

def get_open_indicators(sym: str) -> dict:
    with last_open_ind_lock:
        return last_open_indicators.get(sym, {})


last_intrabar_candle_ts       = 0
last_intrabar_early_candle_ts = 0   # anti-double-entry untuk T3-early
last_intrabar_early_candle_ts = 0   # anti-double-entry untuk T3-early

last_rev_candle_ts = 0   # gating candle baru utk reversal (cegah entry dari candle 8h basi)
# heartbeat state: kapan periode "tidak ada lolos" dimulai & kapan terakhir lapor
heartbeat_window_start = None   # datetime WIB awal periode berjalan
heartbeat_last_sent    = 0.0    # epoch detik notif terakhir
# heartbeat reversal (terpisah, label 8h)
heartbeat_rev_window_start = None
heartbeat_rev_last_sent    = 0.0
# heartbeat 4h (terpisah, label 4h)
heartbeat_4h_window_start  = None
heartbeat_4h_last_sent     = 0.0

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
    # RESET_DEALS=1 -> kosongkan paksa (atasi active_deals.json basi tanpa perang timing).
    # Set env var RESET_DEALS=1 di Railway, restart sekali, lalu HAPUS env var-nya.
    if os.environ.get("RESET_DEALS", "").strip() in ("1", "true", "True", "yes"):
        with active_deals_lock:
            active_deals = {}
        try:
            with open(ACTIVE_DEALS_FILE, 'w') as f: json.dump({}, f)
        except Exception as e:
            log(f"WARN gagal tulis file saat RESET_DEALS: {e}")
        log("   RESET_DEALS aktif -> active_deals DIKOSONGKAN paksa. HAPUS env var ini setelah konfirmasi kosong.")
        return
    if not os.path.exists(ACTIVE_DEALS_FILE):
        log("   active_deals.json tidak ada, mulai kosong."); return
    try:
        with open(ACTIVE_DEALS_FILE,'r') as f: data=json.load(f)
        # RESET_DEAL_SYMBOL=ATMUSDT -> hapus HANYA symbol itu saat startup (deal lain aman).
        # Berguna saat 1 deal nyangkut/basi tp deal lain masih aktif. Hapus env var setelah dipakai.
        # Bisa lebih dari satu, pisah koma: RESET_DEAL_SYMBOL=ATMUSDT,XUSDT
        reset_syms = os.environ.get("RESET_DEAL_SYMBOL", "").strip()
        if reset_syms:
            for s in [x.strip().upper() for x in reset_syms.split(",") if x.strip()]:
                if data.pop(s, None) is not None:
                    log(f"   RESET_DEAL_SYMBOL: {s} dihapus dari active_deals saat startup.")
                else:
                    log(f"   RESET_DEAL_SYMBOL: {s} tidak ditemukan (sudah bersih).")
            try:
                with open(ACTIVE_DEALS_FILE,'w') as f: json.dump(data,f,indent=2,default=_convert)
            except Exception as e:
                log(f"WARN gagal tulis file saat RESET_DEAL_SYMBOL: {e}")
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
    """Kirim webhook ke 3Commas dgn retry utk koneksi/timeout (maks 3x).
    HTTP 4xx dari 3Commas (mis. 401 cooldown) TIDAK diretry — itu penolakan logis, bukan error jaringan."""
    pair = payload.get('pair','')
    url  = "https://3commas.io/trade_signal/trading_view"
    delay = _RETRY_DELAY
    for attempt in range(1, _RETRY_COUNT + 1):
        try:
            resp = session.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                try:
                    body = resp.json()
                    if isinstance(body, dict) and ('error' in body or 'errors' in body):
                        log(f"WARN [3C] {label} ditolak: {body.get('error') or body.get('errors')}")
                        return False
                except Exception:
                    pass
                log(f"OK [3C] {label} terkirim: {pair}"); return True
            elif resp.status_code >= 500:
                # Server error 3Commas — retry
                log(f"WARN [3C] {label} HTTP {resp.status_code} (attempt {attempt}): {resp.text[:120]}")
                if attempt < _RETRY_COUNT:
                    time.sleep(delay); delay *= _RETRY_BACKOFF
            else:
                # 4xx (401 cooldown, 400 bad request, dll) — jangan retry, ini penolakan logis
                log(f"WARN [3C] {label} HTTP {resp.status_code}: {resp.text[:120]}"); return False
        except (_requests_mod.exceptions.ConnectionError,
                _requests_mod.exceptions.Timeout) as e:
            log(f"WARN [3C] {label} koneksi gagal (attempt {attempt}): {type(e).__name__}")
            if attempt < _RETRY_COUNT:
                time.sleep(delay); delay *= _RETRY_BACKOFF
        except Exception as e:
            log(f"WARN [3C] {label} error tak terduga: {e}"); return False
    log(f"WARN [3C] {label} gagal setelah {_RETRY_COUNT} percobaan — sinyal tidak terkirim.")
    return False

def send_open_long(symbol: str, strategy: str = 'brkX2') -> bool:
    bid, tok = commas_creds(strategy)
    return send_3commas({"message_type":"bot","bot_id":bid,
        "email_token":tok,"delay_seconds":COMMAS_DELAY_SEC,
        "pair":to_commas_pair(symbol)}, "open_long")

def send_close_long(symbol: str, strategy: str = 'brkX2') -> bool:
    bid, tok = commas_creds(strategy)
    return send_3commas({"action":"close_at_market_price","message_type":"bot",
        "bot_id":bid,"email_token":tok,
        "delay_seconds":COMMAS_DELAY_SEC,"pair":to_commas_pair(symbol)}, "close_long")

def send_add_funds(symbol: str, volume, strategy: str = 'brkX2', delay: int = 15) -> bool:
    """Add fund senilai `volume` USDT (quote) dengan delay detik. Dipakai utk sizing brkX2."""
    bid, tok = commas_creds(strategy)
    return send_3commas({"action":"add_funds_in_quote","message_type":"bot",
        "bot_id":bid,"email_token":tok,
        "delay_seconds":delay,"pair":to_commas_pair(symbol),
        "volume":volume}, "add_funds")

# ===================== SIZING BERBASIS SKOR SINYAL (brkX2) =====================
# Ambang TETAP tiap dimensi (dari backtest signal_strength, tersil-tinggi).
SCORE_THRESHOLDS = {'brk':3.82, 'vol':2.69, 'rsi':66.98, 'ema':11.49, 'atr':6.16}

def signal_score(row) -> int:
    """Skor 0-5 dari 5 dimensi kekuatan sinyal pada candle entry (row = df.iloc[-1])."""
    sc = 0
    try:
        if row['hh']>0 and (row['close']/row['hh']-1)*100 > SCORE_THRESHOLDS['brk']: sc+=1
        if row['vol_ma']>0 and (row['vol']/row['vol_ma']) > SCORE_THRESHOLDS['vol']: sc+=1
        if not pd.isna(row['rsi']) and row['rsi'] > SCORE_THRESHOLDS['rsi']: sc+=1
        if row['ema_fast']>0 and (row['close']/row['ema_fast']-1)*100 > SCORE_THRESHOLDS['ema']: sc+=1
        if not pd.isna(row['atr_pct']) and row['atr_pct'] > SCORE_THRESHOLDS['atr']: sc+=1
    except Exception:
        return 0
    return sc

def score_to_target_usd(score: int) -> int:
    """tangga_aggr cap $18, 3 tingkat kelipatan $6 (add fund 3Commas minimum $6).
    Skor 0-1 -> $6 (tanpa add fund), 2-3 -> $12 (add $6), 4-5 -> $18 (add $12).
    Basis: backtest_sizing_v2 (155 trade) — aggr$18 ROI 1.44% vs flat 0.66%, walk-forward kalah flat 1/6.
    Add fund minimum $6 (batas terkecil 3Commas/Binance) -> target melompat $6/$12/$18, bukan gradasi halus.
    (Sebelumnya skema B: <3->$6, 3-4->$9, >=5->$12.)"""
    if score >= 4: return 18
    if score >= 2: return 12
    return 6

def open_deal_with_sizing(symbol: str, score: int, strategy: str = 'brkX2'):
    """Buka deal + (kalau skor>=3) add fund selisih dgn delay 15 detik. Return (ok, target_usd, add_usd)."""
    target = score_to_target_usd(score)
    add_usd = target - BASE_ORDER_VOLUME   # selisih di atas base $6
    ok = send_open_long(symbol, strategy)
    if not ok:
        return False, target, 0
    if add_usd > 0:
        # message ke-2 TERPISAH (array multi-instruksi tdk didukung): add fund delay 15 detik
        send_add_funds(symbol, add_usd, strategy, delay=15)
        # ── DEAL LOG ADD_FUND ─────────────────────────────────────────────
        deal_log_write({
            'timestamp_wib': now_wib().strftime('%Y-%m-%d %H:%M:%S'),
            'event_type':    'ADD_FUND',
            'strategy':      strategy,
            'symbol':        to_display_pair(symbol),
            'thread':        'sizing',
            'score':         score,
            'base_usd':      BASE_ORDER_VOLUME,
            'add_usd':       add_usd,
            'total_usd':     target,
        })
    return True, target, add_usd

def send_start_trailing(symbol: str, strategy: str = 'brkX2') -> bool:
    """Aktifkan trailing 3Commas (action start_trailing)."""
    bid, tok = commas_creds(strategy)
    return send_3commas({"action":"start_trailing","message_type":"bot",
        "bot_id":bid,"email_token":tok,
        "delay_seconds":COMMAS_DELAY_SEC,"pair":to_commas_pair(symbol)}, "start_trailing")

# ===================== DATA =====================
def get_usdt_spot_pairs():
    r = _binance_get("/api/v3/exchangeInfo", timeout=30)
    if r is None: return []
    try:
        info = r.json()
        out=[]
        for s in info.get('symbols',[]):
            if s.get('quoteAsset')!='USDT': continue
            if s.get('status')!='TRADING': continue
            if s.get('baseAsset') in EXCLUDED_BASE_ASSETS: continue
            out.append(s['symbol'])
        return out
    except Exception as e:
        log(f"WARN gagal parse exchangeInfo: {e}"); return []

def get_ticker_24h():
    r = _binance_get("/api/v3/ticker/24hr", timeout=30)
    if r is None: return []
    try:
        return r.json()
    except Exception as e:
        log(f"WARN gagal parse ticker24h: {e}"); return []

def get_ohlcv(symbol: str, interval=TIMEFRAME, limit=120):
    r = _binance_get("/api/v3/klines",
                     params={'symbol':symbol,'interval':interval,'limit':limit}, timeout=15)
    if r is None: return None
    try:
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
    r = _binance_get("/api/v3/ticker/price", params={'symbol':symbol}, timeout=10)
    if r is None: return 0.0
    try:
        return float(r.json()['price'])
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
    _macd_df=ta.macd(close,fast=12,slow=26,signal=9)
    df['macd_hist']=_macd_df[[c for c in _macd_df.columns if 'MACDh' in c][0]]
    df['rsi']=ta.rsi(close,length=RSI_LENGTH)
    if STOCH_MAX is not None:
        stoch=ta.stoch(high,low,close,k=14,d=3,smooth_k=3)
        kcol=[c for c in stoch.columns if 'STOCHk' in c][0]
        df['stoch_k']=stoch[kcol]
    return df

# ===================== FILTER CHOPPY/WHIPPY =====================
# Exclude pair yg wick-nya dominan (body kecil dibanding range) secara konsisten -> rawan
# breakout palsu & sinyal reversal lemah. Ukur rata-rata body/range pada N candle terakhir.
CHOPPY_FILTER_ENABLED   = True
CHOPPY_BODY_RANGE_MIN   = 0.40   # rata-rata |close-open|/(high-low) di bawah ini = choppy -> exclude
CHOPPY_LOOKBACK_CANDLES = 10     # jumlah candle tertutup yg dinilai

def is_choppy(df) -> bool:
    """True kalau pair choppy (rata-rata body/range < CHOPPY_BODY_RANGE_MIN selama N candle TERTUTUP).
    Pakai candle yg sudah tutup saja (buang candle berjalan terakhir bila ada). Aman bila data kurang."""
    if not CHOPPY_FILTER_ENABLED:
        return False
    try:
        # ambil N candle terakhir; df di sini sudah berisi candle tertutup utk evaluasi
        sub = df.tail(CHOPPY_LOOKBACK_CANDLES)
        if len(sub) < CHOPPY_LOOKBACK_CANDLES:
            return False  # data kurang -> jangan exclude (hindari false positive)
        rng = (sub['high'] - sub['low']).abs()
        body = (sub['close'] - sub['open']).abs()
        # hindari bagi nol: candle dgn range 0 dianggap body_ratio 0 (choppy ekstrem/flat)
        ratio = body / rng.replace(0, float('nan'))
        avg_ratio = ratio.mean(skipna=True)
        if avg_ratio != avg_ratio:  # semua NaN (range 0 semua) -> anggap choppy
            return True
        return bool(avg_ratio < CHOPPY_BODY_RANGE_MIN)
    except Exception:
        return False  # error -> jangan exclude

def check_entry(df) -> bool:
    """Evaluasi pada candle TERTUTUP terakhir (mode a)."""
    if is_choppy(df): return False
    row = df.iloc[-1]
    if pd.isna(row['ema_fast']) or pd.isna(row['ema_slow']) or pd.isna(row['hh']) or pd.isna(row['vol_ma']):
        return False
    if row['st_dir'] != 1: return False
    if not (row['close'] > row['ema_fast']): return False
    if not (row['ema_fast'] > row['ema_slow']): return False
    if not (row['close'] > row['hh']): return False
    if row['vol'] < VOLUME_MULT * row['vol_ma']: return False
    if pd.isna(row['rsi']) or row['rsi'] > RSI_MAX: return False
    if MACD_FILTER_ENABLED:
        _mh = row.get('macd_hist')
        if _mh is None or pd.isna(_mh) or _mh <= 0: return False
    return True

def entry_detail(df):
    """Untuk heartbeat: kembalikan (n_lolos, total, list_gagal) tanpa mempengaruhi keputusan entry.
    list_gagal = daftar string syarat yg belum terpenuhi + nilai aktualnya. Return None kalau choppy/data kurang."""
    if is_choppy(df): return None
    row = df.iloc[-1]
    if pd.isna(row['ema_fast']) or pd.isna(row['ema_slow']) or pd.isna(row['hh']) or pd.isna(row['vol_ma']):
        return None
    checks = []  # (lolos?, label_gagal)
    checks.append((row['st_dir']==1, "Supertrend (belum up)"))
    checks.append((row['close']>row['ema_fast'], f"close>EMA20 (close {row['close']:.4g} vs EMA20 {row['ema_fast']:.4g})"))
    checks.append((row['ema_fast']>row['ema_slow'], f"EMA20>EMA50 ({row['ema_fast']:.4g} vs {row['ema_slow']:.4g})"))
    checks.append((row['close']>row['hh'], f"breakout10 (close {row['close']:.4g} vs HH {row['hh']:.4g})"))
    vx = (row['vol']/row['vol_ma']) if row['vol_ma'] else 0
    checks.append((row['vol']>=VOLUME_MULT*row['vol_ma'], f"vol>={VOLUME_MULT}xMA (skrg {vx:.2f}x)"))
    rsi_ok = (not pd.isna(row['rsi'])) and row['rsi']<=RSI_MAX
    checks.append((rsi_ok, f"RSI<{RSI_MAX} (skrg {row['rsi']:.1f})" if not pd.isna(row['rsi']) else "RSI (n/a)"))
    if MACD_FILTER_ENABLED:
        _mh2 = row.get('macd_hist')
        _macd_ok = _mh2 is not None and not pd.isna(_mh2) and _mh2 > 0
        _mh2_str = f"{_mh2:.5f}" if (_mh2 is not None and not pd.isna(_mh2)) else "n/a"
        checks.append((_macd_ok, f"MACD hist>0 (skrg {_mh2_str})"))
    if STOCH_MAX is not None:
        sk = row['stoch_k'] if ('stoch_k' in row and not pd.isna(row['stoch_k'])) else None
        stoch_ok = sk is not None and sk < STOCH_MAX
        checks.append((stoch_ok, f"Stoch%K<{STOCH_MAX} (skrg {sk:.1f})" if sk is not None else "Stoch%K (n/a)"))
    n_pass = sum(1 for ok,_ in checks if ok)
    fails = [lab for ok,lab in checks if not ok]
    return (n_pass, len(checks), fails)

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
    """Setup reversal pada candle TERTUTUP terakhir sbg c+2 (titik entry).
    Pola: c-5,c-4,c-3,c-2,c-1 (sebelum doji), c0=doji, c+1 HA bull, c+2=entry.
    Indeks: c-5=df[-8], c-4=df[-7], c-3=df[-6], c-2=df[-5], c-1=df[-4],
            c0=df[-3], c+1=df[-2], c+2=df[-1].
    SYARAT SEBELUM DOJI:
      - 5 candle c-1..c-5 SEMUANYA merah (close<open masing-masing)
      - penurunan total: (close c-1 / open c-5 - 1)*100 <= -5%
    Lalu:
      - close c0 di BAWAH EMA20 & EMA50
      - c0 DOJI (body_ratio < REVERSAL_DOJI_MAX)
      - c+1 HA bullish (1 candle konfirmasi)
      - c+1 ATAU c+2 crossing-up EMA20
    Entry di candle c+2 yg baru tutup (mode a)."""
    if len(df) < 6: return False        # butuh c-3 (df[-6])
    if is_choppy(df): return False
    n = len(df)
    im3, im2, im1 = n-6, n-5, n-4   # c-3..c-1 (3 candle merah)
    i0 = n - 3           # c0
    i1, i2 = n-2, n-1    # c+1, c+2(entry)
    c0 = df.iloc[i0]
    if any(pd.isna(c0[x]) for x in ['ema_fast','ema_slow','body_ratio']): return False
    # syarat 1: 3 candle sebelum doji SEMUA merah
    for idx in (im3, im2, im1):
        cc = df.iloc[idx]
        if not (cc['close'] < cc['open']): return False
    # syarat 2: penurunan total open c-5 -> close c-1 <= -5%
    open_c5 = float(df.iloc[im3]['open'])  # open candle pertama dari 3 merah
    close_c1 = float(df.iloc[im1]['close'])
    if open_c5 <= 0: return False
    drop_pct = (close_c1 / open_c5 - 1) * 100
    if not (drop_pct <= -5.0): return False
    # kondisi awal: c0 di bawah EMA20 & EMA50
    if not (c0['close'] < c0['ema_fast'] and c0['close'] < c0['ema_slow']): return False
    # c0 doji
    if not (c0['body_ratio'] < REVERSAL_DOJI_MAX): return False
    # c+1 HA bullish (1 candle konfirmasi)
    if not bool(df['ha_bull'].iloc[i1]): return False
    # c+1 atau c+2 crossing-up EMA20
    if not (_cross_up(df, i1, 'ema_fast') or _cross_up(df, i2, 'ema_fast')): return False
    return True

def entry_detail_reversal(df):
    """Untuk heartbeat: (n_lolos, 4, list_gagal) tanpa mempengaruhi keputusan. None kalau choppy/data kurang."""
    if len(df) < 6: return None
    if is_choppy(df): return None
    n = len(df)
    im3, im2, im1 = n-6, n-5, n-4
    i0 = n-3; i1, i2 = n-2, n-1
    c0 = df.iloc[i0]
    if any(pd.isna(c0[x]) for x in ['ema_fast','ema_slow','body_ratio']): return None
    checks = []
    # syarat 1: 3 merah + turun >= -5%
    all_red = all(df.iloc[idx]['close'] < df.iloc[idx]['open'] for idx in (im3,im2,im1))
    open_c3 = float(df.iloc[im3]['open']); close_c1 = float(df.iloc[im1]['close'])
    drop = (close_c1/open_c3-1)*100 if open_c3>0 else 0
    n_red = sum(1 for idx in (im3,im2,im1) if df.iloc[idx]['close']<df.iloc[idx]['open'])
    s1 = all_red and drop <= -5.0
    checks.append((s1, f"3 merah+turun>=5% ({n_red}/3 merah, turun {drop:.1f}%)"))
    # syarat 2: c0 doji + di bawah EMA20&50
    s2 = (c0['close']<c0['ema_fast'] and c0['close']<c0['ema_slow']) and (c0['body_ratio']<REVERSAL_DOJI_MAX)
    checks.append((s2, f"doji<{REVERSAL_DOJI_MAX}body & <EMA20/50 (body {c0['body_ratio']:.2f})"))
    # syarat 3: c+1 HA bull
    s3 = bool(df['ha_bull'].iloc[i1])
    checks.append((s3, "c+1 HA bullish (belum)"))
    # syarat 4: cross-up EMA20
    s4 = _cross_up(df, i1, 'ema_fast') or _cross_up(df, i2, 'ema_fast')
    checks.append((s4, "cross-up EMA20 (belum)"))
    n_pass = sum(1 for ok,_ in checks if ok)
    fails = [lab for ok,lab in checks if not ok]
    return (n_pass, 4, fails)

def trailing_dist(atr_pct: float) -> float:
    if atr_pct < 1.0: base = 0.5
    elif atr_pct < 2.0: base = 1.0
    elif atr_pct < 4.0: base = 1.5
    elif atr_pct < 7.0: base = 2.0
    else: base = 1.5   # ATR>=7%: turun dari 2.5% ke 1.5% (backtest_arm_sweep)
    return round(base * TRAILING_FAKTOR, 4)

def get_arm_pct(atr_pct: float) -> float:
    """Arm threshold: ATR>=7% pakai 3.5%, lainnya 2.0% (backtest_arm_sweep optimal)."""
    if atr_pct >= 7.0:
        return 3.5
    return TRAIL_ARM_PCT  # 2.0% untuk tier lain


def trailing_dist_progressive(atr_pct: float, current_profit_pct: float) -> float:
    """Trailing dist progresif: semakin tinggi profit, semakin ketat.
    Optimal: threshold=3%, step=1%, reduce=0.4%, min=0.4%
    """
    base = trailing_dist(atr_pct)
    if not PROG_TRAIL_ENABLED or current_profit_pct < PROG_TRAIL_THRESHOLD:
        return base
    steps_above = int((current_profit_pct - PROG_TRAIL_THRESHOLD) / PROG_TRAIL_STEP) + 1
    reduced = base - steps_above * PROG_TRAIL_REDUCE
    return round(max(PROG_TRAIL_MIN, reduced), 4)


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

def get_ohlcv_htf(symbol: str, interval: str = "3d", limit: int = 120):
    """Ambil OHLCV untuk HTF (3D). Fix: parse response object dengan .json() dulu."""
    r = _binance_get("/api/v3/klines",
                     params={"symbol": symbol, "interval": interval, "limit": limit},
                     timeout=20)
    if r is None: return None
    try:
        raw = r.json()
        if not isinstance(raw, list) or len(raw) < 10: return None
        df = pd.DataFrame(raw, columns=[
            "ts","open","high","low","close","vol",
            "ct","qvol","ntrades","tbbv","tbqv","ig"
        ])
        for col in ["open","high","low","close","vol"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["ts"] = df["ts"].astype("int64")
        return df.reset_index(drop=True)
    except Exception as e:
        log(f"  [HTF] parse error {symbol} {interval}: {e}")
        return None

def compute_indicators_htf(df):
    """Hitung EMA50 dan MACD hist untuk HTF dataframe."""
    import pandas_ta as _pta
    df = df.copy()
    df["htf_ema_slow"] = _pta.ema(df["close"], length=HTF_EMA_SLOW)
    _macd = _pta.macd(df["close"], fast=HTF_MACD_FAST,
                      slow=HTF_MACD_SLOW, signal=HTF_MACD_SIGNAL)
    if _macd is not None:
        _hist_col = [c for c in _macd.columns if "MACDh" in c]
        df["htf_macd_hist"] = _macd[_hist_col[0]] if _hist_col else float("nan")
    else:
        df["htf_macd_hist"] = float("nan")
    return df

def htf_filter_ok(symbol: str) -> bool:
    """
    HTF 3D filter: entry 12h hanya boleh kalau di candle 3D terakhir:
      1. close > EMA50 3D  (PRICE_EMA50)
      2. MACD hist 3D > 0  (MACD)
    Kalau gagal ambil data → jangan blokir (fail-open).
    """
    if not HTF_FILTER_ENABLED:
        return True
    try:
        df = get_ohlcv_htf(symbol, interval=HTF_TIMEFRAME, limit=HTF_CANDLE_LIMIT)
        if df is None or len(df) < HTF_MACD_SLOW + HTF_MACD_SIGNAL + 5:
            return True  # data kurang → fail-open
        df = compute_indicators_htf(df)
        row = df.iloc[-1]
        # Kondisi 1: price > EMA50 3D
        ema_slow = row.get("htf_ema_slow")
        if pd.isna(ema_slow) or row["close"] <= ema_slow:
            return False
        # Kondisi 2: MACD hist 3D > 0
        macd_h = row.get("htf_macd_hist")
        if pd.isna(macd_h) or macd_h <= 0:
            return False
        return True
    except Exception as e:
        log(f"  [HTF] error cek {symbol}: {e} → skip filter")
        return True  # error → fail-open

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGI 3: brkX2-4h — OHLCV, INDIKATOR, ENTRY, HTF FILTER
# ══════════════════════════════════════════════════════════════════════════════
def get_ohlcv_4h(symbol: str, limit: int = 300):
    """Ambil OHLCV 4h dari Binance."""
    r = _binance_get("/api/v3/klines",
                     params={"symbol": symbol, "interval": STRAT4H_TIMEFRAME, "limit": limit},
                     timeout=20)
    if r is None: return None
    try:
        raw = r.json()
        if not isinstance(raw, list) or len(raw) < 10: return None
        df = pd.DataFrame(raw, columns=[
            "ts","open","high","low","close","vol",
            "ct","qvol","ntrades","tbbv","tbqv","ig"
        ])
        for col in ["open","high","low","close","vol"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["qvol"] = pd.to_numeric(df["qvol"], errors="coerce")
        df["ts"]   = df["ts"].astype("int64")
        return df.reset_index(drop=True)
    except Exception as e:
        log(f"  [4h] parse error {symbol}: {e}")
        return None

def compute_indicators_4h(df):
    """Hitung indikator entry 4h: Supertrend, MACD, ATR%, Vol MA, Vol24h."""
    import pandas_ta as _pta
    df = df.copy()
    c, h, l = df["close"], df["high"], df["low"]
    st = _pta.supertrend(h, l, c, length=STRAT4H_ST_LENGTH, multiplier=STRAT4H_ST_MULT)
    df["st_dir"]     = st[[col for col in st.columns if "SUPERTd" in col][0]]
    macd = _pta.macd(c, fast=STRAT4H_MACD_FAST, slow=STRAT4H_MACD_SLOW,
                     signal=STRAT4H_MACD_SIGNAL)
    df["macd_hist"]  = macd[[col for col in macd.columns if "MACDh" in col][0]]
    df["atr_pct"]    = _pta.atr(h, l, c, length=14) / c * 100
    df["vol_ma"]     = df["vol"].rolling(STRAT4H_VOLUME_MA).mean()
    df["vol24h_usd"] = df["qvol"] * 6   # 6 candle 4h = 24h
    return df

def htf_filter_4h_ok(symbol: str) -> bool:
    """
    HTF 3D filter untuk strategi 4h:
      PRICE_EMA50: close 3D > EMA50 3D
      MACD       : MACD hist 3D > 0
      RSI50      : RSI 3D > 50
    Fail-open kalau data tidak cukup.
    """
    try:
        import pandas_ta as _pta
        df = get_ohlcv_htf(symbol, interval=STRAT4H_HTF_TF, limit=STRAT4H_HTF_LIMIT)
        if df is None or len(df) < STRAT4H_HTF_MACD_SLOW + STRAT4H_HTF_MACD_SIGNAL + 5:
            return True  # fail-open
        df = df.copy()
        c = df["close"]
        df["ema50"]  = _pta.ema(c, length=STRAT4H_HTF_EMA_SLOW)
        _macd = _pta.macd(c, fast=STRAT4H_HTF_MACD_FAST,
                          slow=STRAT4H_HTF_MACD_SLOW, signal=STRAT4H_HTF_MACD_SIGNAL)
        df["macd_h"] = _macd[[col for col in _macd.columns if "MACDh" in col][0]]
        df["rsi"]    = _pta.rsi(c, length=STRAT4H_HTF_RSI_LEN)
        row   = df.iloc[-1]
        ema50 = row.get("ema50"); macd_h = row.get("macd_h")
        rsi   = row.get("rsi");   close  = row.get("close")
        if any(pd.isna(v) for v in [ema50, macd_h, rsi, close]): return True
        return (close > ema50) and (macd_h > 0) and (rsi > 50)
    except Exception as e:
        log(f"  [HTF4h] error cek {symbol}: {e} → skip filter")
        return True  # fail-open

def check_entry_4h(df) -> bool:
    """
    Entry 4h:
      - Supertrend dir = +1 (uptrend)
      - MACD hist > 0
      - ATR% >= 2.0%
      - Volume >= 1.5x MA20
      - Vol24h >= $3jt
    """
    if len(df) < STRAT4H_MACD_SLOW + STRAT4H_MACD_SIGNAL + 5: return False
    r = df.iloc[-1]
    sd = r.get("st_dir")
    if pd.isna(sd) or sd != 1: return False
    mh = r.get("macd_hist")
    if pd.isna(mh) or mh <= 0: return False
    atr = r.get("atr_pct")
    if pd.isna(atr) or atr < STRAT4H_ATR_MIN_PCT: return False
    vol_ma = r.get("vol_ma")
    if pd.isna(vol_ma) or vol_ma <= 0: return False
    if r["vol"] < STRAT4H_VOLUME_MULT * vol_ma: return False
    v24 = r.get("vol24h_usd")
    if not pd.isna(v24) and v24 < STRAT4H_MIN_VOL_USD: return False
    return True

def active_deal_count_4h() -> int:
    """Jumlah deal aktif strategi brkX2_4h."""
    with active_deals_lock:
        return sum(1 for d in active_deals.values() if d.get("strategy") == "brkX2_4h")

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
            header = ("HEARTBEAT (Momentum brkX2 (12h)) — START\n"
                      f"Mulai memantau: {start_str} WIB\n"
                      "Notif berikutnya tiap 6 jam.")
        else:
            start_str = heartbeat_window_start.strftime('%d/%m %H:%M')
            end_str   = now_dt.strftime('%d/%m %H:%M')
            header = ("HEARTBEAT 6-jam (Momentum brkX2 (12h))\n"
                      f"Periode: {start_str} -> {end_str} WIB")
        # progress forward-test (dari CSV) — per strategi (target sendiri) + gabungan.
        def _fmt_prog(p):
            if p is None: return "0 selesai (CSV belum ada)"
            nn = p['n']
            wl = f"{p['win']}W/{p['loss']}L" if nn > 0 else "-"
            return f"{nn} selesai ({wl}, total {p['total_pct']:+.1f}%)"
        def _fmt_strat(p, tgt):
            if p is None or p['n']==0: return f"#0/{tgt} (belum ada)"
            nn=p['n']; wl=f"{p['win']}W/{p['loss']}L"
            tag=" TERCAPAI!" if nn>=tgt else ""
            return f"#{nn}/{tgt} ({wl}, total {p['total_pct']:+.1f}%){tag}"
        prog_all = csv_progress()
        prog_brk = csv_progress('brkX2')
        prog_rev = csv_progress('reversal')
        prog_4h  = csv_progress('brkX2_4h')
        if prog_all is None:
            prog_line = "Progress forward-test: 0 trade selesai (CSV belum ada)."
        else:
            prog_line = (f"Progress forward-test (gabungan): {_fmt_prog(prog_all)}\n"
                         f"  - brkX2   : {_fmt_strat(prog_brk, FWDTEST_TARGET_BRKX2)}\n"
                         f"  - reversal: {_fmt_strat(prog_rev, FWDTEST_TARGET_REVERSAL)}\n"
                         f"  - 4h      : {_fmt_strat(prog_4h,  STRAT4H_FWDTEST_TARGET)}")
        # Tambah status T3 (intrabar early + baseline)
        t3_status_str = ""
        try:
            with t3_status_lock:
                es = t3_early_last_status
                bs = t3_base_last_status
                en = t3_early_near_miss[:]
                bn = t3_base_near_miss[:]
            t3_status_str = f"\nIntrabar EARLY (5-10%): {es}"
            if en:
                t3_status_str += " | Kandidat: " + ", ".join(to_display_pair(s) for s,_ in en[:2])
            t3_status_str += f"\nIntrabar BASE (60-75%): {bs}"
            if bn:
                t3_status_str += " | Kandidat: " + ", ".join(to_display_pair(s) for s,_ in bn[:2])
        except: pass
        send_telegram(
            f"{header}\n"
            f"{status_line}\n"
            f"Slot deal: {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}\n"
            f"{prog_line}"
            f"{t3_status_str}\n"
            f"Bot HIDUP & terus memantau."
        )
        log(f"[T1] Heartbeat terkirim ({'START' if first_time else start_str+' -> '+end_str}): {status_line}")
        heartbeat_last_sent = now
        heartbeat_window_start = now_dt  # mulai periode baru



def heartbeat_rev_tick(status_line: str):
    """Heartbeat KHUSUS reversal (label 8h), state sendiri, tiap 6 jam.
    Supaya reversal punya tanda hidup sendiri di Telegram (sebelumnya tak pernah muncul)."""
    global heartbeat_rev_window_start, heartbeat_rev_last_sent
    now = time.time()
    now_dt = now_wib()
    if heartbeat_rev_window_start is None:
        heartbeat_rev_window_start = now_dt
    first_time = (heartbeat_rev_last_sent == 0.0)
    if first_time or (now - heartbeat_rev_last_sent >= HEARTBEAT_INTERVAL_SEC):
        if first_time:
            start_str = now_dt.strftime('%d/%m %H:%M')
            header = ("HEARTBEAT (Reversal Doji+HA (8h)) — START\n"
                      f"Mulai memantau: {start_str} WIB\n"
                      "Notif berikutnya tiap 6 jam.")
        else:
            start_str = heartbeat_rev_window_start.strftime('%d/%m %H:%M')
            end_str   = now_dt.strftime('%d/%m %H:%M')
            header = ("HEARTBEAT 6-jam (Reversal Doji+HA (8h))\n"
                      f"Periode: {start_str} -> {end_str} WIB")
        prev = csv_progress('reversal')
        if prev is None or prev['n']==0:
            prog = f"Progress reversal: #0/{FWDTEST_TARGET_REVERSAL} (belum ada)"
        else:
            nn=prev['n']; wl=f"{prev['win']}W/{prev['loss']}L"
            tag=" TERCAPAI!" if nn>=FWDTEST_TARGET_REVERSAL else ""
            prog = f"Progress reversal: #{nn}/{FWDTEST_TARGET_REVERSAL} ({wl}, total {prev['total_pct']:+.1f}%){tag}"
        send_telegram(
            f"{header}\n"
            f"{status_line}\n"
            f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} | total {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}\n"
            f"{prog}\n"
            f"Bot HIDUP & terus memantau."
        )
        log(f"[T1b] Heartbeat reversal terkirim: {status_line}")
        heartbeat_rev_last_sent = now
        heartbeat_rev_window_start = now_dt


def heartbeat_4h_tick(status_line: str, near_miss_4h: list = None):
    """Heartbeat KHUSUS strategi 4h, tiap 6 jam."""
    global heartbeat_4h_window_start, heartbeat_4h_last_sent
    if not STRAT4H_ENABLED: return
    now    = time.time()
    now_dt = now_wib()
    if heartbeat_4h_window_start is None:
        heartbeat_4h_window_start = now_dt
    first_time = (heartbeat_4h_last_sent == 0.0)
    if not (first_time or (now - heartbeat_4h_last_sent >= HEARTBEAT_INTERVAL_SEC)):
        return

    if first_time:
        start_str = now_dt.strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT (brkX2-4h) — START\n"
                  f"Mulai memantau: {start_str} WIB\n"
                  f"Notif berikutnya tiap 6 jam.")
    else:
        start_str = heartbeat_4h_window_start.strftime('%d/%m %H:%M')
        end_str   = now_dt.strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT 6-jam (brkX2-4h)\n"
                  f"Periode: {start_str} -> {end_str} WIB")

    prev = csv_progress('brkX2_4h')
    if prev is None or prev['n'] == 0:
        prog = f"Progress brkX2-4h: #0/{STRAT4H_FWDTEST_TARGET} (belum ada deal)"
    else:
        nn  = prev['n']; wl = f"{prev['win']}W/{prev['loss']}L"
        tag = " TERCAPAI!" if nn >= STRAT4H_FWDTEST_TARGET else ""
        prog = f"Progress brkX2-4h: #{nn}/{STRAT4H_FWDTEST_TARGET} ({wl}, total {prev['total_pct']:+.1f}%){tag}"

    # Kandidat terdekat 4h
    near_str = ""
    if near_miss_4h:
        lines = []
        for sym, fails in near_miss_4h[:3]:
            fail_str = "; ".join(fails) if fails else "semua lolos"
            lines.append(f"• {to_display_pair(sym)}: belum: {fail_str}")
        near_str = "\nKandidat terdekat 4h:\n" + "\n".join(lines)

    send_telegram(
        f"{header}\n"
        f"{status_line}\n"
        f"Slot 4h: {active_deal_count_4h()}/{STRAT4H_MAX_DEALS} | "
        f"total {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS + STRAT4H_MAX_DEALS}\n"
        f"{prog}"
        f"{near_str}\n"
        f"Bot HIDUP & terus memantau."
    )
    log(f"[T1d] Heartbeat 4h terkirim: {status_line}")
    heartbeat_4h_last_sent    = now
    heartbeat_4h_window_start = now_dt

def format_near_miss(near_miss, total, max_show=5):
    """Format daftar kandidat terdekat utk heartbeat: urut n_pass turun, tampilkan max 5, sisanya diringkas.
    Tiap baris: • PAIR: lolos N/total — belum: syarat1, syarat2"""
    if not near_miss:
        return "Kandidat terdekat: tidak ada (semua pair masih jauh dari lolos)."
    near_miss.sort(key=lambda x: x[0], reverse=True)  # n_pass terbanyak dulu
    lines = ["Kandidat terdekat:"]
    for n_pass, sym, fails in near_miss[:max_show]:
        belum = "; ".join(fails) if fails else "-"
        lines.append(f"• {to_display_pair(sym)}: lolos {n_pass}/{total} — belum: {belum}")
    sisa = len(near_miss) - max_show
    if sisa > 0:
        lines.append(f"(+ {sisa} pair lain lolos >={near_miss[max_show-1][0] if max_show<=len(near_miss) else 5}/{total})")
    return "\n".join(lines)

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
    near_miss = []   # (n_pass, sym, fails) untuk heartbeat kandidat terdekat
    newest_ts = 0
    all_dfs   = {}   # sym -> df terakhir (untuk re-entry indicator comparison)
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
        all_dfs[sym] = df   # simpan untuk perbandingan indikator re-entry
        if check_entry(df):
            # HTF 3D filter: cek kondisi trend 3D sebelum entry
            if HTF_FILTER_ENABLED and not htf_filter_ok(sym):
                log(f"  [T1] {sym} lolos 12h tapi DITOLAK HTF 3D filter (price<EMA50 atau MACD<0)")
                det = entry_detail(df)
                if det is not None:
                    n_pass, total, fails = det
                    near_miss.append((n_pass, sym, fails + ["HTF 3D: bearish"]))
            else:
                sc = signal_score(df.iloc[-1])
                candidates.append((sym, float(df['close'].iloc[-1]), float(df['atr_pct'].iloc[-1]), sc))
        else:
            det = entry_detail(df)
            if det is not None:
                n_pass, total, fails = det
                if n_pass >= 5:   # tampilkan hanya yg lolos >=5/7
                    near_miss.append((n_pass, sym, fails))

    if not candidates:
        log(f"[T1] {len(universe)} coin discan, tidak ada yg lolos syarat entry.")
        last_processed_candle_ts = newest_ts
        return f"TIDAK ADA coin lolos 7 syarat entry. ({len(universe)} coin discan)\n" + format_near_miss(near_miss, 7)

    # urutkan kandidat: ATR% terkecil (paling stabil) dulu
    candidates.sort(key=lambda x: x[2])
    log(f"[T1] {len(candidates)} kandidat lolos. Slot terpakai {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}")

    # GATING CANDLE BARU: hanya buka deal kalau candle terbaru BELUM pernah diproses.
    # Cegah entry dari candle lama yg sdh tutup berjam2 lalu (sinyal basi -> slippage besar,
    # mis. HEI entry 5 jam stlh candle tutup, slippage -11%). Buka hanya saat candle baru tutup.
    if newest_ts <= last_processed_candle_ts:
        # Candle sudah diproses — cek apakah ada kandidat dengan indikator
        # sama atau lebih baik dari saat open deal sebelumnya di pair yang sama.
        # Kalau lebih baik → izinkan re-entry meski candle sama.
        # Kalau lebih jelek → tolak seperti biasa.
        lolos_syms = ", ".join(to_display_pair(c[0]) for c in candidates)
        reentry_ok = []
        reentry_skip = []
        for sym, signal_price, atrp, score in candidates:
            prev_ind = get_open_indicators(sym)
            if not prev_ind:
                # Tidak ada data indikator sebelumnya → tolak (safe default)
                reentry_skip.append(sym)
                log(f"[T1] {sym} candle basi, tidak ada data indikator sebelumnya → skip")
                continue
            # Ambil indikator current dari df
            try:
                df_cur = all_dfs.get(sym)
                if df_cur is None:
                    reentry_skip.append(sym); continue
                r = df_cur.iloc[-1]
                vol_ma = float(r['vol_ma']) if not pd.isna(r.get('vol_ma',float('nan'))) else None
                vol_r  = (float(r['vol'])/vol_ma) if vol_ma and vol_ma > 0 else None
                cur_ind = {
                    'macd_hist':       r.get('macd_hist'),
                    'rsi':             r.get('rsi'),
                    'stoch_k':         r.get('stoch_k'),
                    'vol_ratio':       vol_r,
                    'atr_pct':         r.get('atr_pct'),
                    'ema_fast':        r.get('ema_fast'),
                    'close_price_12h': r.get('close'),
                }
                ok, detail = _indicator_better_or_equal(cur_ind, prev_ind)
                log(f"[T1] {sym} candle basi RE-ENTRY check: {'IZIN' if ok else 'TOLAK'} | {detail}")
                if ok:
                    reentry_ok.append((sym, signal_price, atrp, score, detail))
                else:
                    reentry_skip.append(sym)
            except Exception as e:
                log(f"[T1] {sym} error re-entry check: {e} → skip")
                reentry_skip.append(sym)

        if not reentry_ok:
            log(f"[T1] Candle terbaru sudah diproses (ts={newest_ts}), tidak ada kandidat re-entry yang layak.")
            return (f"{len(candidates)} kandidat LOLOS 7/7 tapi candle sudah diproses "
                    f"(tunggu candle 12h baru): {lolos_syms}")

        # Ada kandidat yang indikatornya lebih baik → proses sebagai re-entry
        log(f"[T1] {len(reentry_ok)} kandidat diizinkan re-entry (indikator sama/lebih baik).")
        candidates = [(sym, sp, atr, sc) for sym, sp, atr, sc, _ in reentry_ok]

    opened_any = False
    cooldown_held = []   # (sym, sisa_detik) -- kandidat 7/7 valid tapi masih cooldown internal
    for sym, signal_price, atrp, score in candidates:
        # berhenti kalau slot brkX2 ATAU total sudah penuh
        if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2 or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
            log(f"[T1] Slot brkX2/total penuh, sisa kandidat tidak dibuka.")
            break
        with active_deals_lock:
            if sym in active_deals:
                continue  # sudah punya deal di pair ini
        sisa = cooldown_remaining(sym)
        if sisa > 0:
            log(f"[T1] {sym} LOLOS 7/7 tapi masih cooldown internal (sisa {sisa/3600:.1f} jam) -> skip, tidak kirim sinyal.")
            cooldown_held.append((sym, sisa))
            continue  # jangan kirim webhook yg pasti ditolak 3Commas (cegah deal hantu)
        log(f"[T1] SINYAL: {sym} close_candle={signal_price:.6g} atr%={atrp:.2f} skor={score}")
        ok, target_usd, add_usd = open_deal_with_sizing(sym, score, 'brkX2')
        if ok:
            entry_price = get_price_now(sym)
            if entry_price <= 0:
                entry_price = signal_price
            slip_pct = (entry_price/signal_price - 1) * 100 if signal_price > 0 else 0.0
            add_to_active_deals(sym, {
                'entry_price': entry_price, 'peak': entry_price,
                'signal_price': signal_price, 'atr_pct': atrp,
                'opened_candle_ts': int(newest_ts), 'trailing_armed': False,
                'strategy': 'brkX2', 'score': score, 'target_usd': target_usd
            })
            # Simpan indikator saat open untuk perbandingan re-entry berikutnya
            try:
                df_saved = all_dfs.get(sym)
                if df_saved is not None:
                    r = df_saved.iloc[-1]
                    vol_ma = float(r['vol_ma']) if not pd.isna(r.get('vol_ma', float('nan'))) else None
                    vol_r  = (float(r['vol'])/vol_ma) if vol_ma and vol_ma > 0 else None
                    save_open_indicators(sym, {
                        'macd_hist':       float(r['macd_hist']) if not pd.isna(r.get('macd_hist', float('nan'))) else None,
                        'rsi':             float(r['rsi'])       if not pd.isna(r.get('rsi', float('nan')))       else None,
                        'stoch_k':         float(r['stoch_k'])   if not pd.isna(r.get('stoch_k', float('nan')))   else None,
                        'vol_ratio':       vol_r,
                        'atr_pct':         float(r['atr_pct'])   if not pd.isna(r.get('atr_pct', float('nan')))   else None,
                        'ema_fast':        float(r['ema_fast'])   if not pd.isna(r.get('ema_fast', float('nan')))  else None,
                        'close_price_12h': float(r['close']),
                    })
            except Exception as e:
                log(f"  [T1] WARN gagal simpan indikator {sym}: {e}")
            addfund_txt = f" (+add ${add_usd} delay 15s)" if add_usd>0 else ""
            send_telegram(
                f"OPEN LONG (Momentum brkX2 (12h))\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (candle close): {signal_price:.6g}\n"
                f"Selisih (lonjakan/slippage): {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Skor sinyal: {score}/5 -> modal ${target_usd}{addfund_txt}\n"
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
                'score': score,
                'strategy': 'brkX2',
            })
            # ── DEAL LOG lengkap ──────────────────────────────────────────
            _ind = _row_indicators(df.iloc[-1], vol_ma=float(df['vol_ma'].iloc[-1]) if 'vol_ma' in df.columns else None)
            _htf = _get_htf_values(sym)
            deal_log_write({
                'timestamp_wib':    now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'event_type':       'OPEN',
                'strategy':         'brkX2',
                'symbol':           to_display_pair(sym),
                'thread':           'T1',
                'signal_price':     f"{signal_price:.6g}",
                'entry_price':      f"{entry_price:.6g}",
                'slip_pct':         f"{slip_pct:+.2f}",
                'score':            score,
                'base_usd':         BASE_ORDER_VOLUME,
                'add_usd':          add_usd if add_usd > 0 else 0,
                'total_usd':        target_usd,
                'trail_dist_pct':   f"{trailing_dist(atrp)}",
                **_ind,
                **_htf,
            })
            opened_any = True

    if opened_any:
        # ada trade -> reset window heartbeat
        heartbeat_window_start = now_wib()
        heartbeat_last_sent = time.time()
    last_processed_candle_ts = newest_ts
    cooldown_txt = ""
    if cooldown_held:
        detail = ", ".join(f"{to_display_pair(s)} (sisa {sisa/3600:.1f}j)" for s, sisa in cooldown_held)
        cooldown_txt = f"\n{len(cooldown_held)} kandidat LOLOS 7/7 tapi masih cooldown internal (cegah re-entry/deal hantu): {detail}"
    if opened_any:
        return cooldown_txt.strip() or None
    return f"{len(candidates)} kandidat lolos tapi tak ada yg jadi dibuka.{cooldown_txt}"

# ===================== THREAD 2: MONITOR + CLOSE (trailing) =====================

# ===================== THREAD 1b: SCAN REVERSAL (8h) + OPEN LONG =====================
def thread1b_scan_reversal():
    """Scan strategi reversal (5 merah+turun>=5% + doji + 1 HA bull + cross EMA20) di timeframe 8h.
    Berbagi pool deal & bot 3Commas dgn brkX2, tapi slot terpisah (MAX_DEALS_REVERSAL)."""
    global last_rev_candle_ts
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
    near_miss = []
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
        else:
            det = entry_detail_reversal(df)
            if det is not None:
                n_pass, total, fails = det
                if n_pass >= 2:   # tampilkan hanya yg lolos >=2/4
                    near_miss.append((n_pass, sym, fails))

    if not candidates:
        log(f"[T1b] {len(universe)} coin discan (reversal), tidak ada yg lolos setup.")
        return f"REVERSAL: tidak ada coin lolos setup. ({len(universe)} discan)\n" + format_near_miss(near_miss, 4)

    # urutkan: ATR% terkecil dulu (paling stabil)
    candidates.sort(key=lambda x: x[2])
    log(f"[T1b] {len(candidates)} kandidat reversal lolos. Slot reversal {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL}")

    # GATING CANDLE BARU (reversal): hanya buka kalau candle 8h terbaru BELUM diproses.
    # Cegah entry dari candle basi (sinyal lama -> slippage besar), sama spt brkX2.
    newest_rev = max(c[3] for c in candidates)
    if newest_rev <= last_rev_candle_ts:
        log(f"[T1b] Candle reversal terbaru sudah diproses (ts={newest_rev}), tidak buka dari candle basi.")
        lolos_syms = ", ".join(to_display_pair(c[0]) for c in candidates)
        return (f"REVERSAL: {len(candidates)} kandidat LOLOS tapi candle sudah diproses "
                f"(tunggu candle 8h baru): {lolos_syms}")

    opened_any = False
    for sym, signal_price, atrp, cts in candidates:
        if deal_count_by_strategy('reversal') >= MAX_DEALS_REVERSAL or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
            log(f"[T1b] Slot reversal/total penuh, sisa kandidat reversal tidak dibuka.")
            break
        with active_deals_lock:
            if sym in active_deals: continue
        log(f"[T1b] SINYAL REVERSAL: {sym} close_candle={signal_price:.6g} atr%={atrp:.2f}")
        if send_open_long(sym, 'reversal'):
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
                f"OPEN LONG (Reversal Doji+HA (8h))\n"
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
                'score': 0,
                'strategy': 'reversal',
            })
            opened_any = True
    last_rev_candle_ts = newest_rev
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
        tdist = trailing_dist_progressive(atrp, prof_peak)
        armed = d.get('trailing_armed', False)

        # arm trailing setelah profit >= +2% (pakai puncak)
        if (not armed) and prof_peak >= get_arm_pct(atrp):
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
        elif d.get('strategy','brkX2') == 'brkX2_4h':
            hold_limit_sec = STRAT4H_MAX_HOLD_CANDLES * STRAT4H_SECONDS
            hold_label = f"batas {STRAT4H_MAX_HOLD_CANDLES} candle 4h"
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
            strat = d.get('strategy','brkX2')
            if strat == 'reversal':
                strat_label = "Reversal Doji+HA (8h)"
            elif strat == 'brkX2_4h':
                strat_label = "Momentum brkX2-4h (4h)"
            else:
                strat_label = "Momentum brkX2 (12h)"
            if send_close_long(sym, strat):
                # catat ke CSV DULU supaya trade ini ikut terhitung di progress
                csv_log_close(
                    to_display_pair(sym),
                    now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                    price, prof_from_entry, reason
                )
                # ── DEAL LOG lengkap CLOSE ────────────────────────────────
                _opened_ts = d.get('opened_candle_ts', 0)
                _hold_c = round((time.time() - _opened_ts) / SECONDS_PER_CANDLE) if _opened_ts > 0 else ''
                deal_log_write({
                    'timestamp_wib': now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                    'event_type':    'CLOSE',
                    'strategy':      strat,
                    'symbol':        to_display_pair(sym),
                    'thread':        'T2',
                    'entry_price':   f"{d.get('entry_price', ''):.6g}" if d.get('entry_price') else '',
                    'exit_price':    f"{price:.6g}",
                    'profit_pct':    f"{prof_from_entry:.2f}",
                    'exit_reason':   reason,
                    'trailing_armed':str(armed),
                    'hold_candles':  str(_hold_c),
                    'atr_pct':       f"{d.get('atr_pct', ''):.2f}" if d.get('atr_pct') else '',
                    'score':         d.get('score', ''),
                    'total_usd':     d.get('target_usd', ''),
                })
                remove_from_active_deals(sym)
                if strat == 'brkX2': record_closed(sym)
                # progress forward-test PER STRATEGI
                if strat == 'reversal':
                    tgt = FWDTEST_TARGET_REVERSAL
                elif strat == 'brkX2_4h':
                    tgt = FWDTEST_TARGET_4H
                else:
                    tgt = FWDTEST_TARGET_BRKX2
                pstrat = csv_progress(strat)
                if pstrat and pstrat['n']>0:
                    done_n = pstrat['n']; wl = f"{pstrat['win']}W/{pstrat['loss']}L"
                    status = "TERCAPAI - waktunya evaluasi!" if done_n>=tgt else f"menuju {tgt}"
                    prog_close = (f"\nForward-test {strat_label}: #{done_n}/{tgt} ({status})"
                                  f"\n  {wl}, total {pstrat['total_pct']:+.1f}%")
                else:
                    prog_close = f"\nForward-test {strat_label}: #?/{tgt} (CSV belum terbaca)"
                send_telegram(
                    f"CLOSE LONG ({strat_label})\n"
                    f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                    f"Pair   : {to_display_pair(sym)}\n"
                    f"Alasan : {reason}\n"
                    f"Profit : {prof_from_entry:.2f}% (dari entry)"
                    f"{prog_close}"
                )
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
                status_rev = thread1b_scan_reversal()
                if status_rev:  # None = ada trade (notif OPEN sudah jalan); selain itu detak heartbeat
                    heartbeat_rev_tick(status_rev)
        except Exception as e: log(f"WARN T1b reversal error: {e}")
        # heartbeat 4h — panggil tiap siklus T1, heartbeat_4h_tick sendiri yg cek interval 6 jam
        try:
            if STRAT4H_ENABLED:
                n4h = active_deal_count_4h()
                if n4h >= STRAT4H_MAX_DEALS:
                    status_4h = f"4h: slot penuh ({n4h}/{STRAT4H_MAX_DEALS}) — deal aktif: " + \
                        ", ".join(to_display_pair(s) for s,d in active_deals.items() if d.get("strategy")=="brkX2_4h")
                else:
                    status_4h = f"4h: memantau sinyal. Slot {n4h}/{STRAT4H_MAX_DEALS}"
                heartbeat_4h_tick(status_4h)
        except Exception as e: log(f"WARN T1 heartbeat 4h error: {e}")
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

def thread1c_scan_intrabar():
    """Scan sinyal brkX2 di tengah candle 12h (60-75% elapsed).
    Lapis 1: indikator candle n-1. Lapis 2: konfirmasi real-time 15m.
    Anti-double-entry per candle via last_intrabar_candle_ts.
    """
    global last_intrabar_candle_ts
    if not INTRABAR_ENABLED:
        return None
    now_ms         = int(time.time() * 1000)
    sec12_ms       = SECONDS_PER_CANDLE * 1000
    candle_open_ms = (now_ms // sec12_ms) * sec12_ms
    elapsed_pct    = (now_ms - candle_open_ms) / sec12_ms
    if elapsed_pct < INTRABAR_ENTRY_PCT or elapsed_pct > INTRABAR_WINDOW_END:
        return None
    if candle_open_ms <= last_intrabar_candle_ts:
        return None
    if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2 or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
        return None
    log(f"[T1c] Intrabar scan ({elapsed_pct*100:.1f}% elapsed)...")
    pairs = get_usdt_spot_pairs()
    if not pairs: return None
    ticker = get_ticker_24h()
    volmap = {}
    for t in ticker:
        try: volmap[t['symbol']] = float(t.get('quoteVolume', 0))
        except: pass
    universe = [p for p in pairs if volmap.get(p, 0) >= MIN_VOLUME_USD]
    if BTC_FILTER_ENABLED and not btc_filter_ok():
        return None
    for sym in universe:
        if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2 or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
            break
        with active_deals_lock:
            if sym in active_deals: continue
        if cooldown_remaining(sym) > 0: continue
        # LAPIS 1: candle 12h n-1
        df12 = get_ohlcv(sym, interval=TIMEFRAME, limit=120)
        if df12 is None: continue
        if df12['ct'].iloc[-1] >= now_ms:
            df12 = df12.iloc[:-1]
        if len(df12) < 60: continue
        df12 = compute_indicators(df12)
        r12  = df12.iloc[-1]
        if pd.isna(r12.get('st_dir')) or r12.get('st_dir') != 1: continue
        if pd.isna(r12.get('ema_fast')) or pd.isna(r12.get('ema_slow')): continue
        if r12['ema_fast'] <= r12['ema_slow']: continue
        if pd.isna(r12.get('hh')) or pd.isna(r12.get('br')): continue
        br_avg = df12['br'].iloc[-CHOPPY_LOOK:].mean()
        if pd.isna(br_avg) or br_avg < CHOPPY_MIN: continue
        # LAPIS 2: data 15m candle aktif
        df15 = get_ohlcv(sym, interval='15m', limit=50)
        if df15 is None: continue
        intra = df15[df15['ts'] >= candle_open_ms]
        if len(intra) == 0: continue
        price_now    = float(intra['close'].iloc[-1])
        vol_so_far   = float(intra['vol'].sum())
        vol_ma12     = float(r12.get('vol_ma', 0)) if not pd.isna(r12.get('vol_ma', 0)) else 0
        vol_projected = vol_so_far / elapsed_pct if elapsed_pct > 0 else vol_so_far
        try:
            rsi15   = ta.rsi(intra['close'], length=14)
            stoch15 = ta.stoch(intra['high'], intra['low'], intra['close'], k=14, d=3, smooth_k=3)
            rsi_now   = float(rsi15.iloc[-1]) if rsi15 is not None and len(rsi15) > 0 and not pd.isna(rsi15.iloc[-1]) else 50.0
            sk_cols   = [c for c in stoch15.columns if 'STOCHk' in c]
            stoch_now = float(stoch15[sk_cols[0]].iloc[-1]) if sk_cols and not pd.isna(stoch15[sk_cols[0]].iloc[-1]) else 50.0
        except Exception:
            rsi_now = 50.0; stoch_now = 50.0
        if price_now <= float(r12['hh']): continue
        if price_now <= float(r12['ema_fast']): continue
        if vol_ma12 > 0 and vol_projected < VOLUME_MULT * vol_ma12: continue
        if rsi_now >= RSI_MAX: continue
        if STOCH_MAX is not None and stoch_now >= STOCH_MAX: continue
        # HTF 3D filter
        if HTF_FILTER_ENABLED and not htf_filter_ok(sym):
            log(f"  [T1c] {sym} lolos intrabar tapi DITOLAK HTF 3D filter (price<EMA50 atau MACD<0)")
            continue
        # LOLOS → ENTRY
        atrp         = float(r12['atr_pct']) if not pd.isna(r12.get('atr_pct')) else 3.0
        score        = signal_score(r12)
        signal_price = float(r12['close'])
        log(f"[T1c] SINYAL INTRABAR: {sym} elapsed={elapsed_pct*100:.1f}% price={price_now:.6g} skor={score}")
        ok, target_usd, add_usd = open_deal_with_sizing(sym, score, 'brkX2')
        if ok:
            entry_price = get_price_now(sym)
            if entry_price <= 0: entry_price = price_now
            slip_pct = (entry_price / signal_price - 1) * 100 if signal_price > 0 else 0.0
            add_to_active_deals(sym, {
                'entry_price': entry_price, 'peak': entry_price,
                'signal_price': signal_price, 'atr_pct': atrp,
                'opened_candle_ts': int(candle_open_ms),
                'trailing_armed': False,
                'strategy': 'brkX2', 'score': score, 'target_usd': target_usd,
            })
            addfund_txt = f" (+add ${add_usd} delay 15s)" if add_usd > 0 else ""
            send_telegram(
                f"OPEN LONG INTRABAR (Momentum brkX2 (12h))\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (candle n-1 close): {signal_price:.6g}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"Elapsed candle 12h: {elapsed_pct*100:.1f}% (jam ke-{elapsed_pct*12:.1f})\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Skor sinyal: {score}/5 -> modal ${target_usd}{addfund_txt}\n"
                f"Slot terpakai: {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}"
            )
            csv_log_open({
                'open_time_wib':  now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol':         to_display_pair(sym),
                'signal_price':   f"{signal_price:.6g}",
                'entry_price':    f"{entry_price:.6g}",
                'slip_pct':       f"{slip_pct:+.2f}",
                'atr_pct':        f"{atrp:.2f}",
                'trail_dist_pct': f"{trailing_dist(atrp)}",
                'base_usd':       BASE_ORDER_VOLUME,
                'score':          score,
                'strategy':       'brkX2',
            })
            # ── DEAL LOG lengkap T1c ──────────────────────────────────────
            _ind = _row_indicators(r12, vol_ma=float(r12.get('vol_ma', 0)) if not pd.isna(r12.get('vol_ma', 0)) else None)
            _htf = _get_htf_values(sym)
            deal_log_write({
                'timestamp_wib':        now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'event_type':           'OPEN',
                'strategy':             'brkX2',
                'symbol':               to_display_pair(sym),
                'thread':               'T1c',
                'signal_price':         f"{signal_price:.6g}",
                'entry_price':          f"{entry_price:.6g}",
                'slip_pct':             f"{slip_pct:+.2f}",
                'score':                score,
                'base_usd':             BASE_ORDER_VOLUME,
                'add_usd':              add_usd if add_usd > 0 else 0,
                'total_usd':            target_usd,
                'trail_dist_pct':       f"{trailing_dist(atrp)}",
                'intrabar_elapsed_pct': f"{elapsed_pct*100:.1f}",
                'intrabar_price_live':  f"{price_now:.6g}",
                **_ind,
                **_htf,
            })
            last_intrabar_candle_ts = candle_open_ms
    return None


# Status tracking T3 untuk heartbeat gabungan
t3_early_last_status = "belum ada scan"
t3_base_last_status  = "belum ada scan"
t3_early_near_miss   = []
t3_base_near_miss    = []
t3_status_lock       = threading.Lock()

def thread1c_scan_intrabar_early():
    """
    T3-EARLY: Scan sinyal brkX2 di awal candle 12h (5-10% elapsed = menit ke 36-72).
    Syarat entry IDENTIK dengan T3-baseline dan T1 (close candle).
    Backtest 17/07/2026: avg +9.519%, WR 75.7%, tona 12, wf6 OK (203 symbol).
    Anti-double-entry per candle via last_intrabar_early_candle_ts.
    """
    global last_intrabar_early_candle_ts
    if not INTRABAR_EARLY_ENABLED:
        return None
    now_ms         = int(time.time() * 1000)
    sec12_ms       = SECONDS_PER_CANDLE * 1000
    candle_open_ms = (now_ms // sec12_ms) * sec12_ms
    elapsed_pct    = (now_ms - candle_open_ms) / sec12_ms

    # Hanya entry di window 5-10% elapsed
    if elapsed_pct < INTRABAR_EARLY_ENTRY_PCT or elapsed_pct > INTRABAR_EARLY_END_PCT:
        return None
    # Anti-double-entry: satu entry per candle per window
    if candle_open_ms <= last_intrabar_early_candle_ts:
        return None
    if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2 or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
        return None

    log(f"[T1c-E] Intrabar EARLY scan ({elapsed_pct*100:.1f}% elapsed)...")
    pairs = get_usdt_spot_pairs()
    if not pairs: return None
    ticker = get_ticker_24h()
    volmap = {}
    for t in ticker:
        try: volmap[t['symbol']] = float(t.get('quoteVolume', 0))
        except: pass
    universe = [p for p in pairs if volmap.get(p, 0) >= MIN_VOLUME_USD]

    if BTC_FILTER_ENABLED and not btc_filter_ok():
        return None

    for sym in universe:
        if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2 or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
            break
        with active_deals_lock:
            if sym in active_deals: continue
        if cooldown_remaining(sym) > 0: continue

        # LAPIS 1: indikator dari candle 12h yang sudah tutup (n-1)
        df12 = get_ohlcv(sym, interval=TIMEFRAME, limit=120)
        if df12 is None: continue
        # Buang candle yang sedang berjalan (belum tutup)
        if df12['ct'].iloc[-1] >= now_ms:
            df12 = df12.iloc[:-1]
        if len(df12) < 60: continue
        df12 = compute_indicators(df12)
        r12  = df12.iloc[-1]

        # Cek semua syarat dari candle n-1 (7 syarat + filter)
        if is_choppy(df12): continue
        if pd.isna(r12.get('st_dir')) or r12.get('st_dir') != 1: continue
        if pd.isna(r12.get('ema_fast')) or pd.isna(r12.get('ema_slow')): continue
        if r12['ema_fast'] <= r12['ema_slow']: continue
        if pd.isna(r12.get('hh')): continue
        if MACD_FILTER_ENABLED:
            mh = r12.get('macd_hist')
            if mh is None or pd.isna(mh) or mh <= 0: continue

        # LAPIS 2: konfirmasi harga live dari data 15m
        df15 = get_ohlcv(sym, interval='15m', limit=50)
        if df15 is None: continue
        intra = df15[df15['ts'] >= candle_open_ms]
        if len(intra) == 0: continue
        price_now  = float(intra['close'].iloc[-1])
        vol_so_far = float(intra['vol'].sum())
        vol_ma12   = float(r12.get('vol_ma', 0)) if not pd.isna(r12.get('vol_ma', 0)) else 0
        # Volume diproyeksikan ke akhir candle
        vol_projected = vol_so_far / elapsed_pct if elapsed_pct > 0 else vol_so_far

        # Cek syarat live
        if price_now <= float(r12['hh']): continue         # breakout HH10
        if price_now <= float(r12['ema_fast']): continue   # price > EMA20
        if vol_ma12 > 0 and vol_projected < VOLUME_MULT * vol_ma12: continue  # volume
        try:
            rsi15 = ta.rsi(intra['close'], length=14)
            stoch15 = ta.stoch(intra['high'], intra['low'], intra['close'], k=14, d=3, smooth_k=3)
            rsi_now   = float(rsi15.iloc[-1]) if rsi15 is not None and len(rsi15) > 0 and not pd.isna(rsi15.iloc[-1]) else 50.0
            sk_cols   = [c for c in stoch15.columns if 'STOCHk' in c]
            stoch_now = float(stoch15[sk_cols[0]].iloc[-1]) if sk_cols and not pd.isna(stoch15[sk_cols[0]].iloc[-1]) else 50.0
        except Exception:
            rsi_now = 50.0; stoch_now = 50.0
        if rsi_now >= RSI_MAX: continue
        if STOCH_MAX is not None and stoch_now >= STOCH_MAX: continue

        # HTF 3D filter
        if HTF_FILTER_ENABLED and not htf_filter_ok(sym):
            log(f"  [T1c-E] {sym} lolos early tapi DITOLAK HTF 3D filter")
            continue

        # LOLOS → ENTRY
        atrp         = float(r12['atr_pct']) if not pd.isna(r12.get('atr_pct')) else 3.0
        score        = signal_score(r12)
        signal_price = float(r12['close'])
        log(f"[T1c-E] SINYAL EARLY: {sym} elapsed={elapsed_pct*100:.1f}% price={price_now:.6g} skor={score}")

        ok, target_usd, add_usd = open_deal_with_sizing(sym, score, 'brkX2')
        if ok:
            entry_price = get_price_now(sym)
            if entry_price <= 0: entry_price = price_now
            slip_pct = (entry_price / signal_price - 1) * 100 if signal_price > 0 else 0.0
            add_to_active_deals(sym, {
                'entry_price': entry_price, 'peak': entry_price,
                'signal_price': signal_price, 'atr_pct': atrp,
                'opened_candle_ts': int(candle_open_ms),
                'trailing_armed': False,
                'strategy': 'brkX2', 'score': score, 'target_usd': target_usd,
            })
            addfund_txt = f" (+add ${add_usd} delay 15s)" if add_usd > 0 else ""
            send_telegram(
                f"OPEN LONG INTRABAR EARLY (Momentum brkX2 (12h))\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (candle n-1 close): {signal_price:.6g}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"Elapsed candle 12h: {elapsed_pct*100:.1f}% (jam ke-{elapsed_pct*12:.1f})\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Skor sinyal: {score}/5 -> modal ${target_usd}{addfund_txt}\n"
                f"Slot terpakai: {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}"
            )
            # Deal log
            _ind = _row_indicators(r12, vol_ma=float(r12.get('vol_ma', 0)) if not pd.isna(r12.get('vol_ma', 0)) else None)
            _htf = _get_htf_values(sym)
            deal_log_write({
                'timestamp_wib':        now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'event_type':           'OPEN',
                'strategy':             'brkX2',
                'symbol':               to_display_pair(sym),
                'thread':               'T1c-E',
                'signal_price':         f"{signal_price:.6g}",
                'entry_price':          f"{entry_price:.6g}",
                'slip_pct':             f"{slip_pct:+.2f}",
                'score':                score,
                'base_usd':             BASE_ORDER_VOLUME,
                'add_usd':              add_usd if add_usd > 0 else 0,
                'total_usd':            target_usd,
                'trail_dist_pct':       f"{trailing_dist(atrp)}",
                'intrabar_elapsed_pct': f"{elapsed_pct*100:.1f}",
                'intrabar_price_live':  f"{price_now:.6g}",
                **_ind,
                **_htf,
            })
            last_intrabar_early_candle_ts = candle_open_ms
    with t3_status_lock:
        t3_early_last_status = "scan selesai"
    return None

def run_thread3_intrabar():
    """Thread T3: intrabar scan tiap INTRABAR_SCAN_INTERVAL detik.
    Menjalankan 2 window: T3-early (5-10%) DAN T3-baseline (60-75%).
    """
    while True:
        try:
            thread1c_scan_intrabar()          # T3-baseline: 60-75%
        except Exception as e:
            log(f"WARN T3 intrabar error: {e}")
        try:
            thread1c_scan_intrabar_early()    # T3-early: 5-10%
        except Exception as e:
            log(f"WARN T3-early intrabar error: {e}")
        time.sleep(INTRABAR_SCAN_INTERVAL)

# ══════════════════════════════════════════════════════════════════════════════
# THREAD T1d: SCAN INTRABAR 4h (menit ke 5-10 setelah candle 4h baru open)
# ══════════════════════════════════════════════════════════════════════════════
last_4h_candle_ts = {}  # sym -> ts candle 4h yang sudah dientry, cegah double entry

def thread1d_scan_4h():
    """
    Scan sinyal strategi ke-3 (brkX2-4h) setiap 3 menit.
    Entry saat elapsed candle 4h berada di menit ke 5-10 (2.08%-4.17%).
    Syarat: Supertrend+1 + MACD>0 + ATR>=2% + Vol>=1.5xMA + HTF3D filter.
    """
    global last_4h_candle_ts
    if not STRAT4H_ENABLED:
        return

    now_ms   = int(time.time() * 1000)
    sec4h    = STRAT4H_SECONDS
    # Open candle 4h saat ini
    candle_open_ms = (now_ms // (sec4h * 1000)) * (sec4h * 1000)
    elapsed_pct    = (now_ms - candle_open_ms) / (sec4h * 1000)

    # Hanya entry di window menit ke-5 sampai ke-10
    if not (STRAT4H_ENTRY_MIN_PCT <= elapsed_pct <= STRAT4H_ENTRY_MAX_PCT):
        return

    # Cek slot tersedia
    n4h = active_deal_count_4h()
    total = active_deal_count()
    if n4h >= STRAT4H_MAX_DEALS:
        return
    if total >= COMMAS_MAX_ACTIVE_DEALS + STRAT4H_MAX_DEALS:
        return

    log(f"[T1d] Scan 4h intrabar ({elapsed_pct*100:.1f}% elapsed)...")
    ticker = get_ticker_24h()
    vol_map = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in ticker} if ticker else {}

    candidates  = []
    near_miss_4h = []   # [(sym, [fails])] — kandidat yang hampir lolos
    with active_deals_lock:
        existing = set(active_deals.keys())

    for sym_info in ticker or []:
        sym = sym_info.get("symbol", "")
        if not sym.endswith("USDT"): continue
        if sym in existing: continue
        if sym in last_4h_candle_ts and last_4h_candle_ts[sym] == candle_open_ms:
            continue
        if vol_map.get(sym, 0) < STRAT4H_MIN_VOL_USD:
            continue

        try:
            df = get_ohlcv_4h(sym, limit=100)
            if df is None or len(df) < 50: continue
            df = compute_indicators_4h(df)

            if not check_entry_4h(df):
                # Cek berapa syarat yang lolos untuk near_miss
                r = df.iloc[-1]
                fails = []
                sd = r.get("st_dir")
                if pd.isna(sd) or sd != 1: fails.append("Supertrend belum up")
                mh = r.get("macd_hist")
                if pd.isna(mh) or mh <= 0: fails.append(f"MACD({mh:.4f if mh==mh else 'n/a'}<=0)")
                atr = r.get("atr_pct")
                if pd.isna(atr) or atr < STRAT4H_ATR_MIN_PCT: fails.append(f"ATR({atr:.2f if atr==atr else 'n/a'}<{STRAT4H_ATR_MIN_PCT}%)")
                vol_ma = r.get("vol_ma")
                if pd.isna(vol_ma) or vol_ma <= 0 or r["vol"] < STRAT4H_VOLUME_MULT * vol_ma:
                    fails.append(f"Vol<{STRAT4H_VOLUME_MULT}xMA")
                if len(fails) <= 1:  # hampir lolos (max 1 syarat gagal)
                    near_miss_4h.append((sym, fails))
                continue

            # HTF 3D filter
            if not htf_filter_4h_ok(sym):
                log(f"  [T1d] {sym} lolos 4h tapi DITOLAK HTF 3D filter")
                near_miss_4h.append((sym, ["HTF 3D: bearish"]))
                continue

            r    = df.iloc[-1]
            atrp = float(r["atr_pct"]) if not pd.isna(r["atr_pct"]) else 3.0
            sc   = 1
            candidates.append((sym, float(r["close"]), atrp, sc))
        except Exception as e:
            log(f"  [T1d] error {sym}: {e}")

    # Status line untuk heartbeat
    n4h_active = active_deal_count_4h()
    if not candidates:
        status_4h = (f"4h: tidak ada sinyal. ({len(ticker or [])} discan, "
                     f"slot {n4h_active}/{STRAT4H_MAX_DEALS})")
        heartbeat_4h_tick(status_4h, near_miss_4h)
        log(f"[T1d] Tidak ada kandidat 4h.")
        return

    status_4h = f"4h: {len(candidates)} kandidat lolos. Slot {n4h_active}/{STRAT4H_MAX_DEALS}"
    heartbeat_4h_tick(status_4h, near_miss_4h)

    log(f"[T1d] {len(candidates)} kandidat 4h. Buka deal terbaik...")
    candidates.sort(key=lambda x: x[3], reverse=True)

    opened_any = False
    for sym, signal_price, atrp, score in candidates:
        n4h = active_deal_count_4h()
        if n4h >= STRAT4H_MAX_DEALS: break
        if sym in (set(active_deals.keys())): continue

        ok, target_usd, add_usd = open_deal_with_sizing(sym, score, strategy="brkX2_4h")
        if not ok: continue

        try:
            ticker_now = _binance_get("/api/v3/ticker/price", {"symbol": sym})
            entry_price = float(ticker_now["price"]) if ticker_now else signal_price
        except: entry_price = signal_price

        slip_pct = (entry_price / signal_price - 1) * 100 if signal_price > 0 else 0

        add_to_active_deals(sym, {
            "strategy":      "brkX2_4h",
            "entry_price":   entry_price,
            "signal_price":  signal_price,
            "atr_pct":       atrp,
            "score":         score,
            "target_usd":    target_usd,
            "add_usd":       add_usd,
            "opened_ts":     time.time(),
            "opened_candle_ts": candle_open_ms / 1000,
            "tf":            STRAT4H_TIMEFRAME,
        })
        last_4h_candle_ts[sym] = candle_open_ms

        trail_arm = get_arm_pct(atrp)
        trail_d   = trailing_dist(atrp)
        msg = (
            f"OPEN LONG (brkX2-4h)\n"
            f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
            f"Pair  : {to_display_pair(sym)}\n"
            f"Harga entry (pasar): {entry_price:.4g}\n"
            f"Harga sinyal (4h live): {signal_price:.4g}\n"
            f"Selisih (slippage): {slip_pct:+.2f}%\n"
            f"ATR%  : {atrp:.2f}  (trailing {trail_d}% stlh +{trail_arm}%)\n"
            f"Skor sinyal: {score}/5 -> modal ${target_usd:.0f}"
            + (f" (+add ${add_usd:.0f} delay 15s)" if add_usd > 0 else "") + "\n"
            f"Slot terpakai: {active_deal_count_4h()}/{STRAT4H_MAX_DEALS} (4h)"
        )
        send_telegram(msg)

        # Deal log
        _htf = _get_htf_values(sym)
        deal_log_write({
            "timestamp_wib":  now_wib().strftime("%Y-%m-%d %H:%M:%S"),
            "event_type":     "OPEN",
            "strategy":       "brkX2_4h",
            "symbol":         to_display_pair(sym),
            "thread":         "T1d",
            "signal_price":   f"{signal_price:.6g}",
            "entry_price":    f"{entry_price:.6g}",
            "slip_pct":       f"{slip_pct:+.2f}",
            "score":          score,
            "base_usd":       BASE_ORDER_VOLUME,
            "add_usd":        add_usd,
            "total_usd":      target_usd,
            "atr_pct":        f"{atrp:.2f}",
            "intrabar_elapsed_pct": f"{elapsed_pct*100:.1f}",
            **_htf,
        })

        prog = csv_progress("brkX2_4h")
        send_telegram(
            f"Forward-test brkX2-4h: #{prog['n_closed']}/{STRAT4H_FWDTEST_TARGET} "
            f"({prog['n_win']}W/{prog['n_closed']-prog['n_win']}L, "
            f"total {prog['total_pct']:+.1f}%)"
        )
        opened_any = True
        log(f"  [T1d] OPEN {sym} @ {entry_price:.4g} (4h intrabar)")

def run_thread1d_4h():
    """Thread T1d: scan 4h intrabar tiap STRAT4H_SCAN_INTERVAL detik."""
    while True:
        try:
            if STRAT4H_ENABLED:
                thread1d_scan_4h()
        except Exception as e:
            log(f"WARN T1d 4h error: {e}")
        time.sleep(STRAT4H_SCAN_INTERVAL)

if __name__ == '__main__':
    log("="*55)
    log("  BINANCE SCREENER -> 3COMMAS + TELEGRAM")
    log("  STRATEGI: MOMENTUM BREAKOUT brkX2 (12h)")
    log("="*55)
    log(f"  Timeframe        : {TIMEFRAME}")
    log(f"  Entry syarat     : ST-up, >EMA20, EMA20>EMA50, breakout{BREAKOUT_LOOKBACK}, vol>={VOLUME_MULT}xMA, RSI<{RSI_MAX}" + (f", Stoch<{STOCH_MAX}" if STOCH_MAX is not None else ""))
    log(f"  Exit             : trailing adaptif (arm +{TRAIL_ARM_PCT}%), batas {MAX_HOLD_DAYS} candle 12h (2.5 hari)")
    log(f"  Trailing FAKTOR  : {TRAILING_FAKTOR*100:.0f}% (jarak trailing = tabel ATR% x {TRAILING_FAKTOR})")
    log(f"  Base order       : ${BASE_ORDER_VOLUME} | Max deal total: {COMMAS_MAX_ACTIVE_DEALS}")
    log(f"  Slot per strategi: brkX2={MAX_DEALS_BRKX2}, reversal={MAX_DEALS_REVERSAL}, 4h={STRAT4H_MAX_DEALS}")
    log(f"  Bot 3Commas      : brkX2 #{COMMAS_BOT_ID} | reversal #{COMMAS_BOT_ID_REVERSAL} | 4h #{COMMAS_BOT_ID_4H}")
    log(f"  Filter choppy    : {'ON' if CHOPPY_FILTER_ENABLED else 'OFF'} (body/range < {CHOPPY_BODY_RANGE_MIN} avg {CHOPPY_LOOKBACK_CANDLES} candle -> exclude)")
    log(f"  MACD filter      : {'ON' if MACD_FILTER_ENABLED else 'OFF'} (MACD histogram > 0)")
    log(f"  Arm threshold    : 2.0% (ATR<7%) / 3.5% (ATR>=7%)")
    log(f"  Trail ATR>=7%    : 1.5% (dari 2.5% baseline, backtest_arm_sweep)")
    log(f"  Intrabar scan    : {'ON' if INTRABAR_ENABLED else 'OFF'} (entry {int(INTRABAR_ENTRY_PCT*100)}%-{int(INTRABAR_WINDOW_END*100)}% elapsed, scan tiap {INTRABAR_SCAN_INTERVAL}s)")
    log(f"  Progressive trail: {'ON' if PROG_TRAIL_ENABLED else 'OFF'} (thr={PROG_TRAIL_THRESHOLD}% stp={PROG_TRAIL_STEP}% red={PROG_TRAIL_REDUCE}% min={PROG_TRAIL_MIN}%)")
    log(f"  Cooldown internal: {COOLDOWN_SECONDS}s ({COOLDOWN_SECONDS/3600:.0f}j, brkX2) -- cegah kirim sinyal yg pasti ditolak 3Commas (deal hantu)")
    log(f"  Add fund auto    : {'ON' if ADD_FUND_AUTO else 'OFF (manual)'}")
    log(f"  Filter BTC L1&L2 : {'ON' if BTC_FILTER_ENABLED else 'OFF'}")
    log(f"  Filter HTF 3D    : {'ON' if HTF_FILTER_ENABLED else 'OFF'}"
        + (f" (price>EMA{HTF_EMA_SLOW} AND MACD>0 di {HTF_TIMEFRAME})" if HTF_FILTER_ENABLED else ""))
    log(f"  Min vol 24h      : ${MIN_VOLUME_USD:,}")
    if REVERSAL_ENABLED:
        log("  " + "-"*51)
        log(f"  STRATEGI 2 REVERSAL: ON | TF {REVERSAL_TIMEFRAME}")
        log(f"  Setup: 3 candle merah+turun>=5%, doji(<{int(REVERSAL_DOJI_MAX*100)}% body), 1 HA bull, cross-up EMA20")
        log(f"  Exit : trailing adaptif (sama brkX2) | add fund: {'ON' if REVERSAL_ADD_FUND else 'OFF'}")
        log(f"  Hold : maks {REVERSAL_MAX_HOLD_CANDLES} candle 8h")
    if STRAT4H_ENABLED:
        log("  " + "-"*51)
        log(f"  STRATEGI 3 brkX2-4h: ON | TF {STRAT4H_TIMEFRAME}")
        log(f"  Entry: ST+1 + MACD>0 + ATR>={STRAT4H_ATR_MIN_PCT}% + Vol>={STRAT4H_VOLUME_MULT}xMA + HTF {STRAT4H_HTF_TF} (PRICE_EMA50+MACD+RSI50)")
        log(f"  Intrabar: menit ke 5-10, scan tiap {STRAT4H_SCAN_INTERVAL}s")
        log(f"  Slot: {STRAT4H_MAX_DEALS} | Target forward-test: {STRAT4H_FWDTEST_TARGET} deal")
        log(f"  Bot : #{COMMAS_BOT_ID_4H}")
    log("="*55)

    load_active_deals()
    load_last_closed()
    try:
        import math as _math
        now_ms = int(time.time()*1000)
        tf_sec = {'8h':8*3600,'12h':12*3600,'1d':86400,'4h':4*3600,'6h':6*3600}
        sec12 = tf_sec.get(TIMEFRAME, 12*3600)
        sec8  = tf_sec.get(REVERSAL_TIMEFRAME, 8*3600)
        last_processed_candle_ts = (now_ms // (sec12*1000)) * (sec12*1000)
        last_rev_candle_ts       = (now_ms // (sec8*1000))  * (sec8*1000)
        log(f"   Init gating candle: brkX2 ts={last_processed_candle_ts}, reversal ts={last_rev_candle_ts} (buka deal hanya di candle TF berikutnya).")
    except Exception as e:
        log(f"   WARN init gating candle gagal: {e}")

    send_telegram(
        "Binance Screener AKTIF (Momentum brkX2 (12h))\n"
        f"Entry: ST-up + >EMA20 + EMA20>EMA50 + breakout{BREAKOUT_LOOKBACK} + vol>={VOLUME_MULT}xMA + RSI<{RSI_MAX}" + (f" + Stoch<{STOCH_MAX}" if STOCH_MAX is not None else "") + "\n"
        f"Exit : trailing adaptif (arm +{TRAIL_ARM_PCT}%, jarak per ATR%), batas {MAX_HOLD_DAYS} candle 12h (2.5 hari)\n"
        f"Base ${BASE_ORDER_VOLUME} | Max deal {COMMAS_MAX_ACTIVE_DEALS} | "
        f"AddFund {'ON' if ADD_FUND_AUTO else 'OFF'} | BTC filter {'ON' if BTC_FILTER_ENABLED else 'OFF'}\n"
        f"Evaluasi: candle {TIMEFRAME} TERTUTUP (mode a)"
    )

    n_threads = 3
    t1 = threading.Thread(target=run_thread1, daemon=True, name="T1-Screener")
    t2 = threading.Thread(target=run_thread2, daemon=True, name="T2-Monitor")
    t3 = threading.Thread(target=run_thread3_intrabar, daemon=True, name="T3-Intrabar")
    threads = [t1, t2, t3]
    if STRAT4H_ENABLED:
        t4 = threading.Thread(target=run_thread1d_4h, daemon=True, name="T1d-4h")
        threads.append(t4)
        n_threads = 4
    for t in threads: t.start()
    log(f"{n_threads} thread aktif (T1=screener, T2=monitor, T3=intrabar 12h" + (", T1d=intrabar 4h" if STRAT4H_ENABLED else "") + "). Ctrl+C untuk berhenti.")
    try:
        while True: time.sleep(60)
    except KeyboardInterrupt:
        log("Dihentikan.")
        sys.exit(0)
    log("="*55)
    log("  BINANCE SCREENER -> 3COMMAS + TELEGRAM")
    log("  STRATEGI: MOMENTUM BREAKOUT brkX2 (12h)")
    log("="*55)
    log(f"  Timeframe        : {TIMEFRAME}")
    log(f"  Entry syarat     : ST-up, >EMA20, EMA20>EMA50, breakout{BREAKOUT_LOOKBACK}, vol>={VOLUME_MULT}xMA, RSI<{RSI_MAX}" + (f", Stoch<{STOCH_MAX}" if STOCH_MAX is not None else ""))
    log(f"  Exit             : trailing adaptif (arm +{TRAIL_ARM_PCT}%), batas {MAX_HOLD_DAYS} candle 12h (2.5 hari)")
    log(f"  Trailing FAKTOR  : {TRAILING_FAKTOR*100:.0f}% (jarak trailing = tabel ATR% x {TRAILING_FAKTOR})")
    log(f"  Base order       : ${BASE_ORDER_VOLUME} | Max deal total: {COMMAS_MAX_ACTIVE_DEALS}")
    log(f"  Slot per strategi: brkX2={MAX_DEALS_BRKX2}, reversal={MAX_DEALS_REVERSAL}")
    log(f"  Bot 3Commas      : brkX2 #{COMMAS_BOT_ID} | reversal #{COMMAS_BOT_ID_REVERSAL} (SPLIT)")
    log(f"  Filter choppy    : {'ON' if CHOPPY_FILTER_ENABLED else 'OFF'} (body/range < {CHOPPY_BODY_RANGE_MIN} avg {CHOPPY_LOOKBACK_CANDLES} candle -> exclude)")
    log(f"  MACD filter      : {'ON' if MACD_FILTER_ENABLED else 'OFF'} (MACD histogram > 0)")
    log(f"  Arm threshold    : 2.0% (ATR<7%) / 3.5% (ATR>=7%)")
    log(f"  Trail ATR>=7%    : 1.5% (dari 2.5% baseline, backtest_arm_sweep)")

    log(f"  Intrabar scan    : {'ON' if INTRABAR_ENABLED else 'OFF'} (entry {int(INTRABAR_ENTRY_PCT*100)}%-{int(INTRABAR_WINDOW_END*100)}% elapsed, scan tiap {INTRABAR_SCAN_INTERVAL}s)")
    log(f"  Intrabar EARLY   : {'ON' if INTRABAR_EARLY_ENABLED else 'OFF'} (entry {int(INTRABAR_EARLY_ENTRY_PCT*100)}%-{int(INTRABAR_EARLY_END_PCT*100)}% elapsed = menit ke {int(INTRABAR_EARLY_ENTRY_PCT*720)}-{int(INTRABAR_EARLY_END_PCT*720)})")
    log(f"  Progressive trail: {'ON' if PROG_TRAIL_ENABLED else 'OFF'} (thr={PROG_TRAIL_THRESHOLD}% stp={PROG_TRAIL_STEP}% red={PROG_TRAIL_REDUCE}% min={PROG_TRAIL_MIN}%)")
    log(f"  Cooldown internal: {COOLDOWN_SECONDS}s ({COOLDOWN_SECONDS/3600:.0f}j, brkX2) -- cegah kirim sinyal yg pasti ditolak 3Commas (deal hantu)")
    log(f"  Add fund auto    : {'ON' if ADD_FUND_AUTO else 'OFF (manual)'}")
    log(f"  Filter BTC L1&L2 : {'ON' if BTC_FILTER_ENABLED else 'OFF'}")
    log(f"  Filter HTF 3D    : {'ON' if HTF_FILTER_ENABLED else 'OFF'}"
        + (f" (price>EMA{HTF_EMA_SLOW} AND MACD>0 di {HTF_TIMEFRAME})" if HTF_FILTER_ENABLED else ""))
    log(f"  Min vol 24h      : ${MIN_VOLUME_USD:,}")
    if REVERSAL_ENABLED:
        log("  " + "-"*51)
        log(f"  STRATEGI 2 REVERSAL: ON | TF {REVERSAL_TIMEFRAME}")
        log(f"  Setup: 3 candle merah+turun>=5%, doji(<{int(REVERSAL_DOJI_MAX*100)}% body), 1 HA bull, cross-up EMA20")
        log(f"  Exit : trailing adaptif (sama brkX2) | add fund: {'ON' if REVERSAL_ADD_FUND else 'OFF'}")
        log(f"  Hold : maks {REVERSAL_MAX_HOLD_CANDLES} candle 8h")
    log("="*55)

    load_active_deals()
    load_last_closed()
    # Init gating candle: anggap candle tertutup TERAKHIR saat startup "sudah diproses",
    # supaya restart di tengah TF tidak memicu entry dari candle yg sdh tutup berjam2 lalu
    # (cegah ulang kasus HEI: deal dibuka dari candle basi stlh restart). Hanya buka di candle BERIKUTNYA.
    try:
        import math as _math
        now_ms = int(time.time()*1000)
        tf_sec = {'8h':8*3600,'12h':12*3600,'1d':86400,'4h':4*3600,'6h':6*3600}
        sec12 = tf_sec.get(TIMEFRAME, 12*3600)
        sec8  = tf_sec.get(REVERSAL_TIMEFRAME, 8*3600)
        # candle close terakhir = pembulatan ke bawah ke kelipatan TF
        last_processed_candle_ts = (now_ms // (sec12*1000)) * (sec12*1000)
        last_rev_candle_ts       = (now_ms // (sec8*1000))  * (sec8*1000)
        log(f"   Init gating candle: brkX2 ts={last_processed_candle_ts}, reversal ts={last_rev_candle_ts} (buka deal hanya di candle TF berikutnya).")
    except Exception as e:
        log(f"   WARN init gating candle gagal: {e}")
    # Tes telegram + notif startup
    send_telegram(
        "Binance Screener AKTIF (Momentum brkX2 (12h))\n"
        f"Entry: ST-up + >EMA20 + EMA20>EMA50 + breakout{BREAKOUT_LOOKBACK} + vol>={VOLUME_MULT}xMA + RSI<{RSI_MAX}" + (f" + Stoch<{STOCH_MAX}" if STOCH_MAX is not None else "") + "\n"
        f"Exit : trailing adaptif (arm +{TRAIL_ARM_PCT}%, jarak per ATR%), batas {MAX_HOLD_DAYS} candle 12h (2.5 hari)\n"
        f"Base ${BASE_ORDER_VOLUME} | Max deal {COMMAS_MAX_ACTIVE_DEALS} | "
        f"AddFund {'ON' if ADD_FUND_AUTO else 'OFF'} | BTC filter {'ON' if BTC_FILTER_ENABLED else 'OFF'}\n"
        f"Evaluasi: candle {TIMEFRAME} TERTUTUP (mode a)"
    )

    t1 = threading.Thread(target=run_thread1, daemon=True, name="T1-Screener")
    t2 = threading.Thread(target=run_thread2, daemon=True, name="T2-Monitor")
    t3 = threading.Thread(target=run_thread3_intrabar, daemon=True, name="T3-Intrabar")
    t1.start(); t2.start(); t3.start()
    log("3 thread aktif (T1=screener, T2=monitor, T3=intrabar). Ctrl+C untuk berhenti.")
    try:
        while True: time.sleep(60)
    except KeyboardInterrupt:
        log("Dihentikan.")
        sys.exit(0)
