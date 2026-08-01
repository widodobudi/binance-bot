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
import time, sys, json, threading, os, csv, pickle
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
BREAKOUT_LOOKBACK = 3   # diubah dari 5 → 3 (backtest_hh34567_sweep.py, 29/07/2026): HH3 avg=+4.099% vs HH5 +4.057%, delta +0.042%, wf6 OK; ATR<10% filter menghapus worst -49.23% → -29.49%
ATR_MAX_PCT       = 9.0   # diubah dari 10 → 9 (backtest_no_ema_no_macd_filter_sweep.py, 30/07/2026): ATR<9+close>EMA50 avg=+3.074% worst=-29.10% wf6 OK
VOLUME_MULT       = 0.6   # turun dari 0.8 → backtest_vol_lower_sweep (22/07/2026): delta avg -0.042% (dalam noise), wf6 OK
VOL_MAX_MULT      = 5.0   # batas ATAS vol (backtest_vol_max_sweep.py, 01/08/2026): <=5.0x delta -0.345% tapi dipakai untuk filter visual cosmetic
MACD_FILTER_ENABLED = False   # dimatikan 30/07/2026 — backtest_no_ema_no_macd: tanpa MACD+close>EMA50 avg=+3.074% wf6 OK

VOLUME_MA_PERIOD  = 20
RSI_LENGTH        = 14
RSI_MAX           = 75
STOCH_MAX         = 70      # syarat ke-7: Stoch %K < 70 (hindari entry terlalu overbought). None = matikan.
MIN_VOLUME_USD    = 3_000_000   # dinaikkan dari 1jt ke 3jt (backtest_entry_filter2)
REVERSAL_MIN_VOL_USD = 1_500_000  # min vol24h khusus reversal (lebih rendah untuk perluas universe)

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

BASE_ORDER_VOLUME       = 50   # diubah dari $6 → $50 (22/07/2026, saldo $131.92, konservatif 1 deal)
COMMAS_MAX_ACTIVE_DEALS = 4      # total kedua bot (brkX2 2 + reversal 2). Tiap bot 3Commas di-set max 2.
MAX_DEALS_BRKX2         = 2      # slot brkX2 (bot existing) — set Max active trades=2 di 3Commas
MAX_DEALS_REVERSAL      = 2      # slot reversal (bot 16921019) — set Max active trades=2 di 3Commas

# ---- STRATEGI 3: brkX2-4h (intrabar 4h, menit ke 5-60) ----
# Hasil backtest: MACD+SUPERTREND+ATR_MIN+VOLUME + HTF 3D PRICE_EMA50+MACD+RSI50
# avg=+3.330% WR=58.4% wf6=OK (backtest_4h_htf.py, 15/07/2026)
STRAT4H_ENABLED         = True
STRAT4H_TIMEFRAME       = "4h"
STRAT4H_SECONDS         = 14400   # 4h dalam detik
STRAT4H_MAX_DEALS       = 5       # slot brkX2-4h — set Max active trades=5 di 3Commas
STRAT4H_MAX_HOLD_CANDLES= 15      # timeout 15 candle 4h = 2.5 hari
STRAT4H_SCAN_INTERVAL   = 180     # scan tiap 3 menit (180 detik)
STRAT4H_ENTRY_MIN_PCT   = 5/240   # menit ke-5 dari 240 menit candle 4h = 2.08%
STRAT4H_ENTRY_MAX_PCT   = 60/240  # menit ke-60 dari 240 menit = 25% elapsed (diubah dari 10/240, 31/07/2026)
STRAT4H_FWDTEST_TARGET  = 7       # target forward-test: 7 deal
# Entry conditions 4h
STRAT4H_EMA_FAST        = 9
STRAT4H_EMA_SLOW        = 21
STRAT4H_ST_LENGTH       = 10
STRAT4H_ST_MULT         = 3.0
STRAT4H_MACD_FAST       = 12; STRAT4H_MACD_SLOW = 26; STRAT4H_MACD_SIGNAL = 9
STRAT4H_ATR_MIN_PCT     = 2.0
STRAT4H_VOLUME_MULT     = 0.25  # diubah dari 0.4 → 0.25 (backtest_4h_vol_sweep.py, 27/07/2026): delta avg -0.008% (dalam noise), frekuensi naik
STRAT4H_VOLUME_MA       = 20
STRAT4H_MIN_VOL_USD     = 3_000_000
STRAT4H_STOCH_MAX       = 80    # Stoch%K < 80 (backtest_4h_rsi_stoch_sweep.py, 31/07/2026): worst -48.39% vs -63.96%, delta avg -0.121%, wf6 OK
# HTF filter baru untuk 4h: vol 12h > X * MA20 volume 12h
# brkX2-4h: vol12h>2.0xMA (backtest_htf_vol_sweep_4h.py, 29/07/2026): avg +5.352% vs lama +1.989%, WR 84.6%, wf6 OK
# CrossEMA-4h: vol12h>1.5xMA (backtest_htf_vol_sweep_4h.py, 29/07/2026): avg +2.587% vs lama +0.787%, wf6 neg=1/6 OK
STRAT4H_HTF_TF          = "12h"   # diubah dari 3d → 12h
STRAT4H_HTF_VOL_MULT    = 2.0     # brkX2-4h: vol12h > 2.0x MA20
STRAT4H_HTF_VOL_MA      = 20
STRAT4H_HTF_LIMIT       = 500     # candle 12h (~1 tahun)
# Parameter lama (tidak dipakai lagi)
STRAT4H_HTF_EMA_SLOW    = 50
STRAT4H_HTF_MACD_FAST   = 12; STRAT4H_HTF_MACD_SLOW = 26; STRAT4H_HTF_MACD_SIGNAL = 9
STRAT4H_HTF_RSI_LEN     = 14
ADD_FUND_AUTO           = False
BTC_FILTER_ENABLED      = False

# ── STRATEGI #4: CrossEMA-4h (Cross-up EMA20 saat ST Downtrend) ──────────────
# Basis: backtest_crossema_sweep2.py (25/07/2026)
# Terbaik: B_NO_PERF+W1 → avg=+24.864% WR=85.5% n=62 wf6=0/6
# Window entry: 5–15% elapsed = menit ke 12–36 dari candle 4h (240 menit)
# Perf filter: OFF (counter-trend, perf filter justru merugikan)
# HTF RSI: ON (identik brkX2-4h)
STRAT_CROSSEMA_ENABLED      = True
STRAT_CROSSEMA_ENTRY_MIN    = 5/240    # 5% elapsed = menit ke-12
STRAT_CROSSEMA_ENTRY_MAX    = 60/240   # 25% elapsed = menit ke-60 (diubah dari 15/240, 31/07/2026)
STRAT_CROSSEMA_SCAN_INTERVAL= 240      # scan tiap 4 menit (~14x dalam window 55 menit)
STRAT_CROSSEMA_MAX_DEALS    = 2        # slot — mulai konservatif
STRAT_CROSSEMA_MAX_HOLD     = 15       # timeout 15 candle 4h = 2.5 hari
STRAT_CROSSEMA_FWDTEST      = 7        # target forward-test
STRAT_CROSSEMA_VOLUME_MULT  = 0.4      # identik brkX2-4h
STRAT_CROSSEMA_VOLUME_MA    = 20
STRAT_CROSSEMA_MIN_VOL_USD  = 1_000_000  # diubah dari 3jt → 1jt (backtest_crossema_vol24h_sweep.py, 28/07/2026): WR 57.6% vs 50.1%, frekuensi +61%, avg -0.048% (dalam noise)
STRAT_CROSSEMA_HTF_VOL_MULT = 1.5     # HTF 12h: vol>1.5xMA (backtest_htf_vol_sweep_4h.py, 29/07/2026): avg +2.587% vs lama +0.787%, wf6 neg=1/6 OK
# PERF_ONLY lebih baik dari baseline: avg +2.711% vs +2.538%, worst -21.15% vs -25.79%, wf6 OK
# Filter usia saja lebih buruk; usia+perf wf6 HATI-HATI → deploy PERF_ONLY saja
# Update 25/07/2026: backtest_perf_weight_sweep → EQUAL_thr0.5 terbaik
#   avg +3.052% WR 77.7% n=1316 vs PINE_thr1.0 avg +2.711% WR 75.5% n=955
#   Semua TF bobot sama (1/6), threshold 0.5 = cukup 3 dari 6 TF positif
PERF_FILTER_ENABLED = True
PERF_SCORE_MIN      = 0.5    # EQUAL_thr0.5: cukup 3 dari 6 TF positif
PERF_TF_CONFIG      = [      # (label, hari_ke_belakang, weight) — equal weight
    ("1D",   1,   1/6),
    ("1W",   7,   1/6),
    ("1M",   30,  1/6),
    ("3M",   90,  1/6),
    ("6M",   180, 1/6),
    ("1Y",   365, 1/6),
]
# Entry 12h hanya boleh kalau di TF 3D: harga > EMA50 DAN MACD hist > 0
# Hasil backtest: avg +2.600% vs baseline +0.770% (+1.830%), WR 61.3%, tona turun 52%
HTF_FILTER_ENABLED  = True
HTF_TIMEFRAME       = "3d"
HTF_EMA_SLOW        = 50       # price > EMA50 3D (lama, tidak dipakai lagi)
HTF_MACD_FAST       = 12
HTF_MACD_SLOW       = 26
HTF_MACD_SIGNAL     = 9
HTF_CANDLE_LIMIT    = 120      # candle 3D yang diambil (~1 tahun)
# HTF filter baru brkX2-12h: vol 3D > HTF_VOL_MULT * MA20 volume 3D
# (backtest_htf_vol_sweep_12h.py, 29/07/2026): avg +6.552% vs lama +4.975%, WR 82.8%, wf6 OK
HTF_VOL_MULT        = 0.8  # diubah dari 1.2 → 0.8 (backtest_htf_vol_sweep_12h.py, 30/07/2026): avg +4.628% WR=79.0% n=2261/bln 126 wf6 OK
HTF_VOL_MA_PERIOD   = 20

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

# T3-EARLY: window intrabar tambahan di awal candle (5-59% elapsed = menit ke 36-424)
# diubah dari 5-10% → 5-59% (29/07/2026): window lebih lebar, tidak overlap T3-BASE (60-75%)
# Hasil backtest_intrabar_early (17/07/2026): avg +9.519%, WR 75.7%, tona 12, wf6 OK
# vs T3-baseline 60-75%: avg +3.332%, WR 61.7%
# vs close candle: avg +0.770%, WR 50.4%
INTRABAR_EARLY_ENABLED   = True
INTRABAR_EARLY_ENTRY_PCT = 0.05    # 5% elapsed = menit ke 36
INTRABAR_EARLY_END_PCT   = 0.59    # 59% elapsed = menit ke 424 (tepat sebelum T3-BASE di 60%)
INTRABAR_EARLY_SCAN_INTERVAL = 240  # 4 menit → scan tiap 4 menit dalam window
# Breakout lookback KHUSUS T3-EARLY: disamakan HH3 (ikuti T1 candle close, 31/07/2026)
# History: HH10 baseline → HH7 (backtest_early_hh_sweep.py 20/07) → HH5 (27/07) → HH3 (31/07)
INTRABAR_EARLY_BREAKOUT_LOOKBACK = 3   # diubah dari 5 → 3 (ikuti T1 HH3, 31/07/2026)

# T3-REV: intrabar reversal (full candle 8h = 480 menit)
# Scan syarat reversal dari candle-candle tertutup (c-3..c+1),
# konfirmasi live: price_now > EMA20 (cross-up intrabar c+2)
REVERSAL_INTRABAR_ENABLED       = True
REVERSAL_INTRABAR_SCAN_INTERVAL = 480   # 8 menit → 60x scan per candle 8h

# ---- PROGRESSIVE TRAILING ----
PROG_TRAIL_ENABLED   = True
PROG_TRAIL_THRESHOLD = 3.0
PROG_TRAIL_STEP      = 1.0
PROG_TRAIL_REDUCE    = 0.4
PROG_TRAIL_MIN       = 0.4

HEARTBEAT_INTERVAL_SEC = 2 * 3600   # notif heartbeat tiap 2 jam, terpaku jam ganjil WIB (01,03,05,...,23)
HEARTBEAT_HOURS_WIB    = {1,3,5,7,9,11,13,15,17,19,21,23}  # jam ganjil WIB
FWDTEST_CHECK_TRADES   = 12         # (lama, gabungan) cek awal: deteksi masalah dini
FWDTEST_TARGET_TRADES  = 25         # (lama, gabungan) evaluasi FINAL
# Target per-strategi utk forward-test berhasil (tiap close update #X/N):
FWDTEST_TARGET_BRKX2    = 15        # target close deal brkX2 utk forward-test berhasil
FWDTEST_TARGET_REVERSAL = 8         # target close deal reversal utk forward-test berhasil
FWDTEST_TARGET_4H       = 7         # target close deal brkX2-4h utk forward-test berhasil
# Offset untuk multi-tahap forward-test brkX2:
# Set ke total deal yang sudah selesai di AKHIR tahap sebelumnya.
# Tahap 1: 0, Tahap 2: dimulai dari 0 (reset manual), Tahap 3: dimulai dari 15
FWDTEST_BRKX2_PHASE_OFFSET = 15    # Tahap 2 sudah selesai 15 deal → Tahap 3 mulai dari 0

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

def csv_progress(strategy: str = None, offset: int = 0):
    """Baca CSV, hitung trade SELESAI (CLOSED), berapa menang/kalah, total profit%.
    Jika strategy diberikan ('brkX2'/'reversal'), hanya hitung trade strategi itu.
    Baris lama tanpa kolom strategy dianggap 'brkX2' (kompatibilitas).
    offset: skip N deal pertama (untuk multi-tahap forward-test).
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
        # Skip deal dari tahap sebelumnya
        if offset > 0:
            closed = closed[offset:]
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

def next_scheduled_heartbeat_wib():
    """Kembalikan datetime WIB heartbeat terjadwal berikutnya (jam ganjil: 01,03,05,...,23)."""
    now = now_wib()
    hour = now.hour
    # Jam ganjil berikutnya
    next_hour = hour + 1 if hour % 2 == 0 else hour + 2
    if next_hour >= 24:
        next_hour -= 24
        next_dt = now.replace(hour=next_hour, minute=0, second=0, microsecond=0)
        next_dt = next_dt + timedelta(days=1)
    else:
        next_dt = now.replace(hour=next_hour, minute=0, second=0, microsecond=0)
    return next_dt

def should_send_heartbeat(last_sent: float) -> bool:
    """True kalau sudah melewati jam terjadwal ganjil berikutnya sejak last_sent."""
    if last_sent == 0.0:
        return True  # pertama kali (START)
    now = now_wib()
    # Gunakan time.time() untuk hitung selisih detik — hindari timezone mismatch
    seconds_since = time.time() - last_sent
    hours_since = seconds_since / 3600
    if hours_since < 1.5:  # minimal 1.5 jam sejak terakhir kirim
        return False
    # Cek apakah sekarang sudah melewati jam ganjil
    return (now.hour % 2 == 1 and now.minute == 0) or hours_since >= 2

def log(msg):
    print(f"[{now_wib().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def send_telegram(message: str, parse_mode: str = None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        payload = {
            "chat_id": TELEGRAM_CHAT_ID, "text": message,
            "disable_notification": False
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        resp = session.post(url, json=payload, timeout=10)
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

GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "").replace(" ", "")
GMAIL_TO           = "widodobudi@gmail.com"
GMAIL_FROM         = "widodobudi@gmail.com"

def send_email_open_long(subject: str, body: str):
    """Kirim email notifikasi open long ke widodobudi@gmail.com via Gmail SMTP."""
    if not GMAIL_APP_PASSWORD:
        log("WARN send_email: GMAIL_APP_PASSWORD belum diset, skip.")
        return
    try:
        import smtplib
        from email.mime.text import MIMEText
        msg          = MIMEText(body, 'plain', 'utf-8')
        msg['Subject'] = subject
        msg['From']    = GMAIL_FROM
        msg['To']      = GMAIL_TO
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=15) as smtp:
            smtp.login(GMAIL_FROM, GMAIL_APP_PASSWORD)
            smtp.sendmail(GMAIL_FROM, [GMAIL_TO], msg.as_string())
        log(f"[EMAIL] Terkirim: {subject}")
    except Exception as e:
        log(f"WARN send_email: {e}")

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
    """Sizing berdasarkan skor sinyal, disesuaikan dengan saldo $131.92 (22/07/2026).
    Base order $50 (BASE_ORDER_VOLUME).
    Skor 0-1 -> $50  (tanpa add fund, sisa saldo $81.92)
    Skor 2-3 -> $100 (add $50, sisa saldo $31.92)
    Skor 4-5 -> $120 (add $70, sisa saldo $11.92)
    Basis: backtest_sizing_v2 (155 trade) — aggr lebih tinggi ROI lebih baik."""
    if score >= 4: return 120
    if score >= 2: return 100
    return 50

def open_deal_with_sizing(symbol: str, score: int, strategy: str = 'brkX2'):
    """Buka deal + simpan add_usd di active_deals untuk dikirim T2 setelah deal confirmed.
    Return (ok, target_usd, add_usd)."""
    target  = score_to_target_usd(score)
    add_usd = target - BASE_ORDER_VOLUME
    ok = send_open_long(symbol, strategy)
    if not ok:
        return False, target, 0
    # add_usd disimpan ke active_deals, T2 yang akan kirim setelah deal confirmed aktif
    # (menghindari race condition: add fund ke deal yang cancelled)
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
            # Exclude tokenized stocks: hanya ambil symbol yang punya permission SPOT murni
            # Crypto murni: permissionSets mengandung "SPOT"
            # Tokenized stocks (bStocks): hanya punya TRD_GRP_* tanpa SPOT
            psets = s.get('permissionSets', [])
            has_spot = any('SPOT' in pset for pset in psets)
            if not has_spot: continue
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
    df['hh_early']=high.rolling(INTRABAR_EARLY_BREAKOUT_LOOKBACK).max().shift(1)
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

# ── Performance filter (Grade >= B, score >= 1.0) ────────────────────────────
_perf_cache_1d: dict = {}   # sym -> (ts_arr, close_arr) — di-load lazy per symbol

def _get_perf_data_1d(sym: str):
    """Load data 1D untuk symbol. Cari dari cache lokal dulu, kalau tidak ada fetch dari Binance."""
    if sym in _perf_cache_1d:
        return _perf_cache_1d[sym]
    # Cari cache dir — bisa di /data/_cache_htf_1D (Railway) atau di folder script (lokal)
    candidates = [
        "/data/_cache_htf_1D",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "_cache_htf_1D"),
    ]
    cache_dir = None
    for d in candidates:
        if os.path.isdir(d):
            cache_dir = d; break
    if cache_dir is None:
        # Buat folder cache di /data jika running di Railway
        try:
            cache_dir = "/data/_cache_htf_1D"
            os.makedirs(cache_dir, exist_ok=True)
        except:
            cache_dir = None
    # Cari file cache yang ada
    best = None; best_len = 0
    if cache_dir and os.path.isdir(cache_dir):
        for fname in os.listdir(cache_dir):
            if not fname.startswith(sym) or not fname.endswith(".pkl"): continue
            try:
                df = pickle.load(open(os.path.join(cache_dir, fname), "rb"))
                if df is not None and len(df) > best_len and "ts" in df.columns:
                    best = df; best_len = len(df)
            except: pass
    # Kalau cache kurang dari 700 candle, fetch dari Binance
    if best is None or best_len < 700:
        try:
            r = _binance_get("/api/v3/klines", {"symbol": sym, "interval": "1d", "limit": 1500})
            if r is not None:
                try: r = r.json()
                except: r = None
            if r and isinstance(r, list) and len(r) >= 30:
                df_new = pd.DataFrame(r, columns=["ts","open","high","low","close","vol",
                                                   "ct","qv","nt","tb","tq","ig"])
                df_new["ts"]    = df_new["ts"].astype(np.int64)
                df_new["close"] = df_new["close"].astype(float)
                if cache_dir:
                    try:
                        with open(os.path.join(cache_dir, f"{sym}_1d_1500.pkl"), "wb") as f:
                            pickle.dump(df_new[["ts","close"]], f)
                    except: pass
                best = df_new[["ts","close"]]
        except: pass
    if best is None or len(best) < 30:
        _perf_cache_1d[sym] = None; return None
    ts_arr    = best["ts"].values.astype(np.int64)
    close_arr = best["close"].values.astype(float)
    result    = (ts_arr, close_arr)
    _perf_cache_1d[sym] = result
    return result

def calc_perf_score(sym: str, query_ts_ms: int) -> float:
    """
    Hitung performance score untuk symbol pada timestamp query_ts_ms.
    Return float score, atau nan kalau data tidak ada.
    Score >= 1.0 = Grade B (lolos filter).
    """
    if not PERF_FILTER_ENABLED:
        return 999.0   # filter off → selalu lolos
    data = _get_perf_data_1d(sym)
    if data is None:
        return float('nan')
    ts_arr, close_arr = data
    # Ambil close terdekat sebelum query_ts_ms
    idx_now = int(np.searchsorted(ts_arr, query_ts_ms, side='right')) - 1
    if idx_now < 0: return float('nan')
    current = float(close_arr[idx_now])
    if current <= 0: return float('nan')
    score = 0.0
    for _label, days_back, weight in PERF_TF_CONFIG:
        past_ts  = query_ts_ms - days_back * 86400 * 1000
        idx_past = int(np.searchsorted(ts_arr, past_ts, side='right')) - 1
        if idx_past < 0: continue
        past = float(close_arr[idx_past])
        if past <= 0: continue
        score += weight if current >= past else -weight
    return score

def check_entry(df) -> bool:
    """Evaluasi pada candle TERTUTUP terakhir (mode a).
    Update 30/07/2026: hapus EMA20>EMA50, hapus MACD hist>0,
    ATR<9% (dari <10%), tambah close>EMA50.
    Backtest: backtest_no_ema_no_macd_filter_sweep.py — ATR<9+close>EMA50:
    avg=+3.074% worst=-29.10% wf6 OK.
    """
    if is_choppy(df): return False
    row = df.iloc[-1]
    if pd.isna(row['ema_fast']) or pd.isna(row['ema_slow']) or pd.isna(row['hh']) or pd.isna(row['vol_ma']):
        return False
    if row['st_dir'] != 1: return False
    if not (row['close'] > row['ema_fast']): return False
    # EMA20>EMA50 dihapus (30/07/2026)
    if not (row['close'] > row['hh']): return False
    if row['vol'] < VOLUME_MULT * row['vol_ma']: return False
    if row['vol_ma'] > 0 and (row['vol'] / row['vol_ma']) > VOL_MAX_MULT: return False  # batas atas vol (01/08/2026)
    if pd.isna(row['rsi']) or row['rsi'] > RSI_MAX: return False
    # MACD filter dihapus (30/07/2026)
    # ATR filter: <9% (diubah dari <10%)
    _atr_pct = row.get('atr_pct')
    if _atr_pct is not None and not pd.isna(_atr_pct) and _atr_pct >= ATR_MAX_PCT:
        return False
    # close > EMA50 (syarat baru pengganti EMA20>EMA50)
    if not (row['close'] > row['ema_slow']): return False
    return True

def entry_detail(df):
    """Untuk heartbeat: kembalikan (n_lolos, total, list_gagal) tanpa mempengaruhi keputusan entry.
    list_gagal = daftar string syarat yg belum terpenuhi + nilai aktualnya. Return None kalau choppy/data kurang."""
    if is_choppy(df): return None
    row = df.iloc[-1]
    if pd.isna(row['ema_fast']) or pd.isna(row['ema_slow']) or pd.isna(row['hh']) or pd.isna(row['vol_ma']):
        return None
    checks = []  # (lolos?, label_gagal)
    checks.append((row['st_dir']==1, "Supertrend (masih Downtrend)"))
    checks.append((row['close']>row['ema_fast'], f"close>EMA20 (close {row['close']:.4g} vs EMA20 {row['ema_fast']:.4g})"))
    # EMA20>EMA50 dihapus 30/07/2026
    checks.append((row['close']>row['hh'], f"breakout{BREAKOUT_LOOKBACK} (close {row['close']:.4g} vs HH {row['hh']:.4g})"))
    vx = (row['vol']/row['vol_ma']) if row['vol_ma'] else 0
    vol_ok = row['vol']>=VOLUME_MULT*row['vol_ma'] and (row['vol_ma']<=0 or vx<=VOL_MAX_MULT)
    checks.append((vol_ok, f"vol>={VOLUME_MULT}x dan <={VOL_MAX_MULT}xMA (skrg {vx:.2f}x)"))
    rsi_ok = (not pd.isna(row['rsi'])) and row['rsi']<=RSI_MAX
    checks.append((rsi_ok, f"RSI<{RSI_MAX} (skrg {row['rsi']:.1f})" if not pd.isna(row['rsi']) else "RSI (n/a)"))
    # MACD filter dihapus 30/07/2026
    if STOCH_MAX is not None:
        sk = row['stoch_k'] if ('stoch_k' in row and not pd.isna(row['stoch_k'])) else None
        stoch_ok = sk is not None and sk < STOCH_MAX
        checks.append((stoch_ok, f"Stoch%K<{STOCH_MAX} (skrg {sk:.1f})" if sk is not None else "Stoch%K (n/a)"))
    # ATR filter <9%
    _atr = row.get('atr_pct')
    if _atr is not None and not pd.isna(_atr):
        atr_ok = _atr < ATR_MAX_PCT
        checks.append((atr_ok, f"ATR%<{ATR_MAX_PCT} (skrg {_atr:.1f}%)"))
    # close > EMA50 (syarat baru pengganti EMA20>EMA50)
    close_ema50_ok = row['close'] > row['ema_slow']
    checks.append((close_ema50_ok, f"close>EMA50 (close {row['close']:.4g} vs EMA50 {row['ema_slow']:.4g})"))
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
    """Untuk heartbeat: (n_lolos, 5, list_gagal) tanpa mempengaruhi keputusan. None kalau choppy/data kurang.
    df[-1] = c+1 (candle konfirmasi, sudah tutup)
    df[-2] = c0 (doji)
    df[-3,-4,-5] = c-1, c-2, c-3 (3 candle merah)
    Total 5 syarat: 3merah, doji, HA bull, cross EMA20, Perf (ditambah di luar)
    """
    if len(df) < 6: return None
    if is_choppy(df): return None
    n = len(df)
    im3, im2, im1 = n-5, n-4, n-3   # c-3, c-2, c-1 (3 merah)
    i0 = n - 2                        # c0 (doji)
    i1 = n - 1                        # c+1 (konfirmasi)
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
    # syarat 4: cross-up EMA20 di c+1
    s4 = _cross_up(df, i1, 'ema_fast')
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

def htf_vol_ratio(symbol: str, interval: str, limit: int, vol_ma_period: int) -> float:
    """Return rasio vol_last/vol_ma untuk display near_miss. Return -1 kalau gagal."""
    try:
        df = get_ohlcv_htf(symbol, interval=interval, limit=limit)
        if df is None or len(df) < vol_ma_period + 2: return -1
        if 'vol' in df.columns and 'volume' not in df.columns:
            df = df.rename(columns={'vol': 'volume'})
        vol_ma = df['volume'].rolling(vol_ma_period).mean().iloc[-1]
        if pd.isna(vol_ma) or vol_ma <= 0: return -1
        return float(df['volume'].iloc[-1]) / vol_ma
    except: return -1

def htf_filter_ok(symbol: str, for_reversal: bool = False) -> bool:
    """
    HTF filter:
    - brkX2-12h & T3: vol 3D > HTF_VOL_MULT * MA20 volume 3D
      (backtest_htf_vol_sweep_12h.py, 29/07/2026): avg +6.552% vs lama +4.975%, wf6 OK
    - Reversal-8h: tidak ada HTF filter (HTF lama = tanpa HTF, identik hasilnya)
    Fail-open kalau data kurang.
    """
    if for_reversal:
        return True  # Reversal-8h: HTF dihapus
    if not HTF_FILTER_ENABLED:
        return True
    try:
        df = get_ohlcv_htf(symbol, interval=HTF_TIMEFRAME, limit=HTF_CANDLE_LIMIT)
        if df is None or len(df) < HTF_VOL_MA_PERIOD + 5:
            return True  # fail-open
        if 'vol' in df.columns and 'volume' not in df.columns:
            df = df.rename(columns={'vol': 'volume'})
        vol_ma = df['volume'].rolling(HTF_VOL_MA_PERIOD).mean().iloc[-1]
        if pd.isna(vol_ma) or vol_ma <= 0:
            return True
        return float(df['volume'].iloc[-1]) > HTF_VOL_MULT * vol_ma
    except Exception as e:
        log(f"  [HTF] error cek {symbol}: {e} → skip filter")
        return True  # fail-open

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

def htf_filter_4h_ok(symbol: str, for_crossema: bool = False) -> bool:
    """
    HTF filter untuk strategi 4h (brkX2-4h dan CrossEMA-4h):
    - brkX2-4h   : vol 12h > STRAT4H_HTF_VOL_MULT (2.0) * MA20 volume 12h
    - CrossEMA-4h: vol 12h > STRAT_CROSSEMA_HTF_VOL_MULT (1.5) * MA20 volume 12h
    (backtest_htf_vol_sweep_4h.py, 29/07/2026)
    Fail-open kalau data tidak cukup.
    """
    try:
        vol_mult = STRAT_CROSSEMA_HTF_VOL_MULT if for_crossema else STRAT4H_HTF_VOL_MULT
        df = get_ohlcv_htf(symbol, interval=STRAT4H_HTF_TF, limit=STRAT4H_HTF_LIMIT)
        if df is None or len(df) < STRAT4H_HTF_VOL_MA + 5:
            return True  # fail-open
        if 'vol' in df.columns and 'volume' not in df.columns:
            df = df.rename(columns={'vol': 'volume'})
        vol_ma = df['volume'].rolling(STRAT4H_HTF_VOL_MA).mean().iloc[-1]
        if pd.isna(vol_ma) or vol_ma <= 0:
            return True
        return float(df['volume'].iloc[-1]) > vol_mult * vol_ma
    except Exception as e:
        log(f"  [HTF4h] error cek {symbol}: {e} → skip filter")
        return True  # fail-open

def check_entry_4h(df) -> bool:
    """
    Entry 4h:
      - Supertrend dir = +1 (uptrend)
      - MACD hist > 0
      - ATR% >= 2.0%
      - Volume >= 0.25x MA20
      - Vol24h >= $3jt
      - Stoch%K < 80 (backtest_4h_rsi_stoch_sweep.py, 31/07/2026): worst -48.39% vs -63.96%
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
    # Stoch%K filter
    sk = r.get("stoch_k")
    if sk is not None and not pd.isna(sk) and sk >= STRAT4H_STOCH_MAX: return False
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
    """Heartbeat brkX2-12h — dikirim saat START dan tiap jam ganjil WIB."""
    global heartbeat_window_start, heartbeat_last_sent
    now = time.time()
    now_dt = now_wib()
    if heartbeat_window_start is None:
        heartbeat_window_start = now_dt
    first_time = (heartbeat_last_sent == 0.0)
    if not (first_time or should_send_heartbeat(heartbeat_last_sent)):
        return
    if first_time:
        start_str = now_dt.strftime('%d/%m %H:%M')
        next_str  = next_scheduled_heartbeat_wib().strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT — START — brkX2-12h\n"
                  f"Mulai memantau: {start_str} WIB\n"
                  f"Notif berikutnya: {next_str} WIB")
    else:
        start_str = heartbeat_window_start.strftime('%d/%m %H:%M')
        end_str   = now_dt.strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT — brkX2-12h\n"
                  f"Periode: {start_str} -> {end_str} WIB")
    # Status T3 intrabar
    t3_str = ""
    try:
        with t3_status_lock:
            es = t3_early_last_status; bs = t3_base_last_status
            en = t3_early_near_miss[:]; bn = t3_base_near_miss[:]
        t3_str = f"\nIntrabar EARLY (5-59%): {es}"
        if en: t3_str += " | " + ", ".join(to_display_pair(s) for s,_ in en[:2])
        t3_str += f"\nIntrabar BASE (60-75%): {bs}"
        if bn: t3_str += " | " + ", ".join(to_display_pair(s) for s,_ in bn[:2])
    except: pass
    send_telegram(
        f"{header}\n"
        f"\n*brkX2-12h*\n"
        f"{status_line}\n"
        f"\nSlot brkX2-12h: {deal_count_by_strategy('brkX2')}/{MAX_DEALS_BRKX2}"
        f"{t3_str}",
        parse_mode="Markdown"
    )
    log(f"[T1] Heartbeat brkX2-12h terkirim ({'START' if first_time else start_str+' -> '+end_str})")
    heartbeat_last_sent    = now
    heartbeat_window_start = now_dt



def heartbeat_rev_tick(status_line: str):
    """Heartbeat KHUSUS Reversal-8h, dikirim saat START dan tiap jam ganjil WIB."""
    global heartbeat_rev_window_start, heartbeat_rev_last_sent
    now = time.time()
    now_dt = now_wib()
    if heartbeat_rev_window_start is None:
        heartbeat_rev_window_start = now_dt
    first_time = (heartbeat_rev_last_sent == 0.0)
    if not (first_time or should_send_heartbeat(heartbeat_rev_last_sent)):
        return
    if first_time:
        start_str = now_dt.strftime('%d/%m %H:%M')
        next_str  = next_scheduled_heartbeat_wib().strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT — START — Reversal-8h\n"
                  f"Mulai memantau: {start_str} WIB\n"
                  f"Notif berikutnya: {next_str} WIB")
    else:
        start_str = heartbeat_rev_window_start.strftime('%d/%m %H:%M')
        end_str   = now_dt.strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT — Reversal-8h\n"
                  f"Periode: {start_str} -> {end_str} WIB")
    prev = csv_progress('reversal')
    if prev is None or prev['n']==0:
        prog = f"Progress reversal-8h: #0/{FWDTEST_TARGET_REVERSAL} (belum ada)"
    else:
        nn=prev['n']; wl=f"{prev['win']}W/{prev['loss']}L"
        tag=" TERCAPAI!" if nn>=FWDTEST_TARGET_REVERSAL else ""
        prog = f"Progress reversal-8h: #{nn}/{FWDTEST_TARGET_REVERSAL} ({wl}, total {prev['total_pct']:+.1f}%){tag}"
    send_telegram(
        f"{header}\n"
        f"\n*Reversal-8h*\n"
        f"{status_line}\n"
        f"\nSlot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL}\n"
        f"{prog}",
        parse_mode="Markdown"
    )
    log(f"[T1b] Heartbeat Reversal-8h terkirim: {status_line}")
    heartbeat_rev_last_sent = now
    heartbeat_rev_window_start = now_dt


def heartbeat_4h_tick(status_line: str, near_miss_4h: list = None):
    """Heartbeat KHUSUS brkX2-4h, dikirim saat START dan tiap jam ganjil WIB."""
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
        header = (f"HEARTBEAT — START — brkX2-4h\n"
                  f"Mulai memantau: {start_str} WIB\n"
                  f"Notif berikutnya: {next_scheduled_heartbeat_wib().strftime('%d/%m %H:%M')} WIB")
    else:
        start_str = heartbeat_4h_window_start.strftime('%d/%m %H:%M')
        end_str   = now_dt.strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT — brkX2-4h\n"
                  f"Periode: {start_str} -> {end_str} WIB")

    prev = csv_progress('brkX2_4h')
    if prev is None or prev['n'] == 0:
        prog = f"Progress brkX2-4h: #0/{STRAT4H_FWDTEST_TARGET} (belum ada deal)"
    else:
        nn  = prev['n']; wl = f"{prev['win']}W/{prev['loss']}L"
        tag = " TERCAPAI!" if nn >= STRAT4H_FWDTEST_TARGET else ""
        prog = f"Progress brkX2-4h: #{nn}/{STRAT4H_FWDTEST_TARGET} ({wl}, total {prev['total_pct']:+.1f}%){tag}"

    prev_cx = csv_progress('brkX2_crossema')
    if prev_cx is None or prev_cx['n'] == 0:
        prog_cx = f"Progress crossema: #0/{STRAT_CROSSEMA_FWDTEST} (belum ada deal)"
    else:
        nn_cx = prev_cx['n']; wl_cx = f"{prev_cx['win']}W/{prev_cx['loss']}L"
        tag_cx = " TERCAPAI!" if nn_cx >= STRAT_CROSSEMA_FWDTEST else ""
        prog_cx = f"Progress crossema: #{nn_cx}/{STRAT_CROSSEMA_FWDTEST} ({wl_cx}, total {prev_cx['total_pct']:+.1f}%){tag_cx}"

    # Kandidat terdekat 4h
    near_str = ""
    try:
        with t1d_near_miss_lock:
            near_list = t1d_near_miss[:]
        if near_miss_4h:
            near_list = near_miss_4h
        if near_list:
            lines = []
            for sym, fails in near_list[:3]:
                fail_str = "; ".join(fails) if fails else "semua lolos"
                lines.append(f"• {to_display_pair(sym)}: belum: {fail_str}")
            near_str = "\nKandidat terdekat 4h:\n" + "\n".join(lines)
    except: pass

    n_cx = sum(1 for d in active_deals.values() if d.get('strategy') == 'brkX2_crossema')
    send_telegram(
        f"{header}\n"
        f"\n*4h* : {status_line}"
        f"{near_str}\n"
        f"\nSlot 4h: {active_deal_count_4h()}/{STRAT4H_MAX_DEALS}\n"
        f"{prog}",
        parse_mode="Markdown"
    )
    log(f"[T1d] Heartbeat 4h terkirim: {status_line}")
    heartbeat_4h_last_sent    = now
    heartbeat_4h_window_start = now_dt


heartbeat_cx_last_sent:    float = 0.0
heartbeat_cx_window_start         = None

def heartbeat_crossema_tick():
    """Heartbeat KHUSUS CrossEMA-4h, dikirim saat START dan tiap jam ganjil WIB."""
    global heartbeat_cx_last_sent, heartbeat_cx_window_start
    if not STRAT_CROSSEMA_ENABLED: return
    now    = time.time()
    now_dt = now_wib()
    if heartbeat_cx_window_start is None:
        heartbeat_cx_window_start = now_dt
    first_time = (heartbeat_cx_last_sent == 0.0)
    if not (first_time or should_send_heartbeat(heartbeat_cx_last_sent)):
        return

    if first_time:
        start_str = now_dt.strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT — START — CrossEMA-4h\n"
                  f"Mulai memantau: {start_str} WIB\n"
                  f"Notif berikutnya: {next_scheduled_heartbeat_wib().strftime('%d/%m %H:%M')} WIB")
    else:
        start_str = heartbeat_cx_window_start.strftime('%d/%m %H:%M')
        end_str   = now_dt.strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT — CrossEMA-4h\n"
                  f"Periode: {start_str} -> {end_str} WIB")

    prev_cx = csv_progress('brkX2_crossema')
    if prev_cx is None or prev_cx['n'] == 0:
        prog_cx = f"Progress crossema: #0/{STRAT_CROSSEMA_FWDTEST} (belum ada deal)"
    else:
        nn_cx = prev_cx['n']; wl_cx = f"{prev_cx['win']}W/{prev_cx['loss']}L"
        tag_cx = " TERCAPAI!" if nn_cx >= STRAT_CROSSEMA_FWDTEST else ""
        prog_cx = f"Progress crossema: #{nn_cx}/{STRAT_CROSSEMA_FWDTEST} ({wl_cx}, total {prev_cx['total_pct']:+.1f}%){tag_cx}"

    n_cx = sum(1 for d in active_deals.values() if d.get('strategy') == 'brkX2_crossema')

    # Kandidat terdekat CrossEMA
    near_str = ""
    try:
        nm = _crossema_near_miss[:]
        if nm:
            lines = ["Kandidat terdekat:"]
            for sym, fails in nm[:5]:
                fs = "; ".join(fails) if fails else "-"
                lines.append(f"• {to_display_pair(sym)}: {fs}")
            near_str = "\n" + "\n".join(lines)
        else:
            near_str = "\nKandidat terdekat: belum ada scan di window ini"
    except: pass

    send_telegram(
        f"{header}\n"
        f"\n*CrossEMA* : Slot {n_cx}/{STRAT_CROSSEMA_MAX_DEALS}"
        f"{near_str}\n"
        f"\n{prog_cx}",
        parse_mode="Markdown"
    )
    log(f"[T_CX] Heartbeat crossema terkirim")
    heartbeat_cx_last_sent    = now
    heartbeat_cx_window_start = now_dt

heartbeat_gen_last_sent:    float = 0.0
heartbeat_gen_window_start        = None

def heartbeat_general_tick():
    """Heartbeat General — dikirim paling akhir, berisi semua strategi + progress gabungan."""
    global heartbeat_gen_last_sent, heartbeat_gen_window_start
    now    = time.time()
    now_dt = now_wib()
    if heartbeat_gen_window_start is None:
        heartbeat_gen_window_start = now_dt
    first_time = (heartbeat_gen_last_sent == 0.0)
    if not (first_time or should_send_heartbeat(heartbeat_gen_last_sent)):
        return
    if first_time:
        start_str = now_dt.strftime('%d/%m %H:%M')
        next_str  = next_scheduled_heartbeat_wib().strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT — START — General\n"
                  f"Mulai memantau: {start_str} WIB\n"
                  f"Notif berikutnya: {next_str} WIB")
    else:
        start_str = heartbeat_gen_window_start.strftime('%d/%m %H:%M')
        end_str   = now_dt.strftime('%d/%m %H:%M')
        header = (f"HEARTBEAT — General\n"
                  f"Periode: {start_str} -> {end_str} WIB")
    # Progress semua strategi
    def _fmt_strat(p, tgt):
        if p is None or p['n']==0: return f"#0/{tgt} (belum ada)"
        nn=p['n']; wl=f"{p['win']}W/{p['loss']}L"
        tag=" TERCAPAI!" if nn>=tgt else ""
        return f"#{nn}/{tgt} ({wl}, total {p['total_pct']:+.1f}%){tag}"
    prog_all = csv_progress()
    prog_brk = csv_progress('brkX2', offset=FWDTEST_BRKX2_PHASE_OFFSET)
    prog_rev = csv_progress('reversal')
    prog_4h  = csv_progress('brkX2_4h')
    prog_cx  = csv_progress('brkX2_crossema')
    if prog_all is None:
        prog_line = "Progress forward-test: 0 trade selesai (CSV belum ada)."
    else:
        nn=prog_all['n']; wl=f"{prog_all['win']}W/{prog_all['loss']}L"
        prog_line = (f"Progress forward-test (gabungan): {nn} selesai ({wl}, total {prog_all['total_pct']:+.1f}%)\n"
                     f"  - brkX2-12h  : {_fmt_strat(prog_brk, FWDTEST_TARGET_BRKX2)}\n"
                     f"  - reversal-8h: {_fmt_strat(prog_rev, FWDTEST_TARGET_REVERSAL)}\n"
                     f"  - brkX2-4h   : {_fmt_strat(prog_4h,  STRAT4H_FWDTEST_TARGET)}\n"
                     f"  - crossema-4h: {_fmt_strat(prog_cx,  STRAT_CROSSEMA_FWDTEST)}")
    # Slot semua
    n_cx = sum(1 for d in active_deals.values() if d.get('strategy') == 'brkX2_crossema')
    slot_line = (f"Slot brkX2-12h: {deal_count_by_strategy('brkX2')}/{MAX_DEALS_BRKX2} | "
                 f"Slot reversal-8h: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL}\n"
                 f"Slot brkX2-4h: {active_deal_count_4h()}/{STRAT4H_MAX_DEALS} | "
                 f"Slot crossema-4h: {n_cx}/{STRAT_CROSSEMA_MAX_DEALS} | "
                 f"Total: {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}")
    send_telegram(
        f"{header}\n"
        f"\n---\n"
        f"brkX2-12h  : ST-up + >EMA20 + close>EMA50 + breakout{BREAKOUT_LOOKBACK} + vol>={VOLUME_MULT}xMA + RSI<{RSI_MAX} + ATR<{ATR_MAX_PCT}%\n"
        f"Reversal-8h: 3 merah+turun>=5% + doji + HA bull + cross-up EMA20\n"
        f"brkX2-4h   : ST-up + MACD>0 + ATR>=2% + vol>={STRAT4H_VOLUME_MULT}xMA + HTF 3D\n"
        f"CrossEMA-4h: ST-1 + cross-up EMA20 intrabar menit 12-36\n"
        f"Exit       : trailing adaptif (arm +{TRAIL_ARM_PCT}%) | Base ${BASE_ORDER_VOLUME} | Perf filter ON\n"
        f"---\n"
        f"{slot_line}\n"
        f"---\n"
        f"{prog_line}\n"
        f"Bot HIDUP & terus memantau."
    )
    log(f"[HB-GEN] Heartbeat General terkirim")
    heartbeat_gen_last_sent    = now
    heartbeat_gen_window_start = now_dt

NEAR_MISS_LOG    = "/data/near_miss_log.txt"
TFPCT_BLOCKED_LOG = "/data/tfpct_blocked_log.txt"

def log_tfpct_blocked(thread: str, strategi: str, elapsed_pct: float, limit_pct: float, keterangan: str):
    """Append ke tfpct_blocked_log.txt saat scan diblokir karena TF% sudah lewat window."""
    try:
        ts = now_wib().strftime('%Y-%m-%d %H:%M')
        line = f"{ts} | {thread:<6} | {strategi:<12} | TF% {elapsed_pct*100:.1f}% > {limit_pct*100:.1f}% | {keterangan}\n"
        with open(TFPCT_BLOCKED_LOG, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        log(f"WARN log_tfpct_blocked: {e}")


def log_near_miss(strategi: str, near_miss_list: list, total_syarat: int):
    """Append near miss ke file akumulatif. Tidak pernah overwrite."""
    try:
        ts = now_wib().strftime('%Y-%m-%d %H:%M')
        lines = []
        for item in near_miss_list:
            if len(item) >= 4:
                n_pass, sym, fails, item_total = item[0], item[1], item[2], item[3]
            elif len(item) == 3:
                n_pass, sym, fails = item
                item_total = total_syarat
            else:
                sym, fails = item
                n_pass = "?"
                item_total = total_syarat
            fails_str = "; ".join(fails) if fails else "semua lolos"
            lines.append(f"{ts} | {strategi} | {sym} | lolos {n_pass}/{item_total} | belum: {fails_str}\n")
        if lines:
            with open(NEAR_MISS_LOG, "a", encoding="utf-8") as f:
                f.writelines(lines)
    except Exception as e:
        log(f"WARN log_near_miss: {e}")

def format_near_miss(near_miss, total, max_show=5):
    """Format daftar kandidat terdekat utk heartbeat.
    near_miss: list of (n_pass, sym, fails) atau (n_pass, sym, fails, total_override)
    total_override dipakai bila ada syarat tambahan (HTF 3D, Perf) sehingga total != total default.
    """
    if not near_miss:
        return "Kandidat terdekat: tidak ada (semua pair masih jauh dari lolos)."
    near_miss.sort(key=lambda x: x[0], reverse=True)
    lines = ["Kandidat terdekat:"]
    for item in near_miss[:max_show]:
        n_pass, sym, fails = item[0], item[1], item[2]
        item_total = item[3] if len(item) > 3 else total
        belum = "; ".join(fails) if fails else "-"
        lines.append(f"• {to_display_pair(sym)}: lolos {n_pass}/{item_total} — belum: {belum}")
    sisa = len(near_miss) - max_show
    if sisa > 0:
        last_shown = near_miss[max_show-1] if max_show <= len(near_miss) else near_miss[-1]
        lines.append(f"(+ {sisa} pair lain lolos >={last_shown[0]}/{total})")
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
            # HTF 3D filter
            if HTF_FILTER_ENABLED and not htf_filter_ok(sym):
                log(f"  [T1] {sym} lolos 12h tapi DITOLAK HTF 3D filter (price<EMA50 atau MACD<0)")
                det = entry_detail(df)
                if det is not None:
                    n_pass, total, fails = det
                    _rvol = htf_vol_ratio(sym, HTF_TIMEFRAME, HTF_CANDLE_LIMIT, HTF_VOL_MA_PERIOD)
                    _rvol_str = f"{_rvol:.2f}xMA" if _rvol >= 0 else "?"
                    _vr_nm = float(df["vol"].iloc[-1])/float(df["vol_ma"].iloc[-1]) if float(df["vol_ma"].iloc[-1])>0 else 0
                near_miss.append((n_pass, sym, fails + [f"HTF 3D: vol<{HTF_VOL_MULT}xMA (skrg {_rvol_str})"], 9, round(_vr_nm,2)))
            else:
                # Performance filter
                if PERF_FILTER_ENABLED:
                    candle_ts = int(df['ct'].iloc[-1])
                    pscore = calc_perf_score(sym, candle_ts)
                    if pd.isna(pscore) or pscore < PERF_SCORE_MIN:
                        det = entry_detail(df)
                        if det is not None:
                            n_pass, total, fails = det
                            _vr_nm = float(df["vol"].iloc[-1])/float(df["vol_ma"].iloc[-1]) if float(df["vol_ma"].iloc[-1])>0 else 0
                            near_miss.append((n_pass, sym, fails + [f"Perf Grade masih <B (score {pscore:.2f})"], 9, round(_vr_nm,2)))
                        continue
                sc = signal_score(df.iloc[-1])
                candidates.append((sym, float(df['close'].iloc[-1]), float(df['atr_pct'].iloc[-1]), sc))
        else:
            det = entry_detail(df)
            if det is not None:
                n_pass, total, fails = det
                if n_pass >= 5:   # tampilkan hanya yg lolos >=5/7
                    _vr_nm = float(df["vol"].iloc[-1])/float(df["vol_ma"].iloc[-1]) if float(df["vol_ma"].iloc[-1])>0 else 0
                    near_miss.append((n_pass, sym, fails, 9, round(_vr_nm,2)))

    if not candidates:
        log(f"[T1] {len(universe)} coin discan, tidak ada yg lolos syarat entry.")
        last_processed_candle_ts = newest_ts
        log_near_miss("brkX2-12h", near_miss, 9)
        update_dashboard_near_miss("brkX2-12h", near_miss)
        return f"TIDAK ADA coin lolos 7 syarat inti + 2 tambahan (HTF 3D, Perf). ({len(universe)} coin discan)\n" + format_near_miss(near_miss, 9)

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
            log_near_miss("brkX2-12h", near_miss, 9)
            update_dashboard_near_miss("brkX2-12h", near_miss)
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
                'strategy': 'brkX2', 'score': score, 'target_usd': target_usd,
                'add_usd': add_usd, 'add_fund_sent': False,
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
            threading.Thread(target=send_email_open_long, args=("OPEN LONG brkX2-12h: " + to_display_pair(sym), 
                f"OPEN LONG (Momentum brkX2 (12h))\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (candle close): {signal_price:.6g}\n"
                f"Selisih (lonjakan/slippage): {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Skor sinyal: {score}/5 -> modal ${target_usd}{addfund_txt}\n"
                f"Slot terpakai: {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}"
            ), daemon=True).start()
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
    universe = [p for p in pairs if volmap.get(p,0) >= REVERSAL_MIN_VOL_USD]
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
            # Performance filter
            if PERF_FILTER_ENABLED:
                candle_ts = int(df['ct'].iloc[-1])
                pscore = calc_perf_score(sym, candle_ts)
                if pd.isna(pscore) or pscore < PERF_SCORE_MIN:
                    det = entry_detail_reversal(df)
                    if det is not None:
                        n_pass, total, fails = det
                        near_miss.append((n_pass, sym, fails + [f"Perf Grade masih <B (score {pscore:.2f})"], 5))
                    continue
            atrp = float(df['atr_pct'].iloc[-1]) if not pd.isna(df['atr_pct'].iloc[-1]) else 3.0
            candidates.append((sym, float(df['close'].iloc[-1]), atrp, int(df['ct'].iloc[-1])))
        else:
            det = entry_detail_reversal(df)
            if det is not None:
                n_pass, total, fails = det
                if n_pass >= 2:   # tampilkan hanya yg lolos >=2/4
                    near_miss.append((n_pass, sym, fails, 5))

    if not candidates:
        log(f"[T1b] {len(universe)} coin discan (reversal), tidak ada yg lolos setup.")
        log_near_miss("Reversal-8h", near_miss, 5)
        update_dashboard_near_miss("Reversal-8h", near_miss)
        return f"REVERSAL: tidak ada coin lolos setup. ({len(universe)} discan)\n" + format_near_miss(near_miss, 5)

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
            threading.Thread(target=send_email_open_long, args=("OPEN LONG Reversal-8h: " + to_display_pair(sym), 
                f"OPEN LONG (Reversal Doji+HA (8h))\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (candle close): {signal_price:.6g}\n"
                f"Selisih (slippage): {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} | total {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}"
            ), daemon=True).start()
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
        entry = d.get('entry_price',0)
        if entry<=0: continue
        price = get_price_now(sym)
        if price<=0: continue

        # ── KIRIM ADD FUND (sekali, setelah deal confirmed aktif) ─────────
        add_usd      = d.get('add_usd', 0)
        add_fund_sent = d.get('add_fund_sent', False)
        if add_usd > 0 and not add_fund_sent:
            if not get_deal_override(sym, 'auto_add_fund', True):
                log(f"[T2] {sym} add fund di-skip (auto_add_fund=OFF via dashboard)")
            else:
                strat = d.get('strategy', 'brkX2')
                log(f"[T2] {sym} kirim add fund ${add_usd} (deal confirmed aktif)")
                send_add_funds(sym, add_usd, strat, delay=0)
                deal_log_write({
                    'timestamp_wib': now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                    'event_type':    'ADD_FUND',
                    'strategy':      strat,
                    'symbol':        to_display_pair(sym),
                    'thread':        'T2',
                    'add_usd':       add_usd,
                    'total_usd':     BASE_ORDER_VOLUME + add_usd,
                })
                with active_deals_lock:
                    if sym in active_deals:
                        active_deals[sym]['add_fund_sent'] = True
                save_active_deals()

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
            if not get_deal_override(sym, 'auto_close', True):
                log(f"[T2] {sym} close di-skip (auto_close=OFF via dashboard)")
                continue
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
                pstrat = csv_progress(strat, offset=FWDTEST_BRKX2_PHASE_OFFSET if strat=='brkX2' else 0)
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
def _send_unified_heartbeat(status_12h, status_rev, status_4h, near_4h):
    """Kirim SATU heartbeat gabungan untuk semua strategi."""
    global heartbeat_last_sent, heartbeat_window_start
    global heartbeat_rev_last_sent, heartbeat_rev_window_start
    global heartbeat_4h_last_sent, heartbeat_4h_window_start

    now    = time.time()
    now_dt = now_wib()
    first_time = (heartbeat_last_sent == 0.0)

    if heartbeat_window_start is None:
        heartbeat_window_start = now_dt

    # Header
    if first_time:
        start_str = now_dt.strftime('%d/%m %H:%M')
        header = f"HEARTBEAT — START\nMulai memantau: {start_str} WIB\nNotif berikutnya: {next_scheduled_heartbeat_wib().strftime('%d/%m %H:%M')} WIB"
    else:
        start_str = heartbeat_window_start.strftime('%d/%m %H:%M')
        end_str   = now_dt.strftime('%d/%m %H:%M')
        header = f"HEARTBEAT 6-jam\nPeriode: {start_str} -> {end_str} WIB"

    # Progress forward-test
    def _fmt_strat(p, tgt):
        if p is None or p['n']==0: return f"#0/{tgt} (belum ada)"
        nn=p['n']; wl=f"{p['win']}W/{p['loss']}L"
        tag=" TERCAPAI!" if nn>=tgt else ""
        return f"#{nn}/{tgt} ({wl}, total {p['total_pct']:+.1f}%){tag}"

    prog_all = csv_progress()
    prog_brk = csv_progress('brkX2', offset=FWDTEST_BRKX2_PHASE_OFFSET)
    prog_rev = csv_progress('reversal')
    prog_4h  = csv_progress('brkX2_4h')
    prog_cx  = csv_progress('brkX2_crossema')

    if prog_all is None:
        prog_line = "Progress forward-test: 0 trade selesai (CSV belum ada)."
    else:
        nn=prog_all['n']; wl=f"{prog_all['win']}W/{prog_all['loss']}L"
        prog_line = (f"Progress forward-test (gabungan): {nn} selesai ({wl}, total {prog_all['total_pct']:+.1f}%)\n"
                     f"  - brkX2    : {_fmt_strat(prog_brk, FWDTEST_TARGET_BRKX2)}\n"
                     f"  - reversal : {_fmt_strat(prog_rev, FWDTEST_TARGET_REVERSAL)}\n"
                     f"  - 4h       : {_fmt_strat(prog_4h,  STRAT4H_FWDTEST_TARGET)}\n"
                     f"  - crossema : {_fmt_strat(prog_cx,  STRAT_CROSSEMA_FWDTEST)}")

    # Status T3 intrabar
    t3_str = ""
    try:
        with t3_status_lock:
            es = t3_early_last_status; bs = t3_base_last_status
            en = t3_early_near_miss[:]; bn = t3_base_near_miss[:]
        t3_str = f"\nIntrabar EARLY (5-59%): {es}"
        if en: t3_str += " | " + ", ".join(to_display_pair(s) for s,_ in en[:2])
        t3_str += f"\nIntrabar BASE (60-75%): {bs}"
        if bn: t3_str += " | " + ", ".join(to_display_pair(s) for s,_ in bn[:2])
    except: pass

    # Kandidat 4h
    near_4h_str = ""
    try:
        if near_4h:
            lines = []
            for sym, fails in near_4h[:3]:
                fs = "; ".join(fails) if fails else "semua lolos"
                lines.append(f"• {to_display_pair(sym)}: belum: {fs}")
            near_4h_str = "\nKandidat terdekat 4h:\n" + "\n".join(lines)
    except: pass

    # Slot info
    slot_12h = f"Slot brkX2-12h: {deal_count_by_strategy('brkX2')}/{MAX_DEALS_BRKX2}"
    slot_rev  = f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL}"
    slot_4h   = f"Slot 4h: {active_deal_count_4h()}/{STRAT4H_MAX_DEALS}"
    slot_cx   = f"Slot crossema: {sum(1 for d in active_deals.values() if d.get('strategy')=='brkX2_crossema')}/{STRAT_CROSSEMA_MAX_DEALS}"
    slot_total= f"Total: {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}"

    msg = (
        f"{header}\n"
        f"---\n"
        f"brkX2-12h: {status_12h}\n"
        f"Reversal : {status_rev or '—'}\n"
        f"4h       : {status_4h or '—'}"
        f"{near_4h_str}\n"
        f"---\n"
        f"{slot_12h} | {slot_rev}\n"
        f"{slot_4h} | {slot_cx} | {slot_total}\n"
        f"---\n"
        f"{prog_line}"
        f"{t3_str}\n"
        f"Bot HIDUP & terus memantau."
    )
    send_telegram(msg)
    log(f"[HB] Heartbeat gabungan terkirim ({'START' if first_time else 'periodik'})")

    # Update semua timer heartbeat sekaligus
    heartbeat_last_sent = now;         heartbeat_window_start = now_dt
    heartbeat_rev_last_sent = now;     heartbeat_rev_window_start = now_dt
    heartbeat_4h_last_sent = now;      heartbeat_4h_window_start = now_dt

def run_thread1():
    while True:
        try:
            status = thread1_scan()
            status_rev = None
            status_4h  = None
            near_4h    = []

            # Scan reversal
            try:
                if REVERSAL_ENABLED:
                    status_rev = thread1b_scan_reversal()
            except Exception as e: log(f"WARN T1b reversal error: {e}")

            # Siapkan status 4h
            try:
                if STRAT4H_ENABLED:
                    n4h = active_deal_count_4h()
                    if n4h >= STRAT4H_MAX_DEALS:
                        status_4h = f"4h: slot penuh ({n4h}/{STRAT4H_MAX_DEALS}) — deal aktif: " + \
                            ", ".join(to_display_pair(s) for s,d in active_deals.items() if d.get("strategy")=="brkX2_4h")
                    else:
                        status_4h = f"4h: memantau sinyal. Slot {n4h}/{STRAT4H_MAX_DEALS}"
                    with t1d_near_miss_lock:
                        near_4h = t1d_near_miss[:]
            except Exception as e: log(f"WARN T1 heartbeat 4h error: {e}")

            # Cek apakah sudah waktunya kirim heartbeat brkX2-12h
            if status is not None:
                heartbeat_tick(status)
            # Reversal heartbeat — skip jika baru saja dikirim dari startup (dalam 60 detik)
            if status_rev is not None:
                try:
                    if time.time() - heartbeat_rev_last_sent > 60:
                        heartbeat_rev_tick(status_rev)
                except Exception as e: log(f"WARN T1b heartbeat error: {e}")
        except Exception as e: log(f"WARN T1 error: {e}")
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
        if elapsed_pct > INTRABAR_WINDOW_END:
            log(f"[T1c] TF% LEWAT window: {elapsed_pct*100:.1f}% > {INTRABAR_WINDOW_END*100:.0f}% (window 60-75% sudah tutup)")
            log_tfpct_blocked("T1c", "brkX2-12h", elapsed_pct, INTRABAR_WINDOW_END, "window 60-75% sudah tutup")
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
        # EMA20>EMA50 dihapus 30/07/2026 — diganti close>EMA50
        if r12['close'] <= r12['ema_slow']: continue
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
                'add_usd': add_usd, 'add_fund_sent': False,
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
            threading.Thread(target=send_email_open_long, args=("OPEN LONG INTRABAR brkX2-12h: " + to_display_pair(sym), 
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
            ), daemon=True).start()
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

# Near miss tracking untuk T1d (4h)
t1d_near_miss        = []
t1d_near_miss_lock   = threading.Lock()

def thread1c_scan_intrabar_early():
    """
    T3-EARLY: Scan sinyal brkX2 di awal candle 12h (5-59% elapsed = menit ke 36-424).
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

    # Hanya entry di window 5-59% elapsed
    if elapsed_pct < INTRABAR_EARLY_ENTRY_PCT or elapsed_pct > INTRABAR_EARLY_END_PCT:
        if elapsed_pct > INTRABAR_EARLY_END_PCT:
            log(f"[T1c-E] TF% LEWAT window: {elapsed_pct*100:.1f}% > {INTRABAR_EARLY_END_PCT*100:.0f}% (window 5-59% sudah tutup)")
            log_tfpct_blocked("T1c-E", "brkX2-12h", elapsed_pct, INTRABAR_EARLY_END_PCT, "window 5-59% sudah tutup")
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
        # EMA20>EMA50 dihapus 30/07/2026 — diganti close>EMA50
        if r12['close'] <= r12['ema_slow']: continue
        if pd.isna(r12.get('hh_early')): continue
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
        if price_now <= float(r12['hh_early']): continue   # breakout HH7 (T3-EARLY, backtest 20/07)
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
                'add_usd': add_usd, 'add_fund_sent': False,
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
            threading.Thread(target=send_email_open_long, args=("OPEN LONG INTRABAR EARLY brkX2-12h: " + to_display_pair(sym), 
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
            ), daemon=True).start()
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
    """Thread T3: intrabar scan 12h.
    - T3-baseline (60-75%): tiap INTRABAR_SCAN_INTERVAL (300s = 5 menit)
    - T3-early (5-10%)    : tiap INTRABAR_EARLY_SCAN_INTERVAL (240s = 4 menit)
    Keduanya jalan di thread yang sama dengan timer masing-masing.
    """
    last_early_scan = 0.0
    last_base_scan  = 0.0
    while True:
        now = time.time()
        try:
            if now - last_base_scan >= INTRABAR_SCAN_INTERVAL:
                thread1c_scan_intrabar()
                last_base_scan = time.time()
        except Exception as e:
            log(f"WARN T3 intrabar error: {e}")
        try:
            if now - last_early_scan >= INTRABAR_EARLY_SCAN_INTERVAL:
                thread1c_scan_intrabar_early()
                last_early_scan = time.time()
        except Exception as e:
            log(f"WARN T3-early intrabar error: {e}")
        time.sleep(60)   # cek tiap menit, eksekusi per timer masing-masing

# ══════════════════════════════════════════════════════════════════════════════
# THREAD T3-REV: SCAN INTRABAR REVERSAL (full candle 8h, tiap 8 menit)
# ══════════════════════════════════════════════════════════════════════════════
# Lapis 1: cek syarat reversal dari candle-candle TERTUTUP (c-3..c+1 sudah ada)
# Lapis 2: konfirmasi live price_now > EMA20 (cross-up intrabar c+2 berjalan)

last_rev_intrabar_candle_ts: dict = {}  # sym -> candle_open_ms yg sudah di-entry intrabar

def thread_rev_intrabar_scan():
    """Scan reversal intrabar: cek setup dari candle tertutup, konfirmasi harga live."""
    global last_rev_intrabar_candle_ts

    if not REVERSAL_INTRABAR_ENABLED:
        return
    if not REVERSAL_ENABLED:
        return
    if deal_count_by_strategy('reversal') >= MAX_DEALS_REVERSAL:
        return
    if active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS:
        return

    now_ms = int(time.time() * 1000)
    sec8   = 8 * 3600
    candle_open_ms = (now_ms // (sec8 * 1000)) * (sec8 * 1000)

    pairs  = get_usdt_spot_pairs()
    ticker = get_ticker_24h()
    volmap = {}
    for t in ticker:
        try: volmap[t['symbol']] = float(t.get('quoteVolume', 0))
        except: pass
    universe = [p for p in pairs if volmap.get(p, 0) >= REVERSAL_MIN_VOL_USD]

    for sym in universe:
        with active_deals_lock:
            if sym in active_deals:
                continue
        # Gating: jangan entry 2x di candle 8h yang sama untuk symbol ini
        if last_rev_intrabar_candle_ts.get(sym) == candle_open_ms:
            continue

        df = get_ohlcv(sym, interval=REVERSAL_TIMEFRAME, limit=120)
        if df is None or len(df) < 10:
            continue

        # Pisahkan candle running vs tertutup; ambil harga live dari candle running
        running_mask = df['ct'] >= now_ms
        if running_mask.any():
            price_now = float(df[running_mask].iloc[-1]['close'])
            df_closed = df[~running_mask].copy()
        else:
            price_now = get_price_now(sym)
            df_closed = df.copy()

        if len(df_closed) < 8:
            continue

        try:
            df_closed = compute_indicators_reversal(df_closed)
        except Exception:
            continue

        # ── Lapis 1: setup dari candle tertutup ──────────────────────────────
        # c+1 = df_closed[-1], c0 = df_closed[-2], c-1..c-3 = df_closed[-3..-5]
        n = len(df_closed)
        im3, im2, im1 = n-5, n-4, n-3
        i0  = n - 2
        i1  = n - 1

        c0 = df_closed.iloc[i0]
        if any(pd.isna(c0.get(x, float('nan'))) for x in ['ema_fast', 'ema_slow', 'body_ratio']):
            continue
        if is_choppy(df_closed):
            continue

        # Syarat 1: 3 candle merah + turun >= 5%
        if not all(df_closed.iloc[idx]['close'] < df_closed.iloc[idx]['open']
                   for idx in (im3, im2, im1)):
            continue
        open_c3  = float(df_closed.iloc[im3]['open'])
        close_c1 = float(df_closed.iloc[im1]['close'])
        if open_c3 <= 0 or (close_c1 / open_c3 - 1) * 100 > -5.0:
            continue

        # Syarat 2: c0 doji + di bawah EMA20 & EMA50
        if not (c0['close'] < c0['ema_fast'] and c0['close'] < c0['ema_slow']):
            continue
        if not (c0['body_ratio'] < REVERSAL_DOJI_MAX):
            continue

        # Syarat 3: c+1 HA bullish
        if not bool(df_closed['ha_bull'].iloc[i1]):
            continue

        # ── Lapis 2: konfirmasi harga live (c+2 sedang berjalan) ─────────────
        ema20_now = float(df_closed['ema_fast'].iloc[i1])
        if price_now <= 0 or price_now <= ema20_now:
            continue   # belum cross-up EMA20

        # ── LOLOS → OPEN DEAL ─────────────────────────────────────────────────
        signal_price = float(df_closed['close'].iloc[i1])
        atrp = float(df_closed['atr_pct'].iloc[i1]) if not pd.isna(df_closed['atr_pct'].iloc[i1]) else 3.0

        log(f"[T3-REV] SINYAL REVERSAL INTRABAR: {sym} price_now={price_now:.6g} "
            f"EMA20={ema20_now:.6g} atr%={atrp:.2f}")

        if send_open_long(sym, 'reversal'):
            entry_price = get_price_now(sym)
            if entry_price <= 0:
                entry_price = price_now
            slip_pct = (entry_price / signal_price - 1) * 100 if signal_price > 0 else 0.0

            add_to_active_deals(sym, {
                'entry_price':      entry_price,
                'peak':             entry_price,
                'signal_price':     signal_price,
                'atr_pct':          atrp,
                'opened_candle_ts': int(candle_open_ms),
                'trailing_armed':   False,
                'strategy':         'reversal',
            })
            send_telegram(
                f"OPEN LONG INTRABAR (Reversal Doji+HA (8h))\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (c+1 close): {signal_price:.6g}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} "
                f"| total {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}"
            )
            threading.Thread(target=send_email_open_long, args=("OPEN LONG brkX2-4h: " + to_display_pair(sym), 
                f"OPEN LONG INTRABAR (Reversal Doji+HA (8h))\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (c+1 close): {signal_price:.6g}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} "
                f"| total {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}"
            ), daemon=True).start()
            threading.Thread(target=send_email_open_long, args=("OPEN LONG INTRABAR Reversal-8h: " + to_display_pair(sym), 
                f"OPEN LONG INTRABAR (Reversal Doji+HA (8h))\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (c+1 close): {signal_price:.6g}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} "
                f"| total {active_deal_count()}/{COMMAS_MAX_ACTIVE_DEALS}"
            ), daemon=True).start()
            csv_log_open({
                'open_time_wib':  now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol':         to_display_pair(sym),
                'signal_price':   f"{signal_price:.6g}",
                'entry_price':    f"{entry_price:.6g}",
                'slip_pct':       f"{slip_pct:+.2f}",
                'atr_pct':        f"{atrp:.2f}",
                'trail_dist_pct': f"{trailing_dist(atrp)}",
                'base_usd':       BASE_ORDER_VOLUME,
                'score':          0,
                'strategy':       'reversal',
            })
            last_rev_intrabar_candle_ts[sym] = candle_open_ms

            if (deal_count_by_strategy('reversal') >= MAX_DEALS_REVERSAL
                    or active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS):
                break

def run_thread_rev_intrabar():
    """Thread T3-REV: scan reversal intrabar tiap 8 menit (REVERSAL_INTRABAR_SCAN_INTERVAL)."""
    while True:
        try:
            thread_rev_intrabar_scan()
        except Exception as e:
            log(f"WARN T3-REV reversal intrabar error: {e}")
        time.sleep(REVERSAL_INTRABAR_SCAN_INTERVAL)
last_4h_candle_ts = {}  # sym -> ts candle 4h yang sudah dientry, cegah double entry

def thread1d_scan_4h():
    """
    Scan sinyal strategi ke-3 (brkX2-4h) setiap 3 menit.
    Entry saat elapsed candle 4h berada di menit ke 5-60 (2.08%-25%).
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
        if elapsed_pct > STRAT4H_ENTRY_MAX_PCT:
            log(f"[T1d] TF% LEWAT window: {elapsed_pct*100:.1f}% > {STRAT4H_ENTRY_MAX_PCT*100:.1f}% (window menit 5-60 sudah tutup)")
            log_tfpct_blocked("T1d", "brkX2-4h", elapsed_pct, STRAT4H_ENTRY_MAX_PCT, "window menit 5-60 sudah tutup")
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
                if pd.isna(sd) or sd != 1: fails.append("Supertrend masih Downtrend")
                mh = r.get("macd_hist")
                if pd.isna(mh) or mh <= 0: fails.append(f"MACD({(f'{mh:.4f}' if mh==mh else 'n/a')}<=0)")
                atr = r.get("atr_pct")
                if pd.isna(atr) or atr < STRAT4H_ATR_MIN_PCT: fails.append(f"ATR({(f'{atr:.2f}' if atr==atr else 'n/a')}<{STRAT4H_ATR_MIN_PCT}%)")
                vol_ma = r.get("vol_ma")
                if pd.isna(vol_ma) or vol_ma <= 0 or r["vol"] < STRAT4H_VOLUME_MULT * vol_ma:
                    vol_ratio_now = (r["vol"] / vol_ma) if (vol_ma and vol_ma > 0) else 0
                    fails.append(f"Vol<{STRAT4H_VOLUME_MULT}xMA (skrg {vol_ratio_now:.2f}x)")
                sk = r.get("stoch_k")
                if sk is not None and not pd.isna(sk) and sk >= STRAT4H_STOCH_MAX:
                    fails.append(f"Stoch%K<{STRAT4H_STOCH_MAX} (skrg {sk:.1f})")
                total_4h = 7  # ST + MACD + ATR + Vol + Stoch + HTF + Perf
                n_pass_4h = total_4h - len(fails)
                if len(fails) <= 1:  # hampir lolos (max 1 syarat gagal)
                    near_miss_4h.append((n_pass_4h, sym, fails, total_4h))
                continue

            # HTF 12h filter
            if not htf_filter_4h_ok(sym):
                log(f"  [T1d] {sym} lolos 4h tapi DITOLAK HTF 12h filter")
                _rvol4h = htf_vol_ratio(sym, STRAT4H_HTF_TF, STRAT4H_HTF_LIMIT, STRAT4H_HTF_VOL_MA)
                _rvol4h_str = f"{_rvol4h:.2f}xMA" if _rvol4h >= 0 else "?"
                near_miss_4h.append((6, sym, [f"HTF 12h: vol<{STRAT4H_HTF_VOL_MULT}xMA (skrg {_rvol4h_str})"], 7))
                continue

            # Performance filter
            if PERF_FILTER_ENABLED:
                candle_ts = int(df['ct'].iloc[-1])
                pscore = calc_perf_score(sym, candle_ts)
                if pd.isna(pscore) or pscore < PERF_SCORE_MIN:
                    near_miss_4h.append((6, sym, [f"Perf Grade masih <B (score {pscore:.2f})"], 7))
                    continue

            r    = df.iloc[-1]
            atrp = float(r["atr_pct"]) if not pd.isna(r["atr_pct"]) else 3.0
            sc   = 1
            candidates.append((sym, float(r["close"]), atrp, sc))
        except Exception as e:
            log(f"  [T1d] error {sym}: {e}")

    # Status line untuk heartbeat
    n4h_active = active_deal_count_4h()
    # Simpan near miss ke global untuk heartbeat T1
    with t1d_near_miss_lock:
        t1d_near_miss.clear()
        t1d_near_miss.extend(near_miss_4h[:5])
    if not candidates:
        status_4h = (f"4h: tidak ada sinyal. ({len(ticker or [])} discan, "
                     f"slot {n4h_active}/{STRAT4H_MAX_DEALS})")
        heartbeat_4h_tick(status_4h, near_miss_4h)
        log_near_miss("brkX2-4h", near_miss_4h, 7)
        update_dashboard_near_miss("brkX2-4h", near_miss_4h)
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
            "opened_candle_ts": int(candle_open_ms),   # ms, konsisten dengan brkX2-12h
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
        # Catat ke trades_forwardtest.csv (wajib agar csv_progress bisa hitung #/7)
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
            'strategy':       'brkX2_4h',
        })
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
    # Delay awal agar heartbeat START 4h/cx/General dikirim SETELAH brkX2-12h START
    # (brkX2-12h START dikirim ~25 detik setelah startup dari T1)
    time.sleep(30)
    while True:
        try:
            if STRAT4H_ENABLED:
                thread1d_scan_4h()
            # Heartbeat 4h/CrossEMA/General dipanggil di sini agar tetap terkirim
            # bahkan saat di luar window intrabar (thread1d_scan_4h return awal).
            # Fungsi heartbeat_*_tick sudah punya guard internal (cek interval),
            # jadi aman dipanggil tiap loop — hanya kirim saat waktunya tiba.
            try:
                n4h_active = active_deal_count_4h()
                status_4h = (f"4h: memantau sinyal. Slot {n4h_active}/{STRAT4H_MAX_DEALS}")
                with t1d_near_miss_lock:
                    near_4h = t1d_near_miss[:]
                heartbeat_4h_tick(status_4h, near_4h)
                heartbeat_crossema_tick()
                heartbeat_general_tick()
            except Exception as e:
                log(f"WARN T1d heartbeat periodik: {e}")
        except Exception as e:
            log(f"WARN T1d 4h error: {e}")
        time.sleep(STRAT4H_SCAN_INTERVAL)

# ══════════════════════════════════════════════════════════════════════════════
# THREAD T_CROSSEMA: Strategi #4 — Cross-up EMA20 saat ST Downtrend (4h)
# Window: 5–15% elapsed = menit ke 12–36, scan tiap 4 menit
# Basis: backtest_crossema_sweep2.py (25/07/2026)
#   B_NO_PERF+W1: avg=+24.864% WR=85.5% n=62 wf6=0/6
# Perf filter OFF (counter-trend), HTF 3D ON (price>EMA50 + MACD>0 + RSI>50)
# ══════════════════════════════════════════════════════════════════════════════
_crossema_last_candle_ts: dict = {}   # sym -> candle_open_ms yg sudah di-entry
_crossema_near_miss: list = []        # [(sym, fails)] — kandidat lolos Lapis 1 tapi gagal Lapis 2

def thread_crossema_scan():
    """Scan CrossEMA intrabar: ST=-1, close<EMA20, lalu price_now>EMA20 (cross-up)."""
    global _crossema_last_candle_ts, _crossema_near_miss
    if not STRAT_CROSSEMA_ENABLED: return

    # Cek slot
    n_crossema = sum(1 for d in active_deals.values()
                     if d.get("strategy") == "brkX2_crossema")
    if n_crossema >= STRAT_CROSSEMA_MAX_DEALS: return
    if active_deal_count() >= COMMAS_MAX_ACTIVE_DEALS + STRAT4H_MAX_DEALS: return

    # Hitung elapsed candle 4h saat ini
    now_ms       = int(time.time() * 1000)
    sec4         = STRAT4H_SECONDS
    candle_open_ms = (now_ms // (sec4 * 1000)) * (sec4 * 1000)
    elapsed_pct  = (now_ms - candle_open_ms) / (sec4 * 1000)

    # Hanya scan dalam window 5–15% elapsed
    if not (STRAT_CROSSEMA_ENTRY_MIN <= elapsed_pct <= STRAT_CROSSEMA_ENTRY_MAX):
        if elapsed_pct > STRAT_CROSSEMA_ENTRY_MAX:
            log(f"[T_CX] TF% LEWAT window: {elapsed_pct*100:.1f}% > {STRAT_CROSSEMA_ENTRY_MAX*100:.1f}% (window menit 5-60 sudah tutup)")
            log_tfpct_blocked("T_CX", "CrossEMA-4h", elapsed_pct, STRAT_CROSSEMA_ENTRY_MAX, "window menit 5-60 sudah tutup")
        return

    ticker = get_ticker_24h()
    if not ticker: return

    with active_deals_lock:
        existing = set(active_deals.keys())

    _crossema_near_miss.clear()  # reset tiap scan

    for sym_info in ticker:
        sym = sym_info.get("symbol", "")
        if not sym.endswith("USDT"): continue
        if sym in existing: continue
        if _crossema_last_candle_ts.get(sym) == candle_open_ms: continue
        vol24 = float(sym_info.get("quoteVolume", 0))
        if vol24 < STRAT_CROSSEMA_MIN_VOL_USD: continue

        try:
            df = get_ohlcv_4h(sym, limit=100)
            if df is None or len(df) < 50: continue
            df = compute_indicators_4h(df)

            # Lapis 1: candle n-1 tertutup — ST=-1, close<EMA20, vol>=0.4xMA
            r = df.iloc[-1]
            sd = r.get("st_dir")
            if pd.isna(sd) or sd != -1: continue
            ef = r.get("ema_fast")
            if pd.isna(ef) or r["close"] >= ef: continue
            vm = r.get("vol_ma")
            if pd.isna(vm) or vm <= 0 or r["vol"] < STRAT_CROSSEMA_VOLUME_MULT * vm: continue

            # HTF 12h filter (CrossEMA: vol12h>1.5xMA)
            if not htf_filter_4h_ok(sym, for_crossema=True): continue

            # Lapis 2: harga live (dari candle berjalan) harus > EMA20
            price_now = get_price_now(sym)
            if price_now <= 0 or price_now <= float(ef):
                # Lolos Lapis 1 tapi belum cross EMA20 → catat sebagai near_miss
                if len(_crossema_near_miss) < 5:
                    gap_pct = (float(ef) / price_now - 1) * 100 if price_now > 0 else 0
                    _crossema_near_miss.append((4, sym, [f"belum cross EMA20 (price {price_now:.4g} vs EMA20 {ef:.4g}, gap {gap_pct:.1f}%)"], 6))
                continue

            # Candle berjalan harus bullish (price > open candle ini)
            df_live = get_ohlcv(sym, interval="15m", limit=5)
            open_now = float(df_live.iloc[-1]["open"]) if df_live is not None and len(df_live) > 0 else price_now * 0.99
            if price_now <= open_now:
                if len(_crossema_near_miss) < 5:
                    _crossema_near_miss.append((5, sym, [f"candle belum bullish (price {price_now:.4g} vs open {open_now:.4g})"], 6))
                continue

            # LOLOS → OPEN DEAL
            atrp         = float(r["atr_pct"]) if not pd.isna(r.get("atr_pct")) else 3.0
            signal_price = float(r["close"])

            log(f"[T_CROSSEMA] SINYAL: {sym} price={price_now:.6g} "
                f"EMA20={ef:.6g} atr%={atrp:.2f} elapsed={elapsed_pct*100:.1f}%")

            ok, target_usd, add_usd = open_deal_with_sizing(sym, 0, strategy="brkX2_crossema")
            if not ok: continue

            entry_price = get_price_now(sym)
            if entry_price <= 0: entry_price = price_now
            slip_pct = (entry_price / signal_price - 1) * 100 if signal_price > 0 else 0.0

            add_to_active_deals(sym, {
                "strategy":         "brkX2_crossema",
                "entry_price":      entry_price,
                "peak":             entry_price,
                "signal_price":     signal_price,
                "atr_pct":          atrp,
                "opened_candle_ts": int(candle_open_ms),
                "trailing_armed":   False,
                "opened_at":        now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                "target_usd":       target_usd,
                "add_usd":          add_usd,
                "tf":               "4h",
            })

            prog = csv_progress("brkX2_crossema")
            send_telegram(
                f"OPEN LONG INTRABAR (CrossEMA Strategi #4 (4h))\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (EMA20 cross): {signal_price:.6g}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Elapsed: {elapsed_pct*100:.1f}% (menit ke ~{int(elapsed_pct*240)})\n"
                f"Slot crossema: {n_crossema+1}/{STRAT_CROSSEMA_MAX_DEALS}"
            )
            threading.Thread(target=send_email_open_long, args=("OPEN LONG INTRABAR CrossEMA-4h: " + to_display_pair(sym), 
                f"OPEN LONG INTRABAR (CrossEMA Strategi #4 (4h))\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {entry_price:.6g}\n"
                f"Harga sinyal (EMA20 cross): {signal_price:.6g}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Elapsed: {elapsed_pct*100:.1f}% (menit ke ~{int(elapsed_pct*240)})\n"
                f"Slot crossema: {n_crossema+1}/{STRAT_CROSSEMA_MAX_DEALS}"
            ), daemon=True).start()
            csv_log_open({
                "open_time_wib":  now_wib().strftime("%Y-%m-%d %H:%M:%S"),
                "symbol":         to_display_pair(sym),
                "signal_price":   f"{signal_price:.6g}",
                "entry_price":    f"{entry_price:.6g}",
                "slip_pct":       f"{slip_pct:+.2f}",
                "atr_pct":        f"{atrp:.2f}",
                "trail_dist_pct": f"{trailing_dist(atrp)}",
                "base_usd":       BASE_ORDER_VOLUME,
                "score":          0,
                "strategy":       "brkX2_crossema",
            })
            _crossema_last_candle_ts[sym] = candle_open_ms

            n_crossema += 1
            if n_crossema >= STRAT_CROSSEMA_MAX_DEALS: break

        except Exception as e:
            log(f"  [T_CROSSEMA] error {sym}: {e}")

    if _crossema_near_miss:
        log_near_miss("CrossEMA-4h", _crossema_near_miss, 3)
        update_dashboard_near_miss("CrossEMA-4h", _crossema_near_miss)

def run_thread_crossema():
    """Thread T_CROSSEMA: scan CrossEMA tiap STRAT_CROSSEMA_SCAN_INTERVAL detik (4 menit)."""
    while True:
        try:
            thread_crossema_scan()
        except Exception as e:
            log(f"WARN T_CROSSEMA error: {e}")
        time.sleep(STRAT_CROSSEMA_SCAN_INTERVAL)

# ══════════════════════════════════════════════════════════════════════════════
# WEB DASHBOARD — Flask server untuk monitoring dan kontrol deal
# ══════════════════════════════════════════════════════════════════════════════

DEAL_OVERRIDES_FILE = "/data/deal_overrides.json"
WEB_PORT = int(os.environ.get("PORT", 8080))

_dashboard_state = {
    "near_miss": {
        "brkX2-12h": [],
        "Reversal-8h": [],
        "brkX2-4h": [],
        "CrossEMA-4h": [],
    },
    "last_scan": {},
}
_dashboard_lock = threading.Lock()

# ── MANUAL SCAN STATE (brkX2-12h on-demand) ──────────────────────────────────
_manual_filters = {
    "vol":   True,
    "rsi":   True,
    "stoch": True,
    "atr":   True,
    "htf":   True,
    "perf":  True,
}
_manual_filters_lock = threading.Lock()
_manual_scan_result  = []   # list of dict per pair hasil scan terakhir
_manual_scan_ts      = ""   # waktu scan terakhir
_manual_scan_lock    = threading.Lock()


def update_dashboard_near_miss(strategi: str, items: list):
    with _dashboard_lock:
        parsed = [
            {
                "sym": item[1] if len(item) > 1 and isinstance(item[1], str) else (item[0] if isinstance(item[0], str) else "?"),
                "n_pass": item[0] if isinstance(item[0], int) else 0,
                "total": item[3] if len(item) > 3 else 9,
                "fails": item[2] if len(item) > 2 else [],
                "vol_ratio": item[4] if len(item) > 4 else None,
            }
            for item in items
        ]
        # Urutkan berdasarkan n_pass descending (terbanyak lolos di atas)
        parsed.sort(key=lambda x: -x["n_pass"])
        _dashboard_state["near_miss"][strategi] = parsed[:10]
        _dashboard_state["last_scan"][strategi] = now_wib().strftime("%H:%M:%S")

def load_deal_overrides() -> dict:
    try:
        if os.path.exists(DEAL_OVERRIDES_FILE):
            with open(DEAL_OVERRIDES_FILE) as f:
                return json.load(f)
    except: pass
    return {}

def save_deal_overrides(overrides: dict):
    try:
        with open(DEAL_OVERRIDES_FILE, "w") as f:
            json.dump(overrides, f, indent=2)
    except Exception as e:
        log(f"WARN save_deal_overrides: {e}")

def get_deal_override(sym: str, key: str, default: bool = True) -> bool:
    return load_deal_overrides().get(sym, {}).get(key, default)

# ── Inline JS untuk dashboard (ASCII-only, served via /dash.js) ──────────────
_DASH_JS = 'var _refreshTimer=null;\nvar _curStrat=\'brkX2-12h\';\nfunction startRefresh(){if(_refreshTimer)return;_refreshTimer=setInterval(function(){window.location.reload();},30000);}\nfunction stopRefresh(){if(_refreshTimer){clearInterval(_refreshTimer);_refreshTimer=null;}}\nfunction isPauseChecked(){var cb=document.getElementById(\'cb-pause-refresh\');return cb&&cb.checked;}\nfunction pauseRefresh(){stopRefresh();}\nfunction resumeRefresh(){if(!isPauseChecked())startRefresh();}\nfunction onPauseRefreshToggle(checked){if(checked){stopRefresh();}else{startRefresh();}}\n\n// Definisi secondary per strategi\nvar STRAT_SECONDARY={\n  \'brkX2-12h\':[\n    {key:\'vol\',label:\'Vol 0.6x--5.0xMA\'},{key:\'rsi\',label:\'RSI<75\'},\n    {key:\'stoch\',label:\'Stoch%K<70\'},{key:\'atr\',label:\'ATR%<9%\'},\n    {key:\'htf\',label:\'HTF 3D vol>0.8xMA\'},{key:\'perf\',label:\'Perf>=0.5\'}\n  ],\n  \'Reversal-8h T1\':[\n    {key:\'ha_bull\',label:\'c+1 HA bullish\'},{key:\'cross\',label:\'cross-up EMA20\'},\n    {key:\'perf\',label:\'Perf>=0.5\'},{key:\'vol24\',label:\'Vol24h>=$1.5jt\'}\n  ],\n  \'Reversal-8h T3-REV\':[\n    {key:\'elapsed\',label:\'Elapsed 5%-50%\'},{key:\'cross_live\',label:\'price_now>EMA20\'},\n    {key:\'perf\',label:\'Perf>=0.5\'},{key:\'vol24\',label:\'Vol24h>=$1.5jt\'}\n  ],\n  \'brkX2-4h\':[\n    {key:\'vol\',label:\'Vol>=0.25xMA\'},{key:\'stoch\',label:\'Stoch%K<80\'},\n    {key:\'htf\',label:\'HTF12h vol>2.0xMA\'},{key:\'perf\',label:\'Perf>=0.5\'}\n  ],\n  \'CrossEMA-4h\':[\n    {key:\'vol\',label:\'Vol>=0.4xMA\'},{key:\'htf\',label:\'HTF12h vol>1.5xMA\'},\n    {key:\'vol24\',label:\'Vol24h>=$1.0jt\'}\n  ]\n};\n\nfunction onStratSelect(strat){\n  _curStrat=strat;\n  // Update dropdown kandidat\n  var opts=document.querySelectorAll(\'.nm-opt\');\n  var count=0;\n  opts.forEach(function(o){\n    var show=o.getAttribute(\'data-strat\')===strat;\n    o.style.display=show?\'\':\'none\';\n    if(show)count++;\n  });\n  document.getElementById(\'nm-count\').textContent=\'(\'+count+\' kandidat dari scan terakhir)\';\n  // Reset pair select\n  var sel=document.getElementById(\'pair-select\');if(sel)sel.value=\'\';\n  // Reset panel\n  var panel=document.getElementById(\'pair-detail\');if(panel)panel.style.display=\'none\';\n  // Update secondary grid\n  renderSecondaryGrid(strat);\n  // Reset primary status\n  var ps=document.getElementById(\'primary-status\');\n  if(ps)ps.innerHTML=\'<span style="color:var(--muted)">-- pilih pair untuk lihat nilai aktual --</span>\';\n}\n\nfunction renderSecondaryGrid(strat){\n  var grid=document.getElementById(\'secondary-grid\');\n  if(!grid)return;\n  var defs=STRAT_SECONDARY[strat]||[];\n  grid.innerHTML=defs.map(function(d){\n    return \'<div class="sec-item" data-key="\'+d.key+\'"><label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:11px"><input type="checkbox" class="sec-cb" data-key="\'+d.key+\'" checked style="cursor:pointer"><span class="sec-label">\'+d.label+\'</span><span class="sec-actual" style="color:var(--muted)">--</span><span class="sec-status">--</span></label></div>\';\n  }).join(\'\');\n  // Re-attach event listeners\n  grid.querySelectorAll(\'.sec-cb\').forEach(function(cb){\n    cb.addEventListener(\'change\',function(){\n      fetch(\'/manual_filter\',{method:\'POST\',headers:{\'Content-Type\':\'application/x-www-form-urlencoded\'},body:\'key=\'+this.dataset.key+\'&value=\'+this.checked});\n    });\n  });\n}\n\ndocument.addEventListener(\'DOMContentLoaded\',function(){\n  startRefresh();\n  onStratSelect(\'brkX2-12h\');\n});\n\nfunction onPairSelect(sym){\n  var panel=document.getElementById(\'pair-detail\');\n  if(!sym){panel.style.display=\'none\';return;}\n  panel.style.display=\'block\';\n  panel.innerHTML=\'Mengambil data \'+sym.replace(\'USDT\',\'/USDT\')+\'...\';\n  pauseRefresh();\n  fetch(\'/api/strategy_detail?sym=\'+encodeURIComponent(sym)+\'&strat=\'+encodeURIComponent(_curStrat))\n    .then(function(r){return r.json();})\n    .then(function(d){\n      resumeRefresh();\n      if(d.error){panel.innerHTML=\'Error: \'+d.error;return;}\n      // Update primary\n      var ps=document.getElementById(\'primary-status\');\n      ps.innerHTML=d.primary.map(function(p){return badge(p.ok,p.label+\' (\'+p.actual+\')\');}).join(\' \');\n      // Update secondary\n      d.secondary.forEach(function(s){updateSec(s.key,s.actual,s.ok);});\n      // Panel ringkasan\n      var allP=d.primary_ok;\n      panel.innerHTML=\'<b style="color:\'+(allP?\'var(--green)\':\'var(--red)\')+\'">\'+sym.replace(\'USDT\',\'/USDT\')+\'</b> | \'+\n        d.primary.map(function(p){return (p.ok?\'<span style="color:var(--green)">\':\'<span style="color:var(--red)">\') + p.label+\': \'+p.actual+\'</span>\';}).join(\' | \')+\n        \' | \'+(allP?\'<span style="color:var(--green)">Primary OK</span>\':\'<span style="color:var(--red)">Primary GAGAL</span>\');\n    })\n    .catch(function(e){resumeRefresh();panel.innerHTML=\'Error: \'+e;});\n}\n\nfunction updateSec(key,actual,ok){\n  document.querySelectorAll(\'.sec-item[data-key="\'+key+\'"]\').forEach(function(item){\n    var a=item.querySelector(\'.sec-actual\'),s=item.querySelector(\'.sec-status\');\n    if(a)a.textContent=\'(skrg \'+actual+\')\';\n    if(s)s.innerHTML=ok?\'<span style="color:var(--green)">OK</span>\':\'<span style="color:var(--red)">X</span>\';\n  });\n}\n\nfunction doManualScan(){\n  var btn=document.getElementById(\'btn-scan\'),st=document.getElementById(\'scan-status\');\n  btn.disabled=true;btn.textContent=\'Scanning...\';\n  st.textContent=\'Sedang scan semua pair... (30-60 detik)\';\n  pauseRefresh();\n  fetch(\'/manual_scan\',{method:\'POST\'}).then(function(r){return r.json();}).then(function(data){\n    btn.disabled=false;btn.textContent=\'Scan Sekarang\';\n    st.textContent=\'Selesai \'+data.ts+\' -- \'+data.pairs.length+\' pair dievaluasi\';\n    renderResults(data.pairs);resumeRefresh();\n  }).catch(function(e){btn.disabled=false;btn.textContent=\'Scan Sekarang\';st.textContent=\'Error: \'+e;resumeRefresh();});\n}\n\nfunction promptOpenLong(){\n  var sel=document.getElementById(\'pair-select\');\n  var sym=sel?sel.value:\'\';\n  if(!sym){alert(\'Pilih pair dari dropdown dulu.\');return;}\n  if(!confirm(\'Open Long: \'+sym.replace(\'USDT\',\'/USDT\')+\'?\'))return;\n  execOpenLong(sym);\n}\n\nfunction execOpenLong(sym){\n  var fd=new FormData();fd.append(\'sym\',sym);\n  var st=document.getElementById(\'scan-status\');\n  if(st)st.textContent=\'Membuka deal \'+sym+\'...\';\n  pauseRefresh();\n  fetch(\'/manual_open\',{method:\'POST\',body:fd}).then(function(r){return r.json();}).then(function(data){\n    resumeRefresh();\n    var msg=data.ok?(\'BERHASIL: \'+sym+\' Score=\'+data.score+\' Target=$\'+data.target_usd):(\'GAGAL: \'+data.error);\n    if(st)st.textContent=msg;alert(msg);\n  }).catch(function(e){resumeRefresh();alert(\'Error: \'+e);});\n}\n\nfunction renderResults(pairs){\n  var el=document.getElementById(\'scan-results\');\n  var sample=pairs.find(function(p){return p.primary_ok;})||pairs[0];\n  if(sample){\n    document.getElementById(\'primary-status\').innerHTML=\n      sample.secondaries?sample.secondaries.map(function(s){return badge(s.ok,s.key+\':\'+s.actual);}).join(\' \'):\'\';\n    if(sample.secondaries)sample.secondaries.forEach(function(s){updateSec(s.key,s.actual,s.ok);});\n  }\n  var cands=pairs.filter(function(p){return p.primary_ok;}).slice(0,20);\n  if(cands.length===0){el.innerHTML=\'<div class="empty">Tidak ada pair lolos syarat primary.</div>\';return;}\n  var rows=cands.map(function(p){\n    var sb=p.secondaries.map(function(s){return \'<span style="color:\'+(s.ok?\'var(--green)\':\'var(--red)\')+\';font-size:10px">\'+s.key+\':\'+s.actual+\'</span>\';}).join(\' \');\n    var ab=p.all_ok?\'<span style="color:var(--green);font-weight:600">LOLOS</span>\':\'<span style="color:var(--yellow)">primary OK</span>\';\n    var ob=\'<button onclick="execOpenLong(this.dataset.sym)" data-sym="\'+p.sym+\'" style="background:\'+(p.all_ok?\'var(--green)\':\'var(--yellow)\')+\';color:#000;border:none;border-radius:3px;padding:3px 8px;font-size:10px;cursor:pointer">\'+(p.all_ok?\'Open Sekarang\':\'Open & Bypass\')+\'</button>\';\n    return \'<tr><td class="sym">\'+p.sym.replace(\'USDT\',\'/USDT\')+\'</td><td>\'+ab+\'</td><td style="font-size:10px">\'+sb+\'</td><td>\'+ob+\'</td></tr>\';\n  }).join(\'\');\n  el.innerHTML=\'<table><thead><tr><th>Pair</th><th>Status</th><th>Secondary</th><th>Aksi</th></tr></thead><tbody>\'+rows+\'</tbody></table>\';\n}\n\nfunction badge(ok,label){return \'<span style="color:\'+(ok?\'var(--green)\':\'var(--red)\')+\';font-size:11px">[\'+(ok?\'OK\':\'X\')+\'] \'+label+\'</span>\';}\nfunction fmt(v){if(v===undefined||v===null)return \'?\';if(v>1000)return v.toFixed(0);if(v>1)return v.toFixed(3);return v.toPrecision(4);}\nfunction doOpenLong(sym){execOpenLong(sym);}\n\nfunction editEntry(sym,curVal){\n  var v=prompt(\'Edit entry price untuk \'+sym.replace(\'USDT\',\'/USDT\')+\':\\n(harga aktual dari 3Commas)\',curVal);\n  if(v===null)return;\n  v=parseFloat(v);\n  if(isNaN(v)||v<=0){alert(\'Nilai tidak valid\');return;}\n  if(!confirm(\'Set entry \'+sym.replace(\'USDT\',\'/USDT\')+\' = \'+v+\'?\'))return;\n  var fd=new FormData();fd.append(\'sym\',sym);fd.append(\'field\',\'entry_price\');fd.append(\'value\',v);\n  pauseRefresh();\n  fetch(\'/edit_deal\',{method:\'POST\',body:fd})\n    .then(function(r){return r.json();})\n    .then(function(data){\n      resumeRefresh();\n      if(data.ok){\n        var el=document.getElementById(\'ep-\'+sym);\n        if(el)el.textContent=v;\n        alert(\'Entry \'+sym.replace(\'USDT\',\'/USDT\')+\' diupdate ke \'+v);\n      } else {\n        alert(\'Gagal: \'+data.error);\n      }\n    })\n    .catch(function(e){resumeRefresh();alert(\'Error: \'+e);});\n}\n'

DASHBOARD_HTML = '''<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<!-- auto-refresh dikendalikan via JS -->
<title>Bot Dashboard</title>
<style>
  :root{--bg:#0f1117;--surface:#1a1d2e;--border:#2a2d3e;--accent:#4f9eff;--green:#00c896;--red:#ff4f6a;--yellow:#ffb84f;--text:#e2e8f0;--muted:#8892a4;--font:'SF Mono','Fira Code',monospace}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:var(--font);font-size:13px}
  .header{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 20px;display:flex;align-items:center;gap:16px}
  .header h1{font-size:15px;color:var(--accent);letter-spacing:.05em}
  .header .status{font-size:11px;color:var(--muted)}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;margin-right:6px;animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  .container{max-width:1200px;margin:0 auto;padding:20px}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:16px}
  .card-header{padding:10px 16px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}
  .card-header h2{font-size:12px;color:var(--accent);text-transform:uppercase;letter-spacing:.08em}
  .card-header .scan-time{font-size:10px;color:var(--muted)}
  .card-body{padding:12px 16px}
  table{width:100%;border-collapse:collapse}
  th{text-align:left;padding:6px 8px;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid var(--border)}
  td{padding:8px;border-bottom:1px solid #1e2133;vertical-align:top}
  tr:last-child td{border-bottom:none}
  .sym{color:var(--accent);font-weight:600}
  .score{color:var(--yellow)}
  .fails{color:var(--muted);font-size:11px;line-height:1.5}
  .profit-pos{color:var(--green)}
  .profit-neg{color:var(--red)}
  .badge{display:inline-block;padding:2px 6px;border-radius:3px;font-size:10px}
  .badge-armed{background:rgba(0,200,150,.15);color:var(--green)}
  .badge-wait{background:rgba(255,184,79,.15);color:var(--yellow)}
  .empty{color:var(--muted);font-size:12px;padding:12px 0;text-align:center}
  .section-title{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:12px;padding-bottom:6px;border-bottom:1px solid var(--border)}
  @media(max-width:768px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body>
<div class="header">
  <span class="dot"></span>
  <h1>TRADING BOT DASHBOARD</h1>
  <span class="status">Refresh dalam <span id="cd">30</span>s &nbsp;|&nbsp; {{ now }}</span>
<script>
  var s=30;setInterval(function(){if(typeof _refreshTimer!=='undefined'&&_refreshTimer){s--;if(s<0)s=30;}document.getElementById('cd').textContent=s;},1000);
</script>
</div>
<div class="container">
  <div class="card">
    <div class="card-header">
      <h2>Active Deals ({{ active_count }})</h2>
    </div>
    <div class="card-body">
    {% if active_deals %}
    <table>
      <thead><tr><th>Pair</th><th>Strategi</th><th>Entry</th><th>Harga Skrg</th><th>Profit</th><th>Status</th><th>Auto Add Fund</th><th>Auto Close</th></tr></thead>
      <tbody>
      {% for sym, d in active_deals.items() %}
      <tr>
        <td class="sym">{{ sym.replace("USDT","/USDT") }}</td>
        <td>{{ d.get("strategy","-") }}</td>
        <td>
          <span id="ep-{{ sym }}" style="cursor:pointer;text-decoration:underline dotted" title="Klik untuk edit" onclick="editEntry('{{ sym }}','{{ \"%.6g\"|format(d.get(\"entry_price\",0)) }}')">{{ "%.4g"|format(d.get("entry_price",0)) }}</span>
        </td>
        <td>{{ "%.4g"|format(d.get("last_price",0)) if d.get("last_price") else "-" }}</td>
        <td class="{{ "profit-pos" if d.get("upnl_pct",0) > 0 else "profit-neg" }}">{{ "%+.2f"|format(d.get("upnl_pct",0)) }}%</td>
        <td>{% if d.get("trailing_armed") %}<span class="badge badge-armed">ARMED</span>{% else %}<span class="badge badge-wait">WAIT</span>{% endif %}</td>
        <td>
          <form method="POST" action="/toggle" style="display:inline">
            <input type="hidden" name="sym" value="{{ sym }}">
            <input type="hidden" name="key" value="auto_add_fund">
            <input type="checkbox" name="value" onchange="this.form.submit()" {{ "checked" if overrides.get(sym,{}).get("auto_add_fund",True) else "" }}>
          </form>
        </td>
        <td>
          <form method="POST" action="/toggle" style="display:inline">
            <input type="hidden" name="sym" value="{{ sym }}">
            <input type="hidden" name="key" value="auto_close">
            <input type="checkbox" name="value" onchange="this.form.submit()" {{ "checked" if overrides.get(sym,{}).get("auto_close",True) else "" }}>
          </form>
        </td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
    {% else %}
    <div class="empty">Tidak ada deal aktif saat ini</div>
    {% endif %}
    </div>
  </div>
  <!-- ═══════════════ MANUAL SCAN ═══════════════ -->
  <div class="section-title">Manual Scan</div>
  <div class="card" style="margin-bottom:16px">
    <div class="card-body">
      <!-- Dropdown strategi -->
      <div style="margin-bottom:12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <label style="font-size:11px;color:var(--muted)">Strategi:</label>
        <select id="strat-select" onchange="onStratSelect(this.value)" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:5px 10px;font-size:12px;font-family:var(--font);cursor:pointer;min-width:180px">
          <option value="brkX2-12h">brkX2-12h</option>
          <option value="Reversal-8h T1">Reversal-8h T1</option>
          <option value="Reversal-8h T3-REV">Reversal-8h T3-REV</option>
          <option value="brkX2-4h">brkX2-4h</option>
          <option value="CrossEMA-4h">CrossEMA-4h</option>
        </select>
      </div>
      <!-- Dropdown kandidat dinamis per strategi -->
      {% set all_nm = {
        "brkX2-12h": near_miss.get("brkX2-12h", []),
        "Reversal-8h T1": near_miss.get("Reversal-8h", []),
        "Reversal-8h T3-REV": near_miss.get("Reversal-8h", []),
        "brkX2-4h": near_miss.get("brkX2-4h", []),
        "CrossEMA-4h": near_miss.get("CrossEMA-4h", [])
      } %}
      <div style="margin-bottom:12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <label style="font-size:11px;color:var(--muted)">Pilih pair kandidat:</label>
        <select id="pair-select" onchange="onPairSelect(this.value)" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:5px 10px;font-size:12px;font-family:var(--font);cursor:pointer;min-width:160px">
          <option value="">-- pilih pair --</option>
          {% for stk, kandidat in all_nm.items() %}{% for item in kandidat %}
          <option value="{{ item.sym }}" class="nm-opt" data-strat="{{ stk }}" style="display:none">{{ item.sym.replace("USDT","/USDT") }} ({{ item.n_pass }}/{{ item.total }})</option>
          {% endfor %}{% endfor %}
        </select>
        <span id="nm-count" style="font-size:10px;color:var(--muted)"></span>
      </div>
      <!-- Pair detail panel -->
      <div id="pair-detail" style="margin-bottom:12px;display:none;background:rgba(255,255,255,0.03);border:1px solid var(--border);border-radius:6px;padding:10px 14px;font-size:11px"></div>
      <!-- Primary conditions (always required) -->
      <div style="margin-bottom:10px">
        <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">Primary (selalu wajib)</div>
        <div id="primary-status" style="display:flex;flex-wrap:wrap;gap:8px;font-size:11px">
          <span style="color:var(--muted)">-- klik Scan untuk lihat nilai aktual --</span>
        </div>
      </div>
      <!-- Secondary conditions (toggleable) -->
      <div style="margin-bottom:12px">
        <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">Secondary (toggle ON/OFF)</div>
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:6px" id="secondary-grid">
          <div class="sec-item" data-key="vol">
            <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:11px">
              <input type="checkbox" class="sec-cb" data-key="vol" checked style="cursor:pointer">
              <span class="sec-label">Vol 0.6x--5.0xMA</span>
              <span class="sec-actual" style="color:var(--muted)">--</span>
              <span class="sec-status">--</span>
            </label>
          </div>
          <div class="sec-item" data-key="rsi">
            <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:11px">
              <input type="checkbox" class="sec-cb" data-key="rsi" checked style="cursor:pointer">
              <span class="sec-label">RSI &lt; 75</span>
              <span class="sec-actual" style="color:var(--muted)">--</span>
              <span class="sec-status">--</span>
            </label>
          </div>
          <div class="sec-item" data-key="stoch">
            <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:11px">
              <input type="checkbox" class="sec-cb" data-key="stoch" checked style="cursor:pointer">
              <span class="sec-label">Stoch%K &lt; 70</span>
              <span class="sec-actual" style="color:var(--muted)">--</span>
              <span class="sec-status">--</span>
            </label>
          </div>
          <div class="sec-item" data-key="atr">
            <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:11px">
              <input type="checkbox" class="sec-cb" data-key="atr" checked style="cursor:pointer">
              <span class="sec-label">ATR% &lt; 9%</span>
              <span class="sec-actual" style="color:var(--muted)">--</span>
              <span class="sec-status">--</span>
            </label>
          </div>
          <div class="sec-item" data-key="htf">
            <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:11px">
              <input type="checkbox" class="sec-cb" data-key="htf" checked style="cursor:pointer">
              <span class="sec-label">HTF 3D vol &gt; 0.8xMA</span>
              <span class="sec-actual" style="color:var(--muted)">--</span>
              <span class="sec-status">--</span>
            </label>
          </div>
          <div class="sec-item" data-key="perf">
            <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:11px">
              <input type="checkbox" class="sec-cb" data-key="perf" checked style="cursor:pointer">
              <span class="sec-label">Perf Grade &gt;= 0.5</span>
              <span class="sec-actual" style="color:var(--muted)">--</span>
              <span class="sec-status">--</span>
            </label>
          </div>
        </div>
      </div>
      <!-- Scan button + status -->
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;flex-wrap:wrap">
        <button id="btn-scan" onclick="doManualScan()" style="background:var(--accent);color:#fff;border:none;border-radius:4px;padding:7px 18px;font-size:12px;cursor:pointer;font-family:var(--font)">🔍 Scan Sekarang</button>
        <button id="btn-open-prompt" onclick="promptOpenLong()" style="background:var(--green);color:#000;border:none;border-radius:4px;padding:7px 18px;font-size:12px;cursor:pointer;font-family:var(--font)">Open Sekarang</button>
        <span id="scan-status" style="font-size:11px;color:var(--muted)"></span>
        <label style="display:flex;align-items:center;gap:6px;font-size:11px;cursor:pointer;margin-left:8px" title="Centang untuk menghentikan auto-refresh selama analisis">
          <input type="checkbox" id="cb-pause-refresh" onchange="onPauseRefreshToggle(this.checked)" style="cursor:pointer">
          <span>Pause auto-refresh</span>
        </label>
      </div>
      <!-- Scan results table -->
      <div id="scan-results"></div>
    </div>
  </div>

  <div class="section-title">Kandidat Terdekat per Strategi</div>
  <div class="grid">
  {% for strategi, items in near_miss.items() %}
  <div class="card">
    <div class="card-header">
      <h2>{{ strategi }}</h2>
      <span class="scan-time">Scan: {{ last_scan.get(strategi,"-") }}</span>
    </div>
    <div class="card-body">
    {% if items %}
    <table>
      <thead><tr><th>Pair</th><th>Lolos</th><th>Belum</th></tr></thead>
      <tbody>
      {% for item in items %}
      <tr>
        <td class="sym">{{ item.sym.replace("USDT","/USDT") }}</td>
        <td class="score">{{ item.n_pass }}/{{ item.total }}</td>
        <td class="fails">{{ ("; ".join(item.fails[:2]))|e }}{% if item.fails|length > 2 %} +{{ item.fails|length - 2 }} lagi{% endif %}</td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
    {% else %}
    <div class="empty">{{ window_info.get(strategi, "Belum ada data scan")|e }}</div>
    {% endif %}
    </div>
  </div>
  {% endfor %}
  </div>
</div>

<script src="/dash.js?v=1785557732"></script>
</body>
</html>
'''

def run_manual_scan() -> dict:
    """Scan on-demand brkX2-12h dengan filter manual dari _manual_filters.
    Return dict: {
        "ts": str,
        "pairs": [ { sym, close, ema20, ema50, hh, st_dir,
                     vol_ratio, rsi, stoch_k, atr_pct, htf_ratio, perf_score,
                     primary_ok, params } ]
    }
    """
    global _manual_scan_result, _manual_scan_ts
    with _manual_filters_lock:
        filters = dict(_manual_filters)

    pairs = get_usdt_spot_pairs()
    if not pairs:
        return {"ts": now_wib().strftime("%H:%M:%S"), "pairs": [], "error": "Gagal ambil pair"}
    ticker = get_ticker_24h()
    volmap = {}
    for t in (ticker or []):
        try: volmap[t['symbol']] = float(t.get('quoteVolume', 0))
        except: pass
    universe = [p for p in pairs if volmap.get(p, 0) >= MIN_VOLUME_USD]

    results = []
    with active_deals_lock:
        existing = set(active_deals.keys())

    for sym in universe:
        if sym in existing: continue
        try:
            df = get_ohlcv(sym, limit=120)
            if df is None: continue
            if df['ct'].iloc[-1] >= int(time.time() * 1000):
                df = df.iloc[:-1]
                if len(df) < 60: continue
            df = compute_indicators(df)
            row = df.iloc[-1]
            if pd.isna(row.get('ema_fast')) or pd.isna(row.get('ema_slow')) or pd.isna(row.get('hh')): continue

            close   = float(row['close'])
            ema20   = float(row['ema_fast'])
            ema50   = float(row['ema_slow'])
            hh      = float(row['hh'])
            st_dir  = int(row['st_dir']) if not pd.isna(row.get('st_dir')) else 0
            vol_ma  = float(row['vol_ma']) if not pd.isna(row.get('vol_ma')) and row['vol_ma'] > 0 else 0
            vol_ratio = float(row['vol']) / vol_ma if vol_ma > 0 else 0
            rsi     = float(row['rsi']) if not pd.isna(row.get('rsi')) else None
            stoch_k = float(row['stoch_k']) if 'stoch_k' in row and not pd.isna(row.get('stoch_k')) else None
            atr_pct = float(row['atr_pct']) if not pd.isna(row.get('atr_pct')) else None
            candle_ts = int(df['ct'].iloc[-1])

            # HTF ratio
            htf_ratio = htf_vol_ratio(sym, HTF_TIMEFRAME, HTF_CANDLE_LIMIT, HTF_VOL_MA_PERIOD)
            # Perf score
            perf_score = calc_perf_score(sym, candle_ts)
            if pd.isna(perf_score): perf_score = None

            # 4 PRIMARY — selalu wajib
            p1_ok = st_dir == 1
            p2_ok = close > ema20
            p3_ok = close > ema50
            p4_ok = close > hh
            p5_ok = vol_ratio <= VOL_MAX_MULT  # batas atas vol — selalu wajib
            primary_ok = p1_ok and p2_ok and p3_ok and p4_ok and p5_ok

            # 6 SECONDARY — nilai & status
            s_vol   = {"key":"vol",   "label":f"Vol>={VOLUME_MULT}xMA",  "threshold":f"{VOLUME_MULT}xMA", "actual":f"{vol_ratio:.2f}x",   "ok": vol_ratio >= VOLUME_MULT,  "enabled": filters["vol"]}
            s_rsi   = {"key":"rsi",   "label":f"RSI<{RSI_MAX}",          "threshold":str(RSI_MAX),        "actual":f"{rsi:.1f}" if rsi is not None else "n/a",   "ok": rsi is not None and rsi <= RSI_MAX,            "enabled": filters["rsi"]}
            s_stoch = {"key":"stoch", "label":f"Stoch%K<{STOCH_MAX}",    "threshold":str(STOCH_MAX),      "actual":f"{stoch_k:.1f}" if stoch_k is not None else "n/a", "ok": stoch_k is not None and stoch_k < STOCH_MAX, "enabled": filters["stoch"]}
            s_atr   = {"key":"atr",   "label":f"ATR%<{ATR_MAX_PCT}%",    "threshold":f"{ATR_MAX_PCT}%",   "actual":f"{atr_pct:.1f}%" if atr_pct is not None else "n/a", "ok": atr_pct is not None and atr_pct < ATR_MAX_PCT, "enabled": filters["atr"]}
            s_htf   = {"key":"htf",   "label":f"HTF3D vol>{HTF_VOL_MULT}xMA", "threshold":f"{HTF_VOL_MULT}xMA", "actual":f"{htf_ratio:.2f}x" if htf_ratio >= 0 else "n/a", "ok": htf_ratio >= HTF_VOL_MULT,              "enabled": filters["htf"]}
            s_perf  = {"key":"perf",  "label":f"Perf>={PERF_SCORE_MIN}", "threshold":str(PERF_SCORE_MIN), "actual":f"{perf_score:.2f}" if perf_score is not None else "n/a", "ok": perf_score is not None and perf_score >= PERF_SCORE_MIN, "enabled": filters["perf"]}
            secondaries = [s_vol, s_rsi, s_stoch, s_atr, s_htf, s_perf]

            # secondary lolos = ok ATAU tidak di-enable
            secondary_ok = all((s["ok"] if s["enabled"] else True) for s in secondaries)
            all_ok = primary_ok and secondary_ok

            # Konversi numpy/pandas types ke Python native agar JSON serializable
            for s in secondaries:
                s["ok"] = bool(s["ok"])
                s["enabled"] = bool(s["enabled"])
            results.append({
                "sym":         sym,
                "close":       float(close),
                "ema20":       float(ema20),
                "ema50":       float(ema50),
                "hh":          float(hh),
                "st_dir":      int(st_dir),
                "primary_ok":  bool(primary_ok),
                "secondary_ok":bool(secondary_ok),
                "all_ok":      bool(all_ok),
                "secondaries": secondaries,
                "p1_ok": bool(p1_ok), "p2_ok": bool(p2_ok), "p3_ok": bool(p3_ok), "p4_ok": bool(p4_ok),
            })
        except Exception as e:
            log(f"  [MANUAL] error {sym}: {e}")

    # Sort: all_ok dulu, lalu primary_ok, lalu jumlah secondary ok
    results.sort(key=lambda x: (not x["all_ok"], not x["primary_ok"],
                                 -sum(1 for s in x["secondaries"] if s["ok"])))

    ts = now_wib().strftime("%H:%M:%S")
    with _manual_scan_lock:
        _manual_scan_result = results
        _manual_scan_ts = ts

    return {"ts": ts, "pairs": results}


def run_web_dashboard():
    """Thread web dashboard Flask."""
    try:
        try:
            from flask import Flask, render_template_string, request, redirect, jsonify
        except ImportError:
            import subprocess
            subprocess.run(["pip", "install", "flask", "--quiet", "--break-system-packages"],
                          capture_output=True)
            from flask import Flask, render_template_string, request, redirect, jsonify

        app = Flask(__name__)

        app.secret_key = os.urandom(24)

        @app.route("/")
        def index():
            with active_deals_lock:
                deals = dict(active_deals)
            deals_display = {}
            for sym, d in deals.items():
                dd = dict(d)
                ep = dd.get("entry_price", 0)
                lp = dd.get("last_price", ep)
                dd["upnl_pct"] = (lp/ep - 1)*100 if ep > 0 else 0
                deals_display[sym] = dd
            with _dashboard_lock:
                nm = dict(_dashboard_state["near_miss"])
                ls = dict(_dashboard_state["last_scan"])
            overrides = load_deal_overrides()
            # Hitung elapsed candle saat ini untuk info window
            now_ms = int(time.time() * 1000)
            el_12h = (now_ms % (SECONDS_PER_CANDLE * 1000)) / (SECONDS_PER_CANDLE * 1000)
            el_4h  = (now_ms % (STRAT4H_SECONDS * 1000)) / (STRAT4H_SECONDS * 1000)
            el_8h  = (now_ms % (REVERSAL_SECONDS_PER_CANDLE * 1000)) / (REVERSAL_SECONDS_PER_CANDLE * 1000)
            def _mnt(pct, sec): return int(pct * sec / 60)
            window_info = {
                "brkX2-12h": (
                    f"Scan tiap candle 12h tutup. "
                    f"Intrabar EARLY menit {_mnt(INTRABAR_EARLY_ENTRY_PCT, SECONDS_PER_CANDLE/60*60)}-"
                    f"{_mnt(INTRABAR_EARLY_END_PCT, SECONDS_PER_CANDLE/60*60)} & "
                    f"BASE menit {_mnt(INTRABAR_ENTRY_PCT, SECONDS_PER_CANDLE/60*60)}-"
                    f"{_mnt(INTRABAR_WINDOW_END, SECONDS_PER_CANDLE/60*60)}. "
                    f"Elapsed skrg: {el_12h*100:.1f}%"
                ),
                "Reversal-8h": (
                    f"Scan tiap candle 8h tutup. "
                    f"Intrabar menit 24-240 (5%-50% elapsed). "
                    f"Elapsed skrg: {el_8h*100:.1f}%"
                ),
                "brkX2-4h": (
                    f"Scan hanya menit ke 5-60 candle 4h (2%-25% elapsed). "
                    f"Elapsed skrg: {el_4h*100:.1f}% — "
                    + ("dalam window, data segera muncul." if STRAT4H_ENTRY_MIN_PCT <= el_4h <= STRAT4H_ENTRY_MAX_PCT
                       else f"tunggu candle berikutnya menit ke {int(STRAT4H_ENTRY_MIN_PCT*240)}-{int(STRAT4H_ENTRY_MAX_PCT*240)}.")
                ),
                "CrossEMA-4h": (
                    f"Scan hanya menit ke 5-60 candle 4h (2%-25% elapsed). "
                    f"Elapsed skrg: {el_4h*100:.1f}% — "
                    + ("dalam window, data segera muncul." if STRAT_CROSSEMA_ENTRY_MIN <= el_4h <= STRAT_CROSSEMA_ENTRY_MAX
                       else f"tunggu candle berikutnya menit ke {int(STRAT_CROSSEMA_ENTRY_MIN*240)}-{int(STRAT_CROSSEMA_ENTRY_MAX*240)}.")
                ),
            }
            return render_template_string(
                DASHBOARD_HTML,
                active_deals=deals_display,
                active_count=len(deals_display),
                near_miss=nm,
                last_scan=ls,
                overrides=overrides,
                now=now_wib().strftime("%d/%m %H:%M:%S WIB"),
                window_info=window_info,
            )

        @app.route("/edit_deal", methods=["POST"])
        def edit_deal():
            sym   = request.form.get("sym", "").upper().strip()
            field = request.form.get("field", "entry_price")
            try:
                val = float(request.form.get("value", "0"))
            except:
                return jsonify({"ok": False, "error": "nilai tidak valid"})
            if not sym:
                return jsonify({"ok": False, "error": "sym kosong"})
            with active_deals_lock:
                if sym not in active_deals:
                    return jsonify({"ok": False, "error": f"{sym} tidak ada di active_deals"})
                active_deals[sym][field] = val
                if field == "entry_price":
                    # Update peak juga kalau peak < entry baru
                    if active_deals[sym].get("peak", 0) < val:
                        active_deals[sym]["peak"] = val
                d = dict(active_deals)
            # Simpan ke file
            try:
                with open(ACTIVE_DEALS_FILE, "w") as f:
                    import json as _j; _j.dump(d, f)
            except Exception as e:
                log(f"[EDIT_DEAL] Gagal simpan file: {e}")
            log(f"[EDIT_DEAL] {sym}.{field} = {val}")
            return jsonify({"ok": True, "sym": sym, "field": field, "value": val})

        @app.route("/toggle", methods=["POST"])
        def toggle():
            sym = request.form.get("sym", "")
            key = request.form.get("key", "")
            value = "value" in request.form
            if sym and key:
                overrides = load_deal_overrides()
                if sym not in overrides:
                    overrides[sym] = {}
                overrides[sym][key] = value
                save_deal_overrides(overrides)
            return redirect("/")

        @app.route("/dash.js")
        def dash_js():
            from flask import Response
            js = open('/app/dash.js', encoding='utf-8').read() if __import__('os').path.exists('/app/dash.js') else _DASH_JS
            return Response(js, mimetype='application/javascript; charset=utf-8')

        @app.route("/api/state")
        def api_state():
            with active_deals_lock:
                deals = dict(active_deals)
            with _dashboard_lock:
                nm = dict(_dashboard_state["near_miss"])
            return jsonify({"active_deals": deals, "near_miss": nm})

        @app.route("/api/pair_detail", methods=["GET"])
        def api_pair_detail():
            sym = request.args.get("sym", "").upper().strip()
            if not sym:
                return jsonify({"error": "sym kosong"})
            try:
                df = get_ohlcv(sym, limit=120)
                if df is None:
                    return jsonify({"error": "Gagal ambil OHLCV"})
                if df['ct'].iloc[-1] >= int(time.time() * 1000):
                    df = df.iloc[:-1]
                if len(df) < 60:
                    return jsonify({"error": "Data kurang"})
                df = compute_indicators(df)
                row = df.iloc[-1]
                close   = float(row['close'])
                ema20   = float(row['ema_fast'])
                ema50   = float(row['ema_slow'])
                hh      = float(row['hh'])
                st_dir  = int(row['st_dir']) if not pd.isna(row.get('st_dir')) else 0
                vol_ma  = float(row['vol_ma']) if not pd.isna(row.get('vol_ma')) and row['vol_ma'] > 0 else 1
                vol_ratio = float(row['vol']) / vol_ma
                rsi     = float(row['rsi']) if not pd.isna(row.get('rsi')) else None
                stoch_k = float(row['stoch_k']) if 'stoch_k' in row and not pd.isna(row.get('stoch_k')) else None
                atr_pct = float(row['atr_pct']) if not pd.isna(row.get('atr_pct')) else None
                htf_ratio  = htf_vol_ratio(sym, HTF_TIMEFRAME, HTF_CANDLE_LIMIT, HTF_VOL_MA_PERIOD)
                perf_score = calc_perf_score(sym, int(df['ct'].iloc[-1]))
                if pd.isna(perf_score): perf_score = None
                return jsonify({
                    "sym": sym,
                    "close": close, "ema20": ema20, "ema50": ema50, "hh": hh, "st_dir": st_dir,
                    "vol_ratio": round(vol_ratio, 2), "vol_thr": VOLUME_MULT, "vol_max_thr": VOL_MAX_MULT,
                    "rsi": round(rsi, 1) if rsi is not None else None, "rsi_thr": RSI_MAX,
                    "stoch_k": round(stoch_k, 1) if stoch_k is not None else None, "stoch_thr": STOCH_MAX,
                    "atr_pct": round(atr_pct, 2) if atr_pct is not None else None, "atr_thr": ATR_MAX_PCT,
                    "htf_ratio": round(htf_ratio, 2) if htf_ratio >= 0 else None, "htf_thr": HTF_VOL_MULT,
                    "perf_score": round(perf_score, 2) if perf_score is not None else None, "perf_thr": PERF_SCORE_MIN,
                    "p1_ok": st_dir == 1,
                    "p2_ok": close > ema20,
                    "p3_ok": close > ema50,
                    "p4_ok": close > hh,
                })
            except Exception as e:
                return jsonify({"error": str(e)})

        def _b(v):
            if v is None: return False
            try: return bool(v)
            except: return False
        def _f(v):
            if v is None: return None
            try: return float(v)
            except: return None
        def _sanitize(obj):
            """Konversi numpy types ke Python native agar JSON serializable."""
            import numpy as np
            if isinstance(obj, dict):
                return {k: _sanitize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_sanitize(v) for v in obj]
            if isinstance(obj, (np.bool_,)): return bool(obj)
            if isinstance(obj, (np.integer,)): return int(obj)
            if isinstance(obj, (np.floating,)): return float(obj)
            return obj

        @app.route("/api/strategy_detail", methods=["GET"])
        def api_strategy_detail():
            import numpy as np
            def _s(obj):
                if isinstance(obj, dict): return {k:_s(v) for k,v in obj.items()}
                if isinstance(obj, list): return [_s(v) for v in obj]
                if isinstance(obj, np.bool_): return bool(obj)
                if isinstance(obj, np.integer): return int(obj)
                if isinstance(obj, np.floating): return float(obj)
                return obj
            sym   = request.args.get("sym", "").upper().strip()
            strat = request.args.get("strat", "brkX2-12h")
            if not sym: return jsonify(_s({"error": "sym kosong"}))

            try:
                if strat == "brkX2-12h":
                    df = get_ohlcv(sym, limit=120)
                    if df is None: return jsonify(_s({"error": "Gagal ambil OHLCV"}))
                    if df['ct'].iloc[-1] >= int(time.time()*1000): df = df.iloc[:-1]
                    if len(df) < 60: return jsonify(_s({"error": "Data kurang"}))
                    df = compute_indicators(df)
                    row = df.iloc[-1]
                    close  = float(row['close']); ema20 = float(row['ema_fast']); ema50 = float(row['ema_slow'])
                    hh     = float(row['hh']); st_dir = int(row['st_dir']) if not pd.isna(row.get('st_dir')) else 0
                    vol_ma = float(row['vol_ma']) if not pd.isna(row.get('vol_ma')) and row['vol_ma']>0 else 1
                    vol_ratio = float(row['vol'])/vol_ma
                    rsi    = float(row['rsi']) if not pd.isna(row.get('rsi')) else None
                    stoch_k= float(row['stoch_k']) if 'stoch_k' in row and not pd.isna(row.get('stoch_k')) else None
                    atr_pct= float(row['atr_pct']) if not pd.isna(row.get('atr_pct')) else None
                    htf_r  = htf_vol_ratio(sym, HTF_TIMEFRAME, HTF_CANDLE_LIMIT, HTF_VOL_MA_PERIOD)
                    perf   = calc_perf_score(sym, int(df['ct'].iloc[-1]))
                    if pd.isna(perf): perf = None
                    p = [st_dir==1, close>ema20, close>ema50, close>hh]
                    return jsonify(_s({"strat": strat, "sym": sym,
                        "primary": [
                            {"label":"ST=+1","ok":p[0],"actual":f"ST={st_dir}"},
                            {"label":f"close>{ema20:.4g} (EMA20)","ok":p[1],"actual":f"{close:.4g}"},
                            {"label":f"close>{ema50:.4g} (EMA50)","ok":p[2],"actual":f"{close:.4g}"},
                            {"label":f"close>HH3 {hh:.4g}","ok":p[3],"actual":f"{close:.4g}"},
                        ],
                        "secondary": [
                            {"key":"vol","label":f"Vol {VOLUME_MULT}x--{VOL_MAX_MULT}xMA","actual":f"{vol_ratio:.2f}x","ok":VOLUME_MULT<=vol_ratio<=VOL_MAX_MULT,"thr":f"{VOLUME_MULT}x-{VOL_MAX_MULT}x"},
                            {"key":"rsi","label":f"RSI<{RSI_MAX}","actual":f"{rsi:.1f}" if rsi else "n/a","ok":rsi is not None and rsi<=RSI_MAX,"thr":str(RSI_MAX)},
                            {"key":"stoch","label":f"Stoch%K<{STOCH_MAX}","actual":f"{stoch_k:.1f}" if stoch_k else "n/a","ok":stoch_k is not None and stoch_k<STOCH_MAX,"thr":str(STOCH_MAX)},
                            {"key":"atr","label":f"ATR%<{ATR_MAX_PCT}%","actual":f"{atr_pct:.1f}%" if atr_pct else "n/a","ok":atr_pct is not None and atr_pct<ATR_MAX_PCT,"thr":f"{ATR_MAX_PCT}%"},
                            {"key":"htf","label":f"HTF3D vol>{HTF_VOL_MULT}xMA","actual":f"{htf_r:.2f}x" if htf_r>=0 else "n/a","ok":htf_r>=HTF_VOL_MULT,"thr":f"{HTF_VOL_MULT}x"},
                            {"key":"perf","label":f"Perf>={PERF_SCORE_MIN}","actual":f"{perf:.2f}" if perf else "n/a","ok":perf is not None and perf>=PERF_SCORE_MIN,"thr":str(PERF_SCORE_MIN)},
                        ],
                        "primary_ok": all(p),
                    }))


                elif strat in ("Reversal-8h T1","Reversal-8h T3-REV"):
                    df = get_ohlcv(sym, interval=REVERSAL_TIMEFRAME, limit=60)
                    if df is None: return jsonify(_s({"error": "Gagal ambil OHLCV 8h"}))
                    if df['ct'].iloc[-1] >= int(time.time()*1000): df = df.iloc[:-1]
                    df = compute_indicators(df)
                    n = len(df)
                    if n < 6: return jsonify(_s({"error": "Data kurang"}))
                    # c0=doji, c-1,c-2,c-3=merah
                    im3,im2,im1 = n-5,n-4,n-3; i0=n-2; i1=n-1
                    c0 = df.iloc[i0]; c1 = df.iloc[i1]
                    all_red = all(df.iloc[i]['close']<df.iloc[i]['open'] for i in (im3,im2,im1))
                    open_c3 = float(df.iloc[im3]['open']); close_c1 = float(df.iloc[im1]['close'])
                    drop = (close_c1/open_c3-1)*100 if open_c3>0 else 0
                    n_red = sum(1 for i in (im3,im2,im1) if df.iloc[i]['close']<df.iloc[i]['open'])
                    doji_ok = float(c0.get('body_ratio',1)) < REVERSAL_DOJI_MAX
                    below_ema = float(c0['close'])<float(c0['ema_fast']) and float(c0['close'])<float(c0['ema_slow'])
                    p1_ok = all_red and drop<=-5.0
                    p2_ok = doji_ok and below_ema
                    # secondary
                    ha_bull = bool(df['ha_bull'].iloc[i1]) if 'ha_bull' in df.columns else False
                    cross_ok= _cross_up(df, i1, 'ema_fast')
                    vol24 = float(c1.get('vol',0)) * float(c1.get('close',0))
                    perf  = calc_perf_score(sym, int(df['ct'].iloc[i0]))
                    if pd.isna(perf): perf = None
                    elapsed_pct = 0
                    if strat == "Reversal-8h T3-REV":
                        price_now = get_price_now(sym)
                        cross_live = price_now > float(c0['ema_fast']) if price_now>0 else False
                        candle_open_ms = int(df['ts'].iloc[-1]) if 'ts' in df.columns else 0
                        now_ms = int(time.time()*1000)
                        elapsed_pct = (now_ms-candle_open_ms)/(8*3600*1000) if candle_open_ms>0 else 0
                        sec_extra = [
                            {"key":"elapsed","label":"Elapsed 5%-50%","actual":f"{elapsed_pct*100:.1f}%","ok":0.05<=elapsed_pct<=0.50,"thr":"5%-50%"},
                            {"key":"cross_live","label":"price_now>EMA20","actual":f"{price_now:.4g}" if price_now>0 else "n/a","ok":cross_live,"thr":f">{c0['ema_fast']:.4g}"},
                        ]
                    else:
                        sec_extra = [
                            {"key":"ha_bull","label":"c+1 HA bullish","actual":"Ya" if ha_bull else "Belum","ok":ha_bull,"thr":"bullish"},
                            {"key":"cross","label":"cross-up EMA20","actual":"Ya" if cross_ok else "Belum","ok":cross_ok,"thr":"cross up"},
                        ]
                    p = [p1_ok, p2_ok]
                    return jsonify(_s({"strat": strat, "sym": sym,
                        "primary": [
                            {"label":f"3 candle merah+turun>=5%","ok":p1_ok,"actual":f"{n_red}/3 merah, turun {drop:.1f}%"},
                            {"label":f"c0 doji<{REVERSAL_DOJI_MAX} & <EMA20/50","ok":p2_ok,"actual":f"body {c0.get('body_ratio',0):.2f}"},
                        ],
                        "secondary": sec_extra + [
                            {"key":"perf","label":f"Perf>={PERF_SCORE_MIN}","actual":f"{perf:.2f}" if perf else "n/a","ok":perf is not None and perf>=PERF_SCORE_MIN,"thr":str(PERF_SCORE_MIN)},
                            {"key":"vol24","label":f"Vol24h>=${REVERSAL_MIN_VOL_USD/1e6:.1f}jt","actual":f"${vol24/1e6:.2f}jt","ok":vol24>=REVERSAL_MIN_VOL_USD,"thr":f"${REVERSAL_MIN_VOL_USD/1e6:.1f}jt"},
                        ],
                        "primary_ok": all(p),
                    }))


                elif strat == "brkX2-4h":
                    df = get_ohlcv_4h(sym, limit=100)
                    if df is None: return jsonify(_s({"error": "Gagal ambil OHLCV 4h"}))
                    if df['ct'].iloc[-1] >= int(time.time()*1000): df = df.iloc[:-1]
                    df = compute_indicators_4h(df)
                    row = df.iloc[-1]
                    st_dir = int(row['st_dir']) if not pd.isna(row.get('st_dir')) else 0
                    macd_h = float(row.get('macd_hist',0)) if not pd.isna(row.get('macd_hist')) else None
                    atr_pct= float(row['atr_pct']) if not pd.isna(row.get('atr_pct')) else None
                    vol_ma = float(row['vol_ma']) if not pd.isna(row.get('vol_ma')) and row['vol_ma']>0 else 1
                    vol_ratio= float(row['vol'])/vol_ma
                    stoch_k= float(row['stoch_k']) if 'stoch_k' in row and not pd.isna(row.get('stoch_k')) else None
                    htf_r  = htf_vol_ratio(sym, STRAT4H_HTF_TF, STRAT4H_HTF_LIMIT, STRAT4H_HTF_VOL_MA)
                    perf   = calc_perf_score(sym, int(df['ct'].iloc[-1]))
                    if pd.isna(perf): perf = None
                    p = [st_dir==1, macd_h is not None and macd_h>0, atr_pct is not None and atr_pct>=STRAT4H_ATR_MIN_PCT]
                    return jsonify(_s({"strat": strat, "sym": sym,
                        "primary": [
                            {"label":"ST=+1","ok":p[0],"actual":f"ST={st_dir}"},
                            {"label":"MACD hist>0","ok":p[1],"actual":f"{macd_h:.4f}" if macd_h else "n/a"},
                            {"label":f"ATR%>={STRAT4H_ATR_MIN_PCT}%","ok":p[2],"actual":f"{atr_pct:.1f}%" if atr_pct else "n/a"},
                        ],
                        "secondary": [
                            {"key":"vol","label":f"Vol>={STRAT4H_VOLUME_MULT}xMA","actual":f"{vol_ratio:.2f}x","ok":vol_ratio>=STRAT4H_VOLUME_MULT,"thr":f"{STRAT4H_VOLUME_MULT}x"},
                            {"key":"stoch","label":f"Stoch%K<{STRAT4H_STOCH_MAX}","actual":f"{stoch_k:.1f}" if stoch_k else "n/a","ok":stoch_k is not None and stoch_k<STRAT4H_STOCH_MAX,"thr":str(STRAT4H_STOCH_MAX)},
                            {"key":"htf","label":f"HTF12h vol>{STRAT4H_HTF_VOL_MULT}xMA","actual":f"{htf_r:.2f}x" if htf_r>=0 else "n/a","ok":htf_r>=STRAT4H_HTF_VOL_MULT,"thr":f"{STRAT4H_HTF_VOL_MULT}x"},
                            {"key":"perf","label":f"Perf>={PERF_SCORE_MIN}","actual":f"{perf:.2f}" if perf else "n/a","ok":perf is not None and perf>=PERF_SCORE_MIN,"thr":str(PERF_SCORE_MIN)},
                        ],
                        "primary_ok": all(p),
                    }))


                elif strat == "CrossEMA-4h":
                    df = get_ohlcv_4h(sym, limit=100)
                    if df is None: return jsonify(_s({"error": "Gagal ambil OHLCV 4h"}))
                    if df['ct'].iloc[-1] >= int(time.time()*1000): df = df.iloc[:-1]
                    df = compute_indicators_4h(df)
                    row = df.iloc[-1]
                    st_dir = int(row['st_dir']) if not pd.isna(row.get('st_dir')) else 0
                    close  = float(row['close']); ema20 = float(row['ema_fast'])
                    vol_ma = float(row['vol_ma']) if not pd.isna(row.get('vol_ma')) and row['vol_ma']>0 else 1
                    vol_ratio= float(row['vol'])/vol_ma
                    price_now= get_price_now(sym)
                    cross_ok = price_now>0 and price_now>ema20
                    htf_r  = htf_vol_ratio(sym, STRAT4H_HTF_TF, STRAT4H_HTF_LIMIT, STRAT4H_HTF_VOL_MA)
                    vol24  = float(row.get('vol24h_usd',0)) if 'vol24h_usd' in row else 0
                    p = [st_dir==-1, close<ema20, cross_ok]
                    return jsonify(_s({"strat": strat, "sym": sym,
                        "primary": [
                            {"label":"ST=-1 (downtrend)","ok":p[0],"actual":f"ST={st_dir}"},
                            {"label":f"close<EMA20 {ema20:.4g}","ok":p[1],"actual":f"{close:.4g}"},
                            {"label":f"price_now>EMA20 (cross)","ok":p[2],"actual":f"{price_now:.4g}" if price_now>0 else "n/a"},
                        ],
                        "secondary": [
                            {"key":"vol","label":f"Vol>={STRAT_CROSSEMA_VOLUME_MULT}xMA","actual":f"{vol_ratio:.2f}x","ok":vol_ratio>=STRAT_CROSSEMA_VOLUME_MULT,"thr":f"{STRAT_CROSSEMA_VOLUME_MULT}x"},
                            {"key":"htf","label":f"HTF12h vol>{STRAT_CROSSEMA_HTF_VOL_MULT}xMA","actual":f"{htf_r:.2f}x" if htf_r>=0 else "n/a","ok":htf_r>=STRAT_CROSSEMA_HTF_VOL_MULT,"thr":f"{STRAT_CROSSEMA_HTF_VOL_MULT}x"},
                            {"key":"vol24","label":f"Vol24h>=${STRAT_CROSSEMA_MIN_VOL_USD/1e6:.1f}jt","actual":f"${vol24/1e6:.2f}jt","ok":vol24>=STRAT_CROSSEMA_MIN_VOL_USD,"thr":f"${STRAT_CROSSEMA_MIN_VOL_USD/1e6:.1f}jt"},
                        ],
                        "primary_ok": all(p),
                    }))


                else:
                    return jsonify(_s({"error": f"Strategi tidak dikenal: {strat}"}))

            except Exception as e:
                return jsonify(_s({"error": str(e)}))

        @app.route("/manual_filter", methods=["POST"])
        def manual_filter():
            key = request.form.get("key", "")
            val = request.form.get("value", "false") == "true"
            with _manual_filters_lock:
                if key in _manual_filters:
                    _manual_filters[key] = val
            return jsonify({"ok": True, "key": key, "value": val})

        @app.route("/manual_scan", methods=["POST"])
        def manual_scan_endpoint():
            result = run_manual_scan()
            return jsonify(result)

        @app.route("/manual_open", methods=["POST"])
        def manual_open():
            sym = request.form.get("sym", "").upper().strip()
            if not sym:
                return jsonify({"ok": False, "error": "sym kosong"})
            if not sym.endswith("USDT"):
                sym = sym + "USDT"
            with active_deals_lock:
                if sym in active_deals:
                    return jsonify({"ok": False, "error": f"{sym} sudah ada di active_deals"})
            # Ambil data indikator untuk log dokumentasi
            try:
                df = get_ohlcv(sym, limit=120)
                if df is None:
                    return jsonify({"ok": False, "error": "Gagal ambil OHLCV"})
                if df['ct'].iloc[-1] >= int(time.time() * 1000):
                    df = df.iloc[:-1]
                df = compute_indicators(df)
                row = df.iloc[-1]
                close   = float(row['close'])
                ema20   = float(row['ema_fast'])
                ema50   = float(row['ema_slow'])
                hh      = float(row['hh'])
                st_dir  = int(row['st_dir']) if not pd.isna(row.get('st_dir')) else 0
                vol_ma  = float(row['vol_ma']) if not pd.isna(row.get('vol_ma')) and row['vol_ma'] > 0 else 0
                vol_ratio = float(row['vol']) / vol_ma if vol_ma > 0 else 0
                rsi     = float(row['rsi']) if not pd.isna(row.get('rsi')) else None
                stoch_k = float(row['stoch_k']) if 'stoch_k' in row and not pd.isna(row.get('stoch_k')) else None
                atr_pct = float(row['atr_pct']) if not pd.isna(row.get('atr_pct')) else None
                htf_ratio  = htf_vol_ratio(sym, HTF_TIMEFRAME, HTF_CANDLE_LIMIT, HTF_VOL_MA_PERIOD)
                perf_score = calc_perf_score(sym, int(df['ct'].iloc[-1]))
                if pd.isna(perf_score): perf_score = None
            except Exception as e:
                return jsonify({"ok": False, "error": f"Gagal ambil indikator: {e}"})

            with _manual_filters_lock:
                filters = dict(_manual_filters)

            ts = now_wib().strftime("%d/%m/%Y %H:%M:%S")
            sc = signal_score(row)
            log(f"[MANUAL-OPEN] {sym} @ {ts}")
            log(f"  PRIMARY  : ST={st_dir} | close={close:.4g} EMA20={ema20:.4g} EMA50={ema50:.4g} HH={hh:.4g}")
            log(f"  SECONDARY: vol={vol_ratio:.2f}x(thr{VOLUME_MULT}) RSI={rsi}(thr{RSI_MAX}) Stoch={stoch_k}(thr{STOCH_MAX}) ATR={atr_pct}%(thr{ATR_MAX_PCT}) HTF={htf_ratio:.2f}x(thr{HTF_VOL_MULT}) Perf={perf_score}(thr{PERF_SCORE_MIN})")
            log(f"  FILTER ON: {[k for k,v in filters.items() if v]}")

            ok, target_usd, add_usd = open_deal_with_sizing(sym, sc, 'brkX2')
            if ok:
                entry_price = get_price_now(sym)
                if entry_price <= 0:
                    entry_price = close  # fallback ke close candle kalau gagal
                add_to_active_deals(sym, {
                    "strategy":       "brkX2",
                    "entry_price":    entry_price,
                    "peak":           entry_price,
                    "signal_price":   close,
                    "atr_pct":        atr_pct or 3.0,
                    "opened_candle_ts": int(df['ct'].iloc[-1]),
                    "trailing_armed": False,
                    "opened_at":      now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                    "target_usd":     target_usd,
                    "add_usd":        add_usd,
                    "tf":             TIMEFRAME,
                    "manual":         True,
                })
                send_telegram(
                    f"OPEN LONG MANUAL (brkX2-12h)\n"
                    f"{ts} WIB\n"
                    f"Pair  : {to_display_pair(sym)}\n"
                    f"Close : {close:.4g} | EMA20: {ema20:.4g} | EMA50: {ema50:.4g}\n"
                    f"ST={st_dir} | Vol={vol_ratio:.2f}x | RSI={rsi} | Stoch={stoch_k}\n"
                    f"ATR={atr_pct}% | HTF={htf_ratio:.2f}x | Perf={perf_score}\n"
                    f"Filter ON: {[k for k,v in filters.items() if v]}\n"
                    f"Score={sc} | Base=${BASE_ORDER_VOLUME}"
                )
                log(f"[MANUAL-OPEN] {sym} BERHASIL — score={sc} target=${target_usd}")
                return jsonify({"ok": True, "sym": sym, "score": sc, "target_usd": target_usd})
            else:
                log(f"[MANUAL-OPEN] {sym} GAGAL — 3Commas tidak menerima")
                return jsonify({"ok": False, "error": "3Commas menolak open long"})

        log(f"[WEB] Dashboard jalan di port {WEB_PORT}")
        app.run(host="0.0.0.0", port=WEB_PORT, debug=False, use_reloader=False)
    except Exception as e:
        log(f"WARN web dashboard error: {e}")

if __name__ == '__main__':
    log("="*55)
    log("  BINANCE SCREENER -> 3COMMAS + TELEGRAM")
    log("  STRATEGI: MOMENTUM BREAKOUT brkX2 (12h)")
    log("="*55)
    log(f"  Timeframe        : {TIMEFRAME}")
    log(f"  Entry syarat     : ST-up, >EMA20, >EMA50, breakout{BREAKOUT_LOOKBACK}, vol>={VOLUME_MULT}xMA, RSI<{RSI_MAX}" + (f", Stoch<{STOCH_MAX}" if STOCH_MAX is not None else "") + (f", ATR<{ATR_MAX_PCT}%" if ATR_MAX_PCT is not None else ""))
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
    log(f"  Intrabar EARLY   : {'ON' if INTRABAR_EARLY_ENABLED else 'OFF'} (entry {int(INTRABAR_EARLY_ENTRY_PCT*100)}%-{int(INTRABAR_EARLY_END_PCT*100)}% elapsed = menit ke {int(INTRABAR_EARLY_ENTRY_PCT*720)}-{int(INTRABAR_EARLY_END_PCT*720)}, breakout HH{INTRABAR_EARLY_BREAKOUT_LOOKBACK}, scan tiap {INTRABAR_EARLY_SCAN_INTERVAL//60}m)")
    log(f"  Reversal intrabar: {'ON' if REVERSAL_INTRABAR_ENABLED else 'OFF'} (full candle 8h, scan tiap {REVERSAL_INTRABAR_SCAN_INTERVAL//60}m)")
    log(f"  Perf filter      : {'ON' if PERF_FILTER_ENABLED else 'OFF'} (Grade>=B, score>={PERF_SCORE_MIN}, TF 1D/1W/1M/3M/6M/1Y)")
    log(f"  ---------------------------------------------------")
    log(f"  STRATEGI #4 CrossEMA-4h: {'ON' if STRAT_CROSSEMA_ENABLED else 'OFF'} | TF 4h")
    log(f"  Entry: ST=-1 + close<EMA20 + vol>={STRAT_CROSSEMA_VOLUME_MULT}xMA + HTF 3D (lalu price cross EMA20 intrabar)")
    log(f"  Window: {int(STRAT_CROSSEMA_ENTRY_MIN*100*240/100)}-{int(STRAT_CROSSEMA_ENTRY_MAX*100*240/100)} menit ({STRAT_CROSSEMA_ENTRY_MIN*100:.0f}%-{STRAT_CROSSEMA_ENTRY_MAX*100:.0f}% elapsed), scan tiap {STRAT_CROSSEMA_SCAN_INTERVAL//60}m")
    log(f"  Slot: {STRAT_CROSSEMA_MAX_DEALS} | Target forward-test: {STRAT_CROSSEMA_FWDTEST} deal | Perf filter: OFF")
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
        log(f"  Min vol reversal : ${REVERSAL_MIN_VOL_USD:,} (lebih luas dari brkX2 ${MIN_VOLUME_USD:,})")
    if STRAT4H_ENABLED:
        log("  " + "-"*51)
        log(f"  STRATEGI 3 brkX2-4h: ON | TF {STRAT4H_TIMEFRAME}")
        log(f"  Entry: ST+1 + MACD>0 + ATR>={STRAT4H_ATR_MIN_PCT}% + Vol>={STRAT4H_VOLUME_MULT}xMA + HTF {STRAT4H_HTF_TF} (PRICE_EMA50+MACD+RSI50)")
        log(f"  Intrabar: menit ke 5-60 (25% elapsed), scan tiap {STRAT4H_SCAN_INTERVAL}s")
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

    n_threads = 4
    t1  = threading.Thread(target=run_thread1, daemon=True, name="T1-Screener")
    t2  = threading.Thread(target=run_thread2, daemon=True, name="T2-Monitor")
    t3  = threading.Thread(target=run_thread3_intrabar, daemon=True, name="T3-Intrabar")
    t3r = threading.Thread(target=run_thread_rev_intrabar, daemon=True, name="T3-REV")
    threads = [t1, t2, t3, t3r]
    if STRAT4H_ENABLED:
        t4 = threading.Thread(target=run_thread1d_4h, daemon=True, name="T1d-4h")
        threads.append(t4)
        n_threads = 5
    if STRAT_CROSSEMA_ENABLED:
        t_cx = threading.Thread(target=run_thread_crossema, daemon=True, name="T-CrossEMA")
        threads.append(t_cx)
        n_threads += 1
    for t in threads: t.start()
    t_web = threading.Thread(target=run_web_dashboard, daemon=True, name="T-Web")
    t_web.start()
    log(f"{n_threads} thread aktif (T1=screener, T2=monitor, T3=intrabar 12h, T3-REV=reversal intrabar"
        + (", T1d=intrabar 4h" if STRAT4H_ENABLED else "")
        + (", T-CrossEMA=strategi#4" if STRAT_CROSSEMA_ENABLED else "")
        + "). Ctrl+C untuk berhenti.")
    # Kirim heartbeat START saat deploy/restart
    # Delay 15 detik agar T1 belum selesai scan pertama saat heartbeat startup dikirim
    # Catatan: heartbeat 4h/CrossEMA/General dihandle oleh T1d loop (run_thread1d_4h)
    # sehingga tidak perlu dikirim di sini — cukup Reversal saja.
    time.sleep(15)
    try: heartbeat_rev_tick("REVERSAL: memulai scan...")
    except Exception as e: log(f"WARN heartbeat rev START: {e}")
    try:
        while True: time.sleep(60)
    except KeyboardInterrupt:
        log("Dihentikan.")
        sys.exit(0)
