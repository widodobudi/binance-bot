#   - EMA20 < EMA50, jarak 0-2.5%              (hunting_ema_gap)
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
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas_ta")
import requests, pandas as pd, pandas_ta as ta, numpy as np
import time, sys, json, threading, os, csv, pickle
sys.stdout.reconfigure(line_buffering=True)   # tiap baris langsung flush ke Railway log
from datetime import datetime, timedelta, timezone
import requests as _requests_mod
# Google Drive sync (opsional — aktif hanya kalau GDRIVE_SERVICE_ACCOUNT di-set di env)
try:
    from google.oauth2 import service_account as _sa
    from googleapiclient.discovery import build as _gdrive_build
    from googleapiclient.http import MediaIoBaseDownload as _GDL, MediaInMemoryUpload as _GMU
    import io as _io
    _GDRIVE_AVAILABLE = True
except ImportError:
    _GDRIVE_AVAILABLE = False

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
# Bot 3Commas untuk strategi Hunting-4h (bot baru #16951566, base order $20, 14/08/2026)
COMMAS_BOT_ID_HUNTING      = int(os.environ.get("COMMAS_BOT_ID_HUNTING", "16951566"))
COMMAS_EMAIL_TOKEN_HUNTING = os.environ.get("COMMAS_EMAIL_TOKEN_HUNTING", "f97400b9-e9a4-4058-913e-35eb8372f920")

def commas_creds(strategy: str):
    """Pilih (bot_id, email_token) sesuai strategi."""
    if strategy == 'reversal':
        return COMMAS_BOT_ID_REVERSAL, COMMAS_EMAIL_TOKEN_REVERSAL
    if strategy == 'brkX2_4h':
        return COMMAS_BOT_ID_4H, COMMAS_EMAIL_TOKEN_4H
    if strategy == 'hunting_4h':
        return COMMAS_BOT_ID_HUNTING, COMMAS_EMAIL_TOKEN_HUNTING
    if strategy in ('akum_entry_a', 'akum_entry_b'):
        return AKUM_ENTRY_BOT_ID, AKUM_ENTRY_EMAIL_TOKEN
    return COMMAS_BOT_ID, COMMAS_EMAIL_TOKEN
COMMAS_DELAY_SEC   = 0

# ── Migrasi Phase 3: direct Binance API (20/08/2026) ──────────────────────────
# Set USE_BINANCE_DIRECT=true di Railway env untuk aktifkan eksekusi langsung ke Binance
# tanpa 3Commas. Saat False (default) = tetap pakai 3Commas webhook.
USE_BINANCE_DIRECT = os.environ.get("USE_BINANCE_DIRECT", "false").lower() == "true"
# Buffer qty_coin sementara antara send_open_long → add_to_active_deals
_binance_pending_qty:   dict = {}
_binance_pending_price: dict = {}
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
EMA20_BAND_MAX_PCT = 0.3   # ditambah 01/09/2026 (Roadmap Uji Pita EMA20, Fase 1): dulu cuma close>EMA20
                           # (tanpa batas atas), sekarang wajib pita 0% s/d +0.3% -- backtest full-lifecycle
                           # (86 pair): baseline n=3688 WR=66.0% avg=+1.09%, pita 0-0.3% n=103 WR=68.9%
                           # avg=+1.27% (dipilih: sample seimbang & uplift terbaik, tren makin sempit makin
                           # baik s/d 0.3%, di atas itu turun lagi ke arah baseline/lebih jelek).

VOLUME_MA_PERIOD  = 20
RSI_LENGTH        = 14
RSI_MAX           = 65      # dilonggarkan dari 60: tambah kandidat tanpa menghapus filter RSI
STOCH_MAX         = 70      # syarat ke-7: Stoch %K < 70 (hindari entry terlalu overbought). None = matikan.
MIN_VOLUME_USD    = 3_000_000   # dinaikkan dari 1jt ke 3jt (backtest_entry_filter2)
SYMBOL_BLACKLIST  = {'GIGGLEUSDT', 'SOXLBUSDT', 'KLAYUSDT'}  # pair blacklist — tidak akan di-scan sama sekali
# KLAYUSDT ditambah 14/08/2026: KLAY sudah delisted dari Binance sejak 28/10/2024, rebranding jadi KAIA/USDT
# (SYMBOL_BLACKLIST_HARDCODED di-freeze belakangan, lihat setelah EXCLUDED_BASE_ASSETS -- fiat/stablecoin/
# komoditas juga ikut digabung ke situ dulu sebelum di-freeze)

# bStocks Binance (tokenized US stocks) — referensi untuk is_bstock_symbol()
# TIDAK diblacklist dari scan — NYSE filter yang menjaga di level eksekusi
# bStocks boleh open long HANYA saat NYSE buka (21:30–04:00 WIB, Senin–Jumat)
BSTOCKS_KNOWN = {
    'NVDABUSDT', 'AAPLBUSDT', 'TSLABUSDT', 'GOOGLBUSDT', 'MSFTBUSDT',
    'AMZNBUSDT', 'METABUSDT', 'PLTRBUSDT', 'QQQBUSDT', 'SPYBUSDT',
    'SOXLBUSDT', 'SOXSBUSDT', 'COINBUSDT', 'MSTRBUSDT', 'NFLXBUSDT',
    'BRKBBUSDT', 'JPMBBUSDT', 'VISABUSDT', 'JNJBUSDT', 'V2XBUSDT',
}

def is_bstock_symbol(sym: str) -> bool:
    """Return True jika symbol adalah tokenized stock (bStock Binance)."""
    if sym in BSTOCKS_KNOWN: return True
    base = sym.replace('USDT', '')
    return base.endswith('B') and len(base) > 3 and base[:-1].isalpha()

# NYSE trading hours (WIB = UTC+7)
# NYSE buka: Senin–Jumat 21:30–04:00 WIB (16:30–23:00 UTC)
# NYSE tutup: weekend + hari libur US (tidak ditrack detail, cukup jam + hari)
NYSE_OPEN_HOUR_WIB  = (21, 30)   # 21:30 WIB
NYSE_CLOSE_HOUR_WIB = (4,  0)    # 04:00 WIB (hari berikutnya)

def is_nyse_open() -> bool:
    """Return True jika NYSE sedang buka (Senin–Jumat, 21:30–04:00 WIB).
    Tidak memperhitungkan hari libur US — cukup untuk proteksi dasar."""
    now = now_wib()
    wd  = now.weekday()   # 0=Senin, 6=Minggu
    h, m = now.hour, now.minute
    t = h * 60 + m       # menit dari tengah malam

    # NYSE tutup Sabtu (5) dan Minggu (6)
    # Sabtu setelah 04:00 → tutup seharian
    # Minggu → tutup seharian
    # Senin sebelum 21:30 → tutup
    if wd == 6: return False   # Minggu
    if wd == 5 and t >= 4*60:  return False   # Sabtu setelah 04:00

    # Sesi buka: 21:30–23:59 (hari yang sama) ATAU 00:00–04:00 (hari berikutnya)
    after_open  = t >= 21*60 + 30   # >= 21:30
    before_close = t < 4*60          # < 04:00
    if after_open or before_close:
        # Pastikan bukan Minggu malam (Minggu 21:30 bukan NYSE)
        if wd == 6: return False
        return True
    return False
REVERSAL_MIN_VOL_USD = 1_500_000  # min vol24h khusus reversal (lebih rendah untuk perluas universe)

TRAIL_ARM_PCT     = 2.0
# FAKTOR pengali jarak trailing. 1.0 = jarak tabel ATR% apa adanya; 1.10 = 10% lebih longgar.
# Diturunkan dari 1.10 -> 1.0 (Opsi B, 04/07): backtest_faktor.py simpulkan 1.0 menang telak;
# kasus HOLO/USDT & SOL/USDT (04/07, dev 2.2% dari 1.10) rugi tipis -0.27%/-0.30%, dgn 1.0
# (dev 2.0%, stop lebih dekat puncak) kemungkinan impas/rugi jauh lebih kecil.
TRAILING_FAKTOR   = 1.0
# Trailing factor variatif untuk Hunting-4h (backtest_trailing_factor_sweep)
TRAIL_FACTOR_NORMAL    = 0.8   # kondisi normal
FEE_ROUND_TRIP_PCT     = 0.2   # biaya Binance: 0.1% buy + 0.1% sell
TRAIL_FACTOR_FOMO      = 1.3   # Uptrend + Stoch%K > 80 (momentum kuat)
TRAIL_FACTOR_TIGHTENED = 0.5   # Stoch%K baru turun dari >80 (lock profit)
TRAIL_HTF_GRACE_SECONDS = 300  # maksimal menahan trailing close sambil cek HTF
TRAIL_HTF_CACHE_SECONDS = 60   # jangan request HTF pada setiap siklus monitor 15 detik
TRAIL_HTF_HEALTH_MIN_VOTES = 3 # minimal indikator sehat per timeframe
# Un-arm histeresis (26/08/2026): trailing di-un-arm kalau ATR% live naik cukup jauh
# sejak arm (bukan cuma nyebrang tier) DAN sudah armed cukup lama — dua syarat sekaligus
# supaya nggak flapping arm<->un-arm tiap ATR% naik-turun tipis di sekitar batas tier.
UNARM_ATR_MARGIN_PP     = 0.5   # ATR% live harus naik >0.5pp dari ATR% live saat arm
UNARM_MIN_ARMED_MINUTES = 15    # minimal armed 15 menit sebelum boleh dievaluasi un-arm
VOLATILITY_GUARD_ATR_PCT = 10.0  # ATR terbaru di atas ini: jangan add fund
VOLATILITY_GUARD_ATR_MULT = 2.0   # ATR melonjak >=2x ATR saat open: jangan add fund
HARD_STOP_MULT = 1.1              # pengali (K) atas base hard-stop per tier ATR% (lihat hard_stop_pct()).
                                   # K=1.1 disepakati 26/08/2026 studi kasus TLM/USDT: base tier Volatil 9%
                                   # x1.1 = 9.9%, cover wick turun sampai -9.9% tanpa ke-exit (wick TLM -9.55%)
HOLD_NO_SELL_WARN_RATIO = 0.85    # checkbox "hold, jangan jual" muncul di 85% dari ambang hard-stop
                                   # tier deal itu (bukan pas 100%) -- kasih ruang waktu react sebelum
                                   # hard-stop beneran kesulut (disepakati 27/08/2026)
MAX_HOLD_DAYS     = 5
# detik per candle sesuai timeframe (utk batas hold yg benar di TF apa pun).
# 1d=86400, 12h=43200, 6h=21600, 4h=14400. Batas hold = MAX_HOLD_DAYS candle.
_TF_SECONDS = {"1d":86400, "12h":43200, "8h":28800, "6h":21600, "4h":14400, "1h":3600}
SECONDS_PER_CANDLE = _TF_SECONDS.get(TIMEFRAME, 86400)

def _candle_seconds_for_strategy(strat: str) -> float:
    """Detik per candle sesuai timeframe strategi -- dipakai buat hitung 'Hold candles' di
    notif CLOSE dgn benar (sebelumnya semua strategi disamain ke SECONDS_PER_CANDLE punya
    brkX2-12h/43200, salah buat strategi 4h/8h)."""
    if strat == 'reversal':
        return REVERSAL_SECONDS_PER_CANDLE
    if strat in ('brkX2_4h', 'brkX2_crossema', 'hunting_4h', 'akum_entry_a', 'akum_entry_b', 'trend_confirm_4h'):
        return STRAT4H_SECONDS
    return SECONDS_PER_CANDLE  # brkX2 (12h), default

BASE_ORDER_VOLUME       = 8    # diubah ke $8 (17/08/2026, saldo $85, agar semua strategi bisa open)
# COMMAS_MAX_ACTIVE_DEALS DIHAPUS (31/08/2026, permintaan user) -- konstanta lama peninggalan era
# 3Commas 2-strategi (brkX2+reversal), sudah lama nggak relevan begitu strategi nambah jadi 6 tapi
# guard-nya nggak pernah diupdate konsisten: brkX2-12h/reversal/brkX2-4h/crossema/hunting masing2
# ke-throttle silang sama nilai basi ini pakai kombinasi yang beda2 & sebagian ketuker (crossema
# misalnya malah pakai STRAT4H_MAX_DEALS, bukan punya dia sendiri) -- padahal Akumulasi-4h sama
# sekali nggak kena. Semua guard sekarang murni cek slot PER STRATEGI masing2 (lihat 6 konstanta
# MAX_DEALS_*/di bawah, semua sudah bisa diatur lewat dashboard Strategy Control -> max_deals di
# STRATEGY_CONFIG_DEFAULTS + sync_max_deals_globals()). Buat DISPLAY total gabungan, pakai
# total_max_deals_all_strategies() yang otomatis jumlahin ke-6 slot per-strategi terkini.
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
STRAT4H_FWDTEST_TARGET  = 30      # target forward-test fase 2 (fase 1 selesai: #15/7, 13W/1L +12.6%)
STRAT4H_LIVE_BASELINE   = 30      # closed deals saat brkX2-4h TERCAPAI target fase 2 -> LIVE
STRAT4H_PHASE2_TARGET   = 15      # target fase-2 (counter "2nd") setelah LIVE, 29/08/2026, samain hunting/reversal
# ── Hunting-4h ───────────────────────────────────────────────────────────────
HUNTING_MAX_DEALS        = 3       # max deal hunting aktif bersamaan
HUNTING_MAX_HOLD_CANDLES = 15      # timeout 15 candle 4h = 2.5 hari (sama brkX2-4h)
HUNTING_FWDTEST_TARGET   = 7       # target forward-test
HUNTING_LIVE_BASELINE    = 7       # closed deals saat Hunting dipromosikan ke LIVE
HUNTING_PHASE2_TARGET    = 15      # target fase-2 (counter "2nd") setelah LIVE, 28/08/2026
HUNTING_MIN_ATR_PCT      = 0.5     # ATR% minimum — filter koin stagnan
HUNTING_RSI_MAX          = 60      # RSI < 60 (backtest_hunting_filter_sweep: +0.714% vs baseline)
HUNTING_EMA20_BAND_PCT   = 0.3     # ditambah 01/09/2026 (Roadmap Uji Pita EMA20, Fase 4): dulu wajib DI ATAS
                                    # EMA20 jarak 0-0.75% (satu sisi), sekarang pita simetris -0.3% s/d +0.3%.
                                    # Backtest full-lifecycle (163 pair): baseline (0-0.75%) n=209 WR=70.3%
                                    # avg=+0.88%, pita simetris ±0.3% n=38 WR=78.9% avg=+1.30% (dipilih --
                                    # sample seimbang & uplift terbaik; 4 varian asimetris tambahan
                                    # -0.3/+0.5,+0.6,+0.75 semua lebih jelek, dikonfirmasi ulang).
HUNTING_CAPITAL_USD      = float(os.environ.get("HUNTING_CAPITAL_USD", "90.0"))  # estimasi total kapital bot (update manual kalau top-up)
HUNTING_ORDER_VOLUME     = 20      # base order hunting-4h ($20, 14/08/2026)
# Entry conditions 4h
STRAT4H_EMA_FAST        = 9
STRAT4H_EMA_SLOW        = 21
STRAT4H_ST_LENGTH       = 10
STRAT4H_ST_MULT         = 3.0
STRAT4H_MACD_FAST       = 12; STRAT4H_MACD_SLOW = 26; STRAT4H_MACD_SIGNAL = 9
STRAT4H_ATR_MIN_PCT     = 2.0
STRAT4H_VOLUME_MULT     = 0.25  # diubah dari 0.4 → 0.25 (backtest_4h_vol_sweep.py, 27/07/2026): delta avg -0.008% (dalam noise), frekuensi naik
STRAT4H_VOLUME_MA       = 20
STRAT4H_MIN_VOL_USD     = 2_000_000  # dilonggarkan dari $3jt untuk menambah universe 4h
STRAT4H_STOCH_MAX       = 89
STRAT4H_ATR_MAX_PCT     = 7.0   # batas atas ATR% brkX2-4h (08/08/2026): hindari entry puncak pump
STRAT4H_VOL_MAX_MULT    = 5.0   # batas atas volume brkX2-4h (08/08/2026): simetris dengan brkX2-12h
STRAT4H_CHG_MAX_PCT     = 3.0   # max price change% dari open candle (11/08/2026, backtest_elapsed_sweep_brkx2_4h: WR 72.7% avg +0.659% vs baseline -0.349%)
STRAT4H_EMA20_BAND_MAX_PCT = 0.3   # harga wajib 0% s/d +0.3% di atas EMA20 (30/08/2026, keputusan Budi setelah
# backtest sweep lebar pita 0.05%-2.0% pakai fungsi asli bot, 76 pair likuid ~83 hari data 4h: 0.3% adalah
# titik terbaik (WR 57.5% avg +1.38% worst -13.06% n=87, vs baseline tanpa syarat EMA WR 45.9% avg +0.95%
# worst -29.69% n=2389). MACD histogram TETAP >0 apa adanya -- backtest ATR-scaled/normalized MACD threshold
# konsisten menunjukkan pelonggaran MACD selalu menurunkan kualitas, jadi tidak diubah.
STRAT4H_RSI_MIN         = 40    # RSI minimum brkX2-4h (14/08/2026, backtest_brkx2_4h_comprehensive_sweep: RSI>40 sweet spot avg +3.785% WR 87%)
STRAT4H_RSI_MAX         = 70    # RSI maximum brkX2-4h (diubah dari 60→70, 18/08/2026, keputusan Budi)
STRAT4H_PERF_MIN        = 0.5    # Perf Grade minimum (sama dengan brkX2-12h)    # Stoch%K < 80 (backtest_4h_rsi_stoch_sweep.py, 31/07/2026): worst -48.39% vs -63.96%, delta avg -0.121%, wf6 OK
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
STRAT_CROSSEMA_ENTRY_MAX    = 75/240   # 75% elapsed = menit ke-180 (dilonggarkan dari 50%, 20/08/2026)
STRAT_CROSSEMA_SCAN_INTERVAL= 240      # scan tiap 4 menit
STRAT_CROSSEMA_MAX_DEALS    = 2
STRAT_CROSSEMA_MAX_HOLD     = 15
STRAT_CROSSEMA_FWDTEST      = 7
STRAT_CROSSEMA_VOLUME_MULT  = 0.10    # dilonggarkan dari 0.25→0.10 (30/08/2026) -- pair yg lagi downtrend (syarat ST=-1) wajar volumenya turun, jadi filter 0.25xMA kegedean
STRAT_CROSSEMA_VOLUME_MA    = 20
STRAT_CROSSEMA_MIN_VOL_USD  = 1_000_000
STRAT_CROSSEMA_HTF_VOL_MULT = 0.7     # dilonggarkan dari 1.0→0.7 (20/08/2026)
STRAT_CROSSEMA_EMA20_TOL_PCT = 1.0    # close tertutup boleh sedikit di atas EMA20 sebelum cross live (dilonggarkan dari 0.3%→1.0%, 30/08/2026, biar kandidat CrossEMA nggak 0 mulu)
STRAT_CROSSEMA_ST_MULT = 2.5          # Supertrend KHUSUS CrossEMA, dilonggarkan dari shared 3.0 (30/08/2026) --
                                       # dipakai lewat kolom st_dir_cx (compute_indicators_4h), TERPISAH dari st_dir/STRAT4H_ST_MULT=3.0
                                       # yang masih dipakai brkX2-4h & Hunting-4h, jadi strategi lain tidak ikut kesenggol
STRAT_CROSSEMA_CROSS_TOL_PCT = 0.5     # toleransi Lapis 2 (live cross EMA20): price_now boleh sedikit DI BAWAH EMA20,
                                       # nggak wajib udah cross penuh (baru, 30/08/2026, atas permintaan user -- kandidat
                                       # sering nyangkut di sini pas harga udah deket banget tinggal selisih tipis)
# PERF_ONLY lebih baik dari baseline: avg +2.711% vs +2.538%, worst -21.15% vs -25.79%, wf6 OK
# Filter usia saja lebih buruk; usia+perf wf6 HATI-HATI → deploy PERF_ONLY saja
# Update 25/07/2026: backtest_perf_weight_sweep → EQUAL_thr0.5 terbaik
#   avg +3.052% WR 77.7% n=1316 vs PINE_thr1.0 avg +2.711% WR 75.5% n=955
#   Semua TF bobot sama (1/6), threshold 0.5 = cukup 3 dari 6 TF positif

# ── STRATEGI #7: TrenKonfirmasi-4h ("tren sudah jalan", 4h) ──────────────────
# Basis: backtest 03/09/2026 -- "R6" (4 syarat) dedup 1-deal/koin, full-lifecycle
# simulate_trade() real hard_stop/trailing. Menang jelas vs syarat live existing
# di brkX2-4h (avg +2.40% vs +1.23%), CrossEMA-4h (avg +2.00% vs +0.95%, WR 75.5%
# vs 71.6%) & brkX2-12h (avg +2.90% vs +1.28%), setara di Reversal-8h. Lihat
# memory project_momentum_confirmation_finding.md utk detail lengkap.
# Filosofi: BUKAN memprediksi breakout sebelum terjadi (backtest gap-EMA20 di
# titik open candle menolak itu -- kondisi 4 aset yg diuji tidak seragam), TAPI
# konfirmasi tren yg SUDAH mulai bergerak (ATR sudah terangkat, harga sudah di
# atas EMA20, volume sudah naik) -- gaya momentum-continuation, bukan reversal.
TRENDCONFIRM_ENABLED         = True
TRENDCONFIRM_TIMEFRAME       = "4h"
TRENDCONFIRM_MAX_DEALS       = 3        # mulai konservatif -- frekuensi sinyal mentah jauh
                                          # lebih tinggi dari strategi lain (lihat memory)
TRENDCONFIRM_MAX_HOLD_CANDLES= 15        # timeout 15 candle 4h = 2.5 hari, sama spt brkX2-4h/hunting
TRENDCONFIRM_SCAN_INTERVAL   = 180       # scan tiap 3 menit, sama spt brkX2-4h
TRENDCONFIRM_MIN_VOL_USD     = 2_000_000 # sama spt brkX2-4h (STRAT4H_MIN_VOL_USD)
TRENDCONFIRM_ATR_MEDIAN_WINDOW = 100     # "ATR tinggi" = relatif thd median ATR% 100 candle terakhir
                                          # koin itu sendiri (self-relative, bukan angka mutlak)
TRENDCONFIRM_RVOL_MIN        = 1.0       # syarat wajib
TRENDCONFIRM_ATR_ABS_MIN     = 3.0       # angka MUTLAK yg benar2 divalidasi backtest 4h (Step 2/3)
                                          # -- versi self-relative (rolling median) di syarat wajib
                                          # dipakai buat generalisasi lintas timeframe di Step 4,
                                          # TERNYATA lebih longgar dari yg divalidasi. Sempat jadi
                                          # syarat wajib TAMBAHAN (03/09/2026), TAPI direvisi lagi
                                          # (03/09/2026, permintaan Mas Budi): dipindah jadi PANDUAN
                                          # EKSPLISIT utk AI di babak 2 (ai_decision_batch_rank),
                                          # BUKAN gerbang wajib keras -- AI yg menimbang, kandidat
                                          # ATR<3.0 boleh tetap dipertimbangkan kalau sisi lain kuat.
TRENDCONFIRM_RSI_MAX          = 75.0     # syarat wajib BARU (03/09/2026, koreksi Mas Budi):
                                          # kandidat live ketahuan lolos dgn RSI 77-89 (overbought
                                          # ekstrem, ADX lemah, BB%b>1 -- ciri exhaustion), padahal
                                          # syarat wajib sebelumnya sama sekali tidak mengecek RSI.
                                          # 5 dari 6 strategi lain SEMUA punya RSI_MAX, ini yg
                                          # ketinggalan waktu strategi baru ini dibangun.
TRENDCONFIRM_BB_PCT_SECONDARY= 0.65      # syarat SEKUNDER (skor/ranking, bukan gerbang wajib)
TRENDCONFIRM_CLOSE_HARD_CEILING_EXTRA_PCT = 5.0  # batas absolut close = hard_stop_pct(atr) + ini,
                                          # TIDAK BISA di-override AI (Fase 2, permintaan Mas Budi 03/09/2026)
TRENDCONFIRM_BATCH_POOL_SIZE = 15        # (03/09/2026, permintaan Mas Budi, direvisi setelah
                                          # koreksi beliau) batas JUMLAH kandidat yg dievaluasi AI
                                          # individual sebelum babak 2 -- BUKAN batas waktu. Versi
                                          # awal (batas 45 detik) ternyata cuma sempat memproses
                                          # ~5-6 kandidat krn latensi AI individual (~7-9 detik/
                                          # kandidat), jadi babak 2 nyaris tidak menyaring apa-apa.
                                          # Dengan batas JUMLAH, kumpulan kandidat yg masuk ke
                                          # babak 2 benar-benar besar (s/d 15) tidak peduli berapa
                                          # lama prosesnya.
TRENDCONFIRM_BATCH_MAX_APPROVE = 5       # batas atas yg diloloskan babak KEDUA (AI batch re-analysis)
                                          # -- Max Deals (3) tetap jadi batas akhir yg benar2 dibuka,
                                          # angka 5 ini kasih sedikit ruang kalau Max Deals dinaikkan

# Konstanta generik pola 2-babak AI (03/09/2026) -- dipakai strategi LAIN selain TrenKonfirmasi-4h
# yg konstanta batch-nya sendiri sudah di atas (TRENDCONFIRM_BATCH_*). Nilai sama, dipisah supaya
# tuning salah satu strategi nggak ikut mengubah yg lain.
AI_BATCH_POOL_SIZE   = 15
AI_BATCH_MAX_APPROVE = 5

# ── STRATEGI #5: Akumulasi Detector (4h, scan periodik) ───────────────────────
# Deteksi cryptopair yang sedang berada di fase AKUMULASI (sideways setelah downtrend).
# Indikator PRIMARY (semua harus lolos):
#   P1. Harga dalam range sideways: (high_max - low_min) / close < AKUM_RANGE_PCT
#   P2. EMA20/50/200 konvergen & datar: gap EMA20 vs EMA200 < AKUM_EMA_GAP_PCT
#   P3. OBV naik (higher lows) saat harga flat: OBV slope positif
#   P4. ATR turun ≥30% dari puncak tren turun sebelumnya
# Indikator SECONDARY (minimal AKUM_MIN_SECONDARY yang harus lolos):
#   S1. Volume hijau > volume merah (asimetri positif)
#   S2. RSI 30-56 (zona netral-rendah, bukan oversold ekstrem)
#   S3. MACD histogram flat dekat nol (|hist| < AKUM_MACD_FLAT_PCT * close)
#   S4. Candle body/range ratio kecil (rata-rata < AKUM_BODY_RATIO_MAX → konsolidasi)
# Output: maks AKUM_MAX_RESULTS pair dengan skor akumulasi tertinggi (max 5)
STRAT_AKUM_ENABLED        = True
AKUM_TIMEFRAME            = "4h"
AKUM_TIMEFRAME_HOURS      = 4           # jam per candle — ubah ini kalau TF berubah
AKUM_SIDEWAYS_DAYS        = 30          # minimum hari sideways (Wyckoff standard: 30-56 hari)
AKUM_SIDEWAYS_CANDLES     = int(AKUM_SIDEWAYS_DAYS * 24 / AKUM_TIMEFRAME_HOURS)  # = 180 candle 4h
AKUM_SCAN_INTERVAL        = 1800        # scan tiap 30 menit
AKUM_CANDLE_LIMIT         = max(500, AKUM_SIDEWAYS_CANDLES * 3)  # buffer 3x jendela = 540
AKUM_RANGE_PCT            = 0.18        # P1: range sideways ≤18% dari close
AKUM_EMA_GAP_PCT          = 0.06        # P2: gap EMA20 vs EMA200 ≤6%
AKUM_ATR_DROP_PCT         = 0.25        # P4: ATR sekarang ≤ (1 - 0.25) * ATR puncak = turun ≥25%
AKUM_EMA_SLOPE_MAX        = 0.04        # P5: EMA20 awal vs akhir jendela, turun >4% = downtrend, BUKAN sideways
AKUM_CLOSE_DRIFT_MAX      = 0.07        # P6: close awal vs close akhir jendela, drift >7% = bukan sideways
# (dilonggarkan dari 6%->7%, 30/08/2026, keputusan Budi -- backtest score_akumulasi/compute_indicators_akum
# asli, 70 pair ~90 hari data 4h, 386 sinyal: kandidat drift 6-7% justru WR 71.4% avg +4.31% (lebih baik dari
# baseline WR 63.7% avg +4.01%), jadi aman dilonggarkan)
AKUM_RANGE_DIST_MAX       = 2.5         # P7: max(range_bagian)/min(range_bagian), >2.5 = volatilitas tidak merata
AKUM_ATR_LOOKBACK         = 100         # candle lookback untuk cari ATR puncak
AKUM_MACD_FLAT_PCT        = 0.005       # S3: |MACD hist| < 0.5% × close → flat
AKUM_BODY_RATIO_MAX       = 0.57        # S4: rata-rata body/range < 0.57 → konsolidasi (naik dari 0.5, 29/08/2026)
AKUM_MIN_SECONDARY        = 2           # minimal 2 dari 4 secondary harus lolos
AKUM_MAX_RESULTS          = 5           # tampilkan maks 5 pair
AKUM_MIN_VOL_USD          = 1_000_000   # min vol24h $1jt
# Ranking berbobot (total 100) — gating wajib: OBV slope+ DAN ATR turun ≥25%
AKUM_W_OBV      = 25   # P3 OBV slope positif
AKUM_W_ATR      = 20   # P4 ATR turun ≥25%
AKUM_W_RANGE    = 15   # P1 Range ≤18%
AKUM_W_VOL      = 15   # S1 Vol hijau > merah
AKUM_W_EMAGAP   = 10   # P2 EMAGap ≤6%
AKUM_W_RSI      = 5    # S2 RSI 30-56
AKUM_W_MACD     = 5    # S3 MACD flat
AKUM_W_BODY     = 5    # S4 Body ratio kecil
_akum_near_miss: list     = []
_akum_top5: list          = []   # top 5 kandidat Akumulasi untuk ditampilkan di General
# Status Entry A/B per pair — diupdate tiap scan entry, dibaca oleh dashboard
_akum_entry_status: dict  = {}
_akum_last_scan_ts: str   = ""
_akum_lock                = threading.Lock()

# ── STRATEGI #6 Hunting-4h state ─────────────────────────────────────────────
_hunting_config: dict  = {
    "hunting_ema_gap": True, "hunting_price_change": True,
    "hunting_above_ema50": True, "hunting_uptrend": True,
}
_hunting_signals: list = []   # list dict hasil scan, maks 50
_hunting_scan_ts: str  = "-"  # timestamp scan terakhir
_hunting_lock          = threading.Lock()

# ── STRATEGI #5 ENTRY (Extension dari Akumulasi Detector) ─────────────────────
AKUM_ENTRY_BOT_ID         = 16945621
AKUM_ENTRY_EMAIL_TOKEN    = "f97400b9-e9a4-4058-913e-35eb8372f920"
AKUM_ENTRY_SCAN_INTERVAL  = 900          # scan tiap 15 menit
AKUM_ENTRY_MAX_DEALS      = 3            # max 3 deal aktif strategi #5
AKUM_ENTRY_TIMEOUT        = 60           # timeout 60 candle 4h = 10 hari
AKUM_ENTRY_SL_BUFFER      = 0.005        # SL 0.5% di bawah Spring low / Resistance
AKUM_ENTRY_FWDTEST_TARGET = 10           # target forward-test 10 deal

# TP khusus Akumulasi — exit berbasis swing high lokal + momentum overbought
AKUM_TP_SWING_LOOKBACK    = 30           # swing high dari N candle 4h terakhir (~5 hari)
AKUM_TP_RSI_OB            = 70           # RSI overbought threshold untuk early exit
AKUM_TP_STOCH_OB          = 75           # Stoch%K overbought threshold
# Tidak pakai trailing arm — exit via TP/SL/Timeout saja

# Entry A — Spring/Fakeout
AKUM_A_VOL_SPIKE_MULT     = 1.8          # volume spike saat breakdown > 1.8x vol MA (relax dari 2.5)
AKUM_A_RSI_MIN            = 40           # RSI sempat < 40 (Spring oversold, dilonggarkan dari 35, 31/08/2026:
# backtest score_akumulasi/detect_entry_a_spring asli, 164 pair ~167 hari, 590 titik primary_ok -- baseline
# 35/50 cuma n=6 & avg return h60 NEGATIF -1.85%; RSI 40/55 n=11 (nyaris 2x) & avg h60 JADI POSITIF +1.06%)
AKUM_A_RSI_MAX_ENTRY      = 55           # RSI sudah naik kembali < 55 saat entry (dilonggarkan dari 50, sama backtest di atas)
AKUM_A_OBV_SLOPE_CANDLES  = 3           # OBV slope dihitung dari 3 candle terakhir (relax dari 5)
AKUM_A_REENTRY_CANDLES    = 15          # max 15 candle (~60 jam) untuk deteksi spring -- TIDAK dilonggarkan,
# backtest 31/08/2026 nunjukkin reentry_candles 20-25 malah menurunkan WR & avg, worst-case sampai -19.78%
AKUM_A_SUPPORT_TOUCH_BUFFER = 0.004     # toleransi 0.4%: low < support*(1+buffer) dianggap spring

# Entry B — Breakout + Retest
AKUM_B_VOL_BREAKOUT_MULT  = 1.0         # volume breakout > 1.0x vol MA (dilonggarkan dari 1.5x, 31/08/2026:
# backtest detect_entry_b_breakout asli, 164 pair ~167 hari -- baseline 1.5x n=30 avg h60 +3.14%, 1.0x n=35
# (lebih banyak sinyal) avg h60 +4.66% (lebih baik) -- naik kualitas DAN kuantitas sekaligus)
AKUM_B_RETEST_TOL_PCT     = 0.02        # retest dalam ±2% dari resistance -- backtest 31/08/2026 nunjukkin
# ini BUKAN penghambat (loosening ke 3-4% sama sekali nggak nambah sinyal), dibiarkan
AKUM_B_RETEST_VOL_MAX     = 0.8         # volume retest < 80% volume breakout -- sama kayak retest_tol,
# backtest nunjukkin bukan penghambat (loosening ke 90-100% nggak nambah sinyal), dibiarkan

PERF_FILTER_ENABLED = False   # Perf Grade: info saja, tidak memblokir open deal (07/08/2026)
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
HTF_VOL_MULT        = 0.7  # diubah dari 0.8 → 0.7 (backtest_brkx2_sweep2.py, 06/08/2026): 3bull_htf0.7 dipilih Budi
HTF_VOL_MA_PERIOD   = 20

# ---- STRATEGI 2: REVERSAL DOJI + HEIKIN ASHI (8h) ----
REVERSAL_ENABLED      = True
# Reversal pakai bot 3Commas terpisah (split). Kalau env var-nya belum diset, matikan reversal
# supaya tidak salah kirim sinyal reversal ke bot brkX2.
# 03/09/2026: syarat kredensial 3Commas di atas HANYA relevan kalau send_open_long() benar2
# lewat jalur 3Commas (USE_BINANCE_DIRECT=False). Saat USE_BINANCE_DIRECT=True (jalur eksekusi
# skrg, direct-Binance), send_open_long() sama sekali tidak baca COMMAS_BOT_ID_REVERSAL/
# COMMAS_EMAIL_TOKEN_REVERSAL -- itu cuma dipakai jalur fallback 3Commas lama yg sudah tidak
# aktif. Gate lama ini jadi mematikan Reversal-8h total di produksi tanpa alasan yg relevan lagi
# secara arsitektur. Diperbaiki: syarat kredensial hanya berlaku kalau BUKAN direct-Binance.
if REVERSAL_ENABLED and not USE_BINANCE_DIRECT and (COMMAS_BOT_ID_REVERSAL == 0 or not COMMAS_EMAIL_TOKEN_REVERSAL):
    print("WARN: REVERSAL aktif tapi COMMAS_BOT_ID_REVERSAL/COMMAS_EMAIL_TOKEN_REVERSAL "
          "belum diset di Railway > Variables. REVERSAL DIMATIKAN sampai env var diisi.")
    REVERSAL_ENABLED = False
REVERSAL_TIMEFRAME    = "8h"
REVERSAL_EMA_FAST     = 20
REVERSAL_EMA_SLOW     = 50
REVERSAL_DOJI_MAX     = 0.20     # badan doji < 20% range
REVERSAL_DROP_MIN_PCT = 3.0      # total drop minimum, dilonggarkan dari 5%
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
PROG_TRAIL_THRESHOLD = 1.0   # diubah dari 3.0 → 1.0 (backtest_prog_trail_sweep_all.py, 01/08/2026): thr1.0_red0.5 delta +0.387% wf6 0/6 semua strategi
PROG_TRAIL_STEP      = 1.0
PROG_TRAIL_REDUCE    = 0.5   # diubah dari 0.4 → 0.5 (backtest_prog_trail_sweep_all.py, 01/08/2026)
PROG_TRAIL_MIN       = 0.4

HEARTBEAT_INTERVAL_SEC = 2 * 3600   # notif heartbeat tiap 2 jam, terpaku jam ganjil WIB (01,03,05,...,23)
HEARTBEAT_TELEGRAM_ENABLED = False  # 19/08/2026: matikan semua heartbeat + akum detector ke Telegram (terlalu bising)
HEARTBEAT_GENERAL_ENABLED  = True   # 19/08/2026: General tetap aktif tiap 6 jam (05:45, 11:45, 17:45, 23:45 WIB) — hanya progress
HEARTBEAT_HOURS_WIB    = {1,3,5,7,9,11,13,15,17,19,21,23}  # jam ganjil WIB
FWDTEST_CHECK_TRADES   = 12         # (lama, gabungan) cek awal: deteksi masalah dini
FWDTEST_TARGET_TRADES  = 25         # (lama, gabungan) evaluasi FINAL
# Target per-strategi utk forward-test berhasil (tiap close update #X/N):
FWDTEST_TARGET_BRKX2    = 15        # target close deal brkX2 utk forward-test berhasil
FWDTEST_TARGET_REVERSAL = 8         # target close deal reversal utk forward-test berhasil
REVERSAL_LIVE_BASELINE  = 8         # closed deals saat Reversal-8h dipromosikan ke LIVE
                                     # (TERCAPAI 28/08/2026 @ #10/8, 8W/2L, +16.4%; baseline=target
                                     # supaya 2 trade yg sudah lewat target ikut kehitung "sejak LIVE")
REVERSAL_PHASE2_TARGET  = 15        # target fase-2 (counter "2nd") setelah LIVE, 28/08/2026
FWDTEST_TARGET_4H       = 7         # target close deal brkX2-4h utk forward-test berhasil
# Offset untuk multi-tahap forward-test brkX2:
# Set ke total deal yang sudah selesai di AKHIR tahap sebelumnya.
# Tahap 1: 0, Tahap 2: dimulai dari 0 (reset manual), Tahap 3: dimulai dari 15
FWDTEST_BRKX2_PHASE_OFFSET = 15    # Tahap 2 sudah selesai 15 deal → Tahap 3 mulai dari 0
HUNTING_FWDTEST_PHASE_OFFSET = 8   # Reset 18/08/2026 setelah ganti param RSI→70, Stoch→89, cross EMA20 0-0.75%, ema_gap→1.5%, price_chg→2.0%, Hammer/StrongBull

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

# Fiat/stablecoin/komoditas di atas juga digabung ke SYMBOL_BLACKLIST (30/08/2026), BUKAN cuma dipakai
# get_usdt_spot_pairs() -- soalnya sejumlah scan loop per-strategi (mis. CrossEMA-4h, brkX2-4h intrabar)
# nggak lewat get_usdt_spot_pairs(), langsung loop ticker mentah + cek "sym in SYMBOL_BLACKLIST" doang.
# Trigger: EUR/USDT (base "EUR", sudah ada di EXCLUDED_BASE_ASSETS) tetap lolos ke-scan & ke-OPEN oleh
# CrossEMA-4h karena loop-nya nggak pernah cek base asset, cuma cek endswith("USDT"). Gabungin ke sini
# sekali biar otomatis kepakai di SEMUA titik scan yang sudah cek SYMBOL_BLACKLIST, tanpa perlu ubah
# tiap loop satu-satu.
for _base in EXCLUDED_BASE_ASSETS:
    SYMBOL_BLACKLIST.add(_base + "USDT")
SYMBOL_BLACKLIST_HARDCODED = frozenset(SYMBOL_BLACKLIST)  # kunci semua (manual + fiat/stablecoin/komoditas) --
# TIDAK bisa di-unblock lewat menu Block Pairs. block_pair()/unblock_pair() cuma nambah/buang isi SET
# SYMBOL_BLACKLIST yang sama ini, jadi semua titik scan yang cek "sym in SYMBOL_BLACKLIST" otomatis ikut.

BASE = "https://data-api.binance.vision"
DATA_DIR = os.environ.get("DATA_DIR", r"D:\tradingview")

# ══════════════════════════════════════════════════════════════════════════════
# GOOGLE DRIVE SYNC
# Env vars: GDRIVE_SERVICE_ACCOUNT (JSON key), DRIVE_FOLDER_ID (folder tradingview)
# File harus sudah ada di Drive dan di-share ke service account dengan role Editor.
# ══════════════════════════════════════════════════════════════════════════════
_DRIVE_FOLDER_ID  = os.environ.get("DRIVE_FOLDER_ID", "1DwtfVtDc1DhoW80AgNUmUO6zYqFi-ZBC")
_drive_service    = None
_drive_lock       = threading.Lock()
_drive_file_ids: dict = {}

def _get_drive_service():
    global _drive_service
    if _drive_service is not None:
        return _drive_service
    if not _GDRIVE_AVAILABLE:
        return None
    sa_json = os.environ.get("GOOGLE_SA_JSON") or os.environ.get("GDRIVE_SERVICE_ACCOUNT", "")
    if not sa_json:
        return None
    try:
        info  = json.loads(sa_json)
        creds = _sa.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/drive"]
        )
        _drive_service = _gdrive_build("drive", "v3", credentials=creds, cache_discovery=False)
        log(f"[DRIVE] Service account terhubung ke Google Drive.")
        return _drive_service
    except Exception as e:
        log(f"WARN [DRIVE] gagal init: {e}")
        return None

def _drive_get_or_create_file_id(svc, filename: str) -> str:
    """Cari file ID — hanya search, tidak buat baru (service account tidak punya storage quota)."""
    if filename in _drive_file_ids:
        return _drive_file_ids[filename]
    try:
        q   = f"name='{filename}' and '{_DRIVE_FOLDER_ID}' in parents and trashed=false"
        res = svc.files().list(q=q, fields="files(id)", pageSize=5).execute()
        files = res.get("files", [])
        if files:
            fid = files[0]["id"]
            _drive_file_ids[filename] = fid
            log(f"[DRIVE] File ditemukan: {filename} ({fid})")
            return fid
        log(f"WARN [DRIVE] File {filename} tidak ditemukan — buat manual di Drive dan share ke service account")
        return ""
    except Exception as e:
        log(f"WARN [DRIVE] get_file_id {filename}: {e}")
        return ""

def _drive_read(svc, file_id: str) -> str:
    try:
        meta = svc.files().get(fileId=file_id, fields="size").execute()
        if int(meta.get("size", 0)) == 0:
            return ""
        req = svc.files().get_media(fileId=file_id)
        buf = _io.BytesIO()
        dl  = _GDL(buf, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        return buf.getvalue().decode("utf-8", errors="replace")
    except Exception as e:
        log(f"WARN [DRIVE] read {file_id}: {e}")
        return ""

def _drive_write(svc, file_id: str, content: str):
    try:
        media = _GMU(content.encode("utf-8"), mimetype="text/plain", resumable=False)
        svc.files().update(fileId=file_id, media_body=media).execute()
    except Exception as e:
        log(f"WARN [DRIVE] write {file_id}: {e}")

def drive_append(filename: str, new_text: str):
    """Append new_text ke file Drive. Thread-safe."""
    with _drive_lock:
        svc = _get_drive_service()
        if not svc:
            return
        try:
            fid  = _drive_get_or_create_file_id(svc, filename)
            if not fid:
                return
            old  = _drive_read(svc, fid)
            sep  = "" if (not old or old.endswith("\n")) else "\n"
            _drive_write(svc, fid, old + sep + new_text)
        except Exception as e:
            log(f"WARN [DRIVE] drive_append {filename}: {e}")

def drive_sync_full(filename: str, full_content: str):
    """TIMPA PENUH isi file Drive dgn full_content (bukan append) -- dipakai buat
    trades_forwardtest.csv, karena baris CSV-nya berubah status OPEN->CLOSED DI
    TEMPAT (bukan cuma nambah baris baru kayak log open-arm-close/near_miss).
    File tujuan HARUS sudah ada duluan di folder Drive (service account nggak
    punya storage quota buat bikin file baru -- lihat _drive_get_or_create_file_id).
    29/08/2026: biar Claude bisa baca data trade mentah langsung dari Drive,
    nggak perlu lagi user download manual & upload ke chat tiap butuh data."""
    with _drive_lock:
        svc = _get_drive_service()
        if not svc:
            return
        try:
            fid = _drive_get_or_create_file_id(svc, filename)
            if not fid:
                return
            _drive_write(svc, fid, full_content)
        except Exception as e:
            log(f"WARN [DRIVE] drive_sync_full {filename}: {e}")

def sync_trades_csv_to_drive():
    """Baca trades_forwardtest.csv lokal, sync penuh ke Drive (background thread,
    non-blocking). Dipanggil tiap kali ada trade OPEN atau CLOSE."""
    try:
        if not os.path.exists(TRADES_CSV):
            return
        with trades_csv_lock:
            with open(TRADES_CSV, 'r', encoding='utf-8') as f:
                content = f.read()
        threading.Thread(target=drive_sync_full, args=("trades_forwardtest.csv", content), daemon=True).start()
    except Exception as e:
        log(f"WARN [DRIVE] sync_trades_csv_to_drive: {e}")

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

# ── Strategy Control config ──────────────────────────────────────────────────
STRATEGY_CONFIG_FILE = os.path.join(DATA_DIR, "strategy_config.json")
# Default values — edit hard-coded di sini untuk ubah nilai RESET
STRATEGY_CONFIG_DEFAULTS = {
    "brkX2":         {"strategy_enabled": True, "sizing_enabled": True, "base_usd": 12, "add_usd": None, "cooldown_enabled": True, "ai_call_open": True, "max_deals": 2},
    "reversal":      {"strategy_enabled": True, "sizing_enabled": True, "base_usd": 15, "add_usd": None, "cooldown_enabled": True, "ai_call_open": True, "max_deals": 2},
    "brkX2_4h":      {"strategy_enabled": True, "sizing_enabled": True, "base_usd": 15, "add_usd": 0,    "cooldown_enabled": True, "ai_call_open": True, "max_deals": 5},
    "brkX2_crossema":{"strategy_enabled": True, "sizing_enabled": True, "base_usd": 8,  "add_usd": None, "cooldown_enabled": True, "ai_call_open": True, "max_deals": 2},
    "akum_entry_a":  {"strategy_enabled": True, "sizing_enabled": True, "base_usd": 8,  "add_usd": None, "cooldown_enabled": True, "ai_call_open": True, "max_deals": 3},
    "hunting_4h":    {"strategy_enabled": True, "sizing_enabled": True, "base_usd": 25, "add_usd": None, "cooldown_enabled": True, "ai_call_open": True, "max_deals": 3},
    "trend_confirm_4h": {"strategy_enabled": True, "sizing_enabled": True, "base_usd": 30, "add_usd": None, "cooldown_enabled": True, "ai_call_open": True, "max_deals": 3},
}
# Nilai "max_deals" di atas SAMA PERSIS dengan default lama MAX_DEALS_BRKX2/MAX_DEALS_REVERSAL/
# STRAT4H_MAX_DEALS/STRAT_CROSSEMA_MAX_DEALS/AKUM_ENTRY_MAX_DEALS/HUNTING_MAX_DEALS (31/08/2026,
# permintaan user: bisa diatur lewat dashboard Strategy Control) -- lihat sync_max_deals_globals()
# di bawah, yang nge-sync field ini KE 6 konstanta lama itu, supaya puluhan titik guard/log yang
# sudah baca konstanta lama otomatis ikut nilai baru tanpa perlu diubah satu-satu.

def load_strategy_config() -> dict:
    try:
        if os.path.exists(STRATEGY_CONFIG_FILE):
            with open(STRATEGY_CONFIG_FILE) as f:
                saved = json.load(f)
            # Merge dengan defaults agar key baru tidak hilang
            result = {}
            for k, v in STRATEGY_CONFIG_DEFAULTS.items():
                saved_cfg = saved.get(k, {})
                # Migrate the old single ``enabled`` flag to the execution flag.
                if "enabled" in saved_cfg and "strategy_enabled" not in saved_cfg:
                    saved_cfg = {**saved_cfg, "strategy_enabled": saved_cfg["enabled"]}
                result[k] = {**v, **saved_cfg}
            return result
    except Exception as e:
        log(f"WARN load_strategy_config: {e}")
    return {k: dict(v) for k, v in STRATEGY_CONFIG_DEFAULTS.items()}

def save_strategy_config(cfg: dict):
    try:
        base = {k: dict(v) for k, v in STRATEGY_CONFIG_DEFAULTS.items()}
        if os.path.exists(STRATEGY_CONFIG_FILE):
            with open(STRATEGY_CONFIG_FILE) as f:
                saved = json.load(f)
            for k, v in saved.items():
                if isinstance(v, dict):
                    base[k] = {**base.get(k, {}), **v}
                else:
                    base[k] = v
        incoming = cfg or {}
        for key, value in incoming.items():
            if isinstance(value, dict):
                if "enabled" in value and "strategy_enabled" not in value:
                    value = {**value, "strategy_enabled": value["enabled"]}
                base[key] = {**base.get(key, {}), **value}
            else:
                base[key] = value
        with open(STRATEGY_CONFIG_FILE, "w") as f:
            json.dump(base, f, indent=2)
    except Exception as e:
        log(f"WARN save_strategy_config: {e}")

def sync_max_deals_globals():
    """Sinkronkan field "max_deals" per strategi (strategy_config.json, bisa diatur lewat
    dashboard Strategy Control) KE 6 konstanta module-level lama (MAX_DEALS_BRKX2 dst) --
    dipanggil sekali saat startup dan tiap kali user Save di dashboard, supaya SEMUA titik
    guard slot & log yang sudah ada (puluhan tempat, sudah dipakai lama sebelum fitur ini)
    otomatis ikut nilai terbaru tanpa perlu diubah manual satu-satu (jauh lebih aman daripada
    ubah puluhan call-site sekaligus)."""
    global MAX_DEALS_BRKX2, MAX_DEALS_REVERSAL, STRAT4H_MAX_DEALS
    global STRAT_CROSSEMA_MAX_DEALS, HUNTING_MAX_DEALS, AKUM_ENTRY_MAX_DEALS
    global TRENDCONFIRM_MAX_DEALS
    try:
        cfg = load_strategy_config()
        MAX_DEALS_BRKX2          = int(cfg.get('brkX2', {}).get('max_deals', MAX_DEALS_BRKX2) or MAX_DEALS_BRKX2)
        MAX_DEALS_REVERSAL       = int(cfg.get('reversal', {}).get('max_deals', MAX_DEALS_REVERSAL) or MAX_DEALS_REVERSAL)
        STRAT4H_MAX_DEALS        = int(cfg.get('brkX2_4h', {}).get('max_deals', STRAT4H_MAX_DEALS) or STRAT4H_MAX_DEALS)
        STRAT_CROSSEMA_MAX_DEALS = int(cfg.get('brkX2_crossema', {}).get('max_deals', STRAT_CROSSEMA_MAX_DEALS) or STRAT_CROSSEMA_MAX_DEALS)
        HUNTING_MAX_DEALS        = int(cfg.get('hunting_4h', {}).get('max_deals', HUNTING_MAX_DEALS) or HUNTING_MAX_DEALS)
        AKUM_ENTRY_MAX_DEALS     = int(cfg.get('akum_entry_a', {}).get('max_deals', AKUM_ENTRY_MAX_DEALS) or AKUM_ENTRY_MAX_DEALS)
        TRENDCONFIRM_MAX_DEALS   = int(cfg.get('trend_confirm_4h', {}).get('max_deals', TRENDCONFIRM_MAX_DEALS) or TRENDCONFIRM_MAX_DEALS)
        log(f"   Max deals per strategi: brkX2={MAX_DEALS_BRKX2} reversal={MAX_DEALS_REVERSAL} "
            f"4h={STRAT4H_MAX_DEALS} crossema={STRAT_CROSSEMA_MAX_DEALS} "
            f"hunting={HUNTING_MAX_DEALS} akum={AKUM_ENTRY_MAX_DEALS} trendconfirm={TRENDCONFIRM_MAX_DEALS}")
    except Exception as e:
        log(f"WARN sync_max_deals_globals: {e}")

def total_max_deals_all_strategies() -> int:
    """Jumlah semua slot maks lintas 6 strategi -- dipakai murni buat DISPLAY (ganti
    COMMAS_MAX_ACTIVE_DEALS yang lama & basi, lihat penjelasan di dekat definisinya)."""
    return (MAX_DEALS_BRKX2 + MAX_DEALS_REVERSAL + STRAT4H_MAX_DEALS
            + STRAT_CROSSEMA_MAX_DEALS + HUNTING_MAX_DEALS + AKUM_ENTRY_MAX_DEALS)

def is_strategy_enabled(strategy: str) -> bool:
    cfg = load_strategy_config()
    item = cfg.get(strategy, {})
    return item.get("strategy_enabled", item.get("enabled", True))

def is_sizing_enabled(strategy: str) -> bool:
    cfg = load_strategy_config()
    return cfg.get(strategy, {}).get("sizing_enabled", True)

def is_cooldown_enabled(strategy: str) -> bool:
    """Default True: skip re-entry pair yang sama dalam COOLDOWN_SECONDS pasca-close.
    Uncheck di dashboard kalau mau matikan proteksi ini per strategi (mis. testing)."""
    cfg = load_strategy_config()
    return cfg.get(strategy, {}).get("cooldown_enabled", True)

def is_ai_call_open_enabled(strategy: str) -> bool:
    """Default FALSE: kalau True, tiap kandidat OPEN yang sudah lolos semua filter rule-based
    strategi ini masih dikonsultasikan ke AI (ai_decision_open) sebelum benar-benar dibuka.
    Beda dari filter numerik lain di bot ini, keputusan AI tidak bisa di-backtest/di-sweep --
    default ON (29/08/2026, maksimalkan pemakaian kredit Anthropic yang jarang kepakai),
    matikan per-strategi lewat Strategy Control kalau mau."""
    cfg = load_strategy_config()
    return cfg.get(strategy, {}).get("ai_call_open", True)

def get_strategy_base_usd(strategy: str) -> float:
    cfg = load_strategy_config()
    if not is_sizing_enabled(strategy):
        return float(STRATEGY_CONFIG_DEFAULTS.get(strategy, {}).get("base_usd", BASE_ORDER_VOLUME))
    return float(cfg.get(strategy, {}).get("base_usd", BASE_ORDER_VOLUME))

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

# Cooldown diperpanjang khusus deal yang di-hold (bukan dijual) saat hard-stop —
# 96 jam / 4 hari, hasil backtest_posthardstop khusus (27/08/2026): itu bucket pertama
# dengan sample cukup (N=336) yang repeat-hard-stop rate & avg outcome-nya sudah setara
# baseline trade normal; bucket di bawahnya (<96j) sample-nya terlalu tipis dipercaya.
HOLD_NO_SELL_COOLDOWN_SECONDS = 96 * 3600
HOLD_NO_SELL_CLOSED_FILE = os.path.join(DATA_DIR, "hold_no_sell_closed.json")
hold_no_sell_ts = {}
hold_no_sell_lock = threading.Lock()

def load_hold_no_sell_closed():
    global hold_no_sell_ts
    if not os.path.exists(HOLD_NO_SELL_CLOSED_FILE):
        return
    try:
        with open(HOLD_NO_SELL_CLOSED_FILE, 'r') as f: data = json.load(f)
        with hold_no_sell_lock:
            hold_no_sell_ts = {k: float(v) for k, v in data.items()}
        log(f"   Loaded hold_no_sell_ts: {len(hold_no_sell_ts)} symbol.")
    except Exception as e:
        log(f"WARN gagal baca hold_no_sell_closed.json: {e}")

def save_hold_no_sell_closed():
    try:
        with hold_no_sell_lock: data = dict(hold_no_sell_ts)
        with open(HOLD_NO_SELL_CLOSED_FILE, 'w') as f: json.dump(data, f, indent=2)
    except Exception as e:
        log(f"WARN gagal simpan hold_no_sell_closed.json: {e}")

def record_hold_no_sell_closed(symbol: str):
    """Catat waktu deal di-hold (bukan dijual) via hard-stop -> cooldown 96 jam, semua strategi."""
    with hold_no_sell_lock:
        hold_no_sell_ts[symbol] = time.time()
    save_hold_no_sell_closed()

# Harga rata-rata beli (avg/base order) koin yang di-hold (bukan dijual) saat hard-stop --
# dipakai dashboard Auto Sell Asset supaya user tahu breakeven-nya tanpa gali log manual
# (kasus TAO/TRUMP 29/08/2026: user bingung nyari harga average-nya di mana).
HOLD_NO_SELL_PRICE_FILE = os.path.join(DATA_DIR, "hold_no_sell_price.json")
hold_no_sell_price = {}   # symbol -> {'entry_price':, 'strategy':, 'ts':}
hold_no_sell_price_lock = threading.Lock()

def load_hold_no_sell_price():
    global hold_no_sell_price
    if not os.path.exists(HOLD_NO_SELL_PRICE_FILE):
        return
    try:
        with open(HOLD_NO_SELL_PRICE_FILE, 'r') as f: data = json.load(f)
        with hold_no_sell_price_lock:
            hold_no_sell_price = data
        log(f"   Loaded hold_no_sell_price: {len(hold_no_sell_price)} symbol.")
    except Exception as e:
        log(f"WARN gagal baca hold_no_sell_price.json: {e}")

def save_hold_no_sell_price():
    try:
        with hold_no_sell_price_lock: data = dict(hold_no_sell_price)
        with open(HOLD_NO_SELL_PRICE_FILE, 'w') as f: json.dump(data, f, indent=2)
    except Exception as e:
        log(f"WARN gagal simpan hold_no_sell_price.json: {e}")

def record_hold_no_sell_price(symbol: str, entry_price: float, strategy: str):
    with hold_no_sell_price_lock:
        hold_no_sell_price[symbol] = {"entry_price": float(entry_price), "strategy": strategy, "ts": time.time()}
    save_hold_no_sell_price()

def get_hold_no_sell_price(asset: str):
    """Return {'entry_price','strategy','ts'} kalau asset ini punya riwayat hold-no-sell, else None."""
    with hold_no_sell_price_lock:
        return hold_no_sell_price.get(str(asset).upper() + "USDT")


# ── BLOCK PAIR (29/08/2026) ────────────────────────────────────────────────
# User bisa blok/unblok pair lewat dashboard (menu "Block Pair", terpisah dari
# Strategy Control -- itu per-strategi, ini per-symbol lintas semua strategi).
# Blok CUMA cegah OPEN BARU (semua 9 titik scan strategi sudah cek keanggotaan
# SYMBOL_BLACKLIST) -- deal yang KEBETULAN lagi aktif di pair itu TETAP dibiarkan
# jalan normal sampai closed sendiri (trailing/hard-stop/manual), tidak di-force-close.
BLOCKED_PAIRS_FILE = os.path.join(DATA_DIR, "blocked_pairs.json")
blocked_pairs_meta_lock = threading.Lock()
blocked_pairs_meta: dict = {}   # symbol (raw, "TRUMPUSDT") -> {"blocked_at": "dd/mm/YYYY HH:MM"}

def load_blocked_pairs():
    global blocked_pairs_meta
    if not os.path.exists(BLOCKED_PAIRS_FILE):
        return
    try:
        with open(BLOCKED_PAIRS_FILE, 'r') as f: data = json.load(f)
        with blocked_pairs_meta_lock:
            blocked_pairs_meta = data
        for sym in blocked_pairs_meta:
            SYMBOL_BLACKLIST.add(sym)
        log(f"   Loaded blocked_pairs: {len(blocked_pairs_meta)} pair -> {sorted(blocked_pairs_meta.keys())}")
    except Exception as e:
        log(f"WARN gagal baca blocked_pairs.json: {e}")

def save_blocked_pairs():
    try:
        with blocked_pairs_meta_lock: data = dict(blocked_pairs_meta)
        with open(BLOCKED_PAIRS_FILE, 'w') as f: json.dump(data, f, indent=2)
    except Exception as e:
        log(f"WARN gagal simpan blocked_pairs.json: {e}")

def block_pair(symbol: str) -> bool:
    """Tambah symbol ke SYMBOL_BLACKLIST (persist) -- otomatis kena di semua titik scan."""
    symbol = str(symbol).upper()
    SYMBOL_BLACKLIST.add(symbol)
    with blocked_pairs_meta_lock:
        blocked_pairs_meta[symbol] = {"blocked_at": now_wib().strftime("%d/%m/%Y %H:%M")}
    save_blocked_pairs()
    return True

def unblock_pair(symbol: str) -> bool:
    """Buang symbol dari SYMBOL_BLACKLIST -- kecuali yg dikunci developer (SYMBOL_BLACKLIST_HARDCODED)."""
    symbol = str(symbol).upper()
    if symbol in SYMBOL_BLACKLIST_HARDCODED:
        return False
    SYMBOL_BLACKLIST.discard(symbol)
    with blocked_pairs_meta_lock:
        blocked_pairs_meta.pop(symbol, None)
    save_blocked_pairs()
    return True

def get_closed_trades_distinct_pairs() -> list:
    """Daftar pair unik (symbol raw, mis. 'TRUMPUSDT') dari trades_forwardtest.csv yang
    statusnya CLOSED -- dipakai sbg sumber dropdown menu Block Pair (bukan full symbol
    list Binance, biar cuma nawarin pair yg memang pernah di-trade bot)."""
    try:
        if not os.path.exists(TRADES_CSV):
            return []
        with trades_csv_lock:
            with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
        seen = set()
        out = []
        for r in rows:
            if r.get('status') != 'CLOSED':
                continue
            disp = (r.get('symbol') or '').strip()
            if not disp:
                continue
            raw = disp.replace('/', '').upper()
            if raw not in seen:
                seen.add(raw)
                out.append(raw)
        return sorted(out)
    except Exception as e:
        log(f"WARN gagal baca pair unik dari {TRADES_CSV}: {e}")
        return []


# ── CIRCUIT BREAKER: BATAS RUGI HARIAN (29/08/2026) ────────────────────────
# Dihitung terus-menerus (realized P&L hari ini + unrealized P&L deal aktif) vs
# modal total. Begitu tersulut, cuma blokir OPEN DEAL BARU lintas SEMUA strategi
# (open_deal_with_sizing() + 2 titik reversal yg bypass itu) -- deal yg sudah
# aktif TETAP dipantau & dieksekusi normal, tidak dipaksa tutup. Otomatis lepas
# blokir lagi begitu P&L hari ini balik di atas -limit% (atau ganti hari WIB baru).
DAILY_LOSS_LIMIT_FILE = os.path.join(DATA_DIR, "daily_loss_limit_config.json")
daily_loss_limit_lock = threading.Lock()
daily_loss_limit_pct: float = 3.0   # default -- disepakati dari analisis data nyata 29/08/2026

def load_daily_loss_limit():
    global daily_loss_limit_pct
    if not os.path.exists(DAILY_LOSS_LIMIT_FILE):
        return
    try:
        with open(DAILY_LOSS_LIMIT_FILE, 'r') as f:
            data = json.load(f)
        with daily_loss_limit_lock:
            daily_loss_limit_pct = float(data.get("limit_pct", 3.0))
        log(f"   Loaded daily_loss_limit_pct: {daily_loss_limit_pct}%")
    except Exception as e:
        log(f"WARN gagal baca daily_loss_limit_config.json: {e}")

def save_daily_loss_limit(pct: float) -> float:
    global daily_loss_limit_pct
    pct = max(0.0, float(pct))
    with daily_loss_limit_lock:
        daily_loss_limit_pct = pct
    try:
        with open(DAILY_LOSS_LIMIT_FILE, 'w') as f:
            json.dump({"limit_pct": pct}, f, indent=2)
    except Exception as e:
        log(f"WARN gagal simpan daily_loss_limit_config.json: {e}")
    return pct

def get_today_pnl_usd() -> float:
    """Realized P&L (trade CLOSED hari ini, WIB) + unrealized P&L (U/PNL semua deal aktif)."""
    today_str = now_wib().strftime('%Y-%m-%d')
    realized = 0.0
    try:
        if os.path.exists(TRADES_CSV):
            with trades_csv_lock:
                with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                    rows = list(csv.DictReader(f))
            for r in rows:
                if r.get('status') != 'CLOSED':
                    continue
                if not (r.get('close_time_wib') or '').strip().startswith(today_str):
                    continue
                try:
                    pct  = float(r.get('profit_pct') or 0)
                    base = float(r.get('base_usd') or 8)
                    realized += pct / 100 * base
                except (ValueError, TypeError):
                    pass
    except Exception as e:
        log(f"WARN get_today_pnl_usd realized: {e}")
    unrealized = 0.0
    try:
        with active_deals_lock:
            deals = dict(active_deals)
        for d in deals.values():
            ep = d.get('entry_price', 0) or 0
            if ep <= 0:
                continue
            lp = d.get('last_price', ep) or ep
            upnl_pct = (lp / ep - 1) * 100 - FEE_ROUND_TRIP_PCT
            unrealized += upnl_pct / 100 * estimate_deal_total_usd(d)
    except Exception as e:
        log(f"WARN get_today_pnl_usd unrealized: {e}")
    return realized + unrealized

def get_total_capital_usd() -> float:
    """Modal total = USDT bebas (free+locked) + modal yang lagi kepakai di semua deal aktif."""
    free_usdt = locked_usdt = 0.0
    try:
        if USE_BINANCE_DIRECT:
            free_usdt, locked_usdt = get_usdt_balance()
    except Exception:
        pass
    deployed = 0.0
    try:
        with active_deals_lock:
            deals = dict(active_deals)
        deployed = sum(estimate_deal_total_usd(d) for d in deals.values())
    except Exception:
        pass
    return free_usdt + locked_usdt + deployed

def get_daily_loss_status() -> dict:
    """Ringkasan buat dashboard: pnl hari ini ($, %), limit, apakah tersulut."""
    cap = get_total_capital_usd()
    pnl_usd = get_today_pnl_usd()
    pnl_pct = (pnl_usd / cap * 100) if cap > 0 else 0.0
    with daily_loss_limit_lock:
        limit_pct = daily_loss_limit_pct
    breached = limit_pct > 0 and pnl_pct <= -abs(limit_pct)
    return {"pnl_usd": pnl_usd, "pnl_pct": pnl_pct, "capital_usd": cap,
            "limit_pct": limit_pct, "breached": breached}

def is_daily_loss_limit_breached() -> bool:
    try:
        with daily_loss_limit_lock:
            limit_pct = daily_loss_limit_pct
        if limit_pct <= 0:
            return False
        cap = get_total_capital_usd()
        if cap <= 0:
            return False
        pnl_pct = get_today_pnl_usd() / cap * 100
        return pnl_pct <= -abs(limit_pct)
    except Exception as e:
        log(f"WARN is_daily_loss_limit_breached: {e}")
        return False


def cooldown_remaining(symbol: str) -> float:
    """Sisa detik cooldown utk symbol ini (ambil yang terpanjang antara cooldown normal
    COOLDOWN_SECONDS dan cooldown diperpanjang pasca hold-no-sell). 0 kalau tidak cooldown."""
    with last_closed_lock:
        ts = last_closed_ts.get(symbol)
    normal_sisa = max(0.0, COOLDOWN_SECONDS - (time.time() - ts)) if ts is not None else 0.0
    with hold_no_sell_lock:
        ts2 = hold_no_sell_ts.get(symbol)
    extended_sisa = max(0.0, HOLD_NO_SELL_COOLDOWN_SECONDS - (time.time() - ts2)) if ts2 is not None else 0.0
    return max(normal_sisa, extended_sisa)

def is_in_cooldown(symbol: str) -> bool:
    return cooldown_remaining(symbol) > 0

# ── Cooldown per-simbol setelah gagal sizing/open (04/09/2026, permintaan Mas Budi) ──
# Kasus nyata semalam: NVDABUSDT lolos AI individual + AI batch TrenKonfirmasi-4h berulang
# ~55x dalam ~3 jam (tiap siklus scan ~3-4 menit), SELALU gagal di send_open_long() dgn
# "Account has insufficient balance for requested action" (saldo Spot USDT free $6, kurang
# dari modal minimum) -- krn kandidat yg gagal dibuka TIDAK PERNAH masuk active_deals, dia
# selalu lolos scan ulang dari nol tiap siklus: AI individual dipanggil lagi, AI batch
# dipanggil lagi (2x biaya API tiap kali), 2 notif Telegram terkirim lagi (Babak 1 + Batch
# Analysis), lalu gagal lagi -- 217 notif dalam 1 malam. Ini berlaku utk SEMUA strategi
# (bukan cuma TrenKonfirmasi-4h) krn semua lewat open_deal_with_sizing()/send_open_long().
# Cooldown ini per-SIMBOL (bukan per-strategi) krn penyebab paling umum (saldo kurang)
# adalah kendala akun yg sama utk simbol apapun -- kalaupun penyebabnya spesifik simbol
# (lot size/min-notional/bStock NYSE tutup), tetap wajar utk tidak dicoba ulang tiap 3
# menit. Dicatat di send_open_long() (gagal order riil) dan open_deal_with_sizing() (bStock
# NYSE tutup) -- BUKAN di guard daily-loss-breach/strategy-disabled, krn itu sudah punya
# throttle/notifikasi bot-wide sendiri (per-simbol di sini jadi berlebihan utk kasus itu).
SIZING_FAIL_COOLDOWN_SEC = 1800   # 30 menit -- cukup redam spam ulang tiap 3-4 menit,
                                  # tapi tetap otomatis coba lagi kalau kondisinya sudah membaik
                                  # (mis. Mas Budi topup saldo) tanpa perlu restart bot
_sizing_fail_ts   = {}   # symbol -> epoch detik gagal sizing/open terakhir
_sizing_fail_lock = threading.Lock()

def record_sizing_fail(symbol: str):
    with _sizing_fail_lock:
        _sizing_fail_ts[symbol] = time.time()

def sizing_fail_remaining(symbol: str) -> float:
    """Sisa detik cooldown gagal-sizing utk symbol ini. 0 kalau tidak ada cooldown aktif."""
    with _sizing_fail_lock:
        ts = _sizing_fail_ts.get(symbol)
    if ts is None:
        return 0.0
    return max(0.0, SIZING_FAIL_COOLDOWN_SEC - (time.time() - ts))

# ── Trend-Continuation Quick Re-entry (khusus brkX2-4h, backtest 28/08/2026) ──
# Kasus nyata: CHIP/USDT closed via trailing 18:00 (+2.16%) lalu price lanjut rally
# tapi bot nggak bisa masuk lagi krn brkX2_4h waktu itu nggak pernah dicatat ke
# record_closed() (bug terpisah, sudah dibenarkan di call site di bawah). Backtest
# 78 symbol/~333 hari (scratchpad backtest_trend_continuation_reentry.py):
#   - Versi longgar (asal price balik naik lagi, TANPA re-cek sinyal): avg +1.7~+2.25%
#     tapi worst-case -47.9% (LEBIH BURUK dari worst baseline -32.7%) -- ditolak.
#   - Versi ketat (price reclaim >= exit+0.5% DAN sinyal breakout brkX2-4h lolos
#     ulang, window <=12 jam): N=37, avg +6.89%, WR 100% (vs baseline avg +1.36%,
#     WR 75.5%) -- dipakai.
# Syarat "sinyal lolos ulang" otomatis terpenuhi di caller (thread1d_scan_4h) krn
# baru dipakai setelah symbol lolos scan candidates (breakout ter-validasi ulang).
TREND_CONT_WINDOW_SECONDS = 12 * 3600
TREND_CONT_MARGIN_PCT     = 0.5
TRAIL_REENTRY_FILE = os.path.join(DATA_DIR, "trail_reentry.json")
trail_reentry_info = {}   # symbol -> {'ts': epoch close, 'price': harga exit trailing terakhir}
trail_reentry_lock = threading.Lock()

def load_trail_reentry():
    global trail_reentry_info
    if not os.path.exists(TRAIL_REENTRY_FILE):
        return
    try:
        with open(TRAIL_REENTRY_FILE, 'r') as f: data = json.load(f)
        with trail_reentry_lock:
            trail_reentry_info = data
        log(f"   Loaded trail_reentry_info: {len(trail_reentry_info)} symbol.")
    except Exception as e:
        log(f"WARN gagal baca trail_reentry.json: {e}")

def save_trail_reentry():
    try:
        with trail_reentry_lock: data = dict(trail_reentry_info)
        with open(TRAIL_REENTRY_FILE, 'w') as f: json.dump(data, f, indent=2)
    except Exception as e:
        log(f"WARN gagal simpan trail_reentry.json: {e}")

def record_trail_close(symbol: str, exit_price: float):
    """Catat harga+waktu close via TRAILING (bukan hard-stop/manual) utk cek quick re-entry."""
    with trail_reentry_lock:
        trail_reentry_info[symbol] = {'ts': time.time(), 'price': exit_price}
    save_trail_reentry()

def trend_continuation_ok(symbol: str, current_price: float) -> bool:
    """True kalau symbol ini baru saja closed via trailing (brkX2-4h), dlm window
    <=12 jam, DAN current_price sudah reclaim >= harga exit + margin konfirmasi 0.5%."""
    with trail_reentry_lock:
        info = trail_reentry_info.get(symbol)
    if not info:
        return False
    age = time.time() - info.get('ts', 0)
    if age > TREND_CONT_WINDOW_SECONDS:
        return False
    thresh = info.get('price', 0) * (1 + TREND_CONT_MARGIN_PCT / 100)
    return current_price >= thresh

# Forward-test terpisah khusus trade yang dibuka lewat Quick-Reentry (trend-
# continuation bypass di atas) -- supaya nggak campur dgn hitungan brkX2-4h
# biasa, dan biar kelihatan sendiri apakah mekanisme ini beneran net-positive
# di produksi kayak hasil backtest-nya (28/08/2026).
QUICK_REENTRY_TARGET    = 10
QUICK_REENTRY_FILE = os.path.join(DATA_DIR, "quick_reentry_trades.json")
quick_reentry_trades = []   # list of {'symbol','ts','profit_pct'}
quick_reentry_lock = threading.Lock()

def load_quick_reentry_trades():
    global quick_reentry_trades
    if not os.path.exists(QUICK_REENTRY_FILE):
        return
    try:
        with open(QUICK_REENTRY_FILE, 'r') as f: data = json.load(f)
        with quick_reentry_lock:
            quick_reentry_trades = data
        log(f"   Loaded quick_reentry_trades: {len(quick_reentry_trades)} trade.")
    except Exception as e:
        log(f"WARN gagal baca quick_reentry_trades.json: {e}")

def save_quick_reentry_trades():
    try:
        with quick_reentry_lock: data = list(quick_reentry_trades)
        with open(QUICK_REENTRY_FILE, 'w') as f: json.dump(data, f, indent=2)
    except Exception as e:
        log(f"WARN gagal simpan quick_reentry_trades.json: {e}")

def record_quick_reentry_close(symbol: str, profit_pct: float):
    with quick_reentry_lock:
        quick_reentry_trades.append({'symbol': symbol, 'ts': time.time(), 'profit_pct': profit_pct})
    save_quick_reentry_trades()

def quick_reentry_progress() -> dict:
    with quick_reentry_lock:
        trades = list(quick_reentry_trades)
    n = len(trades)
    if n == 0:
        return {'n': 0, 'win': 0, 'loss': 0, 'total_pct': 0.0}
    win = sum(1 for t in trades if t.get('profit_pct', 0) > 0)
    total = sum(t.get('profit_pct', 0) for t in trades)
    return {'n': n, 'win': win, 'loss': n - win, 'total_pct': total}

trades_csv_lock = threading.Lock()

# Kolom CSV log forward-test (1 baris per trade; ditulis saat OPEN, dilengkapi saat CLOSE)
CSV_FIELDS = [
    'open_time_wib','symbol','strategy','signal_price','entry_price','slip_pct','atr_pct',
    'trail_dist_pct','base_usd','score','rsi_open',
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
        sync_trades_csv_to_drive()
    except Exception as e:
        log(f"   [CSV] gagal tulis OPEN: {e}")

def csv_log_close(symbol: str, close_time_wib: str, exit_price, profit_pct, exit_reason: str,
                 strategy: str = '', base_usd: float | None = None):
    """Lengkapi baris OPEN terakhir untuk symbol ini dengan data exit (rewrite seluruh file).
    Jika tidak ada baris OPEN (deal dibuka sebelum CSV), tambah baris CLOSED baru.
    base_usd diisi dari kapital real posisi jika tersedia; ini mencegah default $8 di pasangan multi-buy seperti ONDO/USDT.
    """
    try:
        symbol = to_display_pair(symbol)
        if base_usd is None:
            base_usd = 0.0
        with trades_csv_lock:
            _csv_ensure_header()
            with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
            target = None
            for r in reversed(rows):
                r['symbol'] = to_display_pair(r.get('symbol', ''))
                if r.get('symbol') == symbol and r.get('status') == 'OPEN':
                    target = r; break
            ep_str  = f"{_fmt_price(exit_price)}" if isinstance(exit_price,(int,float)) else str(exit_price)
            pct_str = f"{profit_pct:.2f}"         if isinstance(profit_pct,(int,float)) else str(profit_pct)
            if target is None:
                log(f"   [CSV] tidak ketemu baris OPEN utk {symbol} — tambah baris CLOSED baru")
                new_row = {f: '' for f in CSV_FIELDS}
                new_row['open_time_wib']  = ''
                new_row['symbol']         = symbol
                new_row['strategy']       = strategy
                new_row['signal_price']   = ep_str
                new_row['entry_price']    = ep_str
                new_row['close_time_wib'] = close_time_wib
                new_row['exit_price']     = ep_str
                new_row['profit_pct']     = pct_str
                new_row['base_usd']       = f"{float(base_usd):.2f}" if base_usd else ''
                new_row['exit_reason']    = exit_reason
                new_row['status']         = 'CLOSED'
                rows.append(new_row)
            else:
                target['close_time_wib'] = close_time_wib
                target['exit_price']     = ep_str
                target['profit_pct']     = pct_str
                target['base_usd']       = f"{float(base_usd):.2f}" if base_usd else target.get('base_usd', '')
                target['exit_reason']    = exit_reason
                target['status']         = 'CLOSED'
            with open(TRADES_CSV, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=CSV_FIELDS); w.writeheader(); w.writerows(rows)
        log(f"   [CSV] CLOSE dicatat: {symbol} | base_usd={base_usd}")
        sync_trades_csv_to_drive()
    except Exception as e:
        log(f"   [CSV] gagal tulis CLOSE: {e}")


def repair_stale_ondo_base_usd(row: dict) -> dict:
    """Koreksi row historical yang jelas stale untuk ONDO/USDT.
    Tujuannya hanya untuk data lama yang sudah jadi $8 default, tanpa memodifikasi trade normal lain.
    """
    try:
        sym = to_display_pair(row.get('symbol', ''))
        if sym != 'ONDO/USDT':
            return row
        status = (row.get('status') or '').upper()
        if status != 'CLOSED':
            return row
        base_raw = row.get('base_usd', '')
        try:
            base_val = float(base_raw) if str(base_raw).strip() not in ('', 'nan', 'None') else 0.0
        except Exception:
            base_val = 0.0
        if base_val > 8.5:
            return row
        exit_reason = (row.get('exit_reason') or '').lower()
        if 'trailing' not in exit_reason and 'manual_inject' not in exit_reason:
            return row
        row['base_usd'] = '50.00'
        row['profit_usd'] = row.get('profit_usd', '')
        return row
    except Exception:
        return row

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
        closed = []
        for r in rows:
            if r.get('status') == 'CLOSED':
                closed.append(repair_stale_ondo_base_usd(r))
        if strategy is not None:
            if strategy == 'akumulasi':
                closed = [r for r in closed if r.get('strategy') in ('akum_entry_a', 'akum_entry_b')]
            else:
                closed = [r for r in closed if (r.get('strategy') or 'brkX2') == strategy]
        # Skip deal dari tahap sebelumnya
        if offset > 0:
            closed = closed[offset:]
        n = len(closed)
        if n == 0:
            return {'n': 0, 'win': 0, 'loss': 0, 'total_pct': 0.0}

        # Ghost deal exclusion: simbol yang sudah close di broker tapi masih terbaca OPEN di CSV
        _GHOST_EXCLUDE = {}
        win = 0; total = 0.0
        for r in closed:
            if r.get('symbol', '').replace('/', '') in _GHOST_EXCLUDE:
                continue
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


def csv_progress_active() -> dict:
    """Gabungkan hanya trade fase aktif yang dipakai heartbeat Telegram."""
    parts = (
        csv_progress('brkX2', offset=FWDTEST_BRKX2_PHASE_OFFSET),
        csv_progress('reversal'),
        csv_progress('brkX2_4h'),
        csv_progress('brkX2_crossema'),
        csv_progress('hunting_4h', offset=HUNTING_FWDTEST_PHASE_OFFSET),
        csv_progress('akumulasi'),
    )
    if all(part is None for part in parts):
        return None
    return {
        'n': sum(part['n'] for part in parts if part),
        'win': sum(part['win'] for part in parts if part),
        'loss': sum(part['loss'] for part in parts if part),
        'total_pct': sum(part['total_pct'] for part in parts if part),
    }


def hunting_live_progress() -> tuple:
    """Return (completed-active-phase stats, number of closes since LIVE)."""
    progress = csv_progress('hunting_4h', offset=HUNTING_FWDTEST_PHASE_OFFSET)
    if not progress:
        return {'n': 0, 'win': 0, 'loss': 0, 'total_pct': 0.0}, 0
    return progress, max(0, progress['n'] - HUNTING_LIVE_BASELINE)


def csv_last_close(strategy: str = None, offset: int = 0) -> dict:
    """Return info deal CLOSED terakhir: symbol, close_time_wib, profit_pct.
    Dipakai untuk heartbeat 'close deal terakhir'."""
    try:
        if not os.path.exists(TRADES_CSV):
            return {}
        with trades_csv_lock:
            with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                rows = list(csv.DictReader(f))
        closed = []
        for r in rows:
            if r.get('status') == 'CLOSED':
                closed.append(repair_stale_ondo_base_usd(r))
        if strategy:
            if strategy == 'akumulasi':
                closed = [r for r in closed if r.get('strategy') in ('akum_entry_a', 'akum_entry_b')]
            else:
                closed = [r for r in closed if (r.get('strategy') or 'brkX2') == strategy]
        if offset > 0:
            closed = closed[offset:]
        if not closed:
            return {}
        last = closed[-1]
        return {
            'symbol':     last.get('symbol', '?'),
            'time':       last.get('close_time_wib', '?'),
            'profit_pct': last.get('profit_pct', '?'),
            'reason':     last.get('exit_reason', '?'),
        }
    except Exception:
        return {}

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
    """True kalau sudah waktunya kirim heartbeat General: jam 05:45, 11:45, 17:45, 23:45 WIB."""
    HEARTBEAT_HOURS   = {5, 11, 17, 23}
    HEARTBEAT_MINUTE  = 45
    HEARTBEAT_WINDOW  = 10  # menit toleransi (05:45–05:55 masih valid)
    if last_sent == 0.0:
        return True  # pertama kali (START)
    now = now_wib()
    seconds_since = time.time() - last_sent
    hours_since   = seconds_since / 3600
    # Jangan kirim jika baru kirim < 4 jam yang lalu
    if hours_since < 4:
        return False
    # Cek apakah sekarang di window jam target (HH:45 – HH:55)
    in_target_hour   = now.hour in HEARTBEAT_HOURS
    in_target_minute = HEARTBEAT_MINUTE <= now.minute < HEARTBEAT_MINUTE + HEARTBEAT_WINDOW
    return in_target_hour and in_target_minute

_log_lock = threading.Lock()

def log(msg):
    with _log_lock:
        print(f"[{now_wib().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

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

def log_oac(event: str, symbol: str, strategy: str, indicators: dict):
    """Append event Open/Armed/Close + semua nilai indikator ke open-arm-close.txt
    dan kirim notifikasi Telegram yang sama.
    event: 'OPEN' | 'ARMED' | 'CLOSE'
    indicators: dict bebas, misal {'atr_pct': 3.2, 'rsi': 55, 'profit_pct': +4.1}
    """
    ts = now_wib().strftime('%Y-%m-%d %H:%M:%S')
    merged = dict(indicators)
    if event.upper() == "OPEN":
        try:
            strat_key = strategy.lower()
            # Substring match — strategy label bisa punya sufiks, mis. 'Akumulasi-4h Entry B'.
            is_4h_strategy = any(tag in strat_key for tag in ("brkx2-4h", "crossema-4h", "hunting-4h", "akumulasi-4h"))
            frame = get_ohlcv_4h(symbol, limit=120) if is_4h_strategy \
                else get_ohlcv(symbol, interval=REVERSAL_TIMEFRAME if "reversal" in strat_key else "12h", limit=120)
            if frame is not None and len(frame):
                frame = compute_indicators_4h(frame) if is_4h_strategy else compute_indicators(frame)
                row = frame.iloc[-1]
                def read_value(column):
                    value = row.get(column)
                    return None if value is None or pd.isna(value) else value
                inferred = {
                    "rsi": read_value("rsi"), "stoch_k": read_value("stoch_k"),
                    "stoch_d": read_value("stoch_d"), "macd_hist": read_value("macd_hist"),
                    "bb_pct": read_value("bb_pct"), "williams_r": read_value("williams_r"),
                    "cci": read_value("cci"), "obv": read_value("obv"),
                    "ema20": read_value("ema20") or read_value("ema_fast"),
                    "st_dir": read_value("st_dir"),
                }
                for key, value in inferred.items():
                    if key not in merged and value is not None:
                        merged[key] = value
        except Exception as error:
            log(f"WARN log_oac indicator enrichment {symbol}: {error}")
    standard_fields = (
        "entry_price", "peak_price", "peak_profit", "arm_pct", "atr_pct", "trail_dist",
        "rsi", "stoch_k", "stoch_d", "macd_hist", "bb_pct", "williams_r", "cci", "obv",
        "ema20", "st_dir", "add_usd", "total_usd", "profit_pct", "exit_reason",
    )
    for field in standard_fields:
        merged.setdefault(field, "—")
    ind_lines = "\n".join(f"  {k}: {merged[k]}" for k in standard_fields)
    text = (
        f"[{ts} WIB] {event} | {to_display_pair(symbol)} | {strategy}\n"
        f"{ind_lines}\n"
        f"{'─'*36}\n"
    )
    try:
        oac_path = os.path.join("/data", "open-arm-close.txt")
        with open(oac_path, "a", encoding="utf-8") as f:
            f.write(text)
    except Exception as e:
        log(f"WARN log_oac file error: {e}")
    threading.Thread(target=drive_append, args=("open-arm-close.txt", text), daemon=True).start()
    tg_msg = f"📋 *{event}* {to_display_pair(symbol)} `{strategy}`\n" + ind_lines
    send_telegram(tg_msg, parse_mode="Markdown")

# ── Riwayat keputusan AI (04/09/2026, permintaan Mas Budi) ──────────────────────────
# SKIP individual, ringkasan Babak 1, dan hasil AI Batch Analysis TIDAK LAGI kirim
# Telegram (SKIP sudah sejak 03/09/2026; Babak 1 + Batch Analysis sekarang, respon
# 217 notif dalam 1 malam -- lihat commit "fix: add per-symbol cooldown..."). Supaya
# riwayatnya tetap ada utk investigasi/penyelidikan nanti (bukan cuma ephemeral di
# log() console Railway), semua keputusan AI (OPEN, SKIP, Babak 1, Batch Analysis)
# dicatat ke sini -- sama persis polanya dgn near_miss_log.txt/open-arm-close.txt
# (append lokal + mirror Google Drive, tidak pernah overwrite).
AI_DECISIONS_LOG = "/data/ai_decisions_log.txt"

def log_ai_decision(text: str):
    """Append 1 entri riwayat keputusan AI ke ai_decisions_log.txt + mirror Drive."""
    try:
        with open(AI_DECISIONS_LOG, "a", encoding="utf-8") as f:
            f.write(text)
        threading.Thread(target=drive_append, args=("ai_decisions_log.txt", text), daemon=True).start()
    except Exception as e:
        log(f"WARN log_ai_decision: {e}")

def log_ai_babak1(strategy_label: str, symbols_display: list, max_approve: int,
                   total_evaluated: int | None = None):
    """Catat ringkasan Babak 1 (pool kandidat yg lolos AI individual, blm final -- masih
    akan dibandingkan ulang di babak 2) ke ai_decisions_log.txt. GANTI kirim Telegram
    per-siklus scan (04/09/2026) -- riwayatnya tetap ada, tapi tidak spam. total_evaluated
    opsional: dipakai strategi yg juga mau lapor total pool yg dievaluasi (termasuk kasus
    0 lolos), spt TrenKonfirmasi-4h."""
    ts = now_wib().strftime('%Y-%m-%d %H:%M:%S')
    n = len(symbols_display)
    eval_line = f"Dievaluasi: {total_evaluated} kandidat | " if total_evaluated is not None else ""
    if n == 0:
        body = f"{eval_line}Lolos AI individual: 0\nTidak lanjut ke babak 2 (tidak ada kandidat).\n"
    else:
        body = (
            f"{eval_line}Lolos AI individual: {n}\n"
            f"Lolos: {', '.join(symbols_display)}\n"
            f"Lanjut ke babak 2 (AI bandingkan semua sekaligus, maks {max_approve} diloloskan)...\n"
        )
    log_ai_decision(f"[{ts} WIB] AI-BABAK-1 | {strategy_label}\n{body}{'─'*36}\n")

def normalize_binance_symbol(symbol: str) -> str:
    """Standardize symbol ke raw Binance: ONDOUSDT.
    Menerima ONDOUSDT, ONDO/USDT, USDT_ONDO.
    """
    if symbol is None:
        return ""
    s = str(symbol).strip().upper()
    if not s:
        return ""
    s = s.replace("/", "").replace(" ", "")
    if "_" in s and s.startswith("USDT_"):
        return f"{s.split('_', 1)[1]}USDT"
    if s.endswith("USDT"):
        return s
    return s


def to_commas_pair(symbol: str) -> str:
    s = normalize_binance_symbol(symbol)
    if not s:
        return ""
    if s.endswith("USDT"):
        return f"USDT_{s.replace('USDT', '')}"
    return f"USDT_{s}"


def to_display_pair(symbol: str) -> str:
    s = normalize_binance_symbol(symbol)
    if not s:
        return ""
    if s.endswith("USDT"):
        return f"{s.replace('USDT', '')}/USDT"
    return s


def estimate_deal_total_usd(deal: dict) -> float:
    """Hitung total modal positip yang seharusnya ditampilkan di web.
    Prioritas:
    1) qty_coin * entry_price saat Binance direct mode (aktual fill / posisi real)
    2) base_usd + add_usd jika data tersimpan
    3) target_usd fallback
    """
    try:
        qty = float(deal.get('qty_coin', 0) or 0)
        entry = float(deal.get('entry_price', 0) or 0)
        if USE_BINANCE_DIRECT and qty > 0 and entry > 0:
            return max(qty * entry, 0.0)
    except Exception:
        pass

    try:
        base_usd = float(deal.get('base_usd', deal.get('target_usd', BASE_ORDER_VOLUME)) or 0)
    except Exception:
        base_usd = float(BASE_ORDER_VOLUME)
    add_usd = 0.0
    try:
        if deal.get('add_fund_sent'):
            add_usd = float(deal.get('add_usd', 0) or 0)
    except Exception:
        add_usd = 0.0
    return base_usd + add_usd

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
        # ── Reconciliation startup (Binance direct mode only) ──────────────────
        # Cek setiap deal apakah masih ada posisi di Binance wallet.
        # Kalau qty=0 → deal sudah close saat bot mati → remove otomatis.
        if USE_BINANCE_DIRECT and active_deals:
            log("   [RECONCILE] Cek posisi Binance vs active_deals...")
            to_remove = []
            for sym, d in list(active_deals.items()):
                asset = sym.replace("USDT","")
                try:
                    qty_total = binance_get_asset_qty_total(asset)
                    qty_coin_saved = float(d.get("qty_coin", 0) or 0)
                    if qty_total > 0:
                        log(f"   [RECONCILE] {sym}: qty={qty_total:.4f} {asset} ✓")
                    elif qty_coin_saved > 0:
                        # Wallet baca 0 tapi deal punya qty_coin > 0 (mungkin Soft Staking)
                        log(f"   [RECONCILE] {sym}: Binance qty=0 tapi qty_coin={qty_coin_saved:.4f} di deal — dipertahankan")
                    else:
                        to_remove.append(sym)
                        log(f"   [RECONCILE] {sym}: qty=0 di Binance → auto-remove (deal sudah close)")
                except Exception as e:
                    log(f"   [RECONCILE] {sym}: error cek qty ({e}) — dipertahankan")
            if to_remove:
                with active_deals_lock:
                    for sym in to_remove:
                        active_deals.pop(sym, None)
                save_active_deals()
                log(f"   [RECONCILE] Dihapus: {to_remove}")
    except Exception as e:
        log(f"WARN gagal baca active_deals.json: {e}")

def save_active_deals():
    try:
        with active_deals_lock: data=dict(active_deals)
        with open(ACTIVE_DEALS_FILE,'w') as f: json.dump(data,f,indent=2,default=_convert)
    except Exception as e:
        log(f"WARN gagal simpan active_deals.json: {e}")

def add_to_active_deals(symbol: str, data: dict):
    enriched = {**data, 'opened_at': now_wib().strftime('%Y-%m-%d %H:%M:%S')}
    # Kalau Binance direct mode: inject qty_coin dan entry_price aktual dari order fill
    if USE_BINANCE_DIRECT and symbol in _binance_pending_qty:
        enriched['qty_coin']    = _binance_pending_qty.pop(symbol)
        enriched['entry_price'] = _binance_pending_price.pop(symbol, enriched.get('entry_price', 0))
    with active_deals_lock:
        active_deals[symbol] = enriched
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
    """Kirim email notifikasi open long ke widodobudi@gmail.com via Gmail SMTP.
    Email OPEN LONG dimatikan (30/08/2026, permintaan user) -- fungsi TETAP dipertahankan
    (tidak dihapus) karena masih dipakai buat notif lain (mis. "AI Decision kembali ON").
    """
    if subject.startswith("OPEN LONG"):
        return
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

def get_binance_usdt_balance() -> float:
    """Baca saldo USDT free dari Binance API (read-only key). Return -1 jika gagal."""
    import hmac, hashlib, urllib.request as _ur, urllib.parse as _up, time as _time
    api_key    = os.environ.get("BINANCE_API_KEY", "")
    api_secret = os.environ.get("BINANCE_API_SECRET", "")
    if not api_key or not api_secret:
        log("WARN [BINANCE] API key tidak ditemukan, gunakan estimasi lokal")
        return -1.0
    try:
        ts      = int(_time.time() * 1000)
        params  = f"timestamp={ts}"
        sig     = hmac.new(api_secret.encode(), params.encode(), hashlib.sha256).hexdigest()
        url     = f"https://api.binance.com/api/v3/account?{params}&signature={sig}"
        req     = _ur.Request(url, headers={"X-MBX-APIKEY": api_key})
        with _ur.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        for b in data.get("balances", []):
            if b["asset"] == "USDT":
                return float(b["free"])
        return 0.0
    except Exception as e:
        log(f"WARN [BINANCE] gagal baca saldo: {e}")
        return -1.0

def get_binance_open_orders_value(symbol: str) -> float:
    """Baca total volume USDT yang terkunci di open orders Binance untuk satu symbol.
    Return -1 jika gagal."""
    import hmac, hashlib, urllib.request as _ur, time as _time
    api_key    = os.environ.get("BINANCE_API_KEY", "")
    api_secret = os.environ.get("BINANCE_API_SECRET", "")
    if not api_key or not api_secret:
        return -1.0
    try:
        ts     = int(_time.time() * 1000)
        params = f"symbol={symbol}&timestamp={ts}"
        sig    = hmac.new(api_secret.encode(), params.encode(), hashlib.sha256).hexdigest()
        url    = f"https://api.binance.com/api/v3/openOrders?{params}&signature={sig}"
        req    = _ur.Request(url, headers={"X-MBX-APIKEY": api_key})
        with _ur.urlopen(req, timeout=5) as resp:
            orders = json.loads(resp.read())
        # Hitung total USDT dari semua BUY order yang aktif
        total = sum(float(o.get("origQty", 0)) * float(o.get("price", 0))
                    for o in orders if o.get("side") == "BUY" and float(o.get("price", 0)) > 0)
        return total
    except Exception as e:
        log(f"WARN [BINANCE] open orders {symbol}: {e}")
        return -1.0


# ── Binance Direct Trading (Railway Pro + static IP + BINANCE_TRADING_KEY) ──────────
BINANCE_TRADING_BASE = "https://api.binance.com"
AUTO_SELL_CONFIG_FILE = os.path.join(DATA_DIR, "auto_sell_config.json")
_auto_sell_last_prices = {}
_auto_sell_filter_cache = {}

def _binance_trading_request(method: str, path: str, params: dict) -> dict:
    """Helper HMAC-signed request ke Binance trading API menggunakan BINANCE_TRADING_KEY."""
    import hmac, hashlib, urllib.parse as _up
    api_key    = os.environ.get("BINANCE_TRADING_KEY", "")
    api_secret = os.environ.get("BINANCE_TRADING_SECRET", "")
    if not api_key or not api_secret:
        raise ValueError("BINANCE_TRADING_KEY/SECRET tidak di-set di env")
    ts = int(time.time() * 1000)
    params["timestamp"] = ts
    query = _up.urlencode(params)
    sig   = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    query += f"&signature={sig}"
    url     = f"{BINANCE_TRADING_BASE}{path}?{query}"
    headers = {"X-MBX-APIKEY": api_key}
    resp    = session.request(method, url, headers=headers, timeout=10)
    data    = resp.json()
    if resp.status_code != 200:
        raise RuntimeError(f"Binance API {resp.status_code}: {data}")
    return data


def binance_buy_market(symbol: str, usdt_amount: float) -> dict:
    """
    Beli MARKET sejumlah usdt_amount USDT untuk symbol.
    Return: {'symbol', 'orderId', 'qty', 'price_avg', 'cost_usdt'}
    """
    params = {
        "symbol":        symbol,
        "side":          "BUY",
        "type":          "MARKET",
        "quoteOrderQty": str(round(usdt_amount, 2)),
    }
    data   = _binance_trading_request("POST", "/api/v3/order", params)
    fills  = data.get("fills", [])
    qty    = float(data.get("executedQty", 0))
    cost   = sum(float(f["price"]) * float(f["qty"]) for f in fills) if fills else usdt_amount
    price_avg = cost / qty if qty > 0 else 0
    log(f"[BINANCE] BUY {symbol}: qty={qty:.6f} avg={price_avg:.6f} cost={cost:.2f} USDT orderId={data.get('orderId')}")
    return {"symbol": symbol, "orderId": data.get("orderId"),
            "qty": qty, "price_avg": price_avg, "cost_usdt": cost}


def binance_sell_market(symbol: str, qty: float) -> dict:
    """
    Jual MARKET sejumlah qty koin untuk symbol.
    Auto fallback: kalau qty > balance aktual → jual semua yang ada (floor ke stepSize).
    """
    # Cek balance aktual dulu — jual maksimal yang ada
    asset = symbol.replace("USDT", "")
    try:
        balance = binance_get_asset_qty(asset)
        if balance > 0 and qty > balance:
            log(f"[BINANCE] SELL {symbol}: qty={qty:.4f} > balance={balance:.4f} → pakai balance aktual")
            qty = balance
    except Exception as e:
        log(f"WARN [BINANCE] cek balance {asset} sebelum sell: {e}")
    # Round qty sesuai stepSize Binance
    qty_str = _binance_format_qty(symbol, qty)
    params = {
        "symbol":   symbol,
        "side":     "SELL",
        "type":     "MARKET",
        "quantity": qty_str,
    }
    data      = _binance_trading_request("POST", "/api/v3/order", params)
    fills     = data.get("fills", [])
    qty_exec  = float(data.get("executedQty", qty))
    proceeds  = sum(float(f["price"]) * float(f["qty"]) for f in fills) if fills else 0
    price_avg = proceeds / qty_exec if qty_exec > 0 else 0
    log(f"[BINANCE] SELL {symbol}: qty={qty_exec} avg={price_avg:.6f} proceeds={proceeds:.2f} USDT orderId={data.get('orderId')}")
    return {"symbol": symbol, "orderId": data.get("orderId"),
            "qty": qty_exec, "price_avg": price_avg, "proceeds_usdt": proceeds}


def load_auto_sell_config() -> dict:
    """Return {"assets": {ASSET: {"enabled":bool,"threshold_usdt":float}, ...}}.
    Migrasi otomatis dari format lama single-asset ({"enabled","asset","threshold_usdt"})
    supaya setting yang sudah aktif (mis. TAO) nggak hilang saat upgrade ke multi-asset."""
    default = {"assets": {}}
    try:
        if os.path.exists(AUTO_SELL_CONFIG_FILE):
            with open(AUTO_SELL_CONFIG_FILE, encoding="utf-8") as file:
                raw = json.load(file)
            if "assets" in raw and isinstance(raw["assets"], dict):
                default["assets"] = raw["assets"]
            elif raw.get("asset"):
                # format lama -> migrasi 1x, langsung simpan ulang format baru
                default["assets"] = {
                    str(raw["asset"]).upper(): {
                        "enabled": bool(raw.get("enabled", False)),
                        "threshold_usdt": float(raw.get("threshold_usdt", 0) or 0),
                    }
                }
                save_auto_sell_config(default)
                log(f"   Migrasi auto_sell_config lama -> multi-asset: {list(default['assets'].keys())}")
    except Exception as error:
        log(f"WARN [BINANCE] auto-sell config: {error}")
    return default


def save_auto_sell_config(config: dict) -> None:
    assets = config.get("assets", {})
    clean = {}
    for asset, cfg in assets.items():
        clean[str(asset).upper()] = {
            "enabled": bool(cfg.get("enabled", False)),
            "threshold_usdt": float(cfg.get("threshold_usdt", 0) or 0),
            "avg_price": float(cfg.get("avg_price", 0) or 0),
            "convert_leftover_bnb": bool(cfg.get("convert_leftover_bnb", False)),
        }
    with open(AUTO_SELL_CONFIG_FILE, "w", encoding="utf-8") as file:
        json.dump({"assets": clean}, file, indent=2)


def upsert_auto_sell_asset(asset: str, enabled: bool, threshold_usdt: float, avg_price: float = None,
                            convert_leftover_bnb: bool = None) -> dict:
    """Tambah/update 1 entry asset di auto-sell config, tanpa ganggu asset lain.
    avg_price/convert_leftover_bnb=None -> pertahankan nilai yang sudah ada (kalau ada)."""
    config = load_auto_sell_config()
    asset = str(asset).upper()
    existing = config["assets"].get(asset, {})
    if avg_price is None:
        avg_price = existing.get("avg_price", 0)
    if convert_leftover_bnb is None:
        convert_leftover_bnb = existing.get("convert_leftover_bnb", False)
    config["assets"][asset] = {
        "enabled": bool(enabled), "threshold_usdt": float(threshold_usdt),
        "avg_price": float(avg_price or 0), "convert_leftover_bnb": bool(convert_leftover_bnb),
    }
    save_auto_sell_config(config)
    return config


def get_pivot_resistance_4h(symbol: str, N4: int = 4, df4=None):
    """Resistance terdekat DI ATAS harga sekarang, TF 4h, via pivot-high N=4 candle
    kiri-kanan. Diekstrak dari get_sell_suggestion() (04/09/2026) supaya bisa dipanggil
    utk KOIN APA SAJA -- termasuk yang TIDAK sedang dipegang (avg_price=0) -- dipakai
    ulang oleh fitur Convert Aset (cari kandidat convert dari hold_no_sell yang stagnan).
    Return None kalau data kurang / tidak ada pivot di atas harga sekarang.
    `df4` bisa dioper langsung (misal dari scan yang sudah fetch duluan) supaya tidak
    fetch ulang; kalau None, fetch sendiri via get_ohlcv_4h(symbol, limit=100)."""
    try:
        if df4 is None:
            df4 = get_ohlcv_4h(symbol, limit=100)
        if df4 is None or len(df4) <= 20:
            return None
        price_now = float(df4["close"].iloc[-1])
        hi_arr = df4["high"].values
        pivots = []
        for pi in range(N4, len(hi_arr) - N4):
            ph = hi_arr[pi]
            if (all(ph > hi_arr[pi - j] for j in range(1, N4 + 1)) and
                    all(ph > hi_arr[pi + j] for j in range(1, N4 + 1))):
                pivots.append(ph)
        res_above = sorted(ph for ph in pivots if ph > price_now)
        return float(res_above[0]) if res_above else None
    except Exception as e:
        log(f"WARN get_pivot_resistance_4h {symbol}: {e}")
        return None


def get_sell_suggestion(asset: str) -> dict:
    """Hitung suggested minimum sell price (breakeven) + konteks teknikal buat Auto Sell
    Asset: resistance terdekat TF 4h (reuse pivot-high N=4 candle, LOGIC SAMA PERSIS
    dengan TP3 Akumulasi) dan arah tren HTF 12h (Supertrend + struktur EMA20/50).
    Breakeven tetap floor mutlak -- ini cuma nambahin KONTEKS, bukan ganti angkanya."""
    symbol = asset.upper() + "USDT"
    avg_price = get_effective_avg_price(asset)
    result = {"breakeven": 0.0, "resistance": None, "warning": None, "htf_trend": None}
    if avg_price <= 0:
        return result
    result["breakeven"] = round(avg_price * 1.002, 8)  # +0.2% buffer fee round-trip

    try:
        resistance = get_pivot_resistance_4h(symbol)
        if resistance is not None:
            result["resistance"] = resistance
            if result["breakeven"] >= resistance * 0.995:
                result["warning"] = (f"harga harus naik 2 tahap: tembus resistance {_fmt_price(resistance)} dulu, "
                                      f"baru lanjut naik lagi sampai breakeven {_fmt_price(result['breakeven'])}")
    except Exception as e:
        log(f"WARN get_sell_suggestion resistance {asset}: {e}")

    try:
        import pandas_ta as _pta
        df_htf = get_ohlcv(symbol, interval="12h", limit=60)
        if df_htf is not None and len(df_htf) > 20:
            st = _pta.supertrend(df_htf["high"], df_htf["low"], df_htf["close"], length=10, multiplier=3.0)
            st_col = [c for c in st.columns if "SUPERTd" in c]
            st_dir = st[st_col[0]].iloc[-1] if st_col else float("nan")
            ema20 = _pta.ema(df_htf["close"], length=20).iloc[-1]
            ema50 = _pta.ema(df_htf["close"], length=50).iloc[-1]
            price_htf = float(df_htf["close"].iloc[-1])
            if st_dir == 1 and price_htf > ema20 > ema50:
                result["htf_trend"] = "uptrend"
            elif st_dir == -1 and price_htf < ema20 < ema50:
                result["htf_trend"] = "downtrend"
            else:
                result["htf_trend"] = "sideways"
    except Exception as e:
        log(f"WARN get_sell_suggestion htf {asset}: {e}")

    return result


_binance_avg_cost_cache: dict = {}   # asset -> (ts, avg_price) -- TTL cache, myTrades agak berat dipanggil tiap request

def get_binance_avg_cost(asset: str, ttl_seconds: int = 300) -> float:
    """Cost-basis asset yg SAAT INI masih dipegang, dari riwayat transaksi Binance (myTrades)
    diproses berurutan waktu: tiap BELI nambah qty & cost, tiap JUAL ngurangin qty & cost
    SECARA PROPORSIONAL (avg cost sisa posisi tetap sama, bukan re-average) -- jadi beli lama
    yg udah kejual duluan nggak nyampur ke rata-rata sisa coin sekarang. Kalau posisi sempat
    nol lalu beli lagi, cost-basis-nya mulai dari nol lagi (fresh average).
    PERKIRAAN -- dipakai sbg suggestion awal yang bisa diedit manual, buat asset yang sama
    sekali belum ada data avg-nya. Catatan: myTrades cuma nampilin 1000 trade terakhir per
    symbol, kalau riwayat lebih panjang dari itu bisa meleset."""
    asset = str(asset).upper()
    cached = _binance_avg_cost_cache.get(asset)
    if cached and (time.time() - cached[0]) < ttl_seconds:
        return cached[1]
    symbol = asset + "USDT"
    avg = 0.0
    try:
        trades = _binance_trading_request("GET", "/api/v3/myTrades", {"symbol": symbol, "limit": 1000})
        trades = sorted(trades, key=lambda t: t.get("time", 0))
        pos_qty = 0.0
        pos_cost = 0.0
        for t in trades:
            qty = float(t["qty"])
            price = float(t["price"])
            if t.get("isBuyer"):
                pos_qty += qty
                pos_cost += qty * price
            elif pos_qty > 0:
                sell_qty = min(qty, pos_qty)
                avg_before = pos_cost / pos_qty
                pos_cost -= sell_qty * avg_before
                pos_qty -= sell_qty
                if pos_qty <= 1e-12:
                    pos_qty = 0.0
                    pos_cost = 0.0
        if pos_qty > 0:
            avg = pos_cost / pos_qty
        n_buys = sum(1 for t in trades if t.get("isBuyer"))
        n_sells = len(trades) - n_buys
        log(f"DEBUG [AUTO-SELL] avg_cost {symbol}: {len(trades)} trade ({n_buys} buy/{n_sells} sell) "
            f"-> pos_qty={pos_qty} pos_cost={pos_cost} avg={avg}")
    except Exception as error:
        log(f"WARN [AUTO-SELL] gagal tarik myTrades {symbol}: {error}")
    _binance_avg_cost_cache[asset] = (time.time(), avg)
    return avg


def get_active_deal_avg_price(asset: str) -> float:
    """entry_price dari deal AKTIF bot untuk symbol ini (kalau ada) -- sudah otomatis
    ke-average tiap kali add fund/DCA (lihat send_add_funds()), jadi ini sumber PALING
    presisi & real-time, tanpa perlu hitung ulang dari riwayat Binance. 0 kalau asset
    ini nggak lagi jadi deal aktif (sudah closed / memang bukan dikelola bot)."""
    symbol = str(asset).upper() + "USDT"
    with active_deals_lock:
        deal = active_deals.get(symbol)
        return float(deal.get("entry_price", 0) or 0) if deal else 0.0


def get_effective_avg_price(asset: str) -> float:
    """Prioritas: entry_price dari deal AKTIF bot saat ini (paling presisi & real-time)
    -> harga rata-rata dari riwayat hold-no-sell bot (deal LAMA yg di-hold pas hard-stop,
    udah nggak aktif lagi, tapi tetap otoritatif) -> avg_price manual yang diisi user di
    dashboard -> rata-rata riwayat BUY di Binance (perkiraan, buat asset yang belum ada
    data sama sekali) -> 0 (tidak diketahui)."""
    asset = str(asset).upper()
    deal_entry = get_active_deal_avg_price(asset)
    if deal_entry > 0:
        return deal_entry
    auto = get_hold_no_sell_price(asset)
    if auto and auto.get("entry_price", 0) > 0:
        return float(auto["entry_price"])
    cfg = load_auto_sell_config()
    manual = float(cfg.get("assets", {}).get(asset, {}).get("avg_price", 0) or 0)
    if manual > 0:
        return manual
    return get_binance_avg_cost(asset)


# ── Convert Aset Hold-No-Sell (04/09/2026, ide + desain penuh bareng Mas Budi) ─────
# Latar: aset hold_no_sell yang stagnan rugi (mis. BICO -7% s/d -8%, lama tidak gerak)
# di-"convert" (jual lalu beli koin lain) ke koin yang jarak-ke-resistance-nya >= magnitude
# kerugian aset sumber (supaya realistis bisa dipakai buat nutup kerugian tsb) DAN ada
# bukti momentum nyata (bukan cuma jarak jauh) -- pakai persis rule R5/R6 yang sudah
# terbukti dari backtest (RVOL>=1.0, harga>EMA20, ATR% >= median rolling-100 milik
# sendiri) -- SENGAJA TIDAK dilonggarkan meski scan riil awalnya 0 kandidat (keputusan
# final Mas Budi 04/09/2026: "biarkan BICO tanpa perlu longgarkan syarat momentum").
# Scope v1: HANYA asset hold_no_sell (BUKAN deal aktif 7 strategi -- sempat didiskusikan
# lalu sengaja ditunda). Eksekusi: jual market lalu beli market berurutan TANPA jeda
# manual (BUKAN Binance Convert API -- bot tidak pernah integrasi ke situ). Target akhir
# = MAX(target_recover, resistance koin baru) supaya minimal balik modal asset sumber.
# Trigger v1: tombol manual di dashboard; scan (find_convert_candidates) & eksekusi
# (execute_convert) sengaja dipisah dari trigger-nya supaya versi scan-otomatis nanti
# bisa reuse tanpa nulis ulang (pola sama seperti check_hunting_candidate/open_hunting_candidate).
CONVERT_MIN_VOL_USD_24H = 5_000_000   # likuiditas minimum kandidat -- cegah slippage besar
CONVERT_GAP_MARGIN_PCT  = 1.0         # margin tambahan di atas syarat murni jarak-ke-resistance (jaga2 fee)
CONVERT_RVOL_MIN        = 1.0         # SENGAJA TIDAK dilonggarkan, lihat catatan di atas
CONVERT_SELL_FEE_PCT    = 0.1         # estimasi fee taker sisi jual di masa depan, dipakai hitung target_recover
CONVERT_COOLDOWN_SEC    = 86400       # cooldown ringan 24 jam -- cegah convert-ulang beruntun ke asset yang sama
_convert_cooldown_ts: dict = {}
_convert_cooldown_lock = threading.Lock()

def record_converted(asset: str):
    """Catat waktu convert -- cooldown ringan supaya asset hasil convert tidak langsung
    di-convert lagi kalau kebetulan kandidat baru juga jelek."""
    with _convert_cooldown_lock:
        _convert_cooldown_ts[str(asset).upper()] = time.time()

def convert_cooldown_remaining(asset: str) -> float:
    with _convert_cooldown_lock:
        ts = _convert_cooldown_ts.get(str(asset).upper())
    if not ts:
        return 0.0
    return max(0.0, CONVERT_COOLDOWN_SEC - (time.time() - ts))


def find_convert_candidates(source_asset: str, max_results: int = 10) -> dict:
    """Scan seluruh pair USDT likuid (exclude asset yang sudah dipegang + blacklist + yang
    masih cooldown) cari kandidat convert utk source_asset yang sedang stagnan rugi.
    Syarat WAJIB kedua-duanya:
      1. Jarak (gap%) harga sekarang -> resistance pivot 4h >= |loss_pct source| + margin.
      2. Momentum nyata (rule R5/R6): RVOL(vol_ma20)>=1.0 AND harga>EMA20 AND
         ATR% >= median rolling-100 ATR% miliknya sendiri.
    Return {'ok','source_asset','loss_pct','min_gap_pct','candidates':[...],'error'}
    candidates diurutkan dari gap% terbesar, tiap item:
      {'symbol','asset','price','resistance','gap_pct','rvol','rsi','vol24h_usd'}"""
    source_asset = str(source_asset).upper()
    avg_price = get_effective_avg_price(source_asset)
    price_now_src = get_price_now(source_asset + "USDT")
    if avg_price <= 0 or price_now_src <= 0:
        return {"ok": False, "error": f"avg_price/harga {source_asset} tidak diketahui", "candidates": []}
    loss_pct = max(0.0, (1 - price_now_src / avg_price) * 100)   # magnitude kerugian, 0 kalau lagi untung
    min_gap_pct = loss_pct + CONVERT_GAP_MARGIN_PCT

    try:
        held = {a["asset"] + "USDT" for a in get_binance_spot_assets()}
    except Exception:
        held = set()
    held.add(source_asset + "USDT")

    try:
        pairs = get_usdt_spot_pairs()
        ticker = get_ticker_24h()
    except Exception as e:
        return {"ok": False, "error": f"gagal ambil data pasar: {e}", "candidates": []}

    volmap = {}
    for t in ticker:
        try:
            volmap[t["symbol"]] = float(t.get("quoteVolume", 0))
        except Exception:
            pass

    universe = [p for p in pairs
                if p not in held and p not in SYMBOL_BLACKLIST and p not in blocked_pairs_meta
                and volmap.get(p, 0) >= CONVERT_MIN_VOL_USD_24H]

    candidates = []
    for sym in universe:
        asset = sym[:-4]  # strip "USDT"
        if convert_cooldown_remaining(asset) > 0:
            continue
        try:
            df4 = get_ohlcv_4h(sym, limit=100)
            if df4 is None or len(df4) < 60:
                continue
            df4 = compute_indicators_4h(df4)
            r = df4.iloc[-1]
            price_now = float(r["close"])
            resistance = get_pivot_resistance_4h(sym, df4=df4)
            if resistance is None or price_now <= 0:
                continue
            gap_pct = (resistance / price_now - 1) * 100
            if gap_pct < min_gap_pct:
                continue
            ema20 = float(r["ema20"]) if pd.notna(r.get("ema20")) else None
            vol_ma = float(r["vol_ma"]) if pd.notna(r.get("vol_ma")) else None
            vol_now = float(r["vol"]) if pd.notna(r.get("vol")) else None
            rvol = (vol_now / vol_ma) if vol_ma and vol_ma > 0 else None
            atrp = float(r["atr_pct"]) if pd.notna(r.get("atr_pct")) else None
            atr_med = df4["atr_pct"].rolling(100).median().iloc[-1] if len(df4) >= 100 else None
            rsi = float(r["rsi"]) if pd.notna(r.get("rsi")) else None
            above_ema20 = bool(ema20 and price_now > ema20)
            atr_ok = bool(atrp is not None and pd.notna(atr_med) and atrp >= atr_med)
            momentum_ok = bool(above_ema20 and rvol is not None and rvol >= CONVERT_RVOL_MIN and atr_ok)
            if not momentum_ok:
                continue
            candidates.append({
                "symbol": sym, "asset": asset, "price": price_now, "resistance": resistance,
                "gap_pct": round(gap_pct, 2), "rvol": round(rvol, 2) if rvol else None,
                "rsi": round(rsi, 1) if rsi else None, "vol24h_usd": volmap.get(sym, 0),
            })
        except Exception as e:
            log(f"WARN find_convert_candidates {sym}: {e}")
            continue

    candidates.sort(key=lambda c: c["gap_pct"], reverse=True)
    return {"ok": True, "source_asset": source_asset, "loss_pct": round(loss_pct, 2),
            "min_gap_pct": round(min_gap_pct, 2), "candidates": candidates[:max_results], "error": None}


def execute_convert(source_asset: str, target_symbol: str) -> dict:
    """Eksekusi convert: JUAL source_asset (market, seluruh saldo bebas) lalu BELI
    target_symbol (market, pakai seluruh proceeds) BERURUTAN TANPA JEDA MANUAL (bukan
    Binance Convert API). Target jual final auto-registrasi ke Auto Sell Asset
    (hold_no_sell tracking) = MAX(target_recover, resistance koin baru) --
    target_recover dihitung supaya modal awal source_asset (avg_price x qty) balik penuh
    setelah dikurangi estimasi fee jual di masa depan. Cooldown ringan dicatat di akhir
    supaya asset hasil convert ini tidak langsung di-convert lagi dalam waktu dekat."""
    source_asset = str(source_asset).upper()
    target_symbol = str(target_symbol).upper()
    target_asset = target_symbol.replace("USDT", "")
    result = {"ok": False, "step": None, "error": None}
    try:
        qty = binance_get_asset_qty(source_asset)
        if qty <= 0:
            result["error"] = f"Saldo {source_asset} tidak ada / habis"
            return result
        avg_price_src = get_effective_avg_price(source_asset)
        invested_usdt = avg_price_src * qty if avg_price_src > 0 else None

        result["step"] = "sell"
        sell = binance_sell_market(source_asset + "USDT", qty)
        proceeds = sell.get("proceeds_usdt", 0)
        if proceeds <= 0:
            result["error"] = f"Jual {source_asset} gagal / proceeds 0"
            result["sell"] = sell
            return result

        result["step"] = "buy"
        buy = binance_buy_market(target_symbol, proceeds)
        if buy.get("qty", 0) <= 0:
            result["error"] = f"Beli {target_symbol} gagal / qty 0 (dana {source_asset} SUDAH terjual!)"
            result["sell"] = sell
            result["buy"] = buy
            return result

        # target_recover: harga target_asset yang, kalau qty hasil beli dijual di situ
        # (dikurangi estimasi fee jual), proceeds-nya >= modal awal source_asset.
        target_recover = None
        if invested_usdt and buy["qty"] > 0:
            target_recover = invested_usdt / (buy["qty"] * (1 - CONVERT_SELL_FEE_PCT / 100))
        resistance = get_pivot_resistance_4h(target_symbol)
        candidates_final = [v for v in (target_recover, resistance) if v]
        target_final = max(candidates_final) if candidates_final else None

        if target_final:
            upsert_auto_sell_asset(target_asset, True, target_final, avg_price=buy["price_avg"])
        record_converted(target_asset)
        log(f"[CONVERT] {source_asset}->{target_asset}: sold {sell['qty']:.6f} {source_asset} "
            f"@{sell['price_avg']:.6f} (${proceeds:.2f}) -> bought {buy['qty']:.6f} {target_asset} "
            f"@{buy['price_avg']:.6f} | target_recover={target_recover} resistance={resistance} "
            f"target_final={target_final}")
        log_ai_decision(f"[CONVERT] {source_asset} -> {target_asset}: jual {sell['qty']:.6f} "
                         f"@{sell['price_avg']:.6f} (${proceeds:.2f}), beli {buy['qty']:.6f} "
                         f"@{buy['price_avg']:.6f}. target_recover={target_recover} "
                         f"resistance={resistance} target_final={target_final}")
        send_telegram(f"🔄 <b>Convert Aset</b>\n{source_asset} → {target_asset}\n"
                       f"Jual: {sell['qty']:.6f} {source_asset} @ {_fmt_price(sell['price_avg'])} (${proceeds:.2f})\n"
                       f"Beli: {buy['qty']:.6f} {target_asset} @ {_fmt_price(buy['price_avg'])}\n"
                       f"Target jual baru: {_fmt_price(target_final) if target_final else '-'}")

        result.update({"ok": True, "step": "done", "sell": sell, "buy": buy,
                        "target_recover": target_recover, "resistance": resistance,
                        "target_final": target_final})
        return result
    except Exception as e:
        result["error"] = str(e)
        log(f"ERROR execute_convert {source_asset}->{target_symbol}: {e}")
        return result


def remove_auto_sell_asset(asset: str) -> dict:
    config = load_auto_sell_config()
    config["assets"].pop(str(asset).upper(), None)
    save_auto_sell_config(config)
    return config


def get_binance_spot_assets() -> list:
    data = _binance_trading_request("GET", "/api/v3/account", {})
    result = []
    for item in data.get("balances", []):
        asset = str(item.get("asset", "")).upper()
        free = float(item.get("free", 0))
        if asset and free > 0 and asset not in {"USDT", "BIDR", "IDRT"}:
            result.append({"asset": asset, "free": free, "symbol": asset + "USDT"})
    return sorted(result, key=lambda item: item["asset"])


def get_usdt_balance() -> tuple:
    """Saldo USDT Spot wallet saat ini: (free, locked). Return (0.0, 0.0) kalau gagal."""
    try:
        data = _binance_trading_request("GET", "/api/v3/account", {})
        for item in data.get("balances", []):
            if str(item.get("asset", "")).upper() == "USDT":
                return float(item.get("free", 0)), float(item.get("locked", 0))
    except Exception as e:
        log(f"WARN get_usdt_balance: {e}")
    return 0.0, 0.0


def get_earn_flexible_usdt_positions() -> list:
    """List posisi Simple Earn Flexible untuk asset USDT.
    Return list of {"productId": str, "totalAmount": float}. Return [] kalau gagal/kosong."""
    try:
        data = _binance_trading_request("GET", "/sapi/v1/simple-earn/flexible/position", {"asset": "USDT"})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        out = []
        for r in rows:
            amt = float(r.get("totalAmount", 0) or 0)
            if amt > 0 and r.get("productId"):
                out.append({"productId": r["productId"], "totalAmount": amt})
        return out
    except Exception as e:
        log(f"WARN get_earn_flexible_usdt_positions: {e}")
        return []


def redeem_earn_flexible(product_id: str, amount: float = None, redeem_all: bool = False) -> bool:
    """Redeem instant dari Simple Earn Flexible. Return True kalau request diterima."""
    try:
        params = {"productId": product_id, "type": "FAST"}
        if redeem_all:
            params["redeemAll"] = "true"
        else:
            params["amount"] = str(round(amount, 8))
        data = _binance_trading_request("POST", "/sapi/v1/simple-earn/flexible/redeem", params)
        log(f"[EARN] Redeem flexible productId={product_id} amount={amount if not redeem_all else 'ALL'} -> {data}")
        return True
    except Exception as e:
        log(f"WARN redeem_earn_flexible {product_id}: {e}")
        return False


def ensure_spot_usdt(min_needed: float, wait_seconds: int = 12) -> bool:
    """Pastikan saldo Spot USDT free >= min_needed. Kalau kurang, coba redeem otomatis
    dari Simple Earn Flexible sejumlah selisihnya (+buffer kecil utk fee/slippage).
    Return True kalau saldo sudah/berhasil dicukupkan, False kalau tetap kurang."""
    free, _locked = get_usdt_balance()
    if free >= min_needed:
        return True
    shortfall = min_needed - free
    positions = get_earn_flexible_usdt_positions()
    earn_total = sum(p["totalAmount"] for p in positions)
    if earn_total <= 0:
        log(f"[EARN] Saldo Spot USDT kurang (${free:.2f} < ${min_needed:.2f}) dan tidak ada dana di Earn Flexible.")
        return False
    log(f"[EARN] Saldo Spot USDT kurang (${free:.2f} < ${min_needed:.2f}), coba redeem ${shortfall:.2f} dari Earn Flexible (tersedia ${earn_total:.2f})")
    remaining = shortfall * 1.02  # buffer 2% jaga-jaga fee/pembulatan
    for p in positions:
        if remaining <= 0:
            break
        take = min(p["totalAmount"], remaining)
        redeem_all_this = take >= p["totalAmount"]
        if redeem_earn_flexible(p["productId"], amount=take, redeem_all=redeem_all_this):
            remaining -= take
    # Tunggu dana settle ke Spot (redeem flexible FAST biasanya instant, tapi kasih jeda)
    for _ in range(wait_seconds):
        time.sleep(1)
        free, _locked = get_usdt_balance()
        if free >= min_needed:
            log(f"[EARN] Redeem berhasil, saldo Spot USDT sekarang ${free:.2f}")
            return True
    log(f"WARN [EARN] Setelah redeem, saldo Spot USDT masih ${free:.2f} < ${min_needed:.2f}")
    return False


_last_balance_log_ts = 0.0


def _convert_leftover_to_bnb(asset: str, symbol: str, filter_info: dict):
    """Jual sisa saldo asset (leftover ~5% stlh auto-sell utama) -> USDT -> beli BNB,
    biar user bisa numpuk stok BNB buat diskon fee trading 25%. Return dict hasil
    kalau berhasil, None kalau leftover kosong/di bawah minimum order Binance."""
    if asset == "BNB":
        return None
    leftover = binance_get_asset_qty(asset)
    if leftover <= 0:
        return None
    step_size = filter_info.get("step_size", 0)
    if step_size > 0:
        leftover = (leftover // step_size) * step_size
    price_now = get_price_now(symbol)
    if leftover < filter_info.get("min_qty", 0) or leftover * price_now < filter_info.get("min_notional", 0):
        log(f"[AUTO-SELL] {symbol} sisa saldo {leftover} di bawah minimum Binance -- convert BNB dilewati")
        return None
    sell_result = binance_sell_market(symbol, leftover)
    proceeds = sell_result.get("proceeds_usdt", 0)
    if proceeds <= 0:
        return None
    buy_result = binance_buy_market("BNBUSDT", proceeds)
    return {
        "leftover_qty": leftover, "usdt_from_leftover": proceeds,
        "bnb_bought": buy_result.get("qty", 0), "bnb_avg": buy_result.get("price_avg", 0),
    }


def _check_auto_sell_one(asset: str, threshold: float, convert_leftover_bnb: bool = False) -> None:
    """Cek 1 asset: kalau crossing naik lewat threshold, jual 95% saldo bebasnya
    lalu nonaktifkan HANYA entry asset ini (asset lain di daftar tetap jalan).
    convert_leftover_bnb: kalau True, sisa ~5% yg nggak ikut dijual otomatis dikonversi
    ke BNB (jual ke USDT lalu beli BNB) -- buat numpuk stok BNB diskon fee 25%."""
    symbol = asset + "USDT"
    if threshold <= 0:
        return
    price = get_price_now(symbol)
    previous = _auto_sell_last_prices.get(symbol)
    _auto_sell_last_prices[symbol] = price
    if price <= 0 or previous is None or not (previous < threshold <= price):
        return
    quantity = binance_get_asset_qty(asset)
    if quantity <= 0:
        return
    sell_quantity = quantity * 0.95
    try:
        info = _auto_sell_filter_cache.get(symbol)
        if info is None:
            response = session.get(
                f"{BINANCE_TRADING_BASE}/api/v3/exchangeInfo?symbol={symbol}",
                timeout=10,
            )
            response.raise_for_status()
            symbols = response.json().get("symbols", [])
            if not symbols or symbols[0].get("status") != "TRADING":
                log(f"[AUTO-SELL] {symbol} tidak tersedia/aktif di Binance — order dibatalkan")
                return
            filters = {item.get("filterType"): item for item in symbols[0].get("filters", [])}
            lot = filters.get("LOT_SIZE", {})
            min_notional = filters.get("NOTIONAL", filters.get("MIN_NOTIONAL", {}))
            info = {
                "step_size": float(lot.get("stepSize", 1)),
                "min_qty": float(lot.get("minQty", 0)),
                "min_notional": float(min_notional.get("minNotional", 0)),
            }
            _auto_sell_filter_cache[symbol] = info
        step_size = info["step_size"]
        if step_size > 0:
            sell_quantity = (sell_quantity // step_size) * step_size
        if sell_quantity < info["min_qty"] or sell_quantity * price < info["min_notional"]:
            log(f"[AUTO-SELL] {symbol} 95% saldo di bawah filter Binance — order dibatalkan")
            return
    except Exception as error:
        log(f"[AUTO-SELL] {symbol} preflight gagal — order dibatalkan: {error}")
        return
    result = binance_sell_market(symbol, sell_quantity)
    bnb_result = None
    if convert_leftover_bnb:
        try:
            bnb_result = _convert_leftover_to_bnb(asset, symbol, info)
        except Exception as error:
            log(f"WARN [AUTO-SELL] convert leftover ke BNB {asset}: {error}")
    remove_auto_sell_asset(asset)
    msg = (
        f"AUTO SELL {asset}/USDT\n"
        f"Harga crossing: {_fmt_price(price)} USDT\n"
        f"Threshold: {_fmt_price(threshold)} USDT\n"
        f"Saldo bebas: {quantity}\n"
        f"Qty dijual (95%): {sell_quantity}\n"
        f"Hasil: {result.get('proceeds_usdt', 0):.2f} USDT\n"
        "Asset ini otomatis dihapus dari daftar Auto Sell setelah satu eksekusi (asset lain di daftar tetap jalan)."
    )
    if bnb_result:
        msg += (f"\n\nSisa saldo {bnb_result['leftover_qty']} {asset} dikonversi ke BNB:\n"
                f"{bnb_result['usdt_from_leftover']:.2f} USDT -> {bnb_result['bnb_bought']:.6f} BNB "
                f"@ {bnb_result['bnb_avg']:.2f}")
    send_telegram(msg)
    log(f"[AUTO-SELL] {symbol} crossing {threshold} -> sold {quantity}")


def check_auto_sell_crossing() -> None:
    config = load_auto_sell_config()
    for asset, cfg in config.get("assets", {}).items():
        if not cfg.get("enabled"):
            continue
        try:
            _check_auto_sell_one(str(asset).upper(), float(cfg.get("threshold_usdt", 0) or 0),
                                  bool(cfg.get("convert_leftover_bnb", False)))
        except Exception as error:
            log(f"WARN [AUTO-SELL] {asset}: {error}")


# Cache LOT_SIZE stepSize per symbol agar tidak fetch berulang
_lot_size_cache: dict = {}

def _binance_format_qty(symbol: str, qty: float) -> str:
    """
    Format qty sesuai LOT_SIZE stepSize Binance.
    Fetch exchange info kalau belum di-cache.
    Contoh: HOT stepSize=1 → qty=10875.57 → '10875'
            BTC stepSize=0.00001 → qty=0.00008 → '0.00008'
    """
    global _lot_size_cache
    if symbol not in _lot_size_cache:
        try:
            url  = f"https://api.binance.com/api/v3/exchangeInfo?symbol={symbol}"
            resp = session.get(url, timeout=10)
            info = resp.json()
            for filt in info.get("symbols", [{}])[0].get("filters", []):
                if filt.get("filterType") == "LOT_SIZE":
                    step = filt.get("stepSize", "1")
                    _lot_size_cache[symbol] = step
                    break
            else:
                _lot_size_cache[symbol] = "1"
        except Exception as e:
            log(f"WARN [BINANCE] get LOT_SIZE {symbol}: {e}")
            _lot_size_cache[symbol] = "1"
    step_str = _lot_size_cache.get(symbol, "1")
    # Hitung presisi dari stepSize
    if "." in step_str:
        precision = len(step_str.rstrip("0").split(".")[1])
    else:
        precision = 0
    # Round down ke stepSize (floor)
    step = float(step_str)
    if step > 0:
        qty_rounded = (qty // step) * step
    else:
        qty_rounded = qty
    return f"{qty_rounded:.{precision}f}"


def binance_get_asset_qty(asset: str) -> float:
    """Baca qty bebas suatu asset di Binance Spot wallet menggunakan TRADING key."""
    try:
        data = _binance_trading_request("GET", "/api/v3/account", {})
        for b in data.get("balances", []):
            if b["asset"] == asset:
                return float(b["free"])
        return 0.0
    except Exception as e:
        log(f"WARN [BINANCE] get_asset_qty {asset}: {e}")
        return -1.0


def binance_get_asset_qty_total(asset: str) -> float:
    """Baca qty total (free + locked) untuk reconcile — menghindari false-remove akibat Soft Staking."""
    try:
        data = _binance_trading_request("GET", "/api/v3/account", {})
        for b in data.get("balances", []):
            if b["asset"] == asset:
                return float(b["free"]) + float(b.get("locked", 0))
        return 0.0
    except Exception as e:
        log(f"WARN [BINANCE] get_asset_qty_total {asset}: {e}")
        return -1.0


def sync_base_usd_from_binance():
    """Saat startup, baca base_usd dari file persisten /data/deal_base_usd.json
    dan merge ke active_deals. File ini diupdate setiap kali fix_deal_usd dipanggil."""
    base_usd_file = os.path.join(DATA_DIR, "deal_base_usd.json")
    if not os.path.exists(base_usd_file):
        log("[SYNC] deal_base_usd.json belum ada, skip sync")
        return
    try:
        with open(base_usd_file, "r") as f:
            saved = json.load(f)
        updated = 0
        with active_deals_lock:
            for sym, usd in saved.items():
                if sym in active_deals:
                    active_deals[sym]["base_usd"] = float(usd)
                    updated += 1
        if updated > 0:
            save_active_deals()
        log(f"[SYNC] {updated} deal base_usd di-restore dari deal_base_usd.json")
    except Exception as e:
        log(f"WARN [SYNC] gagal baca deal_base_usd.json: {e}")


def get_estimated_locked_usd() -> float:
    """Estimasi USDT yang terkunci di semua active deals (base order saja, konservatif)."""
    with active_deals_lock:
        deals = dict(active_deals)
    total = 0.0
    for d in deals.values():
        # Pakai base_usd kalau ada (lebih akurat), lalu target_usd, lalu fallback per strategi
        if d.get('base_usd'):
            total += float(d['base_usd'])
        elif d.get('target_usd'):
            total += float(d['target_usd'])
        elif d.get('strategy') == 'hunting_4h':
            total += float(HUNTING_ORDER_VOLUME)
        else:
            total += float(BASE_ORDER_VOLUME)
    return total

def has_enough_balance_for_hunting(target_usd: float,
                                   total_capital: float = 90.0) -> bool:
    """Return True kalau estimasi saldo masih cukup untuk open deal senilai target_usd.
    total_capital = estimasi modal awal yang tersedia untuk bot (update manual kalau isi ulang).
    Pakai konstanta HUNTING_CAPITAL_USD di config agar mudah diubah."""
    locked = get_estimated_locked_usd()
    available = total_capital - locked
    ok = available >= target_usd
    if not ok:
        log(f"[HUNT-BALANCE] Saldo estimasi tidak cukup: "
            f"kapital={total_capital} locked={locked:.0f} avail={available:.0f} need={target_usd}")
    return ok

def send_open_long(symbol: str, strategy: str = 'brkX2') -> bool:
    """Buka long position. Kalau USE_BINANCE_DIRECT=True → langsung ke Binance market buy."""
    if not is_strategy_enabled(strategy):
        log(f"[OPEN] {symbol} skip — strategi {strategy} di-disable via Strategy Control")
        return False
    if USE_BINANCE_DIRECT:
        try:
            usd = get_strategy_base_usd(strategy)
            ensure_spot_usdt(float(usd))
            result = binance_buy_market(symbol, float(usd))
            if result.get("qty", 0) > 0:
                # Simpan qty_coin aktual ke active_deals nanti di caller
                _binance_pending_qty[symbol] = result.get("qty", 0)
                _binance_pending_price[symbol] = result.get("price_avg", 0)
                log(f"[BINANCE] OPEN {symbol}: qty={result['qty']} avg={result['price_avg']:.6f}")
                return True
            record_sizing_fail(symbol)
            return False
        except Exception as e:
            log(f"WARN [BINANCE] send_open_long {symbol}: {e}")
            record_sizing_fail(symbol)
            return False
    bid, tok = commas_creds(strategy)
    return send_3commas({"message_type":"bot","bot_id":bid,
        "email_token":tok,"delay_seconds":COMMAS_DELAY_SEC,
        "pair":to_commas_pair(symbol)}, "open_long")

def send_close_long(symbol: str, strategy: str = 'brkX2') -> bool:
    """Close long position. Kalau USE_BINANCE_DIRECT=True → langsung ke Binance market sell."""
    if USE_BINANCE_DIRECT:
        try:
            with active_deals_lock:
                d = active_deals.get(symbol, {})
            qty_coin = d.get("qty_coin", 0)
            asset = symbol.replace("USDT", "")
            # Selalu cek saldo wallet aktual — pakai mana yang lebih besar
            # agar SNX/aset lain yang sempat masuk Earn lalu di-redeem tetap ikut terjual.
            wallet_qty = binance_get_asset_qty(asset)
            if wallet_qty > qty_coin:
                log(f"[BINANCE] CLOSE {symbol}: wallet {wallet_qty:.4f} > qty_coin {qty_coin:.4f} → pakai wallet balance")
                qty_coin = wallet_qty
            if qty_coin <= 0:
                log(f"WARN [BINANCE] send_close_long {symbol}: qty_coin=0, tidak ada posisi")
                return False
            result = binance_sell_market(symbol, qty_coin)
            log(f"[BINANCE] CLOSE {symbol}: proceeds={result.get('proceeds_usdt',0):.2f} USDT")
            return True
        except Exception as e:
            log(f"WARN [BINANCE] send_close_long {symbol}: {e}")
            return False
    bid, tok = commas_creds(strategy)
    return send_3commas({"action":"close_at_market_price","message_type":"bot",
        "bot_id":bid,"email_token":tok,
        "delay_seconds":COMMAS_DELAY_SEC,"pair":to_commas_pair(symbol)}, "close_long")

def send_add_funds(symbol: str, volume, strategy: str = 'brkX2', delay: int = 15) -> bool:
    """Add fund. Kalau USE_BINANCE_DIRECT=True → tambahan buy Binance."""
    if USE_BINANCE_DIRECT:
        try:
            ensure_spot_usdt(float(volume))
            result = binance_buy_market(symbol, float(volume))
            if result.get("qty", 0) > 0:
                # Update qty_coin di active_deals
                with active_deals_lock:
                    if symbol in active_deals:
                        prev_qty = active_deals[symbol].get("qty_coin", 0)
                        new_qty  = prev_qty + result["qty"]
                        prev_cost = prev_qty * active_deals[symbol].get("entry_price", result["price_avg"])
                        new_cost  = prev_cost + result["qty"] * result["price_avg"]
                        active_deals[symbol]["qty_coin"]    = new_qty
                        active_deals[symbol]["entry_price"] = new_cost / new_qty if new_qty > 0 else result["price_avg"]
                log(f"[BINANCE] ADD_FUND {symbol}: +qty={result['qty']} avg={result['price_avg']:.6f}")
                return True
            return False
        except Exception as e:
            log(f"WARN [BINANCE] send_add_funds {symbol}: {e}")
            return False
    bid, tok = commas_creds(strategy)
    return send_3commas({"action":"add_funds_in_quote","message_type":"bot",
        "bot_id":bid,"email_token":tok,
        "delay_seconds":delay,"pair":to_commas_pair(symbol),
        "volume":volume}, "add_funds")

_daily_loss_notify_lock = threading.Lock()
_daily_loss_last_notify_ts = 0.0
_DAILY_LOSS_NOTIFY_COOLDOWN_SEC = 30 * 60  # maks 1x notifikasi klarifikasi tiap 30 menit

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
    """Sizing berdasarkan skor sinyal.
    Base order $8 (BASE_ORDER_VOLUME).
    Skor 0-1 -> $30 (add $22)
    Skor 2-3 -> $45 (add $37)
    Skor 4-5 -> $60 (add $52)
    Basis: backtest_sizing_v2 (155 trade) — aggr lebih tinggi ROI lebih baik. Tier direvisi 24/08/2026."""
    if score >= 4: return 60
    if score >= 2: return 45
    return 30

def open_deal_with_sizing(symbol: str, score: int, strategy: str = 'brkX2'):
    """Buka deal + simpan add_usd di active_deals untuk dikirim T2 setelah deal confirmed.
    Return (ok, target_usd, add_usd)."""
    # Guard: circuit breaker rugi harian (lintas SEMUA strategi) -- lihat is_daily_loss_limit_breached()
    if is_daily_loss_limit_breached():
        log(f"[RISK] {symbol} ({strategy}) skip open -- batas rugi harian tersulut")
        # Notifikasi klarifikasi (03/09/2026, permintaan Mas Budi) -- supaya notif "AI Decision:
        # OPEN" yg terkirim SEBELUM guard ini (dari ai_decision_open, gerbang terpisah) tidak
        # membingungkan: kelihatan "disetujui AI" tapi nyatanya tidak pernah benar2 kebuka.
        # Di-throttle max 1x/30menit (global, lintas symbol/strategi) -- breach ini kondisi
        # bot-wide yg bisa nge-block puluhan kandidat sekaligus, jangan sampai jadi spam baru.
        global _daily_loss_last_notify_ts
        with _daily_loss_notify_lock:
            _now_ts = time.time()
            if _now_ts - _daily_loss_last_notify_ts > _DAILY_LOSS_NOTIFY_COOLDOWN_SEC:
                _daily_loss_last_notify_ts = _now_ts
                _dls = get_daily_loss_status()
                send_telegram(
                    f"⛔ Batas Rugi Harian Tersulut\n"
                    f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                    f"P&L hari ini: {_dls['pnl_pct']:+.2f}% (${_dls['pnl_usd']:+.2f}) | Batas: -{_dls['limit_pct']:.1f}%\n"
                    f"SEMUA open long baru diblokir lintas strategi sampai reset jam 00:00 WIB\n"
                    f"(atau P&L membaik). Contoh yg baru diblokir: {to_display_pair(symbol)} ({strategy}).\n"
                    f"Kalau ada notif 'AI Decision: OPEN' SETELAH pesan ini, itu tetap TIDAK akan\n"
                    f"benar-benar kebuka selama blokir ini masih aktif.",
                    parse_mode=None
                )
        return False, BASE_ORDER_VOLUME, 0
    # Guard: strategi disabled via Strategy Control panel
    if not is_strategy_enabled(strategy):
        log(f"[SIZING] {symbol} skip — strategi {strategy} di-disable via Strategy Control")
        return False, BASE_ORDER_VOLUME, 0
    # Guard: base_usd dari Strategy Control (override konstanta)
    _cfg_base = get_strategy_base_usd(strategy)
    # Guard: bStocks hanya boleh open saat NYSE buka
    if is_bstock_symbol(symbol) and not is_nyse_open():
        log(f"[SIZING] {symbol} adalah bStock, NYSE tutup — open long DIBATALKAN")
        record_sizing_fail(symbol)
        return False, _cfg_base, 0
    if is_bstock_symbol(symbol):
        log(f"[SIZING] {symbol} adalah bStock, NYSE BUKA — open long DIIZINKAN")
    # Hunting pakai base_usd dari Strategy Control (fallback HUNTING_ORDER_VOLUME), tanpa add fund
    if strategy == 'hunting_4h':
        target  = float(_cfg_base if _cfg_base else HUNTING_ORDER_VOLUME)
        add_usd = 0
    # Akumulasi pakai base_usd dari Strategy Control (fallback BASE_ORDER_VOLUME), tanpa add fund
    elif strategy in ('akum_entry_a', 'akum_entry_b'):
        target  = float(_cfg_base if _cfg_base else BASE_ORDER_VOLUME)
        add_usd = 0
    # brkX2_4h di Binance direct: pakai base_usd dari Strategy Control, tanpa sizing skor
    elif strategy == 'brkX2_4h' and USE_BINANCE_DIRECT:
        target  = float(_cfg_base if _cfg_base else BASE_ORDER_VOLUME)
        add_usd = 0
    else:
        target  = max(score_to_target_usd(score), _cfg_base)
        add_usd = max(0, target - _cfg_base)
    ok = send_open_long(symbol, strategy)
    if not ok:
        return False, target, 0
    return True, target, add_usd

def send_start_trailing(symbol: str, strategy: str = 'brkX2') -> bool:
    """Aktifkan trailing. Binance direct mode: trailing dikelola T2 Python, tidak perlu kirim ke 3Commas."""
    if USE_BINANCE_DIRECT:
        log(f"[BINANCE] send_start_trailing {symbol}: skip (trailing dikelola T2 Python)")
        return True  # return True agar alur normal tidak terganggu
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
            # Exclude tokenized stocks (bStocks Binance):
            # Hanya exclude kalau tidak punya permission SPOT sama sekali
            # bStocks yang punya SPOT diizinkan masuk universe — NYSE filter yang jaga di level eksekusi
            psets = s.get('permissionSets', [])
            flat_perms = set()
            for pset in psets:
                if isinstance(pset, list): flat_perms.update(pset)
                else: flat_perms.add(pset)
            has_spot = 'SPOT' in flat_perms
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
    _stoch=ta.stoch(high,low,close,k=14,d=3,smooth_k=3)
    _kcol=[c for c in _stoch.columns if 'STOCHk' in c]
    _dcol=[c for c in _stoch.columns if 'STOCHd' in c]
    df['stoch_k']=_stoch[_kcol[0]] if _kcol else float('nan')
    df['stoch_d']=_stoch[_dcol[0]] if _dcol else float('nan')
    try:
        _bb=ta.bbands(close,length=20,std=2)
        _bb_pct_col=[c for c in _bb.columns if 'BBP' in c]
        df['bb_pct']=_bb[_bb_pct_col[0]] if _bb_pct_col else float('nan')
    except Exception: df['bb_pct']=float('nan')
    try: df['williams_r']=ta.willr(high,low,close,length=14)
    except Exception: df['williams_r']=float('nan')
    try: df['cci']=ta.cci(high,low,close,length=14)
    except Exception: df['cci']=float('nan')
    try: df['obv']=ta.obv(close,df['vol'])
    except Exception: df['obv']=float('nan')
    # 2 dari 3 bar bullish: minimal 2 candle terakhir close > open
    df['bull3'] = (
        (df['close'] > df['open']) &
        (df['close'].shift(1) > df['open'].shift(1)) &
        (df['close'].shift(2) > df['open'].shift(2))
    )
    df['bull2of3'] = (
        (df['close'] > df['open']).astype(int) +
        (df['close'].shift(1) > df['open'].shift(1)).astype(int) +
        (df['close'].shift(2) > df['open'].shift(2)).astype(int) >= 2
    )
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


# ── ATH distance filter (jarak harga ke All-Time-High) ───────────────────────
# Dipicu kasus BICOUSDT (01/09/2026): entry Reversal-8h di harga -97.8% dari ATH
# (turun -93% sejak listing, -49% YTD) -- calc_perf_score() TIDAK menangkap ini krn
# equal-weight count TF positif (BICO tetap skor 0.667 >= 0.5 walau 1Y -77.7%, krn
# 1D/1W/1M/3M/6M semua sempat positif). Backtest full-lifecycle Reversal-8h (180
# trade, ~333 hari) BUKTIKAN jarak-ke-ATH itu sinyal beda -- konsisten & monoton:
# makin dekat ATH, makin bagus rata2 hasil (+2.27% di cap -95% naik ke +4.11% di
# cap -70%; sisi yg diblok konsisten lebih jelek). Threshold dipilih -85% (n=72,
# WR 83.3%, avg +2.99% vs baseline 81.7%/+2.29%) -- keseimbangan sample vs uplift.
REVERSAL_ATH_DIST_MIN_PCT = -85.0   # tolak kalau harga <= 85% di bawah ATH-nya

_ath_cache_1d: dict = {}   # sym -> (ts_arr, high_cummax_arr) — di-load lazy per symbol

def _get_ath_data_1d(sym: str):
    """Load histori HIGH 1D (cummax causal) utk symbol. Cache lokal dulu, fallback fetch Binance.
    Cache file terpisah dari _get_perf_data_1d() (itu cuma simpan close, ini butuh high)."""
    if sym in _ath_cache_1d:
        return _ath_cache_1d[sym]
    candidates = [
        "/data/_cache_ath_1D",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "_cache_ath_1D"),
    ]
    cache_dir = None
    for d in candidates:
        if os.path.isdir(d):
            cache_dir = d; break
    if cache_dir is None:
        try:
            cache_dir = "/data/_cache_ath_1D"
            os.makedirs(cache_dir, exist_ok=True)
        except:
            cache_dir = None
    best = None; best_len = 0
    if cache_dir and os.path.isdir(cache_dir):
        for fname in os.listdir(cache_dir):
            if not fname.startswith(sym) or not fname.endswith(".pkl"): continue
            try:
                df = pickle.load(open(os.path.join(cache_dir, fname), "rb"))
                if df is not None and len(df) > best_len and "ts" in df.columns:
                    best = df; best_len = len(df)
            except: pass
    if best is None or best_len < 700:
        try:
            r = _binance_get("/api/v3/klines", {"symbol": sym, "interval": "1d", "limit": 1000})
            if r is not None:
                try: r = r.json()
                except: r = None
            if r and isinstance(r, list) and len(r) >= 30:
                df_new = pd.DataFrame(r, columns=["ts","open","high","low","close","vol",
                                                   "ct","qv","nt","tb","tq","ig"])
                df_new["ts"]   = df_new["ts"].astype(np.int64)
                df_new["high"] = df_new["high"].astype(float)
                if cache_dir:
                    try:
                        with open(os.path.join(cache_dir, f"{sym}_1d_1000.pkl"), "wb") as f:
                            pickle.dump(df_new[["ts","high"]], f)
                    except: pass
                best = df_new[["ts","high"]]
        except: pass
    if best is None or len(best) < 30:
        _ath_cache_1d[sym] = None; return None
    ts_arr   = best["ts"].values.astype(np.int64)
    high_arr = best["high"].values.astype(float)
    cummax_arr = np.maximum.accumulate(high_arr)
    result = (ts_arr, cummax_arr)
    _ath_cache_1d[sym] = result
    return result

def get_ath_before(sym: str, query_ts_ms: int) -> float:
    """All-Time-High SEBELUM/PADA query_ts_ms (no lookahead). Return nan kalau data tidak ada."""
    data = _get_ath_data_1d(sym)
    if data is None:
        return float('nan')
    ts_arr, cummax_arr = data
    idx_now = int(np.searchsorted(ts_arr, query_ts_ms, side='right')) - 1
    if idx_now < 0: return float('nan')
    ath = float(cummax_arr[idx_now])
    return ath if ath > 0 else float('nan')

def ath_distance_pct(sym: str, current_price: float, query_ts_ms: int) -> float:
    """Jarak current_price ke ATH-nya, dalam %. Negatif = di bawah ATH (mis. -85.0 artinya
    harga cuma 15% dari ATH). Return nan kalau data tidak ada."""
    ath = get_ath_before(sym, query_ts_ms)
    if pd.isna(ath) or ath <= 0 or current_price <= 0:
        return float('nan')
    return (current_price / ath - 1) * 100

def ath_distance_ok(sym: str, current_price: float, query_ts_ms: int, min_pct: float) -> bool:
    """True kalau current_price masih dalam batas min_pct dari ATH (mis. min_pct=-85 →
    harga harus >= 15% dari ATH). True juga kalau data ATH tidak tersedia (fail-open,
    konsisten dgn filter lain di bot ini)."""
    dist = ath_distance_pct(sym, current_price, query_ts_ms)
    if pd.isna(dist):
        return True   # fail-open
    return dist > min_pct

def check_entry(df) -> bool:
    """Evaluasi pada candle TERTUTUP terakhir (mode a).
    Update 30/07/2026: hapus EMA20>EMA50, hapus MACD hist>0,
    ATR<9% (dari <10%), tambah close>EMA50.
    Backtest: backtest_no_ema_no_macd_filter_sweep.py — ATR<9+close>EMA50:
    avg=+3.074% worst=-29.10% wf6 OK.
    Update 01/09/2026: close>EMA20 diganti pita 0% s/d +EMA20_BAND_MAX_PCT%
    (Roadmap Uji Pita EMA20 Fase 1, lihat komentar konstantanya).
    """
    if is_choppy(df): return False
    row = df.iloc[-1]
    if pd.isna(row['ema_fast']) or pd.isna(row['ema_slow']) or pd.isna(row['vol_ma']):
        return False
    if row['st_dir'] != 1: return False
    _gap_ema20_pct = (row['close'] / row['ema_fast'] - 1) * 100 if row['ema_fast'] > 0 else -999
    if not (0 <= _gap_ema20_pct <= EMA20_BAND_MAX_PCT): return False
    # HH3 diganti minimal 2 dari 3 bar bullish
    if not row.get('bull2of3', False): return False
    if row['vol'] < VOLUME_MULT * row['vol_ma']: return False
    if row['vol_ma'] > 0 and (row['vol'] / row['vol_ma']) > VOL_MAX_MULT: return False
    if pd.isna(row['rsi']) or row['rsi'] > RSI_MAX: return False
    _atr_pct = row.get('atr_pct')
    if _atr_pct is not None and not pd.isna(_atr_pct) and _atr_pct >= ATR_MAX_PCT:
        return False
    # close > EMA50 dihapus (07/08/2026, keputusan Budi)
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
    _gap_ema20_pct_d = (row['close']/row['ema_fast']-1)*100 if row['ema_fast'] > 0 else -999
    ema20_band_ok = 0 <= _gap_ema20_pct_d <= EMA20_BAND_MAX_PCT
    checks.append((ema20_band_ok, f"pita EMA20 0%-{EMA20_BAND_MAX_PCT}% (skrg {_gap_ema20_pct_d:+.2f}%, close {_fmt_price(row['close'])} vs EMA20 {_fmt_price(row['ema_fast'])})"))
    # Minimal 2 dari 3 bar bullish
    bull_count = sum(bool(df['close'].iloc[idx] > df['open'].iloc[idx]) for idx in (-1, -2, -3))
    checks.append((bull_count >= 2, f"minimal 2/3 bar bullish ({bull_count}/3)"))
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
    checks.append((close_ema50_ok, f"close>EMA50 (close {_fmt_price(row['close'])} vs EMA50 {_fmt_price(row['ema_slow'])})"))
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
    # Indikator tambahan (untuk laporan notif OPEN/CLOSE, tidak dipakai syarat entry)
    df['rsi'] = ta.rsi(close, length=14)
    _stoch = ta.stoch(high, low, close, k=14, d=3, smooth_k=3)
    _kcol = [c for c in _stoch.columns if 'STOCHk' in c]
    _dcol = [c for c in _stoch.columns if 'STOCHd' in c]
    df['stoch_k'] = _stoch[_kcol[0]] if _kcol else float('nan')
    df['stoch_d'] = _stoch[_dcol[0]] if _dcol else float('nan')
    _macd_df = ta.macd(close, fast=12, slow=26, signal=9)
    df['macd_hist'] = _macd_df[[c for c in _macd_df.columns if 'MACDh' in c][0]]
    try:
        _bb = ta.bbands(close, length=20, std=2)
        _bb_pct_col = [c for c in _bb.columns if 'BBP' in c]
        df['bb_pct'] = _bb[_bb_pct_col[0]] if _bb_pct_col else float('nan')
    except Exception: df['bb_pct'] = float('nan')
    try: df['williams_r'] = ta.willr(high, low, close, length=14)
    except Exception: df['williams_r'] = float('nan')
    try: df['cci'] = ta.cci(high, low, close, length=14)
    except Exception: df['cci'] = float('nan')
    try: df['obv'] = ta.obv(close, df['vol'])
    except Exception: df['obv'] = float('nan')
    return df

def _cross_up(df, idx, ema_col):
    """close transisi dari < EMA ke >= EMA pada idx (vs idx-1)."""
    if idx < 1: return False
    p = df.iloc[idx-1]; cur = df.iloc[idx]
    if pd.isna(p[ema_col]) or pd.isna(cur[ema_col]): return False
    return p['close'] < p[ema_col] and cur['close'] >= cur[ema_col]

def reversal_blockers(df, sym: str | None = None) -> list:
    """Return all Reversal conditions that fail for the current closed-candle setup.
    `sym` opsional -- kalau dikasih, ikut cek jarak-ke-ATH (lihat REVERSAL_ATH_DIST_MIN_PCT)."""
    failures = []
    if len(df) < 6:
        return ["Data candle kurang"]
    if is_choppy(df):
        failures.append("Choppy market")
    n = len(df)
    im3, im2, im1 = n-6, n-5, n-4
    i0, i1, i2 = n-3, n-2, n-1
    c0 = df.iloc[i0]
    if any(pd.isna(c0[x]) for x in ['ema_fast', 'ema_slow', 'body_ratio']):
        failures.append("Indikator tidak tersedia")
        return failures
    bearish_count = sum(df.iloc[idx]['close'] < df.iloc[idx]['open'] for idx in (im3, im2, im1))
    if bearish_count < 2:
        failures.append("Minimal 2/3 candle bearish")
    open_c5 = float(df.iloc[im3]['open'])
    close_c1 = float(df.iloc[im1]['close'])
    drop_pct = (close_c1 / open_c5 - 1) * 100 if open_c5 > 0 else 0
    if not (open_c5 > 0 and drop_pct <= -REVERSAL_DROP_MIN_PCT):
        failures.append(f"Drop minimum {REVERSAL_DROP_MIN_PCT:.0f}%")
    if not (c0['close'] < c0['ema_fast'] and c0['close'] < c0['ema_slow']):
        failures.append("Close di bawah EMA20/50")
    if not (c0['body_ratio'] < REVERSAL_DOJI_MAX):
        failures.append("Doji body ratio")
    if not bool(df['ha_bull'].iloc[i1]):
        failures.append("HA bullish")
    if not (_cross_up(df, i1, 'ema_fast') or _cross_up(df, i2, 'ema_fast')):
        failures.append("Cross up EMA20")
    if sym:
        last = df.iloc[-1]
        entry_price = float(last['close'])
        entry_ts_ms = int(last['ct']) if 'ct' in last.index and not pd.isna(last.get('ct')) else None
        if entry_ts_ms is not None:
            if not ath_distance_ok(sym, entry_price, entry_ts_ms, REVERSAL_ATH_DIST_MIN_PCT):
                failures.append(f"Jarak ke ATH (min {REVERSAL_ATH_DIST_MIN_PCT:.0f}%)")
    return failures

def check_entry_reversal(df, sym: str | None = None) -> bool:
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
    return not reversal_blockers(df, sym)

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
    n_red = sum(df.iloc[idx]['close'] < df.iloc[idx]['open'] for idx in (im3,im2,im1))
    open_c3 = float(df.iloc[im3]['open']); close_c1 = float(df.iloc[im1]['close'])
    drop = (close_c1/open_c3-1)*100 if open_c3>0 else 0
    s1 = n_red >= 2 and drop <= -REVERSAL_DROP_MIN_PCT
    checks.append((s1, f"minimal 2/3 merah+turun>={REVERSAL_DROP_MIN_PCT:.0f}% ({n_red}/3 merah, turun {drop:.1f}%)"))
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

def hard_stop_pct(atr_pct: float):
    """Hard stop % dari entry, di-tier berdasar ATR% (volatilitas pair) lalu dikali HARD_STOP_MULT.
    Pair makin volatil (ATR% tinggi) dikasih toleransi turun lebih lebar supaya noise wajar
    pair itu sendiri nggak langsung kena stop; pair tenang tetap diproteksi ketat.
    Return: (label_rating, base_pct_sebelum_K, final_pct_setelah_dikali_K)
    """
    if atr_pct < 1.0:   label, base = "Tenang",  4.0
    elif atr_pct < 2.0: label, base = "Normal",  5.5
    elif atr_pct < 4.0: label, base = "Aktif",   7.0
    elif atr_pct < 7.0: label, base = "Volatil", 9.0
    else:                label, base = "Ekstrem", 11.0
    return label, base, round(base * HARD_STOP_MULT, 4)

def get_arm_pct(atr_pct: float) -> float:
    """Arm threshold, 5 tier ATR% (konsisten dgn hard_stop_pct()/trailing_dist()).
    Titik Aktif=2.0% & Ekstrem=3.5% tervalidasi backtest_arm_sweep; 3 titik tengah interpolasi.
    """
    if atr_pct < 1.0:   return 1.0    # Tenang
    elif atr_pct < 2.0: return 1.5    # Normal
    elif atr_pct < 4.0: return 2.0    # Aktif
    elif atr_pct < 7.0: return 2.75   # Volatil
    else:                return 3.5    # Ekstrem


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


def estimate_closest_deal_to_close(deals_display: dict):
    """Heuristik (BUKAN prediksi harga pasti): tebak deal Active Deals mana yang paling
    berpotensi closing lebih dulu, berdasar 3 tingkat urgensi (tier 1 paling mendesak):
      1. Trailing sudah armed & harga sudah dekat (<5%) ke garis trailing stop -- bisa
         closing kapan aja kalau harga sedikit koreksi.
      2. Harga sudah dekat (<5%) ke level hard-stop -- berisiko closing rugi.
      3. Fallback: sisa waktu menuju batas timeout candle strategi itu (garansi closing
         paling lambat di titik ini kalau tidak ada trigger lain).
    Return dict {sym, strategy, tier, urgency, note} punya deal paling mendesak, atau None
    kalau tidak ada active deal yang bisa dihitung.
    """
    candidates = []
    now = time.time()
    for sym, d in deals_display.items():
        strat = d.get("strategy", "brkX2")
        entry = d.get("entry_price", 0) or 0
        last  = d.get("last_price", entry) or entry
        peak  = d.get("peak", entry) or entry
        atrp  = d.get("atr_pct", 3.0) or 3.0
        if entry <= 0 or last <= 0:
            continue
        is_akum = strat in ("akum_entry_a", "akum_entry_b")

        opened_ts = (d.get("opened_candle_ts", 0) or 0) / 1000.0
        if strat == "reversal":
            hold_limit_sec = REVERSAL_MAX_HOLD_CANDLES * REVERSAL_SECONDS_PER_CANDLE
        elif strat == "brkX2_4h":
            hold_limit_sec = STRAT4H_MAX_HOLD_CANDLES * STRAT4H_SECONDS
        elif strat in ("brkX2_crossema", "hunting_4h"):
            hold_limit_sec = HUNTING_MAX_HOLD_CANDLES * STRAT4H_SECONDS
        elif is_akum:
            hold_limit_sec = (d.get("timeout_candles", AKUM_ENTRY_TIMEOUT) or AKUM_ENTRY_TIMEOUT) * STRAT4H_SECONDS
        else:
            hold_limit_sec = MAX_HOLD_DAYS * SECONDS_PER_CANDLE
        hours_to_timeout = max(0.0, (hold_limit_sec - (now - opened_ts)) / 3600) if opened_ts > 0 else None

        if hours_to_timeout is not None:
            tier, urgency, note = 3, hours_to_timeout, f"timeout ~{hours_to_timeout:.1f}j lagi"
        else:
            tier, urgency, note = 3, 999.0, "-"

        if not is_akum:
            if d.get("trailing_armed") and peak > 0:
                prof_peak = (peak / entry - 1) * 100 - FEE_ROUND_TRIP_PCT
                tdist = trailing_dist_progressive(atrp, prof_peak)
                stop_price = peak * (1 - tdist / 100)
                if stop_price > 0:
                    gap_pct = (last / stop_price - 1) * 100
                    if gap_pct < 5:
                        tier, urgency, note = 1, gap_pct, f"trailing armed, jarak ke stop {gap_pct:.2f}%"
            if tier == 3:
                _, _, hs_full = hard_stop_pct(atrp)
                hs_price = entry * (1 - hs_full / 100)
                if hs_price > 0:
                    gap_pct = (last / hs_price - 1) * 100
                    if gap_pct < 5:
                        tier, urgency, note = 2, gap_pct, f"mendekati hard-stop, jarak {gap_pct:.2f}%"

        candidates.append({"sym": sym, "strategy": strat, "tier": tier, "urgency": urgency, "note": note})

    if not candidates:
        return None
    candidates.sort(key=lambda c: (c["tier"], c["urgency"]))
    return candidates[0]


def get_hunting_trail_factor(stoch_k: float, st_dir: int, in_fomo: bool) -> float:
    """
    Return trailing factor variatif untuk Hunting-4h:
    - FOMO     : Uptrend + Stoch%K > 80  → TRAIL_FACTOR_FOMO (beri ruang napas)
    - TIGHTENED: in_fomo=True + Stoch%K <= 80 (momentum mulai turun) → TRAIL_FACTOR_TIGHTENED
    - NORMAL   : kondisi lain → TRAIL_FACTOR_NORMAL
    """
    uptrend = (st_dir == 1)
    if uptrend and stoch_k > 80:
        return TRAIL_FACTOR_FOMO
    elif in_fomo and stoch_k <= 80:
        return TRAIL_FACTOR_TIGHTENED
    return TRAIL_FACTOR_NORMAL


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

_trail_htf_cache = {}
_trail_htf_cache_lock = threading.Lock()

def trailing_htf_is_healthy(symbol: str) -> bool:
    """Return True only when both 3D and 1W have a majority of healthy votes."""
    now = time.time()
    with _trail_htf_cache_lock:
        cached = _trail_htf_cache.get(symbol)
        if cached and now - cached[0] < TRAIL_HTF_CACHE_SECONDS:
            return cached[1]
    healthy = False
    try:
        timeframe_results = []
        for interval in ("3d", "1w"):
            frame = get_ohlcv_htf(symbol, interval=interval, limit=120)
            if frame is None or len(frame) < 60:
                raise ValueError(f"data {interval} kurang")
            if frame['ct'].iloc[-1] >= int(now * 1000):
                frame = frame.iloc[:-1]
            if len(frame) < 55:
                raise ValueError(f"candle tertutup {interval} kurang")
            frame = compute_indicators_htf(frame)
            close = frame['close']
            ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
            row = frame.iloc[-1]
            rsi = ta.rsi(close, length=14).iloc[-1]
            macd_ok = not pd.isna(row.get('htf_macd_hist')) and float(row['htf_macd_hist']) > 0
            votes = [
                float(row['close']) > float(ema20),
                float(row['close']) > float(row['htf_ema_slow']),
                macd_ok,
                not pd.isna(rsi) and 50 <= float(rsi) <= 70,
                float(row['close']) > float(row['open']),
            ]
            timeframe_results.append(sum(votes) >= TRAIL_HTF_HEALTH_MIN_VOTES)
        healthy = len(timeframe_results) == 2 and all(timeframe_results)
    except Exception as error:
        log(f"[T2] HTF trailing check {symbol} gagal: {error}")
        healthy = False
    with _trail_htf_cache_lock:
        _trail_htf_cache[symbol] = (now, healthy)
    return healthy

LIVE_ATR_CACHE_SECONDS = 300  # 5 menit — TTL ATR% live utk arm/trailing (26/08/2026)
_live_atr_cache = {}
_live_atr_cache_lock = threading.Lock()

def get_live_atr_pct(symbol: str, strategy: str, fallback: float) -> float:
    """ATR% live (bukan snapshot saat OPEN), dipakai utk tier arm_pct/trailing_dist
    yang reaktif terhadap perubahan volatilitas selama deal berjalan. Cache TTL
    LIVE_ATR_CACHE_SECONDS supaya tidak fetch OHLCV tiap siklus monitor 15 detik
    (ATR sendiri cuma berubah berarti tiap candle closed, bukan tiap detik).
    Gagal fetch -> pakai fallback (biasanya snapshot ATR% saat OPEN)."""
    now = time.time()
    with _live_atr_cache_lock:
        cached = _live_atr_cache.get(symbol)
        if cached and now - cached[0] < LIVE_ATR_CACHE_SECONDS:
            return cached[1]
    atrp = fallback
    try:
        if strategy in ('brkX2_4h', 'brkX2_crossema', 'hunting_4h'):
            frame = get_ohlcv_4h(symbol, limit=30)
        elif strategy == 'reversal':
            frame = get_ohlcv(symbol, interval=REVERSAL_TIMEFRAME, limit=30)
        else:
            frame = get_ohlcv(symbol, interval=TIMEFRAME, limit=30)
        if frame is not None and len(frame) >= 20:
            atr = ta.atr(frame['high'], frame['low'], frame['close'], length=14)
            val = atr.iloc[-1] / frame['close'].iloc[-1] * 100
            if not pd.isna(val):
                atrp = float(val)
    except Exception as error:
        log(f"[T2] live ATR% {symbol} gagal, pakai fallback {fallback:.2f}%: {error}")
    with _live_atr_cache_lock:
        _live_atr_cache[symbol] = (now, atrp)
    return atrp

def current_volatility_guard(symbol: str, strategy: str, entry_atr: float) -> tuple:
    """Return (allowed, atr_pct) for add-fund; unavailable data fails closed."""
    try:
        frame = (get_ohlcv_4h(symbol, limit=60)
                 if strategy in ('brkX2_4h', 'brkX2_crossema', 'hunting_4h')
                 else get_ohlcv(symbol, interval=TIMEFRAME, limit=60))
        if frame is None or len(frame) < 20:
            return False, None
        if 'vol' not in frame.columns:
            return False, None
        frame = frame.copy()
        frame['atr_pct'] = ta.atr(frame['high'], frame['low'], frame['close'], length=14) / frame['close'] * 100
        atr_pct = frame['atr_pct'].iloc[-1]
        if pd.isna(atr_pct):
            return False, None
        atr_pct = float(atr_pct)
        allowed = atr_pct < VOLATILITY_GUARD_ATR_PCT
        if entry_atr > 0 and atr_pct >= entry_atr * VOLATILITY_GUARD_ATR_MULT:
            allowed = False
        return allowed, atr_pct
    except Exception as error:
        log(f"[T2] volatility guard {symbol} gagal: {error}")
        return False, None

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
    """Hitung indikator entry 4h: Supertrend, MACD, ATR%, Vol MA, Vol24h, Stoch, BB%b, WR, CCI, OBV."""
    import pandas_ta as _pta
    df = df.copy()
    c, h, l = df["close"], df["high"], df["low"]
    st = _pta.supertrend(h, l, c, length=STRAT4H_ST_LENGTH, multiplier=STRAT4H_ST_MULT)
    df["st_dir"]     = st[[col for col in st.columns if "SUPERTd" in col][0]]
    # Supertrend KHUSUS CrossEMA-4h (multiplier lebih sensitif, lihat STRAT_CROSSEMA_ST_MULT) --
    # kolom terpisah biar brkX2-4h & Hunting-4h yang masih baca "st_dir" tidak ikut kesenggol
    st_cx = _pta.supertrend(h, l, c, length=STRAT4H_ST_LENGTH, multiplier=STRAT_CROSSEMA_ST_MULT)
    df["st_dir_cx"]  = st_cx[[col for col in st_cx.columns if "SUPERTd" in col][0]]
    macd = _pta.macd(c, fast=STRAT4H_MACD_FAST, slow=STRAT4H_MACD_SLOW,
                     signal=STRAT4H_MACD_SIGNAL)
    df["macd_hist"]  = macd[[col for col in macd.columns if "MACDh" in col][0]]
    df["atr_pct"]    = _pta.atr(h, l, c, length=14) / c * 100
    df["vol_ma"]     = df["vol"].rolling(STRAT4H_VOLUME_MA).mean()
    df["vol24h_usd"] = df["qvol"] * 6   # 6 candle 4h = 24h
    stoch = _pta.stoch(h, l, c)
    sk_col = [col for col in stoch.columns if col.startswith("STOCHk")]
    sd_col = [col for col in stoch.columns if col.startswith("STOCHd")]
    df["stoch_k"] = stoch[sk_col[0]] if sk_col else float("nan")
    df["stoch_d"] = stoch[sd_col[0]] if sd_col else float("nan")
    df["rsi"]   = _pta.rsi(c, length=14)
    df["ema20"] = _pta.ema(c, length=20)
    df["ema50"] = _pta.ema(c, length=50)
    df["chg_from_open"] = (df["close"] - df["open"]) / df["open"].replace(0, float("nan")) * 100
    # Tambahan indicators untuk AI decision context
    try:
        bb = _pta.bbands(c, length=20, std=2)
        bb_pct_col = [col for col in bb.columns if "BBP" in col]
        df["bb_pct"] = bb[bb_pct_col[0]] if bb_pct_col else float("nan")
    except Exception:
        df["bb_pct"] = float("nan")
    try:
        df["williams_r"] = _pta.willr(h, l, c, length=14)
    except Exception:
        df["williams_r"] = float("nan")
    try:
        df["cci"] = _pta.cci(h, l, c, length=14)
    except Exception:
        df["cci"] = float("nan")
    try:
        df["obv"] = _pta.obv(c, df["vol"])
    except Exception:
        df["obv"] = float("nan")
    return df

def htf_filter_4h_ok(symbol: str, for_crossema: bool = False) -> bool:
    """
    HTF filter untuk strategi 4h:
    - brkX2-4h   : 3 candle 12h terakhir BERTURUTAN BULLISH (close > open) — 07/08/2026, keputusan Budi
    - CrossEMA-4h: vol 12h > STRAT_CROSSEMA_HTF_VOL_MULT (1.0) * MA20 volume 12h (dilonggarkan 18/08/2026)
    Fail-open kalau data tidak cukup.
    """
    try:
        df = get_ohlcv_htf(symbol, interval=STRAT4H_HTF_TF, limit=10)
        if df is None or len(df) < 4:
            return True  # fail-open
        if for_crossema:
            # CrossEMA-4h: tetap pakai vol threshold
            if 'vol' in df.columns and 'volume' not in df.columns:
                df = df.rename(columns={'vol': 'volume'})
            vol_ma = df['volume'].rolling(STRAT4H_HTF_VOL_MA).mean().iloc[-1]
            if pd.isna(vol_ma) or vol_ma <= 0:
                return True
            return float(df['volume'].iloc[-1]) > STRAT_CROSSEMA_HTF_VOL_MULT * vol_ma
        else:
            # brkX2-4h: 3 candle 12h terakhir CLOSED berturutan bullish
            # FIX 14/08/2026: iloc[-1]=candle berjalan (belum close) → skip
            # Pakai iloc[-2],[-3],[-4] = 3 candle yang sudah close
            if len(df) < 5: return True
            c1 = df.iloc[-2]; c2 = df.iloc[-3]; c3 = df.iloc[-4]
            return (float(c1['close']) > float(c1['open']) and
                    float(c2['close']) > float(c2['open']) and
                    float(c3['close']) > float(c3['open']))
    except Exception as e:
        log(f"  [HTF4h] error cek {symbol}: {e} → skip filter")
        return True  # fail-open

def blockers_entry_4h(df) -> list:
    """Return every brkX2-4h condition that fails for the latest candle."""
    failures = []
    if len(df) < STRAT4H_MACD_SLOW + STRAT4H_MACD_SIGNAL + 5:
        return ["Data candle kurang"]
    r = df.iloc[-1]
    sd = r.get("st_dir")
    if pd.isna(sd) or sd != 1:
        failures.append("Supertrend bullish")
    mh = r.get("macd_hist")
    if pd.isna(mh) or mh < 0:
        failures.append("MACD histogram")
    atr = r.get("atr_pct")
    if pd.isna(atr) or atr < STRAT4H_ATR_MIN_PCT:
        failures.append("ATR minimum")
    vol_ma = r.get("vol_ma")
    if pd.isna(vol_ma) or vol_ma <= 0:
        failures.append("Volume MA tersedia")
    elif r["vol"] < STRAT4H_VOLUME_MULT * vol_ma:
        failures.append("Volume minimum")
    v24 = r.get("vol24h_usd")
    if not pd.isna(v24) and v24 < STRAT4H_MIN_VOL_USD:
        failures.append("Volume 24h minimum")
    sk = r.get("stoch_k")
    if sk is not None and not pd.isna(sk) and sk >= STRAT4H_STOCH_MAX:
        failures.append("Stoch oversold ceiling")
    rsi = r.get("rsi")
    if rsi is not None and not pd.isna(rsi) and rsi >= STRAT4H_RSI_MAX:
        failures.append("RSI maximum")
    if rsi is not None and not pd.isna(rsi) and rsi <= STRAT4H_RSI_MIN:
        failures.append("RSI minimum")
    if not pd.isna(atr) and atr >= STRAT4H_ATR_MAX_PCT:
        failures.append("ATR maximum")
    if pd.notna(vol_ma) and vol_ma > 0 and r["vol"] > STRAT4H_VOL_MAX_MULT * vol_ma:
        failures.append("Volume maximum")
    chg = r.get("chg_from_open")
    if chg is not None and not pd.isna(chg) and chg > STRAT4H_CHG_MAX_PCT:
        failures.append("Change from open maximum")
    ema20 = r.get("ema20")
    if pd.isna(ema20) or ema20 <= 0:
        failures.append("EMA20 tidak tersedia")
    else:
        gap_ema20_pct = (float(r["close"]) / float(ema20) - 1) * 100
        if gap_ema20_pct < 0 or gap_ema20_pct > STRAT4H_EMA20_BAND_MAX_PCT:
            failures.append("Harga di luar pita EMA20")
    return failures

def check_entry_4h(df) -> bool:
    """
    Entry 4h:
      - Supertrend dir = +1 (uptrend)
      - MACD hist > 0
      - ATR% >= 2.0%
      - Volume >= 0.25x MA20
      - Vol24h >= $3jt
      - Stoch%K < 80 (backtest_4h_rsi_stoch_sweep.py, 31/07/2026)
      - RSI < 60 (07/08/2026, keputusan Budi): hindari entry saat harga sudah terlalu tinggi
      - Harga 0% s/d +0.3% di atas EMA20 (30/08/2026, keputusan Budi, hasil backtest sweep lebar pita):
        entry harus masih dekat EMA20 (baru saja cross-up / belum lari jauh), bukan momentum yang sudah
        lama berjalan jauh dari EMA20
    """
    return not blockers_entry_4h(df)

def active_deal_count_4h() -> int:
    """Jumlah deal aktif strategi brkX2_4h."""
    with active_deals_lock:
        return sum(1 for d in active_deals.values() if d.get("strategy") == "brkX2_4h")

def active_deal_count_akum() -> int:
    """Jumlah deal aktif strategi akumulasi entry A/B."""
    with active_deals_lock:
        return sum(1 for d in active_deals.values()
                   if d.get("strategy") in ("akum_entry_a", "akum_entry_b"))

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
    log(f"[T1] Heartbeat brkX2-12h {'START' if first_time else start_str+' -> '+end_str} — near-miss digabung ke General")
    if HEARTBEAT_TELEGRAM_ENABLED:
        send_telegram(
            f"{header}\n"
            f"\nbrkX2-12h\n"
            f"{status_line}\n"
            f"{t3_str}\n"
            f"\nSlot brkX2-12h: {deal_count_by_strategy('brkX2')}/{MAX_DEALS_BRKX2}",
        )
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
    log(f"[T1b] Heartbeat Reversal-8h — ({status_line})")
    if HEARTBEAT_TELEGRAM_ENABLED:
        send_telegram(
            f"{header}\n"
            f"\nReversal-8h\n"
            f"{status_line}\n"
            f"\nSlot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL}\n"
            f"{prog}"
        )
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
    log(f"[T1d] Heartbeat 4h: {status_line}")
    if HEARTBEAT_TELEGRAM_ENABLED:
        send_telegram(
            f"{header}\n"
            f"\n*4h* : {status_line}"
            f"{near_str}\n"
            f"\nSlot 4h: {active_deal_count_4h()}/{STRAT4H_MAX_DEALS}\n"
            f"{prog}",
            parse_mode="Markdown"
        )
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

    log(f"[T_CX] Heartbeat crossema terkirim")
    n_cx = sum(1 for d in active_deals.values() if d.get('strategy') == 'brkX2_crossema')
    if HEARTBEAT_TELEGRAM_ENABLED:
        send_telegram(
            f"{header}\n"
            f"\nCrossEMA : Slot {n_cx}/{STRAT_CROSSEMA_MAX_DEALS}"
            f"{near_str}\n"
            f"\n{prog_cx}",
            parse_mode="Markdown"
        )
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
    def _fmt_strat(p, tgt, last_close=None):
        if p is None or p['n']==0: return f"#0/{tgt} (belum ada)"
        nn=p['n']; wl=f"{p['win']}W/{p['loss']}L"
        tag=" TERCAPAI!" if nn>=tgt else ""
        base = f"#{nn}/{tgt} ({wl}, total {p['total_pct']:+.1f}%){tag}"
        if last_close and last_close.get('time'):
            sym = last_close.get('symbol','?')
            t   = last_close.get('time','?')
            pct = last_close.get('profit_pct','?')
            base += f"\n    ↳ Last Close: {sym} {t} WIB {pct}%"
        return base
    def _fmt_hunting_live(p, last_close=None):
        if p is None or p['n'] == 0:
            return "LIVE (belum ada close fase aktif)"
        base = f"LIVE: {p['n']} closed ({p['win']}W/{p['loss']}L, total {p['total_pct']:+.1f}%)"
        if last_close and last_close.get('time'):
            base += f"\n    ↳ Last Close: {last_close.get('symbol','?')} {last_close.get('time','?')} WIB {last_close.get('profit_pct','?')}%"
        return base
    prog_all  = csv_progress_active()
    prog_brk  = csv_progress('brkX2', offset=FWDTEST_BRKX2_PHASE_OFFSET)
    prog_rev  = csv_progress('reversal')
    prog_4h   = csv_progress('brkX2_4h')
    prog_cx   = csv_progress('brkX2_crossema')
    prog_hunt = csv_progress('hunting_4h', offset=HUNTING_FWDTEST_PHASE_OFFSET)
    prog_akum = csv_progress('akumulasi')
    prog_rev2  = csv_progress('reversal',    offset=REVERSAL_LIVE_BASELINE)
    prog_4h2   = csv_progress('brkX2_4h',    offset=STRAT4H_LIVE_BASELINE)
    prog_hunt2 = csv_progress('hunting_4h',  offset=HUNTING_FWDTEST_PHASE_OFFSET + HUNTING_LIVE_BASELINE)
    lc_brk    = csv_last_close('brkX2',        offset=FWDTEST_BRKX2_PHASE_OFFSET)
    lc_cx     = csv_last_close('brkX2_crossema')
    lc_akum   = csv_last_close('akumulasi')
    if prog_all is None:
        prog_line = "Progress forward-test: 0 trade selesai (CSV belum ada)."
    else:
        nn=prog_all['n']; wl=f"{prog_all['win']}W/{prog_all['loss']}L"
        prog_qr = quick_reentry_progress()
        prog_line = (f"Progress forward-test (gabungan): {nn} selesai ({wl}, total {prog_all['total_pct']:+.1f}%)\n"
                     f"  - brkX2-12h  : {_fmt_strat(prog_brk,  FWDTEST_TARGET_BRKX2,      lc_brk)}\n"
                     f"  - reversal-8h: {_fmt_hunting_live(prog_rev)}\n"
                     f"    reversal-8h: 2nd {_fmt_strat(prog_rev2, REVERSAL_PHASE2_TARGET)}\n"
                     f"  - brkX2-4h   : {_fmt_hunting_live(prog_4h)}\n"
                     f"    brkX2-4h: 2nd {_fmt_strat(prog_4h2, STRAT4H_PHASE2_TARGET)}\n"
                     f"    brkX2-4h: Quick-Reentry {_fmt_strat(prog_qr, QUICK_REENTRY_TARGET)}\n"
                     f"  - crossema-4h: {_fmt_strat(prog_cx,   STRAT_CROSSEMA_FWDTEST,    lc_cx)}\n"
                     f"  - hunting-4h : {_fmt_hunting_live(prog_hunt)}\n"
                     f"    hunting-4h: 2nd {_fmt_strat(prog_hunt2, HUNTING_PHASE2_TARGET)}\n"
                     f"  - akumulasi-4h: {_fmt_strat(prog_akum, AKUM_ENTRY_FWDTEST_TARGET, lc_akum)}")
    # Slot semua
    n_cx = sum(1 for d in active_deals.values() if d.get('strategy') == 'brkX2_crossema')
    slot_line = (f"Slot brkX2-12h: {deal_count_by_strategy('brkX2')}/{MAX_DEALS_BRKX2} | "
                 f"Slot reversal-8h: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL}\n"
                 f"Slot brkX2-4h: {active_deal_count_4h()}/{STRAT4H_MAX_DEALS} | "
                 f"Slot crossema-4h: {n_cx}/{STRAT_CROSSEMA_MAX_DEALS} | "
                 f"Total: {active_deal_count()}/{total_max_deals_all_strategies()}")
    # ── Near-miss gabungan semua strategi ────────────────────────────────────
    near_lines = []
    # brkX2-12h near-miss — format: (n_pass, sym, fails, total, *extra)
    try:
        en = t3_early_near_miss[:]; bn = t3_base_near_miss[:]
        nm12 = []
        for item in (en + bn)[:3]:
            n_pass, sym = item[0], item[1]
            fails = item[2] if len(item) > 2 else []
            total = item[3] if len(item) > 3 else "?"
            nm12.append(f"  • {to_display_pair(sym)} ({n_pass}/{total}): {'; '.join(fails[:2])}")
        if nm12:
            near_lines.append("brkX2-12h near-miss:")
            near_lines.extend(nm12)
    except: pass
    # brkX2-4h near-miss — format: (n_pass, sym, fails, total)
    try:
        with t1d_near_miss_lock:
            nm4h = t1d_near_miss[:]
        if nm4h:
            near_lines.append("brkX2-4h near-miss:")
            for item in nm4h[:3]:
                n_pass, sym = item[0], item[1]
                fails = item[2] if len(item) > 2 else []
                total = item[3] if len(item) > 3 else "?"
                near_lines.append(f"  • {to_display_pair(sym)} ({n_pass}/{total}): {'; '.join(fails[:2])}")
    except: pass
    # CrossEMA near-miss — format: (n_pass, sym, fails, total)
    try:
        nm_cx = _crossema_near_miss[:]
        if nm_cx:
            near_lines.append("CrossEMA-4h near-miss:")
            for item in nm_cx[:3]:
                n_pass, sym = item[0], item[1]
                fails = item[2] if len(item) > 2 else []
                total = item[3] if len(item) > 3 else "?"
                near_lines.append(f"  • {to_display_pair(sym)} ({n_pass}/{total}): {'; '.join(fails[:2])}")
    except: pass
    # Akumulasi top 5
    try:
        akum5 = _akum_top5[:]
        if akum5:
            near_lines.append("Akumulasi-4h top 5:")
            for r in akum5:
                gate_str = "✓" if r.get('gating_ok') else "⚠"
                near_lines.append(
                    f"  • {to_display_pair(r['sym'])} {gate_str} skor {r.get('weighted_score',0)}/100"
                    f" | {_fmt_price(r.get('close',0))}"
                    f" | sideways: {r.get('sideways_start','-')}"
                )
    except: pass
    near_section = ("\n---\nKandidat terdekat:\n" + "\n".join(near_lines)) if near_lines else ""

    if HEARTBEAT_GENERAL_ENABLED: send_telegram(
        f"{header}\n"
        f"---\n"
        f"{prog_line}\n\n"
        f"Bot HIDUP & terus memantau."
    )
    log(f"[HB-GEN] Heartbeat General terkirim")

    # ── Milestone reminder: analisis waktu terbaik deal ──────────────────────
    # Saat brkX2-4h mencapai tepat 30 deal closed, kirim notif Telegram sekali.
    # Tujuan: ingatkan Budi untuk jalankan analyze_deal_timing.py dan cek apakah
    # ada pola jam tertentu yang konsisten lebih profitable setelah cooldown 8j aktif.
    # Background: DODO loss -23.48% (6 Agt) terjadi karena tidak ada cooldown —
    # sudah dipatch. Analisis ulang di 30 deal akan konfirmasi apakah filter jam
    # juga diperlukan atau tidak.
    if prog_4h and prog_4h.get('n') == 30:
        send_telegram(
            "📊 *MILESTONE brkX2-4h: 30 deal closed!*\n\n"
            "Waktunya analisis pola waktu terbaik:\n"
            "1. Download `trades_forwardtest.csv` dari Railway `/data/`\n"
            "2. Jalankan: `py -3.12 analyze_deal_timing.py --csv trades_forwardtest.csv`\n"
            "3. Cek apakah jam 15:00 WIB masih buruk setelah cooldown 8j aktif\n"
            "4. Kalau iya → pertimbangkan filter waktu open deal\n\n"
            "_(notif ini hanya muncul sekali di milestone 30 deal)_",
            parse_mode="Markdown"
        )
        log("[HB-GEN] Milestone 30 deal brkX2-4h — notif analisis timing terkirim")

    # ── Milestone lanjutan: ulangi analisis timing dgn sample lebih besar ────
    # Analisis di #30 (28/08/2026) pakai log Drive open-arm-close.txt (BUKAN
    # analyze_deal_timing.py -- script itu ternyata nggak pernah dibuat):
    # jam 15:00 WIB justru avg terbaik (+4.12%, WR 100%, N=4), tapi sample
    # kelewat tipis (16 trade total, cuma 4 jam kerepresentasi) utk simpulkan
    # apa pun, dan fix cooldown brkX2-4h (commit 61c265c) baru live SETELAH
    # window itu jadi belum ada data pasca-fix sama sekali. Cek ulang di #60
    # (30 trade lagi, kemungkinan besar sudah pasca-fix semua) dgn sample yg
    # lebih layak.
    if prog_4h and prog_4h.get('n') == 60:
        send_telegram(
            "📊 *MILESTONE brkX2-4h: 60 deal closed!*\n\n"
            "Lanjutan analisis pola jam dari #30 (28/08/2026) — waktu itu jam "
            "15:00 WIB kelihatan justru terbaik tapi sample cuma 16 trade "
            "(kelewat tipis) & belum ada data pasca-fix cooldown brkX2-4h.\n\n"
            "Sekarang sample 2x lebih besar & mayoritas pasca-fix — minta "
            "Claude analisis ulang breakdown per jam open (pakai log Drive "
            "open-arm-close.txt atau trades_forwardtest.csv), cek lagi apakah "
            "ada jam yang konsisten lebih jelek.\n\n"
            "_(notif ini hanya muncul sekali di milestone 60 deal)_",
            parse_mode="Markdown"
        )
        log("[HB-GEN] Milestone 60 deal brkX2-4h — notif analisis timing lanjutan terkirim")
    heartbeat_gen_last_sent    = now
    heartbeat_gen_window_start = now_dt

NEAR_MISS_LOG    = "/data/near_miss_log.txt"
TFPCT_BLOCKED_LOG = "/data/tfpct_blocked_log.txt"
SCAN_BLOCKERS_FILE = os.path.join(DATA_DIR, "scan_blockers.json")
_scan_blockers = {}
_scan_blockers_lock = threading.Lock()

def load_scan_blockers():
    """Muat snapshot scan blocker terakhir dari file -- biar survive restart/redeploy.
    Tanpa ini, strategi dgn jendela scan sempit (brkX2-4h, CrossEMA-4h) bakal tampil
    'Belum ada snapshot' tiap restart sampai jendela scan berikutnya kebuka, padahal
    snapshot scan sebelumnya (n-1, n-2, dst) masih valid buat ditampilkan. 29/08/2026."""
    global _scan_blockers
    if not os.path.exists(SCAN_BLOCKERS_FILE):
        return
    try:
        with open(SCAN_BLOCKERS_FILE, 'r') as f: data = json.load(f)
        with _scan_blockers_lock:
            _scan_blockers = data
        log(f"   Loaded scan_blockers: {len(_scan_blockers)} strategi.")
    except Exception as e:
        log(f"WARN gagal baca scan_blockers.json: {e}")

def save_scan_blockers():
    try:
        with _scan_blockers_lock: data = dict(_scan_blockers)
        with open(SCAN_BLOCKERS_FILE, 'w') as f: json.dump(data, f, indent=2)
    except Exception as e:
        log(f"WARN gagal simpan scan_blockers.json: {e}")

def record_scan_blockers(strategy: str, total: int, candidates: int, blockers: dict) -> None:
    """Simpan ringkasan scan terakhir; satu pair boleh gagal di beberapa indikator."""
    with _scan_blockers_lock:
        _scan_blockers[strategy] = {
            "strategy": strategy,
            "scan_time": now_wib().strftime("%Y-%m-%d %H:%M:%S"),
            "total_scanned": total,
            "candidates": candidates,
            "blockers": dict(sorted(blockers.items(), key=lambda item: item[1], reverse=True)),
        }
    save_scan_blockers()

def get_scan_blockers() -> dict:
    with _scan_blockers_lock:
        return {key: dict(value, blockers=dict(value["blockers"])) for key, value in _scan_blockers.items()}

def count_blocker(blockers: dict, label: str, failed: bool) -> None:
    if failed:
        blockers[label] = blockers.get(label, 0) + 1

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
            text = "".join(lines)
            with open(NEAR_MISS_LOG, "a", encoding="utf-8") as f:
                f.write(text)
            # Sync ke Google Drive folder tradingview
            try:
                threading.Thread(target=drive_append, args=("near_miss_log.txt", text), daemon=True).start()
            except Exception:
                pass
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
    if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2:
        log(f"[T1] Slot brkX2 penuh ({deal_count_by_strategy('brkX2')}/{MAX_DEALS_BRKX2}) "
            f"atau total ({active_deal_count()}/{total_max_deals_all_strategies()}), tidak cari entry.")
        with active_deals_lock:
            syms = ", ".join(to_display_pair(s) for s in active_deals.keys()) or "-"
        return f"Slot brkX2/total penuh — deal aktif: {syms}. Tidak cari entry baru."

    # filter BTC (kalau diaktifkan)
    if BTC_FILTER_ENABLED and not btc_filter_ok():
        log("[T1] Filter BTC: kondisi tidak lolos, scan dibatalkan.")
        return "Filter BTC aktif & tidak lolos — scan dibatalkan periode ini."

    candidates = []
    near_miss = []   # (n_pass, sym, fails) untuk heartbeat kandidat terdekat
    scan_total = 0
    scan_blockers = {}
    newest_ts = 0
    all_dfs   = {}   # sym -> df terakhir (untuk re-entry indicator comparison)
    for sym in universe:
        with active_deals_lock:
            if sym in active_deals: continue
        if sym in SYMBOL_BLACKLIST: continue
        df = get_ohlcv(sym, limit=120)
        if df is None: continue
        # mode (a): pastikan candle terakhir SUDAH tutup
        # candle tutup saat ct < waktu sekarang (ms)
        if df['ct'].iloc[-1] >= int(time.time()*1000):
            df = df.iloc[:-1]  # buang candle berjalan, pakai yg sudah tutup
            if len(df) < 60: continue
        df = compute_indicators(df)
        scan_total += 1
        newest_ts = max(newest_ts, int(df['ct'].iloc[-1]))
        all_dfs[sym] = df   # simpan untuk perbandingan indikator re-entry
        row = df.iloc[-1]
        count_blocker(scan_blockers, "Supertrend", row.get('st_dir') != 1)
        count_blocker(scan_blockers, "Close vs EMA20", not (row.get('close', 0) > row.get('ema_fast', 0)))
        count_blocker(scan_blockers, "Minimal 2/3 candle bullish", not bool(row.get('bull2of3', False)))
        vol_ratio = (row.get('vol', 0) / row.get('vol_ma')) if row.get('vol_ma', 0) else 0
        count_blocker(scan_blockers, "Volume minimum", vol_ratio < VOLUME_MULT)
        count_blocker(scan_blockers, "Volume maksimum", vol_ratio > VOL_MAX_MULT)
        count_blocker(scan_blockers, "RSI", pd.isna(row.get('rsi')) or row.get('rsi') > RSI_MAX)
        count_blocker(scan_blockers, "ATR maksimum", row.get('atr_pct') is not None and not pd.isna(row.get('atr_pct')) and row.get('atr_pct') >= ATR_MAX_PCT)
        if check_entry(df):
            # HTF 3D filter
            if HTF_FILTER_ENABLED and not htf_filter_ok(sym):
                count_blocker(scan_blockers, "HTF 3D", True)
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
                        count_blocker(scan_blockers, "Performance score", True)
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

    record_scan_blockers("brkX2-12h", scan_total, len(candidates), scan_blockers)
    if not candidates:
        log(f"[T1] {len(universe)} coin discan, tidak ada yg lolos syarat entry.")
        last_processed_candle_ts = newest_ts
        log_near_miss("brkX2-12h", near_miss, 9)
        update_dashboard_near_miss("brkX2-12h", near_miss)
        return f"TIDAK ADA coin lolos 7 syarat inti + 2 tambahan (HTF 3D, Perf). ({len(universe)} coin discan)\n" + format_near_miss(near_miss, 9)

    # urutkan kandidat: ATR% terkecil (paling stabil) dulu
    candidates.sort(key=lambda x: x[2])
    log(f"[T1] {len(candidates)} kandidat lolos. Slot terpakai {active_deal_count()}/{total_max_deals_all_strategies()}")

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

    held_bx12 = {}   # 03/09/2026 pola 2-babak: symbol -> data kandidat yg lolos babak 1
    cooldown_held = []   # (sym, sisa_detik) -- kandidat 7/7 valid tapi masih cooldown internal
    for sym, signal_price, atrp, score in candidates:
        if len(held_bx12) >= AI_BATCH_POOL_SIZE:
            log(f"[T1] Kolam babak-1 penuh ({AI_BATCH_POOL_SIZE}), berhenti kumpulkan kandidat baru siklus ini.")
            break
        with active_deals_lock:
            if sym in active_deals:
                continue  # sudah punya deal di pair ini
        sisa = cooldown_remaining(sym) if is_cooldown_enabled('brkX2') else 0
        if sisa > 0:
            log(f"[T1] {sym} LOLOS 7/7 tapi masih cooldown internal (sisa {sisa/3600:.1f} jam) -> skip, tidak kirim sinyal.")
            cooldown_held.append((sym, sisa))
            continue  # jangan kirim webhook yg pasti ditolak 3Commas (cegah deal hantu)
        if sizing_fail_remaining(sym) > 0:
            log(f"[T1] {sym} baru gagal sizing/open (sisa {sizing_fail_remaining(sym)/60:.0f} menit) — skip, jangan coba lagi dulu.")
            continue
        log(f"[T1] SINYAL: {sym} close_candle={_fmt_price(signal_price)} atr%={atrp:.2f} skor={score}")

        _df_ai = all_dfs.get(sym)
        _rsi_ai_val = None
        _rvol_bx12 = 0.0; _bb_bx12 = 0.0; _gap_bx12 = 0.0
        if _df_ai is not None and len(_df_ai) > 0:
            _r_ai = _df_ai.iloc[-1]
            if not pd.isna(_r_ai.get('rsi')): _rsi_ai_val = float(_r_ai['rsi'])
            _vm_bx12 = _r_ai.get('vol_ma')
            if not pd.isna(_vm_bx12) and _vm_bx12 > 0: _rvol_bx12 = float(_r_ai['vol']) / float(_vm_bx12)
            if not pd.isna(_r_ai.get('bb_pct')): _bb_bx12 = float(_r_ai['bb_pct'])
            _ef_bx12 = _r_ai.get('ema_fast')
            if not pd.isna(_ef_bx12) and _ef_bx12 > 0:
                _gap_bx12 = (signal_price / float(_ef_bx12) - 1) * 100

        # AI decision jika toggle "AI Call on Open" aktif utk strategi brkX2 (Strategy Control)
        if is_ai_call_open_enabled('brkX2'):
            _ai_ind = {'atr_pct': f"{atrp:.2f}%", 'score': score, 'signal_price': _fmt_price(signal_price)}
            if _rsi_ai_val is not None: _ai_ind['rsi'] = f"{_rsi_ai_val:.1f}"
            if not ai_decision_open(sym, 'brkX2-12h', _ai_ind, active_deal_count(), notify=False):
                log(f"[T1] {sym} babak-1 di-skip oleh AI individual")
                continue

        held_bx12[sym] = {
            'signal_price': signal_price, 'atrp': atrp, 'score': score,
            'detail': {'atr_pct': atrp, 'rvol': _rvol_bx12, 'bb_pct': _bb_bx12,
                       'gap_ema20_pct': _gap_bx12, 'rsi': _rsi_ai_val or 0.0},
        }

    # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus ============
    approved_bx12 = []
    if held_bx12:
        log(f"[T1] Babak 1 selesai: {len(held_bx12)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
        log_ai_babak1('brkX2-12h', [to_display_pair(s) for s in held_bx12.keys()], AI_BATCH_MAX_APPROVE)
        batch_input_bx12 = [{'symbol': s, 'strategy': 'brkX2', 'score': v['score'], 'detail': v['detail']}
                             for s, v in held_bx12.items()]
        approved_bx12 = ai_decision_batch_rank(
            batch_input_bx12, strategy_label='brkX2-12h', max_approve=AI_BATCH_MAX_APPROVE,
            criteria_note="Kriteria: skor, ATR%, RVOL, BB%b, jarak EMA20, RSI",
        )

    opened_any = False
    for sym in approved_bx12:
        # berhenti kalau slot brkX2 ATAU total sudah penuh
        if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2:
            log(f"[T1] Slot brkX2/total penuh, sisa kandidat tidak dibuka.")
            break
        with active_deals_lock:
            if sym in active_deals:
                continue  # sudah punya deal di pair ini
        v = held_bx12[sym]
        signal_price = v['signal_price']; atrp = v['atrp']; score = v['score']

        ok, target_usd, add_usd = open_deal_with_sizing(sym, score, 'brkX2')
        if ok:
            entry_price = get_price_now(sym)
            if entry_price <= 0:
                entry_price = signal_price
            slip_pct = (entry_price/signal_price - 1) * 100 if signal_price > 0 else 0.0
            # Ambil row indikator terkini (dipakai utk _open fields + notif + re-entry compare)
            df_saved = all_dfs.get(sym)
            r = df_saved.iloc[-1] if df_saved is not None else None
            def _rv(col):
                if r is None: return None
                v = r.get(col)
                return None if v is None or pd.isna(v) else float(v)
            _open_fields = {
                'rsi_open': _rv('rsi'), 'stoch_k_open': _rv('stoch_k'), 'stoch_d_open': _rv('stoch_d'),
                'macd_hist_open': _rv('macd_hist'), 'bb_pct_open': _rv('bb_pct'),
                'williams_r_open': _rv('williams_r'), 'cci_open': _rv('cci'), 'obv_open': _rv('obv'),
                'ema20_open': _rv('ema_fast'),
            }
            add_to_active_deals(sym, {
                'entry_price': entry_price, 'peak': entry_price,
                'signal_price': signal_price, 'atr_pct': atrp,
                'opened_candle_ts': int(newest_ts), 'trailing_armed': False,
                'strategy': 'brkX2', 'score': score, 'target_usd': target_usd,
                'add_usd': add_usd, 'add_fund_sent': False,
                **_open_fields,
            })
            # Simpan indikator saat open untuk perbandingan re-entry berikutnya
            try:
                if df_saved is not None:
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
            _ind_block = _fmt_indicators_open_block(_open_fields)
            send_telegram(
                f"brkX2-12h | OPEN LONG\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (candle close): {_fmt_price(signal_price)}\n"
                f"Selisih (lonjakan/slippage): {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Skor sinyal: {score}/5 -> modal ${target_usd}{addfund_txt}\n"
                f"Slot terpakai: {active_deal_count()}/{total_max_deals_all_strategies()}\n"
                f"{_ind_block}"
            )
            threading.Thread(target=send_email_open_long, args=("OPEN LONG brkX2-12h: " + to_display_pair(sym), 
                f"brkX2-12h | OPEN LONG\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (candle close): {_fmt_price(signal_price)}\n"
                f"Selisih (lonjakan/slippage): {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Skor sinyal: {score}/5 -> modal ${target_usd}{addfund_txt}\n"
                f"Slot terpakai: {active_deal_count()}/{total_max_deals_all_strategies()}\n"
                f"{_ind_block}"
            ), daemon=True).start()
            
            csv_log_open({
                'open_time_wib': now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': to_display_pair(sym),
                'signal_price': f"{_fmt_price(signal_price)}",
                'entry_price': f"{_fmt_price(entry_price)}",
                'slip_pct': f"{slip_pct:+.2f}",
                'atr_pct': f"{atrp:.2f}",
                'trail_dist_pct': f"{trailing_dist(atrp)}",
                'base_usd': target_usd,
                'score': score,
                'rsi_open': f"{_open_fields['rsi_open']:.1f}" if _open_fields.get('rsi_open') is not None else '',
                'strategy': 'brkX2',
            })
            _r12 = r   # r == df_saved.iloc[-1] (dihitung di atas), bukan df.iloc[-1] (sisa loop scan simbol lain)
            log_oac('OPEN', sym, 'brkX2-12h', {
                'entry_price':  _fmt_price(entry_price),
                'slip_pct':     f"{slip_pct:+.2f}%",
                'atr_pct':      f"{atrp:.2f}%",
                'rsi':          f"{_r12.get('rsi', float('nan')):.1f}",
                'stoch_k':      f"{_r12.get('stoch_k', float('nan')):.1f}",
                'vol_ratio':    f"{(_r12['vol']/_r12['vol_ma']):.2f}x" if _r12.get('vol_ma') else 'n/a',
                'bull3':        str(_r12.get('bull3', False)),
                'htf_vol':      str(_get_htf_values(sym).get('htf_vol_ratio', 'n/a')),
                'score':        score,
                'trail_dist':   f"{trailing_dist(atrp)}%",
            })
            
            # ── DEAL LOG lengkap ──────────────────────────────────────────
            _ind = _row_indicators(r, vol_ma=float(df_saved['vol_ma'].iloc[-1]) if df_saved is not None and 'vol_ma' in df_saved.columns else None)
            _htf = _get_htf_values(sym)
            deal_log_write({
                'timestamp_wib':    now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'event_type':       'OPEN',
                'strategy':         'brkX2',
                'symbol':           to_display_pair(sym),
                'thread':           'T1',
                'signal_price':     f"{_fmt_price(signal_price)}",
                'entry_price':      f"{_fmt_price(entry_price)}",
                'slip_pct':         f"{slip_pct:+.2f}",
                'score':            score,
                'base_usd':         target_usd,
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
    """Scan strategi reversal (minimal 2/3 merah+turun>=3% + doji + 1 HA bull + cross EMA20) di timeframe 8h.
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
    if deal_count_by_strategy('reversal') >= MAX_DEALS_REVERSAL:
        log(f"[T1b] Slot reversal penuh ({deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL}) "
            f"atau total ({active_deal_count()}/{total_max_deals_all_strategies()}).")
        return f"Slot reversal/total penuh. Tidak cari entry reversal."

    candidates = []
    near_miss = []
    scan_blockers = {}
    all_dfs_rev = {}   # sym -> df terakhir (utk isi indikator notif OPEN)
    for sym in universe:
        with active_deals_lock:
            if sym in active_deals:
                count_blocker(scan_blockers, "Deal aktif", True)
                continue   # satu coin, satu deal (lintas strategi)
        if sym in SYMBOL_BLACKLIST:
            count_blocker(scan_blockers, "Blacklist", True)
            continue
        df = get_ohlcv(sym, interval=REVERSAL_TIMEFRAME, limit=120)
        if df is None or len(df) < 60:
            count_blocker(scan_blockers, "Data OHLCV kurang", True)
            continue
        # mode (a): pastikan candle terakhir SUDAH tutup
        if df['ct'].iloc[-1] >= int(time.time()*1000):
            df = df.iloc[:-1]
            if len(df) < 60:
                count_blocker(scan_blockers, "Data candle tertutup kurang", True)
                continue
        df = compute_indicators_reversal(df)
        all_dfs_rev[sym] = df   # simpan utk isi indikator notif OPEN
        failures = reversal_blockers(df, sym)
        for failure in failures:
            count_blocker(scan_blockers, failure, True)
        if not failures:
            # Performance filter
            if PERF_FILTER_ENABLED:
                candle_ts = int(df['ct'].iloc[-1])
                pscore = calc_perf_score(sym, candle_ts)
                if pd.isna(pscore) or pscore < PERF_SCORE_MIN:
                    count_blocker(scan_blockers, "Performance score", True)
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

        record_scan_blockers("Reversal-8h", len(universe), len(candidates), scan_blockers)
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

    held_rev = {}   # 03/09/2026 pola 2-babak: symbol -> data kandidat yg lolos babak 1
    for sym, signal_price, atrp, cts in candidates:
        if len(held_rev) >= AI_BATCH_POOL_SIZE:
            log(f"[T1b] Kolam babak-1 penuh ({AI_BATCH_POOL_SIZE}), berhenti kumpulkan kandidat baru siklus ini.")
            break
        with active_deals_lock:
            if sym in active_deals: continue
        if is_cooldown_enabled('reversal') and cooldown_remaining(sym) > 0:
            log(f"[T1b] {sym} cooldown {cooldown_remaining(sym)/3600:.1f}j — skip")
            continue
        if sizing_fail_remaining(sym) > 0:
            log(f"[T1b] {sym} baru gagal sizing/open (sisa {sizing_fail_remaining(sym)/60:.0f} menit) — skip, jangan coba lagi dulu.")
            continue
        log(f"[T1b] SINYAL REVERSAL: {sym} close_candle={_fmt_price(signal_price)} atr%={atrp:.2f}")

        # Perkaya konteks TF sendiri (8h) buat AI -- sebelumnya cuma atr_pct+signal_price,
        # padahal compute_indicators_reversal() sudah hitung RSI/Stoch/MACD/BB%b/Williams%R/
        # CCI/OBV/EMA20 (dipakai utk _open_fields_rev SETELAH open, tapi tidak pernah dikirim
        # ke AI). Reuse df yg sudah ada di all_dfs_rev (tanpa fetch tambahan). 01/09/2026.
        _dfr_ai = all_dfs_rev.get(sym)
        _rsi = _sk = _sd = _mh = _bb = _wr = _cci = _ema20 = None
        if _dfr_ai is not None and len(_dfr_ai) >= 2:
            _rr_ai = _dfr_ai.iloc[-1]; _rp_ai = _dfr_ai.iloc[-2]
            def _aiv(col):
                v = _rr_ai.get(col) if col in _rr_ai.index else None
                return None if v is None or pd.isna(v) else float(v)
            _rsi = _aiv('rsi'); _sk = _aiv('stoch_k'); _sd = _aiv('stoch_d')
            _mh = _aiv('macd_hist'); _bb = _aiv('bb_pct'); _wr = _aiv('williams_r'); _cci = _aiv('cci')
            _ema20 = _aiv('ema_fast')

        if is_ai_call_open_enabled('reversal'):
            _ai_ind = {'atr_pct': f"{atrp:.2f}%", 'signal_price': _fmt_price(signal_price)}
            if _rsi is not None: _ai_ind['rsi'] = f"{_rsi:.1f}"
            if _sk is not None and _sd is not None: _ai_ind['stoch'] = f"K={_sk:.1f} D={_sd:.1f}"
            if _mh is not None: _ai_ind['macd_hist'] = f"{_mh:+.6f}"
            if _bb is not None: _ai_ind['bb_pct'] = f"{_bb:.2f}"
            if _wr is not None: _ai_ind['williams_r'] = f"{_wr:.1f}"
            if _cci is not None: _ai_ind['cci'] = f"{_cci:.1f}"
            if _ema20 is not None and _ema20 > 0:
                _ai_ind['gap_ema20'] = f"{(signal_price/_ema20-1)*100:+.2f}%"
            if _dfr_ai is not None and len(_dfr_ai) >= 2:
                _obv_now = _aiv('obv')
                _obv_prv = _rp_ai.get('obv') if 'obv' in _rp_ai.index else None
                if _obv_now is not None and _obv_prv is not None and not pd.isna(_obv_prv):
                    _ai_ind['obv'] = '↑' if _obv_now > float(_obv_prv) else '↓'
            if not ai_decision_open(sym, 'Reversal-8h', _ai_ind, active_deal_count(), notify=False):
                log(f"[T1b] {sym} babak-1 di-skip oleh AI individual")
                continue

        _gap_rev = (signal_price / _ema20 - 1) * 100 if _ema20 and _ema20 > 0 else 0.0
        held_rev[sym] = {
            'signal_price': signal_price, 'atrp': atrp, 'cts': cts,
            'detail': {'atr_pct': atrp, 'rvol': 0.0, 'bb_pct': _bb or 0.0,
                       'gap_ema20_pct': _gap_rev, 'rsi': _rsi or 0.0},
        }

    # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus ============
    approved_rev = []
    if held_rev:
        log(f"[T1b] Babak 1 selesai: {len(held_rev)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
        log_ai_babak1('Reversal-8h', [to_display_pair(s) for s in held_rev.keys()], AI_BATCH_MAX_APPROVE)
        batch_input_rev = [{'symbol': s, 'strategy': 'reversal', 'score': 0, 'detail': v['detail']}
                            for s, v in held_rev.items()]
        approved_rev = ai_decision_batch_rank(
            batch_input_rev, strategy_label='Reversal-8h', max_approve=AI_BATCH_MAX_APPROVE,
            criteria_note="Kriteria: ATR%, BB%b, jarak EMA20, RSI (reversal 8h tidak hitung RVOL)",
        )

    opened_any = False
    for sym in approved_rev:
        if is_daily_loss_limit_breached():
            log(f"[T1b] Batas rugi harian tersulut, sisa kandidat reversal tidak dibuka.")
            break
        if deal_count_by_strategy('reversal') >= MAX_DEALS_REVERSAL:
            log(f"[T1b] Slot reversal/total penuh, sisa kandidat reversal tidak dibuka.")
            break
        with active_deals_lock:
            if sym in active_deals: continue
        v = held_rev[sym]
        signal_price = v['signal_price']; atrp = v['atrp']; cts = v['cts']

        if send_open_long(sym, 'reversal'):
            # 03/09/2026: target_usd tidak pernah didefinisikan di fungsi ini (beda dari
            # brkX2 yg pakai open_deal_with_sizing() yg return target_usd -- reversal pakai
            # send_open_long() yg cuma return bool) -- csv_log_open() di bawah baca
            # 'base_usd': target_usd dan akan NameError begitu reversal berhasil buka deal.
            # Belum pernah ketahuan krn REVERSAL_ENABLED selalu False di produksi sampai
            # perbaikan gate 3Commas di atas. Nilai sama dgn yg dipakai send_open_long()
            # sendiri (get_strategy_base_usd) supaya CSV konsisten dgn modal riil terpakai.
            target_usd = get_strategy_base_usd('reversal')
            entry_price = get_price_now(sym)
            if entry_price <= 0: entry_price = signal_price
            slip_pct = (entry_price/signal_price - 1) * 100 if signal_price > 0 else 0.0
            _dfr = all_dfs_rev.get(sym)
            _rr  = _dfr.iloc[-1] if _dfr is not None else None
            def _rvv(col):
                if _rr is None: return None
                v = _rr.get(col)
                return None if v is None or pd.isna(v) else float(v)
            _open_fields_rev = {
                'rsi_open': _rvv('rsi'), 'stoch_k_open': _rvv('stoch_k'), 'stoch_d_open': _rvv('stoch_d'),
                'macd_hist_open': _rvv('macd_hist'), 'bb_pct_open': _rvv('bb_pct'),
                'williams_r_open': _rvv('williams_r'), 'cci_open': _rvv('cci'), 'obv_open': _rvv('obv'),
                'ema20_open': _rvv('ema_fast'),
            }
            add_to_active_deals(sym, {
                'entry_price': entry_price, 'peak': entry_price,
                'signal_price': signal_price, 'atr_pct': atrp,
                'opened_candle_ts': int(cts), 'trailing_armed': False,
                'strategy': 'reversal',
                **_open_fields_rev,
            })
            send_telegram(
                f"Reversal-8h | OPEN LONG\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (candle close): {_fmt_price(signal_price)}\n"
                f"Selisih (slippage): {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} | total {active_deal_count()}/{total_max_deals_all_strategies()}\n"
                f"{_fmt_indicators_open_block(_open_fields_rev)}"
            )
            threading.Thread(target=send_email_open_long, args=("OPEN LONG Reversal-8h: " + to_display_pair(sym), 
                f"Reversal-8h | OPEN LONG\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (candle close): {_fmt_price(signal_price)}\n"
                f"Selisih (slippage): {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} | total {active_deal_count()}/{total_max_deals_all_strategies()}\n"
                f"{_fmt_indicators_open_block(_open_fields_rev)}"
            ), daemon=True).start()
            
            csv_log_open({
                'open_time_wib': now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': to_display_pair(sym),
                'signal_price': f"{_fmt_price(signal_price)}",
                'entry_price': f"{_fmt_price(entry_price)}",
                'slip_pct': f"{slip_pct:+.2f}",
                'atr_pct': f"{atrp:.2f}",
                'trail_dist_pct': f"{trailing_dist(atrp)}",
                'base_usd': target_usd,
                'score': 0,
                'strategy': 'reversal',
                'rsi_open': f"{_open_fields_rev['rsi_open']:.1f}" if _open_fields_rev.get('rsi_open') is not None else '',
            })
            log_oac('OPEN', sym, 'Reversal-8h', {
                'entry_price': _fmt_price(entry_price),
                'slip_pct':    f"{slip_pct:+.2f}%",
                'atr_pct':     f"{atrp:.2f}%",
                'trail_dist':  f"{trailing_dist(atrp)}%",
            })
            opened_any = True
            
    last_rev_candle_ts = newest_rev
    return None if opened_any else f"{len(candidates)} kandidat reversal lolos tapi tak ada yg dibuka."
def _fmt_indicators_open_block(d: dict) -> str:
    """Format blok indikator _open (RSI/Stoch/MACD/BB/WR/CCI/OBV) utk notif Telegram."""
    def g(key, digits=1):
        v = d.get(key)
        return f"{float(v):.{digits}f}" if v is not None else "—"
    return (
        f"RSI: {g('rsi_open')} | Stoch K: {g('stoch_k_open')} | Stoch D: {g('stoch_d_open')}\n"
        f"MACD hist: {g('macd_hist_open', 5)} | BB%: {g('bb_pct_open', 3)}\n"
        f"Williams R: {g('williams_r_open')} | CCI: {g('cci_open')} | OBV: {g('obv_open', 0)}"
    )

def enrich_deal_open_indicators(symbol: str, deal: dict) -> dict:
    """Isi indikator report yang belum tersimpan dari candle timeframe strategi."""
    indicator_keys = (
        'rsi_open', 'stoch_k_open', 'stoch_d_open', 'macd_hist_open',
        'bb_pct_open', 'williams_r_open', 'cci_open', 'obv_open', 'ema20_open',
    )
    if all(deal.get(key) is not None for key in indicator_keys):
        return deal
    try:
        strategy = deal.get('strategy', 'brkX2')
        if strategy in ('brkX2_4h', 'brkX2_crossema', 'hunting_4h', 'akum_entry_a', 'akum_entry_b', 'trend_confirm_4h'):
            frame = get_ohlcv_4h(symbol, limit=120)
            frame = compute_indicators_4h(frame) if frame is not None else None
        else:
            interval = REVERSAL_TIMEFRAME if strategy == 'reversal' else '12h'
            frame = get_ohlcv(symbol, interval=interval, limit=120)
            frame = compute_indicators(frame) if frame is not None else None
        if frame is None or len(frame) == 0:
            return deal
        row = frame.iloc[-1]
        def value(column):
            raw = row.get(column)
            return None if raw is None or pd.isna(raw) else float(raw)
        updates = {
            'rsi_open': value('rsi'),
            'stoch_k_open': value('stoch_k'),
            'stoch_d_open': value('stoch_d'),
            'macd_hist_open': value('macd_hist'),
            'bb_pct_open': value('bb_pct'),
            'williams_r_open': value('williams_r'),
            'cci_open': value('cci'),
            'obv_open': value('obv'),
            'ema20_open': value('ema20') or value('ema_fast'),
        }
        for key, new_value in updates.items():
            if deal.get(key) is None and new_value is not None:
                deal[key] = new_value
        with active_deals_lock:
            if symbol in active_deals:
                active_deals[symbol].update({k: v for k, v in updates.items() if v is not None})
        save_active_deals()
    except Exception as error:
        log(f"[T2] {symbol} indicator report backfill gagal: {error}")
    return deal


def thread2_monitor():
    global _last_balance_log_ts
    want_fast = False  # jadi True jika ada deal armed yg harganya bergerak cepat
    if USE_BINANCE_DIRECT and (time.time() - _last_balance_log_ts) >= 300:
        _last_balance_log_ts = time.time()
        try:
            _usdt_free, _usdt_locked = get_usdt_balance()
            log(f"[T2] Saldo USDT Spot wallet: free=${_usdt_free:.2f} locked=${_usdt_locked:.2f}")
        except Exception as _e:
            log(f"WARN [T2] gagal cek saldo USDT: {_e}")
    try:
        check_auto_sell_crossing()
    except Exception as error:
        log(f"WARN [AUTO-SELL] monitor error: {error}")
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
        # Simpan last_price sesegera mungkin agar dashboard tetap update walau
        # langkah berikutnya (mis. add fund gagal) menyebabkan 'continue'.
        with active_deals_lock:
            if sym in active_deals: active_deals[sym]['last_price'] = price
        d['last_price'] = price
        d = enrich_deal_open_indicators(sym, d)

        # Update stoch_k dan st_dir untuk trailing factor variatif hunting
        if d.get('strategy') == 'hunting_4h':
            try:
                _df4 = get_ohlcv_4h(sym, limit=60)
                if _df4 is not None:
                    _df4 = compute_indicators_4h(_df4)
                    _r4  = _df4.iloc[-1]
                    _sk_new  = float(_r4.get('stoch_k', float('nan'))) if 'stoch_k' in _r4.index and not pd.isna(_r4.get('stoch_k')) else float('nan')
                    _std_new = int(_r4.get('st_dir', 0)) if 'st_dir' in _r4.index and not pd.isna(_r4.get('st_dir')) else 0
                    with active_deals_lock:
                        if sym in active_deals:
                            active_deals[sym]['last_stoch_k'] = _sk_new
                            active_deals[sym]['last_st_dir']  = _std_new
                    d = dict(active_deals.get(sym, d))
            except Exception as _e:
                pass
        add_usd      = d.get('add_usd', 0)
        add_fund_sent = d.get('add_fund_sent', False)
        # brkX2 (12h): recompute add_usd pakai tier sizing TERKINI (bukan nilai lama saat open),
        # supaya perubahan score_to_target_usd langsung berlaku ke deal yg belum add-fund.
        if d.get('strategy', 'brkX2') == 'brkX2' and not add_fund_sent and 'score' in d:
            _cur_base     = get_strategy_base_usd('brkX2')
            _fresh_target = max(score_to_target_usd(d.get('score', 0)), _cur_base)
            _fresh_add    = max(0, _fresh_target - _cur_base)
            if _fresh_add != add_usd:
                log(f"[T2] {sym} add_usd disesuaikan ke tier terkini: ${add_usd} -> ${_fresh_add}")
                add_usd = _fresh_add
                with active_deals_lock:
                    if sym in active_deals:
                        active_deals[sym]['add_usd'] = _fresh_add
        if add_usd > 0 and not add_fund_sent:
            if not get_deal_override(sym, 'auto_add_fund', True):
                log(f"[T2] {sym} add fund di-skip (auto_add_fund=OFF via dashboard)")
            elif is_bstock_symbol(sym) and not is_nyse_open():
                log(f"[T2] {sym} add fund di-skip (bStock + NYSE tutup)")
            else:
                strat = d.get('strategy', 'brkX2')
                vol_ok, current_atr = current_volatility_guard(sym, strat, float(d.get('atr_pct', 0) or 0))
                if not vol_ok:
                    log(f"[T2] {sym} add fund di-skip: volatilitas tinggi atau data ATR tidak tersedia "
                        f"(ATR sekarang={current_atr if current_atr is not None else 'n/a'})")
                else:
                    # AI-gate add-fund (Fase 2, 03/09/2026, permintaan Mas Budi) -- HANYA
                    # TrenKonfirmasi-4h, 5 strategi lain tetap add-fund otomatis murni tanpa
                    # perubahan. Kalau AI TUNDA, dicoba lagi setelah cooldown 15 menit (bukan
                    # ditandai add_fund_sent -- add_usd tetap >0, jadi otomatis dicek ulang
                    # siklus T2 berikutnya begitu cooldown lewat).
                    _addfund_ai_ok = True
                    if strat == 'trend_confirm_4h':
                        _af_hold_until = float(d.get('add_fund_ai_hold_until', 0) or 0)
                        if time.time() < _af_hold_until:
                            _addfund_ai_ok = False
                            log(f"[T2] {sym} add fund AI-gate: masih cooldown ({(_af_hold_until - time.time())/60:.1f} menit lagi)")
                        elif not ai_decision_add_fund(sym, strat, d, add_usd, current_atr):
                            _addfund_ai_ok = False
                            with active_deals_lock:
                                if sym in active_deals:
                                    active_deals[sym]['add_fund_ai_hold_until'] = time.time() + 15 * 60
                            save_active_deals()
                            log(f"[T2] {sym} add fund di-TUNDA oleh AI decision (cooldown 15 menit)")
                    if _addfund_ai_ok:
                        log(f"[T2] {sym} kirim add fund ${add_usd} (deal confirmed aktif, ATR={current_atr:.2f}%)")
                        add_ok = send_add_funds(sym, add_usd, strat, delay=0)
                        if not add_ok:
                            log(f"[T2] {sym} add fund gagal; akan dicoba lagi pada siklus berikutnya")
                        else:
                            log_oac('ADD_FUND', sym, strat, {
                                'add_usd':      f"${add_usd:.0f}",
                                'total_usd':    f"${BASE_ORDER_VOLUME + add_usd:.0f}",
                                'entry_price':  _fmt_price(d.get('entry_price', 0)),
                                'atr_pct':      f"{d.get('atr_pct', 0):.2f}%",
                                'rsi':          f"{d['rsi_open']:.1f}"        if d.get('rsi_open')        is not None else "—",
                                'stoch_k':      f"{d['stoch_k_open']:.1f}"   if d.get('stoch_k_open')    is not None else "—",
                                'stoch_d':      f"{d['stoch_d_open']:.1f}"   if d.get('stoch_d_open')    is not None else "—",
                                'macd_hist':    f"{d['macd_hist_open']:.5f}" if d.get('macd_hist_open')  is not None else "—",
                                'bb_pct':       f"{d['bb_pct_open']:.3f}"    if d.get('bb_pct_open')     is not None else "—",
                                'williams_r':   f"{d['williams_r_open']:.1f}"if d.get('williams_r_open') is not None else "—",
                                'cci':          f"{d['cci_open']:.1f}"        if d.get('cci_open')        is not None else "—",
                                'obv':          f"{d['obv_open']:.0f}"        if d.get('obv_open')        is not None else "—",
                                'ema20':        f"{d['ema20_open']:.6f}"      if d.get('ema20_open')      is not None else "—",
                                'st_dir':       str(d.get('last_st_dir'))     if d.get('last_st_dir')     is not None else "—",
                            })
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
        prof_from_entry = (price/entry-1)*100 - FEE_ROUND_TRIP_PCT
        prof_peak       = (peak/entry-1)*100
        atrp = d.get('atr_pct',3.0)
        strat = d.get('strategy', 'brkX2')
        live_atrp = get_live_atr_pct(sym, strat, atrp)

        # Trailing factor variatif untuk hunting_4h
        if strat == 'hunting_4h':
            _sk   = d.get('last_stoch_k', float('nan'))
            _std  = d.get('last_st_dir', 0)
            _ifomo = d.get('in_fomo', False)
            _factor = get_hunting_trail_factor(
                _sk if not (isinstance(_sk, float) and (_sk != _sk)) else 50.0,
                _std, _ifomo
            )
            # Update in_fomo state
            if not (isinstance(_sk, float) and (_sk != _sk)):
                new_fomo = (_std == 1 and _sk > 80)
                if new_fomo != _ifomo:
                    with active_deals_lock:
                        if sym in active_deals:
                            active_deals[sym]['in_fomo'] = new_fomo
            # Trailing dist dengan factor variatif, ATR% live
            base_td = trailing_dist(live_atrp)
            tdist   = round(base_td * _factor, 4)
        else:
            tdist = trailing_dist_progressive(live_atrp, prof_peak)
        armed = d.get('trailing_armed', False)

        # arm trailing setelah profit >= arm_pct tier ATR% LIVE (pakai puncak, bukan
        # snapshot ATR% saat entry — supaya reaktif ke perubahan volatilitas terkini)
        # — SKIP untuk akumulasi: exit via TP/SL/Timeout, bukan trailing
        _is_akum = strat in ('akum_entry_a', 'akum_entry_b')
        if (not armed) and (not _is_akum) and get_deal_override(sym, 'arm_trailing_enabled', True) and prof_peak >= get_arm_pct(live_atrp):
            # AI decision jika ai_call=True untuk deal ini
            if get_deal_override(sym, 'ai_call', True):
                if not ai_decision_armed(sym, d.get('strategy', 'brkX2'), d, price, peak):
                    log(f"[T2] {sym} ARM ditahan oleh AI decision")
                    # skip arm kali ini, cek lagi di siklus berikutnya
                    with active_deals_lock:
                        if sym in active_deals: active_deals[sym]['last_price'] = price
                    save_active_deals()
                    continue
            armed = True
            log(f"[T2] {sym} trailing ARMED (peak profit {prof_peak:.2f}%, ATR% live {live_atrp:.2f}%)")
            # Simpan waktu arm, ATR% live saat arm (acuan un-arm), dan stoch saat arm
            with active_deals_lock:
                if sym in active_deals:
                    active_deals[sym]['armed_at_wib'] = now_wib().strftime('%d/%m/%Y %H:%M')
                    active_deals[sym]['armed_price']   = peak
                    active_deals[sym]['armed_prof_pct'] = prof_peak
                    active_deals[sym]['armed_since_ts'] = time.time()
                    active_deals[sym]['armed_live_atr']  = live_atrp
            save_active_deals()
            log_oac('ARMED', sym, d.get('strategy', 'brkX2'), {
                'peak_profit':  f"{prof_peak:.2f}%",
                'arm_pct':      f"{get_arm_pct(live_atrp):.1f}%",
                'atr_pct':      f"{live_atrp:.2f}%",
                'trail_dist':   f"{tdist:.2f}%",
                'entry_price':  _fmt_price(d.get('entry_price', 0)),
                'peak_price':   _fmt_price(peak),
                'rsi':          f"{d['rsi_open']:.1f}"        if d.get('rsi_open')        is not None else "—",
                'stoch_k':      f"{d['stoch_k_open']:.1f}"   if d.get('stoch_k_open')    is not None else "—",
                'stoch_d':      f"{d['stoch_d_open']:.1f}"   if d.get('stoch_d_open')    is not None else "—",
                'macd_hist':    f"{d['macd_hist_open']:.5f}" if d.get('macd_hist_open')  is not None else "—",
                'bb_pct':       f"{d['bb_pct_open']:.3f}"    if d.get('bb_pct_open')     is not None else "—",
                'williams_r':   f"{d['williams_r_open']:.1f}"if d.get('williams_r_open') is not None else "—",
                'cci':          f"{d['cci_open']:.1f}"        if d.get('cci_open')        is not None else "—",
                'obv':          f"{d['obv_open']:.0f}"        if d.get('obv_open')        is not None else "—",
                'ema20':        f"{d['ema20_open']:.6f}"      if d.get('ema20_open')      is not None else "—",
                'st_dir':       str(d.get('last_st_dir'))     if d.get('last_st_dir')     is not None else "—",
            })

        # un-arm: ATR% live naik cukup jauh sejak arm (histeresis: margin UNARM_ATR_MARGIN_PP)
        # DAN sudah armed minimal UNARM_MIN_ARMED_MINUTES (anti-flapping) DAN peak profit yang
        # sudah dicapai tidak lagi cukup utk tier arm_pct yang berlaku sekarang — trailing ketat
        # yang di-set saat market tenang jadi nggak relevan lagi di kondisi volatil sekarang,
        # fallback ke hard stop saja sampai nanti profit cukup utk re-arm di tier baru.
        if armed and not _is_akum:
            _armed_ref_atr = d.get('armed_live_atr', atrp)
            _armed_since   = d.get('armed_since_ts', 0)
            _armed_min     = (time.time() - _armed_since) / 60.0 if _armed_since > 0 else 999.0
            if (prof_peak < get_arm_pct(live_atrp)
                    and live_atrp >= _armed_ref_atr + UNARM_ATR_MARGIN_PP
                    and _armed_min >= UNARM_MIN_ARMED_MINUTES):
                armed = False
                log(f"[T2] {sym} trailing UN-ARMED (ATR% live naik {_armed_ref_atr:.2f}%→{live_atrp:.2f}% "
                    f"sejak arm, peak profit {prof_peak:.2f}% < arm_pct tier sekarang {get_arm_pct(live_atrp):.1f}%)")
                with active_deals_lock:
                    if sym in active_deals:
                        active_deals[sym]['trailing_armed'] = False
                        active_deals[sym].pop('armed_since_ts', None)
                        active_deals[sym].pop('armed_live_atr', None)
                        active_deals[sym].pop('trail_htf_grace_started_at', None)
                save_active_deals()
                d.pop('trail_htf_grace_started_at', None)

        # deteksi pergerakan cepat (HANYA relevan saat armed) utk polling adaptif
        last_price = d.get('last_price', price)
        if last_price > 0:
            move_pct = abs(price/last_price - 1)*100
            if armed and move_pct >= T2_FAST_TRIGGER_PCT:
                want_fast = True

        do_close=False; reason=""; hard_stop_triggered=False; trail_stop_triggered=False; timeout_triggered=False
        # TP custom: tutup otomatis begitu U/PnL bersih (net -0.2% fee) sudah >= target $ per deal
        # (default $1.00 kalau belum diisi manual). Pakai estimate_deal_total_usd()
        # (qty_coin * entry_price aktual) supaya modal yang dipakai cek TP sama persis dengan
        # yang ditampilkan di dashboard (base_usd/add_usd field bisa basi/tidak sinkron kalau
        # ada add-fund manual atau posisi ganda).
        _total_usd_now = estimate_deal_total_usd(d)
        _upnl_usd_now  = prof_from_entry / 100 * _total_usd_now
        _tp_mode = get_deal_override(sym, 'tp1_target_mode', 'usd')
        if _tp_mode == 'pct':
            _tp_target_pct = float(get_deal_override(sym, 'tp1_target_pct', 1.0) or 1.0)
            if get_deal_override(sym, 'tp1usd', False) and prof_from_entry >= _tp_target_pct:
                do_close = True
                reason = f"TP {_tp_target_pct:.2f}%: profit bersih {prof_from_entry:.2f}% (modal ${_total_usd_now:.0f})"
        elif _tp_mode == 'price':
            # Target HARGA koin (USDT), bukan target profit -- close begitu harga pasar >= target,
            # berapapun itu representasi profit %/$-nya.
            _tp_target_price = float(get_deal_override(sym, 'tp1_target_price', 0) or 0)
            if get_deal_override(sym, 'tp1usd', False) and _tp_target_price > 0 and price >= _tp_target_price:
                do_close = True
                reason = f"TP harga {_fmt_price(_tp_target_price)}: harga sekarang {_fmt_price(price)} (profit bersih ${_upnl_usd_now:.2f})"
        else:
            _tp_target_usd = float(get_deal_override(sym, 'tp1_target_usd', 1.0) or 1.0)
            if get_deal_override(sym, 'tp1usd', False) and _upnl_usd_now >= _tp_target_usd:
                do_close = True
                reason = f"TP ${_tp_target_usd:.2f}: profit bersih ${_upnl_usd_now:.2f} (modal ${_total_usd_now:.0f})"
        if not do_close and not _is_akum:
            _hs_label, _hs_base, _hs_pct = hard_stop_pct(atrp)
            if price <= entry * (1 - _hs_pct / 100):
                do_close = True
                reason = (f"hard stop volatilitas [{_hs_label}, ATR {atrp:.2f}%]: price {_fmt_price(price)} "
                          f"turun >= {_hs_pct:.2f}% dari entry (base {_hs_base:.1f}% x K{HARD_STOP_MULT:.2f})")
                hard_stop_triggered = True
        if not do_close and armed and not _is_akum:
            stop = peak*(1 - tdist/100)
            if price <= stop:
                trail_reason = f"trailing (turun ke {_fmt_price(price)} dari puncak {_fmt_price(peak)}, dev {tdist}%)"
                trail_grace_started = float(d.get('trail_htf_grace_started_at', 0) or 0)
                trail_grace_age = time.time() - trail_grace_started if trail_grace_started > 0 else 0
                grace_strategy = strat in ('brkX2', 'brkX2_4h', 'brkX2_crossema', 'hunting_4h', 'reversal', 'trend_confirm_4h')
                if (grace_strategy and prof_from_entry > 0
                        and trail_grace_age < TRAIL_HTF_GRACE_SECONDS
                        and trailing_htf_is_healthy(sym)):
                    if trail_grace_started <= 0:
                        trail_grace_started = time.time()
                        with active_deals_lock:
                            if sym in active_deals:
                                active_deals[sym]['trail_htf_grace_started_at'] = trail_grace_started
                        d['trail_htf_grace_started_at'] = trail_grace_started
                    log(f"[T2] {sym} trailing ditahan: HTF sehat, grace tersisa "
                        f"{max(0, TRAIL_HTF_GRACE_SECONDS - (time.time() - trail_grace_started)):.0f}s")
                else:
                    do_close=True; reason=trail_reason; trail_stop_triggered=True
            elif d.get('trail_htf_grace_started_at'):
                with active_deals_lock:
                    if sym in active_deals:
                        active_deals[sym].pop('trail_htf_grace_started_at', None)
                d.pop('trail_htf_grace_started_at', None)

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
        elif d.get('strategy','brkX2') in ('brkX2_crossema', 'hunting_4h'):
            hold_limit_sec = HUNTING_MAX_HOLD_CANDLES * STRAT4H_SECONDS
            hold_label = (f"batas {HUNTING_MAX_HOLD_CANDLES} candle 4h (crossema)"
                          if d.get('strategy') == 'brkX2_crossema'
                          else f"batas {HUNTING_MAX_HOLD_CANDLES} candle 4h (hunting)")
        elif d.get('strategy','brkX2') == 'trend_confirm_4h':
            hold_limit_sec = TRENDCONFIRM_MAX_HOLD_CANDLES * STRAT4H_SECONDS
            hold_label = f"batas {TRENDCONFIRM_MAX_HOLD_CANDLES} candle 4h (trendconfirm)"
        elif d.get('strategy','') in ('akum_entry_a', 'akum_entry_b'):
            timeout_c = d.get('timeout_candles', AKUM_ENTRY_TIMEOUT)
            hold_limit_sec = timeout_c * STRAT4H_SECONDS
            hold_label = f"batas {timeout_c} candle 4h (akumulasi)"
            # Cek SL khusus akumulasi
            sl_price = d.get('sl_price', 0)
            if sl_price > 0 and price <= sl_price:
                do_close = True
                reason = f"SL akumulasi tercapai (price {_fmt_price(price)} <= SL {_fmt_price(sl_price)})"
            # Cek TP akumulasi: swing high lokal ATAU momentum overbought
            if not do_close:
                try:
                    _df4 = get_ohlcv_4h(sym, limit=max(AKUM_TP_SWING_LOOKBACK + 10, 80))
                    if _df4 is not None and len(_df4) >= 10:
                        import pandas_ta as _pta
                        _swing_hi = get_akum_swing_high(_df4, AKUM_TP_SWING_LOOKBACK)
                        # TP1: harga menyentuh swing high lokal
                        if _swing_hi > 0 and price >= _swing_hi:
                            do_close = True
                            reason = f"TP akumulasi: price {_fmt_price(price)} >= swing_high_30c {_fmt_price(_swing_hi)}"
                        # TP2: momentum overbought (Stoch%K>75 AND MACD hist mulai turun) OR RSI>70
                        if not do_close:
                            _df4c = _df4.copy()
                            _rsi = _pta.rsi(_df4c['close'], length=14)
                            _stoch = _pta.stoch(_df4c['high'], _df4c['low'], _df4c['close'], k=14, d=3)
                            _macd  = _pta.macd(_df4c['close'], fast=12, slow=26, signal=9)
                            _rsi_now   = float(_rsi.iloc[-1]) if _rsi is not None and not pd.isna(_rsi.iloc[-1]) else 0
                            _sk_now    = float(_stoch.iloc[-1, 0]) if _stoch is not None and not pd.isna(_stoch.iloc[-1, 0]) else 0
                            _sk_prev   = float(_stoch.iloc[-2, 0]) if _stoch is not None and len(_stoch) >= 2 and not pd.isna(_stoch.iloc[-2, 0]) else 0
                            _macd_hist_now  = float(_macd.iloc[-1, 2]) if _macd is not None and not pd.isna(_macd.iloc[-1, 2]) else 0
                            _macd_hist_prev = float(_macd.iloc[-2, 2]) if _macd is not None and len(_macd) >= 2 and not pd.isna(_macd.iloc[-2, 2]) else 0
                            _stoch_ob_cross = (_sk_now > AKUM_TP_STOCH_OB and _sk_now < _sk_prev)  # Stoch OB + mulai turun
                            _macd_turn      = (_macd_hist_now < _macd_hist_prev)                   # MACD hist turun
                            _rsi_ob         = (_rsi_now >= AKUM_TP_RSI_OB)
                            if _rsi_ob or (_stoch_ob_cross and _macd_turn):
                                do_close = True
                                reason = (f"TP akumulasi momentum: RSI={_rsi_now:.1f} Stoch={_sk_now:.1f}→{_sk_prev:.1f} "
                                          f"MACD_hist={_macd_hist_now:.4f}→{_macd_hist_prev:.4f}")
                        # TP3: upper wick menyentuh pivot high terdekat di atas price (strength N=4)
                        if not do_close:
                            _N4   = 4
                            _hiArr = _df4['high'].values
                            _cur_high = _hiArr[-1]  # high candle terkini (upper wick / spike)
                            _pivots = []
                            for _pi in range(_N4, len(_hiArr) - _N4):
                                _ph = _hiArr[_pi]
                                if (all(_ph > _hiArr[_pi - _j] for _j in range(1, _N4 + 1)) and
                                        all(_ph > _hiArr[_pi + _j] for _j in range(1, _N4 + 1))):
                                    _pivots.append(_ph)
                            # resistance terdekat di atas current price
                            _res_above = sorted(_ph for _ph in _pivots if _ph > price)
                            if _res_above:
                                _nearest_res = _res_above[0]
                                if _cur_high >= _nearest_res:
                                    do_close = True
                                    reason = (f"TP3 akumulasi: upper_wick {_fmt_price(_cur_high)} "
                                              f">= pivot_resistance {_fmt_price(_nearest_res)}")
                except Exception as _e:
                    log(f"[T2] {sym} akum TP check error: {_e}")
        else:
            hold_limit_sec = MAX_HOLD_DAYS * SECONDS_PER_CANDLE
            hold_label = f"batas {MAX_HOLD_DAYS} candle"
        if opened_ts>0 and (time.time()-opened_ts) >= hold_limit_sec:
            do_close=True; reason=hold_label+" tercapai"; timeout_triggered=True
        elif opened_ts>0 and get_deal_override(sym, 'ai_call', True):
            # Tanya AI saat tersisa 2 candle menuju timeout
            elapsed_sec = time.time() - opened_ts
            candle_sec  = hold_limit_sec / (
                REVERSAL_MAX_HOLD_CANDLES if d.get('strategy') == 'reversal'
                else STRAT4H_MAX_HOLD_CANDLES if d.get('strategy') == 'brkX2_4h'
                else HUNTING_MAX_HOLD_CANDLES if d.get('strategy') in ('brkX2_crossema', 'hunting_4h')
                else TRENDCONFIRM_MAX_HOLD_CANDLES if d.get('strategy') == 'trend_confirm_4h'
                else d.get('timeout_candles', AKUM_ENTRY_TIMEOUT) if d.get('strategy','') in ('akum_entry_a','akum_entry_b')
                else MAX_HOLD_DAYS
            )
            hold_candle_now = int(elapsed_sec / candle_sec)
            max_candle      = int(hold_limit_sec / candle_sec)
            sisa_candle     = max_candle - hold_candle_now
            if sisa_candle <= 1 and sisa_candle >= 0:
                # Guard: hanya panggil AI sekali per candle near-timeout
                _nt_key = f"nt_ai_called_{hold_candle_now}"
                _already_called = d.get(_nt_key, False)
                if not _already_called:
                    with active_deals_lock:
                        if sym in active_deals:
                            active_deals[sym][_nt_key] = True
                    save_active_deals()
                    log(f"[T2] {sym} near-timeout ({hold_candle_now}/{max_candle} candle) → tanya AI")
                    if ai_decision_near_timeout(sym, d.get('strategy','brkX2'), d, price, peak,
                                                hold_candle_now, max_candle):
                        do_close = True
                        reason   = f"AI decision: close menjelang timeout ({hold_candle_now}/{max_candle} candle)"

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
            # Batas absolut TrenKonfirmasi-4h (Fase 2, 03/09/2026, permintaan Mas Budi):
            # begitu rugi >= hard-stop normal + TRENDCONFIRM_CLOSE_HARD_CEILING_EXTRA_PCT
            # poin, AI TIDAK BOLEH menahan lagi -- lewati gerbang ai_decision_close supaya
            # deal ini DIPUTUSKAN (lanjut ke pipeline close normal di bawah, yg masih bisa
            # kena hold_no_sell kalau toggle itu aktif -- ceiling ini bukan maksa JUAL,
            # tapi maksa deal-nya berhenti "digantung" AI tanpa batas berkali-kali).
            _ai_override_bypassed = False
            if do_close and d.get('strategy') == 'trend_confirm_4h' and not _is_akum:
                _hs_label_c, _hs_base_c, _hs_pct_c = hard_stop_pct(atrp)
                _absolute_ceiling_pct = _hs_pct_c + TRENDCONFIRM_CLOSE_HARD_CEILING_EXTRA_PCT
                if entry > 0 and price <= entry * (1 - _absolute_ceiling_pct / 100):
                    _ai_override_bypassed = True
                    log(f"[T2] {sym} BATAS ABSOLUT tercapai (rugi >= {_absolute_ceiling_pct:.2f}%) -- AI tidak bisa menahan lagi")
                    reason += f" [BATAS ABSOLUT {_absolute_ceiling_pct:.2f}%: AI override dilewati]"
            # AI decision jika ai_call=True untuk deal ini -- pakai cooldown 10 menit (bug
            # fix 03/09/2026, permintaan Mas Budi): SEBELUMNYA ai_decision_close() ditanya
            # ULANG tiap siklus T2 (~15 detik) selama do_close masih terpicu & AI masih
            # HOLD -- kalau 1 deal nyangkut di zona itu berjam-jam bisa spam 300-400+
            # notifikasi Telegram + boros API call AI utk keputusan yg pada dasarnya sama
            # berulang-ulang. Generik lintas SEMUA strategi (bukan cuma trend_confirm_4h),
            # krn ini bug pemborosan, bukan pilihan desain per-strategi.
            if not _ai_override_bypassed and get_deal_override(sym, 'ai_call', True):
                _close_ai_hold_until = float(d.get('close_ai_hold_until', 0) or 0)
                if time.time() < _close_ai_hold_until:
                    log(f"[T2] {sym} CLOSE di-hold (cooldown AI {(_close_ai_hold_until - time.time())/60:.1f} menit lagi, reason: {reason})")
                    continue
                if not ai_decision_close(sym, d.get('strategy', 'brkX2'), d, price, peak, reason):
                    with active_deals_lock:
                        if sym in active_deals:
                            active_deals[sym]['close_ai_hold_until'] = time.time() + 10 * 60
                    save_active_deals()
                    log(f"[T2] {sym} CLOSE di-hold oleh AI decision (reason: {reason})")
                    continue
            log(f"[T2] CLOSE {sym}: {reason} | profit {prof_from_entry:.2f}%")
            strat = d.get('strategy','brkX2')
            if strat == 'reversal':
                strat_label = "Reversal Doji+HA (8h)"
            elif strat == 'brkX2_4h':
                strat_label = "Momentum brkX2-4h (4h)"
            elif strat == 'brkX2_crossema':
                strat_label = "CrossEMA-4h"
            elif strat == 'hunting_4h':
                strat_label = "Hunting-4h"
            elif strat == 'akum_entry_a':
                strat_label = "Akumulasi-4h Entry A"
            elif strat == 'akum_entry_b':
                strat_label = "Akumulasi-4h Entry B"
            elif strat == 'trend_confirm_4h':
                strat_label = "TrenKonfirmasi-4h"
            else:
                strat_label = "Momentum brkX2 (12h)"
            # Hold-no-sell: kalau ini hard-stop, ATAU timeout ("batas N candle tercapai") sementara
            # deal lagi RUGI (semua strategi -- 30/08/2026, permintaan user), DAN checkbox "hold,
            # jangan jual" aktif (default True) untuk deal ini -> tetap catat sebagai loss (statistik
            # tetap jujur), tapi SKIP jual — koin tetap di wallet. Cooldown 96 jam berlaku ke symbol
            # ini lintas semua strategi. Timeout yg closing UNTUNG tetap dijual normal seperti biasa.
            _hold_no_sell = (hard_stop_triggered or (timeout_triggered and prof_from_entry < 0)) \
                             and get_deal_override(sym, 'hold_no_sell', True)
            if _hold_no_sell:
                reason += " [HOLD: koin tidak dijual, cooldown 96j]"
            if _hold_no_sell or send_close_long(sym, strat):
                # catat ke CSV DULU supaya trade ini ikut terhitung di progress
                total_usd = estimate_deal_total_usd(d)
                csv_log_close(
                    to_display_pair(sym),
                    now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                    price, prof_from_entry, reason,
                    strategy=strat,
                    base_usd=total_usd
                )
                # ── DEAL LOG lengkap CLOSE ────────────────────────────────
                # opened_candle_ts disimpan dalam MILIDETIK -- wajib dibagi 1000 dulu sebelum
                # dikurangi time.time() (detik), dan pembagi candle wajib sesuai timeframe
                # strategi (dulu semua strategi disamain ke SECONDS_PER_CANDLE/12h, bikin
                # "Hold candles" salah total buat brkX2-4h/reversal/dll -- termasuk pernah
                # muncul angka rusak kayak -41348942 gara-gara unit ms vs detik ketuker).
                _opened_ts = (d.get('opened_candle_ts', 0) or 0) / 1000.0
                _hold_c = round((time.time() - _opened_ts) / _candle_seconds_for_strategy(strat)) if _opened_ts > 0 else ''
                deal_log_write({
                    'timestamp_wib': now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                    'event_type':    'CLOSE',
                    'strategy':      strat,
                    'symbol':        to_display_pair(sym),
                    'thread':        'T2',
                    'entry_price':   _fmt_price(d.get('entry_price')) if d.get('entry_price') else '',
                    'exit_price':    f"{_fmt_price(price)}",
                    'profit_pct':    f"{prof_from_entry:.2f}",
                    'exit_reason':   reason,
                    'trailing_armed':str(armed),
                    'hold_candles':  str(_hold_c),
                    'atr_pct':       f"{d.get('atr_pct', ''):.2f}" if d.get('atr_pct') else '',
                    'score':         d.get('score', ''),
                    'total_usd':     d.get('target_usd', ''),
                })

                log_oac('CLOSE', sym, strat, {
                    'exit_price':   _fmt_price(price),
                    'profit_pct':   f"{prof_from_entry:.2f}%",
                    'exit_reason':  reason,
                    'peak_price':   _fmt_price(peak),
                    'peak_profit':  f"{prof_peak:.2f}%",
                    'arm_pct':      f"{get_arm_pct(atrp):.1f}% ({'armed' if armed else 'belum armed'})",
                    'trail_dist':   f"{tdist:.2f}%" if armed else "—",
                    'atr_pct':      f"{d.get('atr_pct', 0):.2f}%",
                    'add_usd':      f"${d.get('add_usd', 0):.0f}" if d.get('add_usd') else "—",
                    'total_usd':    f"${d.get('target_usd', BASE_ORDER_VOLUME):.0f}",
                    'hold_candles': str(_hold_c),
                    'entry_price':  _fmt_price(d.get('entry_price', 0)),
                    'rsi':          f"{d['rsi_open']:.1f}"        if d.get('rsi_open')        is not None else "—",
                    'stoch_k':      f"{d['stoch_k_open']:.1f}"   if d.get('stoch_k_open')    is not None else "—",
                    'stoch_d':      f"{d['stoch_d_open']:.1f}"   if d.get('stoch_d_open')    is not None else "—",
                    'macd_hist':    f"{d['macd_hist_open']:.5f}" if d.get('macd_hist_open')  is not None else "—",
                    'bb_pct':       f"{d['bb_pct_open']:.3f}"    if d.get('bb_pct_open')     is not None else "—",
                    'williams_r':   f"{d['williams_r_open']:.1f}"if d.get('williams_r_open') is not None else "—",
                    'cci':          f"{d['cci_open']:.1f}"        if d.get('cci_open')        is not None else "—",
                    'obv':          f"{d['obv_open']:.0f}"        if d.get('obv_open')        is not None else "—",
                    'ema20':        f"{d['ema20_open']:.6f}"      if d.get('ema20_open')      is not None else "—",
                    'st_dir':       str(d.get('last_st_dir'))     if d.get('last_st_dir')     is not None else "—",
                })
                remove_from_active_deals(sym)
                if strat in ('brkX2', 'hunting_4h', 'reversal', 'brkX2_4h', 'brkX2_crossema', 'akum_entry_a', 'akum_entry_b', 'trend_confirm_4h'): record_closed(sym)
                if strat == 'brkX2_4h' and trail_stop_triggered: record_trail_close(sym, price)
                if strat == 'brkX2_4h' and d.get('quick_reentry_open'): record_quick_reentry_close(sym, prof_from_entry)
                if _hold_no_sell:
                    record_hold_no_sell_closed(sym)
                    record_hold_no_sell_price(sym, d.get('entry_price', 0) or 0, strat)

                # progress forward-test PER STRATEGI
                if strat == 'reversal':
                    tgt = FWDTEST_TARGET_REVERSAL
                elif strat == 'brkX2_4h':
                    tgt = FWDTEST_TARGET_4H
                elif strat == 'hunting_4h':
                    tgt = HUNTING_FWDTEST_TARGET
                else:
                    tgt = FWDTEST_TARGET_BRKX2
                pstrat = csv_progress(strat, offset=FWDTEST_BRKX2_PHASE_OFFSET if strat=='brkX2' else (HUNTING_FWDTEST_PHASE_OFFSET if strat=='hunting_4h' else 0))
                if pstrat and pstrat['n']>0:
                    done_n = pstrat['n']; wl = f"{pstrat['win']}W/{pstrat['loss']}L"
                    status = "TERCAPAI - waktunya evaluasi!" if done_n>=tgt else f"menuju {tgt}"
                    if strat in ('hunting_4h', 'reversal', 'brkX2_4h'):
                        _live_baseline  = (HUNTING_LIVE_BASELINE if strat == 'hunting_4h'
                                            else REVERSAL_LIVE_BASELINE if strat == 'reversal'
                                            else STRAT4H_LIVE_BASELINE)
                        _phase2_target  = (HUNTING_PHASE2_TARGET if strat == 'hunting_4h'
                                            else REVERSAL_PHASE2_TARGET if strat == 'reversal'
                                            else STRAT4H_PHASE2_TARGET)
                        _phase2_offset  = (HUNTING_FWDTEST_PHASE_OFFSET if strat == 'hunting_4h' else 0) + _live_baseline
                        _p2 = csv_progress(strat, offset=_phase2_offset)
                        _p2_n   = _p2['n'] if _p2 else 0
                        _p2_wl  = f"{_p2['win']}W/{_p2['loss']}L" if _p2 and _p2['n'] > 0 else "0W/0L"
                        _p2_tot = _p2['total_pct'] if _p2 else 0.0
                        prog_close = (f"\n{strat_label} LIVE: {done_n} closed"
                                      f"\n  {wl}, total {pstrat['total_pct']:+.1f}%"
                                      f"\n{strat_label}: 2nd #{_p2_n}/{_phase2_target} ({_p2_wl}, total {_p2_tot:+.1f}%)")
                    else:
                        prog_close = (f"\nForward-test {strat_label}: #{done_n}/{tgt} ({status})"
                                      f"\n  {wl}, total {pstrat['total_pct']:+.1f}%")
                else:
                    prog_close = f"\nForward-test {strat_label}: #?/{tgt} (CSV belum terbaca)"
                _base_usd_cl = float(d.get('base_usd', d.get('target_usd', BASE_ORDER_VOLUME)))
                _add_usd_cl  = float(d.get('add_usd', 0)) if d.get('add_fund_sent') else 0.0
                _total_usd_cl = _base_usd_cl + _add_usd_cl
                _upnl_usd_cl  = round(prof_from_entry / 100 * _total_usd_cl, 2)
                send_telegram(
                    f"{strat_label} | CLOSE LONG\n"
                    f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                    f"Pair   : {to_display_pair(sym)}\n"
                    f"Alasan : {reason}\n"
                    f"Profit : {prof_from_entry:.2f}% (net -0.2% fee)\n"
                    f"U/PnL  : {_upnl_usd_cl:+.2f} USD (modal ${_total_usd_cl:.0f})\n"
                    f"Hold candles: {_hold_c}\n"
                    f"{_fmt_indicators_open_block(d)}"
                    f"{prog_close}"
                )
                # Notif timeline lengkap khusus hunting_4h
                if strat == 'hunting_4h':
                    opened_at   = d.get('opened_at_wib', '?')
                    armed_at    = d.get('armed_at_wib', '—')
                    armed_price = d.get('armed_price', None)
                    armed_prof  = d.get('armed_prof_pct', None)
                    sk_op       = d.get('stoch_k_open', None)
                    sd_op       = d.get('stoch_d_open', None)
                    st_op       = d.get('st_dir_open', None)
                    stoch_open_str = f"Stoch %K={sk_op:.1f} %D={sd_op:.1f}" if sk_op is not None and sd_op is not None else "—"
                    st_open_str    = ("Uptrend" if st_op == 1 else "Downtrend") if st_op is not None else "—"
                    armed_line  = (f"Armed  : {armed_at} WIB @ {_fmt_price(armed_price)} (profit pk {armed_prof:+.2f}%)" 
                                   if armed_at != '—' and armed_price is not None else "Armed  : Tidak pernah arm")
                    send_telegram(
                        f"📊 Hunting-4h | RINGKASAN DEAL\n"
                        f"Pair   : {to_display_pair(sym)}\n"
                        f"——— TIMELINE ———\n"
                        f"Open   : {opened_at} WIB\n"
                        f"  Entry: {_fmt_price(d.get('entry_price',0))} | ATR%={d.get('atr_pct',0):.2f}\n"
                        f"  ST 4h: {st_open_str} | {stoch_open_str}\n"
                        f"{armed_line}\n"
                        f"Close  : {now_wib().strftime('%d/%m/%Y %H:%M')} WIB @ {_fmt_price(price)}\n"
                        f"  Alasan: {reason}\n"
                        f"  Profit: {prof_from_entry:+.2f}% dari entry",
                        parse_mode=None
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
    def _fmt_hunting_live(p):
        if p is None or p['n'] == 0:
            return "LIVE (belum ada close fase aktif)"
        return f"LIVE: {p['n']} closed ({p['win']}W/{p['loss']}L, total {p['total_pct']:+.1f}%)"

    prog_all  = csv_progress_active()
    prog_brk  = csv_progress('brkX2', offset=FWDTEST_BRKX2_PHASE_OFFSET)
    prog_rev  = csv_progress('reversal')
    prog_4h   = csv_progress('brkX2_4h')
    prog_cx   = csv_progress('brkX2_crossema')
    prog_hunt = csv_progress('hunting_4h', offset=HUNTING_FWDTEST_PHASE_OFFSET)
    prog_rev2  = csv_progress('reversal',   offset=REVERSAL_LIVE_BASELINE)
    prog_hunt2 = csv_progress('hunting_4h', offset=HUNTING_FWDTEST_PHASE_OFFSET + HUNTING_LIVE_BASELINE)

    if prog_all is None:
        prog_line = "Progress forward-test: 0 trade selesai (CSV belum ada)."
    else:
        nn=prog_all['n']; wl=f"{prog_all['win']}W/{prog_all['loss']}L"
        prog_qr = quick_reentry_progress()
        prog_line = (f"Progress forward-test (gabungan): {nn} selesai ({wl}, total {prog_all['total_pct']:+.1f}%)\n"
                     f"  - brkX2    : {_fmt_strat(prog_brk,  FWDTEST_TARGET_BRKX2)}\n"
                     f"  - reversal : {_fmt_hunting_live(prog_rev)}\n"
                     f"    reversal : 2nd {_fmt_strat(prog_rev2, REVERSAL_PHASE2_TARGET)}\n"
                     f"  - 4h       : {_fmt_strat(prog_4h,   STRAT4H_FWDTEST_TARGET)}\n"
                     f"    4h       : Quick-Reentry {_fmt_strat(prog_qr, QUICK_REENTRY_TARGET)}\n"
                     f"  - crossema : {_fmt_strat(prog_cx,   STRAT_CROSSEMA_FWDTEST)}\n"
                     f"  - hunting  : {_fmt_hunting_live(prog_hunt)}\n"
                     f"    hunting  : 2nd {_fmt_strat(prog_hunt2, HUNTING_PHASE2_TARGET)}")

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
    slot_total= f"Total: {active_deal_count()}/{total_max_deals_all_strategies()}"

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
        f"{t3_str}\n\n"
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
    if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2:
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
    held_bx12c = {}   # 03/09/2026 pola 2-babak: symbol -> data kandidat yg lolos babak 1
    for sym in universe:
        if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2:
            break
        if len(held_bx12c) >= AI_BATCH_POOL_SIZE:
            log(f"[T1c] Kolam babak-1 penuh ({AI_BATCH_POOL_SIZE}), berhenti kumpulkan kandidat baru siklus ini.")
            break
        with active_deals_lock:
            if sym in active_deals: continue
        if is_cooldown_enabled('brkX2') and cooldown_remaining(sym) > 0: continue
        if sizing_fail_remaining(sym) > 0: continue   # baru gagal sizing/open, jangan coba lagi dulu
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
        if pd.isna(r12.get('hh')): continue
        # 03/09/2026: syarat choppy lama pakai kolom 'br' + CHOPPY_LOOK/CHOPPY_MIN yang
        # TIDAK PERNAH ada di codebase (df12['br'] tidak pernah di-set, CHOPPY_LOOK/CHOPPY_MIN
        # tidak pernah didefinisikan) -- akibatnya r12.get('br') selalu None, pd.isna(None)
        # selalu True, baris ini selalu `continue` utk SEMUA kandidat sejak kode ini ada.
        # T1c (scan intrabar baseline) akibatnya tidak pernah meloloskan satupun kandidat.
        # Diganti is_choppy(df12) -- fungsi choppy-filter asli yg benar & valid, sudah dipakai
        # T1c-E (intrabar EARLY). Ditemukan & diperbaiki saat dry-run restrukturisasi 2-babak.
        if is_choppy(df12): continue
        # LAPIS 2: data 15m candle aktif
        # 03/09/2026: limit dinaikkan dari 50 -> 100 (get_ohlcv() menolak & return None
        # kalau hasil < 60 baris, jadi limit=50 bikin baris ini SELALU None -> selalu
        # `continue` di semua kandidat, LAPIS 2 tidak pernah tercapai). Kolom df15['ts']
        # diganti df15['ot'] (open time) -- get_ohlcv() tidak pernah punya kolom 'ts',
        # cuma 'ot'/'ct', jadi baris ini juga selalu KeyError kalau df15 sempat non-None.
        # Ditemukan & diperbaiki saat dry-run restrukturisasi 2-babak.
        df15 = get_ohlcv(sym, interval='15m', limit=100)
        if df15 is None: continue
        intra = df15[df15['ot'] >= candle_open_ms]
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
        # LOLOS syarat teknikal -> babak 1: AI individual (notify=False), TAHAN dulu
        atrp         = float(r12['atr_pct']) if not pd.isna(r12.get('atr_pct')) else 3.0
        score        = signal_score(r12)
        signal_price = float(r12['close'])
        log(f"[T1c] SINYAL INTRABAR: {sym} elapsed={elapsed_pct*100:.1f}% price={price_now:.6g} skor={score}")

        _rsi_ai = r12.get('rsi') if 'rsi' in r12.index else None
        _rsi_ai_val = float(_rsi_ai) if _rsi_ai is not None and not pd.isna(_rsi_ai) else None
        if is_ai_call_open_enabled('brkX2'):
            _ai_ind = {'atr_pct': f"{atrp:.2f}%", 'score': score, 'signal_price': _fmt_price(signal_price)}
            if _rsi_ai_val is not None: _ai_ind['rsi'] = f"{_rsi_ai_val:.1f}"
            if not ai_decision_open(sym, 'brkX2-12h', _ai_ind, active_deal_count(), notify=False):
                log(f"[T1c] {sym} babak-1 di-skip oleh AI individual")
                continue

        _rvol_bx12c = float(r12['vol']) / vol_ma12 if vol_ma12 > 0 else 0.0
        _bb_bx12c = float(r12.get('bb_pct')) if not pd.isna(r12.get('bb_pct')) else 0.0
        _gap_bx12c = (signal_price / float(r12['ema_fast']) - 1) * 100 if r12.get('ema_fast') else 0.0
        held_bx12c[sym] = {
            'signal_price': signal_price, 'atrp': atrp, 'score': score,
            'price_now': price_now, 'r12': r12,
            'detail': {'atr_pct': atrp, 'rvol': _rvol_bx12c, 'bb_pct': _bb_bx12c,
                       'gap_ema20_pct': _gap_bx12c, 'rsi': _rsi_ai_val or 0.0},
        }

    # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus ============
    approved_bx12c = []
    if held_bx12c:
        log(f"[T1c] Babak 1 selesai: {len(held_bx12c)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
        log_ai_babak1('brkX2-12h (intrabar)', [to_display_pair(s) for s in held_bx12c.keys()], AI_BATCH_MAX_APPROVE)
        batch_input_bx12c = [{'symbol': s, 'strategy': 'brkX2', 'score': v['score'], 'detail': v['detail']}
                              for s, v in held_bx12c.items()]
        approved_bx12c = ai_decision_batch_rank(
            batch_input_bx12c, strategy_label='brkX2-12h', max_approve=AI_BATCH_MAX_APPROVE,
            criteria_note="Kriteria: skor, ATR%, RVOL, BB%b, jarak EMA20, RSI",
        )

    for sym in approved_bx12c:
        if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2:
            break
        with active_deals_lock:
            if sym in active_deals: continue
        v = held_bx12c[sym]
        signal_price = v['signal_price']; atrp = v['atrp']; score = v['score']
        price_now = v['price_now']; r12 = v['r12']

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
                f"brkX2-12h | OPEN LONG INTRABAR\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (candle n-1 close): {_fmt_price(signal_price)}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"Elapsed candle 12h: {elapsed_pct*100:.1f}% (jam ke-{elapsed_pct*12:.1f})\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Skor sinyal: {score}/5 -> modal ${target_usd}{addfund_txt}\n"
                f"Slot terpakai: {active_deal_count()}/{total_max_deals_all_strategies()}"
            )
            threading.Thread(target=send_email_open_long, args=("OPEN LONG INTRABAR brkX2-12h: " + to_display_pair(sym), 
                f"brkX2-12h | OPEN LONG INTRABAR\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (candle n-1 close): {_fmt_price(signal_price)}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"Elapsed candle 12h: {elapsed_pct*100:.1f}% (jam ke-{elapsed_pct*12:.1f})\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Skor sinyal: {score}/5 -> modal ${target_usd}{addfund_txt}\n"
                f"Slot terpakai: {active_deal_count()}/{total_max_deals_all_strategies()}"
            ), daemon=True).start()
            csv_log_open({
                'open_time_wib':  now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol':         to_display_pair(sym),
                'signal_price':   f"{_fmt_price(signal_price)}",
                'entry_price':    f"{_fmt_price(entry_price)}",
                'slip_pct':       f"{slip_pct:+.2f}",
                'atr_pct':        f"{atrp:.2f}",
                'trail_dist_pct': f"{trailing_dist(atrp)}",
                'base_usd':       target_usd,
                'score':          score,
                'strategy':       'brkX2',
                'rsi_open':       f"{float(r12['rsi']):.1f}" if 'rsi' in r12.index and not pd.isna(r12.get('rsi')) else '',
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
                'signal_price':         f"{_fmt_price(signal_price)}",
                'entry_price':          f"{_fmt_price(entry_price)}",
                'slip_pct':             f"{slip_pct:+.2f}",
                'score':                score,
                'base_usd':             target_usd,
                'add_usd':              add_usd if add_usd > 0 else 0,
                'total_usd':            target_usd,
                'trail_dist_pct':       f"{trailing_dist(atrp)}",
                'intrabar_elapsed_pct': f"{elapsed_pct*100:.1f}",
                'intrabar_price_live':  _fmt_price(price_now),
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
    if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2:
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

    held_bx12e = {}   # 03/09/2026 pola 2-babak: symbol -> data kandidat yg lolos babak 1
    for sym in universe:
        if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2:
            break
        if len(held_bx12e) >= AI_BATCH_POOL_SIZE:
            log(f"[T1c-E] Kolam babak-1 penuh ({AI_BATCH_POOL_SIZE}), berhenti kumpulkan kandidat baru siklus ini.")
            break
        with active_deals_lock:
            if sym in active_deals: continue
        if is_cooldown_enabled('brkX2') and cooldown_remaining(sym) > 0: continue
        if sizing_fail_remaining(sym) > 0: continue   # baru gagal sizing/open, jangan coba lagi dulu

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
        # 03/09/2026: limit dinaikkan dari 50 -> 100 (get_ohlcv() menolak & return None
        # kalau hasil < 60 baris, jadi limit=50 bikin baris ini SELALU None -> selalu
        # `continue` di semua kandidat, LAPIS 2 tidak pernah tercapai). Kolom df15['ts']
        # diganti df15['ot'] (open time) -- get_ohlcv() tidak pernah punya kolom 'ts',
        # cuma 'ot'/'ct', jadi baris ini juga selalu KeyError kalau df15 sempat non-None.
        # Ditemukan & diperbaiki saat dry-run restrukturisasi 2-babak.
        df15 = get_ohlcv(sym, interval='15m', limit=100)
        if df15 is None: continue
        intra = df15[df15['ot'] >= candle_open_ms]
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

        # LOLOS syarat teknikal -> babak 1: AI individual (notify=False), TAHAN dulu
        atrp         = float(r12['atr_pct']) if not pd.isna(r12.get('atr_pct')) else 3.0
        score        = signal_score(r12)
        signal_price = float(r12['close'])
        log(f"[T1c-E] SINYAL EARLY: {sym} elapsed={elapsed_pct*100:.1f}% price={price_now:.6g} skor={score}")

        if is_ai_call_open_enabled('brkX2'):
            _ai_ind = {'atr_pct': f"{atrp:.2f}%", 'score': score, 'signal_price': _fmt_price(signal_price), 'rsi': f"{rsi_now:.1f}"}
            if not ai_decision_open(sym, 'brkX2-12h', _ai_ind, active_deal_count(), notify=False):
                log(f"[T1c-E] {sym} babak-1 di-skip oleh AI individual")
                continue

        _rvol_bx12e = float(r12['vol']) / vol_ma12 if vol_ma12 > 0 else 0.0
        _bb_bx12e = float(r12.get('bb_pct')) if not pd.isna(r12.get('bb_pct')) else 0.0
        _gap_bx12e = (signal_price / float(r12['ema_fast']) - 1) * 100 if r12.get('ema_fast') else 0.0
        held_bx12e[sym] = {
            'signal_price': signal_price, 'atrp': atrp, 'score': score,
            'price_now': price_now, 'r12': r12,
            'detail': {'atr_pct': atrp, 'rvol': _rvol_bx12e, 'bb_pct': _bb_bx12e,
                       'gap_ema20_pct': _gap_bx12e, 'rsi': rsi_now},
        }

    # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus ============
    approved_bx12e = []
    if held_bx12e:
        log(f"[T1c-E] Babak 1 selesai: {len(held_bx12e)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
        log_ai_babak1('brkX2-12h (intrabar EARLY)', [to_display_pair(s) for s in held_bx12e.keys()], AI_BATCH_MAX_APPROVE)
        batch_input_bx12e = [{'symbol': s, 'strategy': 'brkX2', 'score': v['score'], 'detail': v['detail']}
                              for s, v in held_bx12e.items()]
        approved_bx12e = ai_decision_batch_rank(
            batch_input_bx12e, strategy_label='brkX2-12h', max_approve=AI_BATCH_MAX_APPROVE,
            criteria_note="Kriteria: skor, ATR%, RVOL, BB%b, jarak EMA20, RSI",
        )

    for sym in approved_bx12e:
        if deal_count_by_strategy('brkX2') >= MAX_DEALS_BRKX2:
            break
        with active_deals_lock:
            if sym in active_deals: continue
        v = held_bx12e[sym]
        signal_price = v['signal_price']; atrp = v['atrp']; score = v['score']
        price_now = v['price_now']; r12 = v['r12']

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
                f"brkX2-12h | OPEN LONG INTRABAR EARLY\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (candle n-1 close): {_fmt_price(signal_price)}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"Elapsed candle 12h: {elapsed_pct*100:.1f}% (jam ke-{elapsed_pct*12:.1f})\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Skor sinyal: {score}/5 -> modal ${target_usd}{addfund_txt}\n"
                f"Slot terpakai: {active_deal_count()}/{total_max_deals_all_strategies()}"
            )
            threading.Thread(target=send_email_open_long, args=("OPEN LONG INTRABAR EARLY brkX2-12h: " + to_display_pair(sym), 
                f"brkX2-12h | OPEN LONG INTRABAR EARLY\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (candle n-1 close): {_fmt_price(signal_price)}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"Elapsed candle 12h: {elapsed_pct*100:.1f}% (jam ke-{elapsed_pct*12:.1f})\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Skor sinyal: {score}/5 -> modal ${target_usd}{addfund_txt}\n"
                f"Slot terpakai: {active_deal_count()}/{total_max_deals_all_strategies()}"
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
                'signal_price':         f"{_fmt_price(signal_price)}",
                'entry_price':          f"{_fmt_price(entry_price)}",
                'slip_pct':             f"{slip_pct:+.2f}",
                'score':                score,
                'base_usd':             target_usd,
                'add_usd':              add_usd if add_usd > 0 else 0,
                'total_usd':            target_usd,
                'trail_dist_pct':       f"{trailing_dist(atrp)}",
                'intrabar_elapsed_pct': f"{elapsed_pct*100:.1f}",
                'intrabar_price_live':  _fmt_price(price_now),
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

    held_rev2 = {}   # 03/09/2026 pola 2-babak: symbol -> data kandidat yg lolos babak 1
    for sym in universe:
        if is_daily_loss_limit_breached():
            log(f"[T3-REV] Batas rugi harian tersulut, scan intrabar reversal dihentikan.")
            break
        with active_deals_lock:
            if sym in active_deals:
                continue
        if is_cooldown_enabled('reversal') and cooldown_remaining(sym) > 0:
            continue
        if sizing_fail_remaining(sym) > 0:
            continue   # baru gagal sizing/open, jangan coba lagi dulu
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

        # Syarat 1: minimal 2 dari 3 candle merah + turun >= 5%
        n_red = sum(df_closed.iloc[idx]['close'] < df_closed.iloc[idx]['open']
                for idx in (im3, im2, im1))
        if n_red < 2:
            continue
        open_c3  = float(df_closed.iloc[im3]['open'])
        close_c1 = float(df_closed.iloc[im1]['close'])
        if open_c3 <= 0 or (close_c1 / open_c3 - 1) * 100 > -REVERSAL_DROP_MIN_PCT:
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

        # Jarak ke ATH (01/09/2026, kasus BICOUSDT -- lihat REVERSAL_ATH_DIST_MIN_PCT)
        if not ath_distance_ok(sym, price_now, now_ms, REVERSAL_ATH_DIST_MIN_PCT):
            continue

        # ── LOLOS → OPEN DEAL ─────────────────────────────────────────────────
        signal_price = float(df_closed['close'].iloc[i1])
        atrp = float(df_closed['atr_pct'].iloc[i1]) if not pd.isna(df_closed['atr_pct'].iloc[i1]) else 3.0

        log(f"[T3-REV] SINYAL REVERSAL INTRABAR: {sym} price_now={price_now:.6g} "
            f"EMA20={ema20_now:.6g} atr%={atrp:.2f}")

        # Sama seperti thread1b_scan_reversal() (01/09/2026): perkaya konteks TF sendiri
        # (8h) buat AI pakai indikator yg sudah dihitung compute_indicators_reversal() di
        # row c+1 (df_closed.iloc[i1]) -- termasuk gap_ema20 pakai price_now LIVE (bukan
        # cuma close candle) krn di jalur intrabar ini price_now-lah yg jadi harga entry.
        _rr_ai = df_closed.iloc[i1]; _rp_ai = df_closed.iloc[i0]
        def _aiv(col):
            v = _rr_ai.get(col) if col in _rr_ai.index else None
            return None if v is None or pd.isna(v) else float(v)
        _rsi = _aiv('rsi'); _sk = _aiv('stoch_k'); _sd = _aiv('stoch_d')
        _mh = _aiv('macd_hist'); _bb = _aiv('bb_pct'); _wr = _aiv('williams_r'); _cci = _aiv('cci')

        if is_ai_call_open_enabled('reversal'):
            _ai_ind = {'atr_pct': f"{atrp:.2f}%", 'signal_price': _fmt_price(signal_price)}
            if _rsi is not None: _ai_ind['rsi'] = f"{_rsi:.1f}"
            if _sk is not None and _sd is not None: _ai_ind['stoch'] = f"K={_sk:.1f} D={_sd:.1f}"
            if _mh is not None: _ai_ind['macd_hist'] = f"{_mh:+.6f}"
            if _bb is not None: _ai_ind['bb_pct'] = f"{_bb:.2f}"
            if _wr is not None: _ai_ind['williams_r'] = f"{_wr:.1f}"
            if _cci is not None: _ai_ind['cci'] = f"{_cci:.1f}"
            if ema20_now > 0 and price_now > 0:
                _ai_ind['gap_ema20'] = f"{(price_now/ema20_now-1)*100:+.2f}%"
            _obv_now = _aiv('obv')
            _obv_prv = _rp_ai.get('obv') if 'obv' in _rp_ai.index else None
            if _obv_now is not None and _obv_prv is not None and not pd.isna(_obv_prv):
                _ai_ind['obv'] = '↑' if _obv_now > float(_obv_prv) else '↓'
            if not ai_decision_open(sym, 'Reversal-8h', _ai_ind, active_deal_count(), notify=False):
                log(f"[T3-REV] {sym} babak-1 di-skip oleh AI individual")
                continue

        # LOLOS syarat teknikal -> babak 1: TAHAN dulu (jangan langsung buka)
        _gap_rev2 = (price_now / ema20_now - 1) * 100 if ema20_now > 0 and price_now > 0 else 0.0
        held_rev2[sym] = {
            'signal_price': signal_price, 'atrp': atrp, 'price_now': price_now, 'rsi_open': _rsi,
            'detail': {'atr_pct': atrp, 'rvol': 0.0, 'bb_pct': _bb or 0.0,
                       'gap_ema20_pct': _gap_rev2, 'rsi': _rsi or 0.0},
        }
        if len(held_rev2) >= AI_BATCH_POOL_SIZE:
            log(f"[T3-REV] Kolam babak-1 penuh ({AI_BATCH_POOL_SIZE}), berhenti kumpulkan kandidat baru siklus ini.")
            break

    # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus ============
    approved_rev2 = []
    if held_rev2:
        log(f"[T3-REV] Babak 1 selesai: {len(held_rev2)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
        log_ai_babak1('Reversal-8h (intrabar)', [to_display_pair(s) for s in held_rev2.keys()], AI_BATCH_MAX_APPROVE)
        batch_input_rev2 = [{'symbol': s, 'strategy': 'reversal', 'score': 0, 'detail': v['detail']}
                             for s, v in held_rev2.items()]
        approved_rev2 = ai_decision_batch_rank(
            batch_input_rev2, strategy_label='Reversal-8h', max_approve=AI_BATCH_MAX_APPROVE,
            criteria_note="Kriteria: ATR%, BB%b, jarak EMA20, RSI (reversal 8h tidak hitung RVOL)",
        )

    for sym in approved_rev2:
        if is_daily_loss_limit_breached():
            log(f"[T3-REV] Batas rugi harian tersulut, sisa kandidat reversal intrabar tidak dibuka.")
            break
        if deal_count_by_strategy('reversal') >= MAX_DEALS_REVERSAL:
            break
        with active_deals_lock:
            if sym in active_deals: continue
        v = held_rev2[sym]
        signal_price = v['signal_price']; atrp = v['atrp']; price_now = v['price_now']

        if send_open_long(sym, 'reversal'):
            target_usd = get_strategy_base_usd('reversal')
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
                f"Reversal-8h | OPEN LONG INTRABAR\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (c+1 close): {_fmt_price(signal_price)}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} "
                f"| total {active_deal_count()}/{total_max_deals_all_strategies()}"
            )
            threading.Thread(target=send_email_open_long, args=("OPEN LONG Reversal-8h: " + to_display_pair(sym),
                f"Reversal-8h | OPEN LONG INTRABAR\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (c+1 close): {_fmt_price(signal_price)}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} "
                f"| total {active_deal_count()}/{total_max_deals_all_strategies()}"
            ), daemon=True).start()
            threading.Thread(target=send_email_open_long, args=("OPEN LONG INTRABAR Reversal-8h: " + to_display_pair(sym), 
                f"Reversal-8h | OPEN LONG INTRABAR\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"Pair  : {to_display_pair(sym)}\n"
                f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                f"Harga sinyal (c+1 close): {_fmt_price(signal_price)}\n"
                f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                f"Base  : ${BASE_ORDER_VOLUME}\n"
                f"Slot reversal: {deal_count_by_strategy('reversal')}/{MAX_DEALS_REVERSAL} "
                f"| total {active_deal_count()}/{total_max_deals_all_strategies()}"
            ), daemon=True).start()
            csv_log_open({
                'open_time_wib':  now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                'symbol':         to_display_pair(sym),
                'signal_price':   f"{_fmt_price(signal_price)}",
                'entry_price':    f"{_fmt_price(entry_price)}",
                'slip_pct':       f"{slip_pct:+.2f}",
                'atr_pct':        f"{atrp:.2f}",
                'trail_dist_pct': f"{trailing_dist(atrp)}",
                'base_usd':       target_usd,
                'score':          0,
                'strategy':       'reversal',
                'rsi_open':       f"{v['rsi_open']:.1f}" if v.get('rsi_open') is not None else '',
            })
            log_oac('OPEN', sym, 'Reversal-8h', {
                'entry_price': _fmt_price(entry_price),
                'slip_pct':    f"{slip_pct:+.2f}%",
                'atr_pct':     f"{atrp:.2f}%",
                'trail_dist':  f"{trailing_dist(atrp)}%",
            })
            last_rev_intrabar_candle_ts[sym] = candle_open_ms

            if deal_count_by_strategy('reversal') >= MAX_DEALS_REVERSAL:
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
    if n4h >= STRAT4H_MAX_DEALS:
        return

    log(f"[T1d] Scan 4h intrabar ({elapsed_pct*100:.1f}% elapsed)...")
    ticker = get_ticker_24h()
    vol_map = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in ticker} if ticker else {}

    candidates  = []
    near_miss_4h = []   # [(sym, [fails])] — kandidat yang hampir lolos
    scan_blockers_4h = {}
    scan_total_4h = 0
    with active_deals_lock:
        existing = set(active_deals.keys())

    for sym_info in ticker or []:
        sym = sym_info.get("symbol", "")
        if not sym.endswith("USDT"): continue
        if sym in existing: continue
        if sym in SYMBOL_BLACKLIST: continue
        if sym in last_4h_candle_ts and last_4h_candle_ts[sym] == candle_open_ms:
            continue
        if vol_map.get(sym, 0) < STRAT4H_MIN_VOL_USD:
            continue

        try:
            scan_total_4h += 1
            df = get_ohlcv_4h(sym, limit=500)
            if df is None or len(df) < 500:
                count_blocker(scan_blockers_4h, "Data OHLCV kurang", True)
                continue  # filter pair baru (<~83 hari listing)
            df = compute_indicators_4h(df)

            failures_4h = blockers_entry_4h(df)
            for failure in failures_4h:
                count_blocker(scan_blockers_4h, failure, True)
            if failures_4h:
                # Cek berapa syarat yang lolos untuk near_miss
                r = df.iloc[-1]
                if len(failures_4h) <= 1:
                    near_miss_4h.append((max(0, 11 - len(failures_4h)), sym, failures_4h, 11))
                continue

            # HTF 12h filter
            if not htf_filter_4h_ok(sym):
                count_blocker(scan_blockers_4h, "HTF 12h", True)
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
                    count_blocker(scan_blockers_4h, "Performance score", True)
                    near_miss_4h.append((6, sym, [f"Perf Grade masih <B (score {pscore:.2f})"], 7))
                    continue

            r    = df.iloc[-1]
            atrp = float(r["atr_pct"]) if not pd.isna(r["atr_pct"]) else 3.0
            sc   = 1
            candidates.append((sym, float(r["close"]), atrp, sc))
        except Exception as e:
            log(f"  [T1d] error {sym}: {e}")

    record_scan_blockers("brkX2-4h", scan_total_4h, len(candidates), scan_blockers_4h)
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

    held_bx = {}   # 03/09/2026 pola 2-babak: symbol -> data kandidat yg lolos babak 1
    for sym, signal_price, atrp, score in candidates:
        if len(held_bx) >= AI_BATCH_POOL_SIZE:
            log(f"[T1d] Kolam babak-1 penuh ({AI_BATCH_POOL_SIZE}), berhenti kumpulkan kandidat baru siklus ini.")
            break
        if sym in (set(active_deals.keys())): continue
        _quick_reentry = False
        if is_cooldown_enabled('brkX2_4h') and cooldown_remaining(sym) > 0:
            if trend_continuation_ok(sym, signal_price):
                _quick_reentry = True
                log(f"[T1d] {sym} trend-continuation quick re-entry: cooldown "
                    f"{cooldown_remaining(sym)/3600:.1f}j di-bypass (price {_fmt_price(signal_price)} "
                    f"reclaim exit+{TREND_CONT_MARGIN_PCT:.1f}%, sinyal breakout lolos ulang)")
            else:
                log(f"[T1d] {sym} cooldown {cooldown_remaining(sym)/3600:.1f}j — skip")
                continue
        if sizing_fail_remaining(sym) > 0:
            log(f"[T1d] {sym} baru gagal sizing/open (sisa {sizing_fail_remaining(sym)/60:.0f} menit) — skip, jangan coba lagi dulu.")
            continue

        # Data tambahan (RSI/RVOL/BB%b/gap-EMA20) untuk AI individual + babak 2 nanti
        _rsi_val_bx = None
        _rvol_bx = 0.0; _bb_bx = 0.0; _gap_bx = 0.0
        try:
            _df_ai4 = get_ohlcv_4h(sym, limit=100)
            if _df_ai4 is not None and len(_df_ai4) >= 30:
                _df_ai4 = compute_indicators_4h(_df_ai4)
                _r_ai4 = _df_ai4.iloc[-1]
                if not pd.isna(_r_ai4.get('rsi')): _rsi_val_bx = float(_r_ai4['rsi'])
                _vm_bx = _r_ai4.get('vol_ma')
                if not pd.isna(_vm_bx) and _vm_bx > 0: _rvol_bx = float(_r_ai4['vol']) / float(_vm_bx)
                if not pd.isna(_r_ai4.get('bb_pct')): _bb_bx = float(_r_ai4['bb_pct'])
                _ema20_bx = _r_ai4.get('ema20')
                if not pd.isna(_ema20_bx) and _ema20_bx > 0:
                    _gap_bx = (signal_price / float(_ema20_bx) - 1) * 100
        except Exception:
            pass

        if is_ai_call_open_enabled('brkX2_4h'):
            _ai_ind = {'atr_pct': f"{atrp:.2f}%", 'score': score, 'signal_price': _fmt_price(signal_price)}
            if _rsi_val_bx is not None: _ai_ind['rsi'] = f"{_rsi_val_bx:.1f}"
            if not ai_decision_open(sym, 'brkX2-4h', _ai_ind, active_deal_count(), notify=False):
                log(f"[T1d] {sym} babak-1 di-skip oleh AI individual")
                continue

        held_bx[sym] = {
            'signal_price': signal_price, 'atrp': atrp, 'score': score,
            'quick_reentry': _quick_reentry,
            'detail': {'atr_pct': atrp, 'rvol': _rvol_bx, 'bb_pct': _bb_bx,
                       'gap_ema20_pct': _gap_bx, 'rsi': _rsi_val_bx or 0.0},
        }

    # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus ============
    approved_bx = []
    if held_bx:
        log(f"[T1d] Babak 1 selesai: {len(held_bx)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
        log_ai_babak1('brkX2-4h', [to_display_pair(s) for s in held_bx.keys()], AI_BATCH_MAX_APPROVE)
        batch_input_bx = [{'symbol': s, 'strategy': 'brkX2_4h', 'score': v['score'], 'detail': v['detail']}
                           for s, v in held_bx.items()]
        approved_bx = ai_decision_batch_rank(
            batch_input_bx, strategy_label='brkX2-4h', max_approve=AI_BATCH_MAX_APPROVE,
            criteria_note="Kriteria: skor, ATR%, RVOL, BB%b, jarak EMA20, RSI",
        )

    opened_any = False
    for sym in approved_bx:
        n4h = active_deal_count_4h()
        if n4h >= STRAT4H_MAX_DEALS: break
        with active_deals_lock:
            if sym in active_deals: continue
        v = held_bx[sym]
        signal_price = v['signal_price']; atrp = v['atrp']; score = v['score']
        _quick_reentry = v['quick_reentry']

        ok, target_usd, add_usd = open_deal_with_sizing(sym, score, strategy="brkX2_4h")
        if not ok: continue

        # Konfirmasi real-time: price_now harus cross EMA20 ke atas, jarak 0–0.75%
        # (fetch fresh per-simbol -- babak 2 bisa berjarak waktu dari babak 1, dan ini
        #  juga dipakai ulang sbg _r4msg utk indikator open, ganti fetch _df4 terpisah)
        _r4msg = pd.Series(dtype=float)
        try:
            _pnow = get_price_now(sym)
            _dfx = get_ohlcv_4h(sym, limit=60)
            if _dfx is not None and len(_dfx) >= 20:
                _dfx = compute_indicators_4h(_dfx)
                _r4msg = _dfx.iloc[-1]
            _ema20_rt = float(_r4msg.get("ema20", 0)) if not pd.isna(_r4msg.get("ema20", float('nan'))) else 0.0
            if _pnow > 0 and _ema20_rt > 0:
                _dist_rt = (_pnow - _ema20_rt) / _ema20_rt * 100
                if not (0.0 <= _dist_rt <= 0.75):
                    log(f"[T1d] {sym} SKIP: price_now {_fmt_price(_pnow)} vs EMA20 {_fmt_price(_ema20_rt)} dist={_dist_rt:+.2f}% (harus 0-0.75%)")
                    continue
        except Exception as _e:
            log(f"[T1d] {sym} cross EMA20 check error: {_e} — lanjut")

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
            "opened_candle_ts": int(candle_open_ms),
            "tf":            STRAT4H_TIMEFRAME,
            "quick_reentry_open": _quick_reentry,
            # Indikator saat open (untuk log ARMED/CLOSE)
            "rsi_open":        float(_r4msg.get("rsi", float("nan"))) if not pd.isna(_r4msg.get("rsi", float("nan"))) else None,
            "stoch_k_open":    float(_r4msg.get("stoch_k", float("nan"))) if not pd.isna(_r4msg.get("stoch_k", float("nan"))) else None,
            "stoch_d_open":    float(_r4msg.get("stoch_d", float("nan"))) if not pd.isna(_r4msg.get("stoch_d", float("nan"))) else None,
            "macd_hist_open":  float(_r4msg.get("macd_hist", float("nan"))) if not pd.isna(_r4msg.get("macd_hist", float("nan"))) else None,
            "bb_pct_open":     float(_r4msg.get("bb_pct", float("nan"))) if not pd.isna(_r4msg.get("bb_pct", float("nan"))) else None,
            "williams_r_open": float(_r4msg.get("williams_r", float("nan"))) if not pd.isna(_r4msg.get("williams_r", float("nan"))) else None,
            "cci_open":        float(_r4msg.get("cci", float("nan"))) if not pd.isna(_r4msg.get("cci", float("nan"))) else None,
            "obv_open":        float(_r4msg.get("obv", float("nan"))) if not pd.isna(_r4msg.get("obv", float("nan"))) else None,
            "ema20_open":      float(_r4msg.get("ema20", float("nan"))) if not pd.isna(_r4msg.get("ema20", float("nan"))) else None,
        })
        last_4h_candle_ts[sym] = candle_open_ms

        trail_arm = get_arm_pct(atrp)
        trail_d   = trailing_dist(atrp)

        _rsi_val   = _r4msg.get("rsi", float("nan"))
        _stoch_val = _r4msg.get("stoch_k", float("nan"))
        _ema20_val = _r4msg.get("ema20", float("nan"))
        _ema50_val = _r4msg.get("ema50", float("nan"))
        _rsi_str   = f"{_rsi_val:.1f}" if _rsi_val == _rsi_val else "n/a"
        _stoch_str = f"{_stoch_val:.1f}" if _stoch_val == _stoch_val else "n/a"
        _e20_str   = _fmt_price(_ema20_val) if _ema20_val == _ema20_val else "n/a"
        _e50_str   = _fmt_price(_ema50_val) if _ema50_val == _ema50_val else "n/a"
        msg = (
            f"brkX2-4h | OPEN LONG\n"
            f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
            f"Pair  : {to_display_pair(sym)}\n"
            f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
            f"Harga sinyal (4h live): {_fmt_price(signal_price)}\n"
            f"Selisih (slippage): {slip_pct:+.2f}%\n"
            f"ATR%  : {atrp:.2f}  (trailing {trail_d}% stlh +{trail_arm}%)\n"
            f"RSI   : {_rsi_str}  |  Stoch%K: {_stoch_str}\n"
            f"EMA20 : {_e20_str}  |  EMA50 : {_e50_str}\n"
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
            'signal_price':   f"{_fmt_price(signal_price)}",
            'entry_price':    f"{_fmt_price(entry_price)}",
            'slip_pct':       f"{slip_pct:+.2f}",
            'atr_pct':        f"{atrp:.2f}",
            'trail_dist_pct': f"{trailing_dist(atrp)}",
            'base_usd':       target_usd,
            'score':          score,
            'strategy':       'brkX2_4h',
            'rsi_open':       f"{_rsi_val:.1f}" if _rsi_val == _rsi_val else '',
        })
        if not _r4msg.empty:
            log_oac('OPEN', sym, 'brkX2-4h', {
                'entry_price': _fmt_price(entry_price),
                'slip_pct':    f"{slip_pct:+.2f}%",
                'atr_pct':     f"{atrp:.2f}%",
                'rsi':         f"{_r4msg.get('rsi', float('nan')):.1f}" if hasattr(_r4msg.get('rsi', None), '__float__') else 'n/a',
                'stoch_k':     f"{_r4msg.get('stoch_k', float('nan')):.1f}",
                'macd_hist':   f"{_r4msg.get('macd_hist', 0):.4f}",
                'vol_ratio':   f"{(_r4msg['vol']/_r4msg['vol_ma']):.2f}x" if _r4msg.get('vol_ma') else 'n/a',
                'score':       score,
                'trail_dist':  f"{trailing_dist(atrp)}%",
            })

        deal_log_write({
            "timestamp_wib":  now_wib().strftime("%Y-%m-%d %H:%M:%S"),
            "event_type":     "OPEN",
            "strategy":       "brkX2_4h",
            "symbol":         to_display_pair(sym),
            "thread":         "T1d",
            "signal_price":   f"{_fmt_price(signal_price)}",
            "entry_price":    f"{_fmt_price(entry_price)}",
            "slip_pct":       f"{slip_pct:+.2f}",
            "score":          score,
            "base_usd":       BASE_ORDER_VOLUME,
            "add_usd":        add_usd,
            "total_usd":      target_usd,
            "atr_pct":        f"{atrp:.2f}",
            "intrabar_elapsed_pct": f"{elapsed_pct*100:.1f}",
            **_htf,
        })

        opened_any = True
        log(f"  [T1d] OPEN {sym} @ {_fmt_price(entry_price)} (4h intrabar)")

    # ── Hunting-4h: scan semua pair USDT setelah loop brkX2-4h selesai ──
    # ── Hunting-4h: scan + auto open deal ──────────────────────────────────
    hunting_display = []   # untuk update dashboard (pair yang lolos filter)
    held_hunt = {}   # 03/09/2026 pola 2-babak: symbol -> data kandidat yg lolos babak 1
    with _hunting_lock:
        _cfg_hunt = dict(_hunting_config)
    for sym_info2 in (ticker or []):
        sym2 = sym_info2.get("symbol", "")
        if not sym2.endswith("USDT"): continue
        try:
            df2 = get_ohlcv_4h(sym2, limit=100)
            if df2 is None or len(df2) < 51: continue
            # Cek sinyal (untuk display dashboard)
            hit = check_hunting_strategy(df2, sym_info2, _cfg_hunt)
            if hit:
                hunting_display.append(hit)
                # Babak 1: tahan dulu kalau pool belum penuh (jangan langsung buka)
                if len(held_hunt) < AI_BATCH_POOL_SIZE:
                    _cand = check_hunting_candidate(sym_info2, df2, _cfg_hunt, notify=False)
                    if _cand is not None:
                        held_hunt[sym2] = _cand
        except Exception as _he:
            pass
    ts_hunt = now_wib().strftime("%d/%m/%Y %H:%M:%S")
    with _hunting_lock:
        _hunting_signals.clear()
        _hunting_signals.extend(hunting_display[:50])
        globals()["_hunting_scan_ts"] = ts_hunt
    if hunting_display:
        log(f"[T1d-HUNT] {len(hunting_display)} kandidat lolos filter, slot hunting: {active_deal_count_hunting()}/{HUNTING_MAX_DEALS}")

    # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus ============
    if held_hunt:
        log(f"[T1d-HUNT] Babak 1 selesai: {len(held_hunt)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
        log_ai_babak1('Hunting-4h', [to_display_pair(s) for s in held_hunt.keys()], AI_BATCH_MAX_APPROVE)
        batch_input_hunt = [{'symbol': s, 'strategy': 'hunting_4h', 'score': v['score'], 'detail': v['detail']}
                             for s, v in held_hunt.items()]
        approved_hunt = ai_decision_batch_rank(
            batch_input_hunt, strategy_label='Hunting-4h', max_approve=AI_BATCH_MAX_APPROVE,
            criteria_note="Kriteria: ATR%, BB%b, jarak EMA20, RSI (hunting-4h tidak hitung RVOL)",
        )
        for sym in approved_hunt:
            open_hunting_candidate(held_hunt[sym])

def scan_hunting_signals_only():
    """Scan Hunting-4h independen — tidak diblokir gating window intrabar brkX2-4h.
    Dipanggil tiap loop run_thread1d_4h() agar dashboard selalu update."""
    try:
        ticker = get_ticker_24h()
        if not ticker:
            return
        hunting_display = []
        held_hunt2 = {}   # 03/09/2026 pola 2-babak: symbol -> data kandidat yg lolos babak 1
        hunting_blockers = {}
        hunting_scanned = 0
        with _hunting_lock:
            _cfg_hunt = dict(_hunting_config)
        for sym_info2 in ticker:
            sym2 = sym_info2.get("symbol", "")
            if not sym2.endswith("USDT"): continue
            hunting_scanned += 1
            try:
                df2 = get_ohlcv_4h(sym2, limit=100)
                if df2 is None or len(df2) < 51:
                    count_blocker(hunting_blockers, "Data candle kurang", True)
                    continue
                hit = check_hunting_strategy(df2, sym_info2, _cfg_hunt, hunting_blockers)
                if hit:
                    hunting_display.append(hit)
                    if len(held_hunt2) < AI_BATCH_POOL_SIZE:
                        _cand2 = check_hunting_candidate(sym_info2, df2, _cfg_hunt, notify=False)
                        if _cand2 is not None:
                            held_hunt2[sym2] = _cand2
            except Exception as _he:
                pass
        ts_hunt = now_wib().strftime("%d/%m/%Y %H:%M:%S")
        with _hunting_lock:
            _hunting_signals.clear()
            _hunting_signals.extend(hunting_display[:50])
            globals()["_hunting_scan_ts"] = ts_hunt
        record_scan_blockers("Hunting-4h", hunting_scanned, len(hunting_display), hunting_blockers)

        # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus ============
        if held_hunt2:
            log(f"[T1d-HUNT] Babak 1 selesai: {len(held_hunt2)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
            log_ai_babak1('Hunting-4h', [to_display_pair(s) for s in held_hunt2.keys()], AI_BATCH_MAX_APPROVE)
            batch_input_hunt2 = [{'symbol': s, 'strategy': 'hunting_4h', 'score': v['score'], 'detail': v['detail']}
                                  for s, v in held_hunt2.items()]
            approved_hunt2 = ai_decision_batch_rank(
                batch_input_hunt2, strategy_label='Hunting-4h', max_approve=AI_BATCH_MAX_APPROVE,
                criteria_note="Kriteria: ATR%, BB%b, jarak EMA20, RSI (hunting-4h tidak hitung RVOL)",
            )
            for sym in approved_hunt2:
                open_hunting_candidate(held_hunt2[sym])
        log(f"[T1d-HUNT] scan selesai: {len(hunting_display)} kandidat | slot {active_deal_count_hunting()}/{HUNTING_MAX_DEALS}")
    except Exception as e:
        log(f"WARN scan_hunting_signals_only: {e}")

def run_thread1d_4h():
    """Thread T1d: scan 4h intrabar tiap STRAT4H_SCAN_INTERVAL detik."""
    # Delay awal agar heartbeat START 4h/cx/General dikirim SETELAH brkX2-12h START
    # (brkX2-12h START dikirim ~25 detik setelah startup dari T1)
    time.sleep(30)
    while True:
        try:
            if STRAT4H_ENABLED:
                thread1d_scan_4h()
            # Hunting scan selalu jalan terlepas dari window intrabar brkX2-4h
            scan_hunting_signals_only()
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

    # Hitung elapsed candle 4h saat ini
    now_ms       = int(time.time() * 1000)
    sec4         = STRAT4H_SECONDS
    candle_open_ms = (now_ms // (sec4 * 1000)) * (sec4 * 1000)
    elapsed_pct  = (now_ms - candle_open_ms) / (sec4 * 1000)

    # Hanya scan dalam window 5–15% elapsed
    if not (STRAT_CROSSEMA_ENTRY_MIN <= elapsed_pct <= STRAT_CROSSEMA_ENTRY_MAX):
        if elapsed_pct > STRAT_CROSSEMA_ENTRY_MAX:
            log(f"[T_CX] TF% LEWAT window: {elapsed_pct*100:.1f}% > {STRAT_CROSSEMA_ENTRY_MAX*100:.1f}% (window menit 5-180 sudah tutup)")
            log_tfpct_blocked("T_CX", "CrossEMA-4h", elapsed_pct, STRAT_CROSSEMA_ENTRY_MAX, "window menit 5-180 sudah tutup")
        return

    ticker = get_ticker_24h()
    if not ticker: return

    with active_deals_lock:
        existing = set(active_deals.keys())

    _crossema_near_miss.clear()  # reset tiap scan
    scan_blockers_cx = {}
    scan_total_cx = 0
    held_cx = {}   # 03/09/2026 pola 2-babak: symbol -> data kandidat yg lolos babak 1

    for sym_info in ticker:
        sym = sym_info.get("symbol", "")
        if not sym.endswith("USDT"): continue
        if sym in existing: continue
        if sym in SYMBOL_BLACKLIST: continue
        if _crossema_last_candle_ts.get(sym) == candle_open_ms: continue
        vol24 = float(sym_info.get("quoteVolume", 0))
        if vol24 < STRAT_CROSSEMA_MIN_VOL_USD: continue

        try:
            scan_total_cx += 1
            df = get_ohlcv_4h(sym, limit=500)
            if df is None or len(df) < 500:
                count_blocker(scan_blockers_cx, "Data OHLCV kurang", True)
                continue  # filter pair baru (<~83 hari listing)
            df = compute_indicators_4h(df)

            # Lapis 1: candle n-1 tertutup — ST=-1, close<EMA20, vol>=VOLUME_MULT×MA
            r = df.iloc[-1]
            sd = r.get("st_dir_cx")   # ST khusus CrossEMA (lebih sensitif, lihat STRAT_CROSSEMA_ST_MULT)
            if pd.isna(sd) or sd != -1:
                count_blocker(scan_blockers_cx, "Supertrend bearish", True)
                continue
            ef = r.get("ema20")
            if pd.isna(ef) or r["close"] > ef * (1 + STRAT_CROSSEMA_EMA20_TOL_PCT / 100):
                count_blocker(scan_blockers_cx, "Close di bawah EMA20", True)
                continue
            vm = r.get("vol_ma")
            if pd.isna(vm) or vm <= 0 or r["vol"] < STRAT_CROSSEMA_VOLUME_MULT * vm:
                count_blocker(scan_blockers_cx, "Volume minimum", True)
                continue

            # HTF 12h filter (CrossEMA: vol12h>1.5xMA)
            if not htf_filter_4h_ok(sym, for_crossema=True):
                count_blocker(scan_blockers_cx, "HTF 12h volume", True)
                continue

            # Lapis 2: harga live (dari candle berjalan) harus > EMA20 (dengan toleransi kecil, lihat STRAT_CROSSEMA_CROSS_TOL_PCT)
            price_now = get_price_now(sym)
            if price_now <= 0 or price_now <= float(ef) * (1 - STRAT_CROSSEMA_CROSS_TOL_PCT / 100):
                count_blocker(scan_blockers_cx, "Cross up EMA20 live", True)
                # Lolos Lapis 1 tapi belum cross EMA20 → catat sebagai near_miss
                if len(_crossema_near_miss) < 5:
                    gap_pct = (float(ef) / price_now - 1) * 100 if price_now > 0 else 0
                    _crossema_near_miss.append((4, sym, [f"belum cross EMA20 (price {_fmt_price(price_now)} vs EMA20 {_fmt_price(ef)}, gap {gap_pct:.1f}%)"], 6))
                continue

            # Candle berjalan harus bullish (price > open candle ini)
            df_live = get_ohlcv(sym, interval="15m", limit=5)
            open_now = float(df_live.iloc[-1]["open"]) if df_live is not None and len(df_live) > 0 else price_now * 0.99
            if price_now <= open_now:
                count_blocker(scan_blockers_cx, "Candle 15m bullish", True)
                if len(_crossema_near_miss) < 5:
                    _crossema_near_miss.append((5, sym, [f"candle belum bullish (price {_fmt_price(price_now)} vs open {_fmt_price(open_now)})"], 6))
                continue

            # LOLOS syarat teknikal -> babak 1: AI individual (notify=False), TAHAN dulu
            # (03/09/2026, pola 2-babak, permintaan Mas Budi -- sama spt TrenKonfirmasi-4h)
            atrp         = float(r["atr_pct"]) if not pd.isna(r.get("atr_pct")) else 3.0
            signal_price = float(r["close"])

            if is_cooldown_enabled('brkX2_crossema') and cooldown_remaining(sym) > 0:
                log(f"[T_CROSSEMA] {sym} cooldown {cooldown_remaining(sym)/3600:.1f}j — skip")
                continue
            if sizing_fail_remaining(sym) > 0:
                log(f"[T_CROSSEMA] {sym} baru gagal sizing/open (sisa {sizing_fail_remaining(sym)/60:.0f} menit) — skip, jangan coba lagi dulu.")
                continue

            log(f"[T_CROSSEMA] SINYAL: {sym} price={price_now:.6g} "
                f"EMA20={ef:.6g} atr%={atrp:.2f} elapsed={elapsed_pct*100:.1f}%")

            _rsi_ai_cx = r.get('rsi') if 'rsi' in r.index else None
            _rsi_val_cx = float(_rsi_ai_cx) if _rsi_ai_cx is not None and not pd.isna(_rsi_ai_cx) else None
            if is_ai_call_open_enabled('brkX2_crossema'):
                _ai_ind = {'atr_pct': f"{atrp:.2f}%", 'signal_price': _fmt_price(signal_price)}
                if _rsi_val_cx is not None: _ai_ind['rsi'] = f"{_rsi_val_cx:.1f}"
                if not ai_decision_open(sym, 'CrossEMA-4h', _ai_ind, active_deal_count(), notify=False):
                    log(f"[T_CROSSEMA] {sym} babak-1 di-skip oleh AI individual")
                    continue

            _vm_cx = r.get('vol_ma')
            _rvol_cx = float(r['vol']) / float(_vm_cx) if not pd.isna(_vm_cx) and _vm_cx > 0 else 0.0
            _bb_cx = float(r.get('bb_pct')) if not pd.isna(r.get('bb_pct')) else 0.0
            _gap_cx = (price_now / float(ef) - 1) * 100 if ef and float(ef) > 0 else 0.0
            held_cx[sym] = {
                'atrp': atrp, 'signal_price': signal_price, 'price_now': price_now,
                'elapsed_pct': elapsed_pct, 'rsi': _rsi_val_cx,
                'detail': {'atr_pct': atrp, 'rvol': _rvol_cx, 'bb_pct': _bb_cx,
                           'gap_ema20_pct': _gap_cx, 'rsi': _rsi_val_cx or 0.0},
            }
            if len(held_cx) >= AI_BATCH_POOL_SIZE:
                log(f"[T_CROSSEMA] Kolam babak-1 penuh ({AI_BATCH_POOL_SIZE}), berhenti scan simbol baru siklus ini.")
                break

        except Exception as e:
            log(f"  [T_CROSSEMA] error {sym}: {e}")

    # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus ============
    if held_cx:
        log(f"[T_CROSSEMA] Babak 1 selesai: {len(held_cx)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
        log_ai_babak1('CrossEMA-4h', [to_display_pair(s) for s in held_cx.keys()], AI_BATCH_MAX_APPROVE)
        batch_input_cx = [{'symbol': s, 'strategy': 'brkX2_crossema', 'score': 1, 'detail': v['detail']}
                           for s, v in held_cx.items()]
        approved_cx = ai_decision_batch_rank(
            batch_input_cx, strategy_label='CrossEMA-4h', max_approve=AI_BATCH_MAX_APPROVE,
            criteria_note="Kriteria: ATR%, RVOL, BB%b, jarak EMA20 (live), RSI",
        )
        for sym in approved_cx:
            n_crossema = sum(1 for d in active_deals.values() if d.get("strategy") == "brkX2_crossema")
            if n_crossema >= STRAT_CROSSEMA_MAX_DEALS: break
            with active_deals_lock:
                if sym in active_deals: continue
            v = held_cx[sym]
            atrp = v['atrp']; signal_price = v['signal_price']; price_now = v['price_now']
            elapsed_pct = v['elapsed_pct']; _rsi_val_cx = v['rsi']
            try:
                ok, target_usd, add_usd = open_deal_with_sizing(sym, 0, strategy="brkX2_crossema")
                if not ok:
                    count_blocker(scan_blockers_cx, "Order open", True)
                    continue

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

                send_telegram(
                    f"CrossEMA-4h | OPEN LONG INTRABAR\n"
                    f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                    f"Pair  : {to_display_pair(sym)}\n"
                    f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                    f"Harga sinyal (EMA20 cross): {_fmt_price(signal_price)}\n"
                    f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                    f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                    f"Base  : ${BASE_ORDER_VOLUME}\n"
                    f"Elapsed: {elapsed_pct*100:.1f}% (menit ke {int(elapsed_pct*240)})\n"
                    f"Slot crossema: {n_crossema+1}/{STRAT_CROSSEMA_MAX_DEALS}"
                )
                threading.Thread(target=send_email_open_long, args=("OPEN LONG INTRABAR CrossEMA-4h: " + to_display_pair(sym),
                    f"CrossEMA-4h | OPEN LONG INTRABAR\n"
                    f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                    f"Pair  : {to_display_pair(sym)}\n"
                    f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
                    f"Harga sinyal (EMA20 cross): {_fmt_price(signal_price)}\n"
                    f"Selisih entry vs sinyal: {slip_pct:+.2f}%\n"
                    f"ATR%  : {atrp:.2f}  (trailing {trailing_dist(atrp)}% stlh +{TRAIL_ARM_PCT}%)\n"
                    f"Base  : ${BASE_ORDER_VOLUME}\n"
                    f"Elapsed: {elapsed_pct*100:.1f}% (menit ke {int(elapsed_pct*240)})\n"
                    f"Slot crossema: {n_crossema+1}/{STRAT_CROSSEMA_MAX_DEALS}"
                ), daemon=True).start()
                csv_log_open({
                    "open_time_wib":  now_wib().strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol":         to_display_pair(sym),
                    "signal_price":   f"{_fmt_price(signal_price)}",
                    "entry_price":    f"{_fmt_price(entry_price)}",
                    "slip_pct":       f"{slip_pct:+.2f}",
                    "atr_pct":        f"{atrp:.2f}",
                    "trail_dist_pct": f"{trailing_dist(atrp)}",
                    "base_usd":       BASE_ORDER_VOLUME,
                    "score":          0,
                    "strategy":       "brkX2_crossema",
                    "rsi_open":       f"{_rsi_val_cx:.1f}" if _rsi_val_cx is not None else '',
                })
                log_oac('OPEN', sym, 'CrossEMA-4h', {
                    'entry_price': _fmt_price(entry_price),
                    'slip_pct':    f"{slip_pct:+.2f}%",
                    'atr_pct':     f"{atrp:.2f}%",
                    'elapsed':     f"{elapsed_pct*100:.1f}%",
                    'trail_dist':  f"{trailing_dist(atrp)}%",
                })
                _crossema_last_candle_ts[sym] = candle_open_ms
            except Exception as e:
                log(f"  [T_CROSSEMA] error buka {sym}: {e}")

    n_crossema = sum(1 for d in active_deals.values() if d.get("strategy") == "brkX2_crossema")
    record_scan_blockers("CrossEMA-4h", scan_total_cx, n_crossema, scan_blockers_cx)
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
# STRATEGI #7: TrenKonfirmasi-4h — konfirmasi tren yang SUDAH bergerak (4h)
# Basis: backtest 03/09/2026, lihat konstanta TRENDCONFIRM_* & memory
# project_momentum_confirmation_finding.md. Scan candle-closed periodik (BUKAN
# intrabar live-cross spt CrossEMA-4h) -- lebih mirip pola thread1d_scan_4h.
# ══════════════════════════════════════════════════════════════════════════════
_trendconfirm_last_candle_ts: dict = {}

def check_trendconfirm_entry(df):
    """Cek syarat TrenKonfirmasi-4h di candle TERAKHIR df (sudah closed & sudah
    lewat compute_indicators_4h()). Return (lolos, score, detail) -- score
    1=cuma syarat wajib, 2=+syarat sekunder (BB%b, dipakai buat ranking bukan gerbang)."""
    if df is None or len(df) < TRENDCONFIRM_ATR_MEDIAN_WINDOW + 20:
        return False, 0, {}
    r = df.iloc[-1]
    ema20 = r.get('ema20')
    if pd.isna(ema20) or ema20 <= 0:
        return False, 0, {}
    atrp = r.get('atr_pct'); bbp = r.get('bb_pct'); rsi = r.get('rsi')
    vol_ma20 = df['vol'].rolling(20).mean().iloc[-1]
    if pd.isna(atrp) or pd.isna(bbp) or pd.isna(vol_ma20) or vol_ma20 <= 0 or pd.isna(rsi):
        return False, 0, {}
    rvol = float(r['vol']) / float(vol_ma20)
    atr_med = df['atr_pct'].rolling(TRENDCONFIRM_ATR_MEDIAN_WINDOW).median().iloc[-1]
    if pd.isna(atr_med):
        return False, 0, {}
    gap_ema20 = (float(r['close']) / float(ema20) - 1) * 100

    # --- Syarat WAJIB (semuanya harus lolos) ---
    # ATR% dikembalikan ke self-relative SAJA (03/09/2026, permintaan Mas Budi) -- TRENDCONFIRM_ATR_ABS_MIN
    # (3.0, angka tervalidasi backtest) TIDAK lagi jadi gerbang wajib keras di sini, dipindah jadi
    # panduan eksplisit utk AI di babak 2 (ai_decision_batch_rank) supaya AI yg menimbang, bukan
    # gerbang mutlak yg langsung mencoret kandidat lain yg mungkin masih bagus meski ATR<3.0.
    if not (float(atrp) >= float(atr_med)): return False, 0, {}
    if not (gap_ema20 >= 0.0): return False, 0, {}
    if not (rvol >= TRENDCONFIRM_RVOL_MIN): return False, 0, {}
    if not (float(rsi) < TRENDCONFIRM_RSI_MAX): return False, 0, {}

    # --- Syarat SEKUNDER (dipakai utk skor/ranking, BUKAN gerbang wajib) ---
    score = 2 if float(bbp) >= TRENDCONFIRM_BB_PCT_SECONDARY else 1

    detail = {'atr_pct': float(atrp), 'atr_median100': float(atr_med), 'rvol': rvol,
               'bb_pct': float(bbp), 'gap_ema20_pct': gap_ema20, 'rsi': float(rsi)}
    return True, score, detail

def thread_trendconfirm_scan():
    """Scan sinyal TrenKonfirmasi-4h. Dipanggil periodik tiap TRENDCONFIRM_SCAN_INTERVAL detik."""
    global _trendconfirm_last_candle_ts
    if not TRENDCONFIRM_ENABLED: return

    # Cek circuit breaker rugi harian DI SINI, SEBELUM evaluasi kandidat apa pun (fix
    # 03/09/2026, permintaan Mas Budi): sebelumnya breaker cuma dicek di dalam
    # open_deal_with_sizing() SETELAH AI dipanggil per-kandidat -- kalau breaker aktif,
    # setiap kandidat GAGAL dibuka jadi n_active tidak pernah nambah, jadi guard
    # "n_active>=Max Deals: break" di loop bawah nggak pernah kepicu, dan loop
    # menghabiskan SEMUA kandidat lewat AI (bisa belasan) walau semuanya pasti gagal.
    # Cek di sini = 0 AI call & 0 notifikasi kalau breaker aktif, bukan menunggu gagal
    # satu-satu di akhir.
    if is_daily_loss_limit_breached():
        log(f"[T_TRENDCONFIRM] Scan di-skip -- batas rugi harian tersulut (cek di awal, bukan per-kandidat)")
        return

    n_active = deal_count_by_strategy('trend_confirm_4h')
    if n_active >= TRENDCONFIRM_MAX_DEALS: return

    now_ms = int(time.time() * 1000)
    sec4h  = STRAT4H_SECONDS
    candle_open_ms = (now_ms // (sec4h * 1000)) * (sec4h * 1000)

    ticker = get_ticker_24h()
    if not ticker: return
    vol_map = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in ticker}

    with active_deals_lock:
        existing = set(active_deals.keys())

    candidates = []
    scan_blockers_tc = {}
    scan_total_tc = 0
    for sym_info in ticker:
        sym = sym_info.get("symbol", "")
        if not sym.endswith("USDT"): continue
        if sym in existing: continue
        if sym in SYMBOL_BLACKLIST: continue
        if _trendconfirm_last_candle_ts.get(sym) == candle_open_ms: continue
        if vol_map.get(sym, 0) < TRENDCONFIRM_MIN_VOL_USD: continue

        try:
            scan_total_tc += 1
            df = get_ohlcv_4h(sym, limit=max(TRENDCONFIRM_ATR_MEDIAN_WINDOW + 20, 200))
            if df is None or len(df) < TRENDCONFIRM_ATR_MEDIAN_WINDOW + 20:
                count_blocker(scan_blockers_tc, "Data OHLCV kurang", True)
                continue
            df = compute_indicators_4h(df)
            ok, score, detail = check_trendconfirm_entry(df)
            if not ok:
                count_blocker(scan_blockers_tc, "Syarat wajib belum lolos", True)
                continue
            r = df.iloc[-1]
            candidates.append((sym, float(r['close']), float(r['atr_pct']), score, detail, df))
        except Exception as e:
            log(f"  [T_TRENDCONFIRM] error {sym}: {e}")

    record_scan_blockers("TrenKonfirmasi-4h", scan_total_tc, len(candidates), scan_blockers_tc)
    if not candidates:
        log(f"[T_TRENDCONFIRM] Tidak ada kandidat ({scan_total_tc} discan).")
        return

    # Ranking: skor (syarat sekunder) dulu, lalu kekuatan sinyal (gap-EMA20 makin besar makin kuat)
    candidates.sort(key=lambda x: (x[3], x[4].get('gap_ema20_pct', 0)), reverse=True)
    log(f"[T_TRENDCONFIRM] {len(candidates)} kandidat. Buka deal terbaik (slot {n_active}/{TRENDCONFIRM_MAX_DEALS})...")

    # ============ BABAK 1: AI individual per-kandidat (notify=False), kumpulkan yg lolos ============
    # (03/09/2026, permintaan Mas Budi) -- dievaluasi s/d TRENDCONFIRM_BATCH_POOL_SIZE kandidat
    # teratas (ranking), BUKAN dibuka langsung begitu lolos -- ditahan dulu, dikumpulkan, baru
    # dibandingkan sekaligus di babak 2 di bawah. notify=False supaya user tidak dapat notif
    # "OPEN" individual yg belum final (bisa saja dibuang di babak 2).
    pool = candidates[:TRENDCONFIRM_BATCH_POOL_SIZE]
    held = {}   # symbol -> (signal_price, atrp, score, detail, df, rsi_val)
    for sym, signal_price, atrp, score, detail, df in pool:
        with active_deals_lock:
            if sym in active_deals: continue
        if is_cooldown_enabled('trend_confirm_4h') and cooldown_remaining(sym) > 0:
            log(f"[T_TRENDCONFIRM] {sym} cooldown {cooldown_remaining(sym)/3600:.1f}j — skip")
            continue
        if sizing_fail_remaining(sym) > 0:
            log(f"[T_TRENDCONFIRM] {sym} baru gagal sizing/open (sisa {sizing_fail_remaining(sym)/60:.0f} menit) — skip, jangan coba lagi dulu.")
            continue
        r = df.iloc[-1]
        rsi_val = r.get('rsi', float('nan'))
        if is_ai_call_open_enabled('trend_confirm_4h'):
            _ai_ind = {'atr_pct': f"{atrp:.2f}%", 'score': score, 'signal_price': _fmt_price(signal_price),
                       'rvol': f"{detail.get('rvol',0):.2f}x", 'bb_pct': f"{detail.get('bb_pct',0):.2f}",
                       'gap_ema20': f"{detail.get('gap_ema20_pct',0):+.2f}%"}
            if not pd.isna(rsi_val): _ai_ind['rsi'] = f"{float(rsi_val):.1f}"
            if not ai_decision_open(sym, 'TrenKonfirmasi-4h', _ai_ind, active_deal_count(), notify=False):
                log(f"[T_TRENDCONFIRM] {sym} babak-1 di-skip oleh AI individual")
                continue
        held[sym] = (signal_price, atrp, score, detail, df, rsi_val)

    if not held:
        log(f"[T_TRENDCONFIRM] Babak 1 selesai: 0/{len(pool)} lolos AI individual — tidak lanjut ke babak 2.")
        log_ai_babak1('TrenKonfirmasi-4h', [], TRENDCONFIRM_BATCH_MAX_APPROVE, total_evaluated=len(pool))
        return
    log(f"[T_TRENDCONFIRM] Babak 1 selesai: {len(held)}/{len(pool)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
    log_ai_babak1('TrenKonfirmasi-4h', [to_display_pair(s) for s in held.keys()],
                  TRENDCONFIRM_BATCH_MAX_APPROVE, total_evaluated=len(pool))

    # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus, pangkas ke maks 5 ============
    batch_input = [{'symbol': sym, 'strategy': 'trend_confirm_4h', 'score': v[2], 'detail': v[3]}
                   for sym, v in held.items()]
    approved_syms = ai_decision_batch_rank(
        batch_input, strategy_label='TrenKonfirmasi-4h', max_approve=TRENDCONFIRM_BATCH_MAX_APPROVE,
        extra_guidance=(
            f"Panduan tambahan: ATR% >= {TRENDCONFIRM_ATR_ABS_MIN:.1f}% adalah level yang tervalidasi "
            f"backtest untuk strategi ini (momentum/volatilitas cukup kuat) -- prioritaskan kandidat "
            f"dengan ATR% di atas level itu, tapi ini BUKAN syarat mutlak: kandidat dengan ATR% sedikit "
            f"di bawah itu tetap boleh dipilih kalau sisi lain (RVOL, BB%b, jarak EMA20, RSI) jauh lebih "
            f"meyakinkan dibanding kandidat lain di daftar ini."
        ),
        criteria_note=f"Kriteria: skor, ATR%, RVOL, BB%b, jarak EMA20, RSI (ATR>={TRENDCONFIRM_ATR_ABS_MIN:.1f}% diprioritaskan, bukan wajib)",
    )
    if not approved_syms:
        log(f"[T_TRENDCONFIRM] Babak 2: 0 dari {len(held)} kandidat diloloskan.")
        return

    # ============ BUKA DEAL utk yg lolos babak 2, urut prioritas AI, tetap dibatasi Max Deals ============
    for sym in approved_syms:
        n_active = deal_count_by_strategy('trend_confirm_4h')
        if n_active >= TRENDCONFIRM_MAX_DEALS: break
        with active_deals_lock:
            if sym in active_deals: continue
        signal_price, atrp, score, detail, df, rsi_val = held[sym]

        ok, target_usd, add_usd = open_deal_with_sizing(sym, score, strategy="trend_confirm_4h")
        if not ok: continue

        try:
            entry_price = get_price_now(sym)
            if entry_price <= 0: entry_price = signal_price
        except Exception:
            entry_price = signal_price
        slip_pct = (entry_price / signal_price - 1) * 100 if signal_price > 0 else 0.0

        add_to_active_deals(sym, {
            "strategy":         "trend_confirm_4h",
            "entry_price":      entry_price,
            "peak":             entry_price,
            "signal_price":     signal_price,
            "atr_pct":          atrp,
            "score":            score,
            "opened_ts":        time.time(),
            "opened_candle_ts": int(candle_open_ms),
            "trailing_armed":   False,
            "opened_at":        now_wib().strftime('%Y-%m-%d %H:%M:%S'),
            "target_usd":       target_usd,
            "add_usd":          add_usd,
            "tf":               TRENDCONFIRM_TIMEFRAME,
            "rsi_open":         float(rsi_val) if not pd.isna(rsi_val) else None,
        })
        _trendconfirm_last_candle_ts[sym] = candle_open_ms

        trail_arm = get_arm_pct(atrp)
        trail_d   = trailing_dist(atrp)
        msg = (
            f"TrenKonfirmasi-4h | OPEN LONG\n"
            f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
            f"Pair  : {to_display_pair(sym)}\n"
            f"Harga entry (pasar): {_fmt_price(entry_price)}\n"
            f"Harga sinyal (4h closed): {_fmt_price(signal_price)}\n"
            f"Selisih (slippage): {slip_pct:+.2f}%\n"
            f"ATR%  : {atrp:.2f}  (trailing {trail_d}% stlh +{trail_arm}%)\n"
            f"RVOL  : {detail.get('rvol',0):.2f}x  |  BB%b: {detail.get('bb_pct',0):.2f}  |  vs EMA20: {detail.get('gap_ema20_pct',0):+.2f}%  |  RSI: {detail.get('rsi',0):.1f}\n"
            f"Skor sinyal: {score}/2 -> modal ${target_usd:.0f}"
            + (f" (+add ${add_usd:.0f} delay 15s)" if add_usd > 0 else "") + "\n"
            f"Slot terpakai: {n_active+1}/{TRENDCONFIRM_MAX_DEALS}"
        )
        send_telegram(msg)

        csv_log_open({
            'open_time_wib':  now_wib().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol':         to_display_pair(sym),
            'signal_price':   f"{_fmt_price(signal_price)}",
            'entry_price':    f"{_fmt_price(entry_price)}",
            'slip_pct':       f"{slip_pct:+.2f}",
            'atr_pct':        f"{atrp:.2f}",
            'trail_dist_pct': f"{trailing_dist(atrp)}",
            'base_usd':       target_usd,
            'score':          score,
            'strategy':       'trend_confirm_4h',
            'rsi_open':       f"{float(rsi_val):.1f}" if not pd.isna(rsi_val) else '',
        })
        log_oac('OPEN', sym, 'TrenKonfirmasi-4h', {
            'entry_price': _fmt_price(entry_price),
            'slip_pct':    f"{slip_pct:+.2f}%",
            'atr_pct':     f"{atrp:.2f}%",
            'rvol':        f"{detail.get('rvol',0):.2f}x",
            'bb_pct':      f"{detail.get('bb_pct',0):.2f}",
            'gap_ema20':   f"{detail.get('gap_ema20_pct',0):+.2f}%",
            'score':       score,
            'trail_dist':  f"{trailing_dist(atrp)}%",
        })

        n_active += 1
        if n_active >= TRENDCONFIRM_MAX_DEALS: break

def run_thread_trendconfirm():
    """Thread T_TRENDCONFIRM: scan TrenKonfirmasi-4h tiap TRENDCONFIRM_SCAN_INTERVAL detik."""
    while True:
        try:
            thread_trendconfirm_scan()
        except Exception as e:
            log(f"WARN T_TRENDCONFIRM error: {e}")
        time.sleep(TRENDCONFIRM_SCAN_INTERVAL)


# ══════════════════════════════════════════════════════════════════════════════
# STRATEGI #6: Momentum Filter 4h (observasi, tidak buka deal)
# Kondisi: price_live >= EMA20 AND price_live >= EMA50 AND ST dir == +1
# Output : append ke tes6.txt di folder proyek + notifikasi Telegram ringkas
# ══════════════════════════════════════════════════════════════════════════════
STRAT6_SCAN_INTERVAL = 900   # scan tiap 15 menit
STRAT6_MIN_VOL_USD   = 3_000_000

def check_strat6_4h():
    """
    Scan 4h: Catat pair yang memenuhi ST+1 + price>=EMA20 + price>=EMA50.
    Tulis indikator ke tes6.txt dan kirim Telegram ringkas.
    Tidak membuka deal — hanya observasi untuk kalibrasi Strategi #6.
    """
    try:
        now_ms  = int(time.time() * 1000)
        ticker  = get_ticker_24h()
        if not ticker:
            return
        vol_map = {}
        for t in ticker:
            try: vol_map[t['symbol']] = float(t.get('quoteVolume', 0))
            except: pass

        hits = []
        for sym, vol24 in vol_map.items():
            if not sym.endswith('USDT'): continue
            if sym in SYMBOL_BLACKLIST: continue
            if vol24 < STRAT6_MIN_VOL_USD: continue
            try:
                df = get_ohlcv_4h(sym, limit=100)
                if df is None or len(df) < 60: continue
                df = compute_indicators_4h(df)
                r  = df.iloc[-1]

                sd = r.get('st_dir')
                if pd.isna(sd) or sd != 1: continue

                ef = r.get('ema_fast')   # EMA9
                es = r.get('ema_slow')   # EMA21
                if pd.isna(ef) or pd.isna(es): continue

                price_now = get_price_now(sym)
                if price_now <= 0: continue
                if price_now < float(ef): continue   # price < EMA20 (proxy EMA9 di 4h)
                if price_now < float(es): continue   # price < EMA50 (proxy EMA21 di 4h)

                # Kumpulkan indikator
                mh    = r.get('macd_hist')
                rsi   = r.get('rsi') if 'rsi' in df.columns else None
                atr   = r.get('atr_pct')
                sk    = r.get('stoch_k')
                vm    = r.get('vol_ma')
                rvol  = (float(r['vol']) / float(vm)) if (vm and float(vm) > 0) else None
                cps   = calc_perf_score(sym, now_ms)

                hits.append({
                    'sym':      sym,
                    'price':    price_now,
                    'ema20':    float(ef),
                    'ema50':    float(es),
                    'macd_h':   round(float(mh), 6)  if mh   is not None and mh  == mh  else None,
                    'rsi':      round(float(rsi), 1)  if rsi  is not None and rsi == rsi else None,
                    'atr_pct':  round(float(atr), 2)  if atr  is not None and atr == atr else None,
                    'stoch_k':  round(float(sk), 1)   if sk   is not None and sk  == sk  else None,
                    'rvol':     round(rvol, 2)         if rvol is not None else None,
                    'perf':     round(cps, 3)          if cps  == cps      else None,
                })
            except Exception as e:
                log(f"  [S6] error {sym}: {e}")

        if not hits:
            return

        ts_str  = now_wib().strftime('%Y-%m-%d %H:%M')
        out_path = os.path.join(DATA_DIR, 'tes6.txt')
        try:
            with open(out_path, 'a', encoding='utf-8') as f:
                f.write(f"\n── {ts_str} WIB  ({len(hits)} pair) ──\n")
                for h in hits:
                    f.write(
                        f"{to_display_pair(h['sym']):<14}"
                        f" price={_fmt_price(h['price'])}"
                        f" EMA20={_fmt_price(h['ema20'])}"
                        f" EMA50={_fmt_price(h['ema50'])}"
                        f" MACD={h['macd_h']}"
                        f" RSI={h['rsi']}"
                        f" ATR%={h['atr_pct']}"
                        f" Stoch={h['stoch_k']}"
                        f" RVol={h['rvol']}"
                        f" Perf={h['perf']}\n"
                    )
        except Exception as e:
            log(f"WARN [S6] tulis tes6.txt gagal: {e}")

        # Telegram: kirim ringkas top-5
        tg_lines = [f"📊 *Strategi #6 obs* | {ts_str} WIB | {len(hits)} pair lolos"]
        for h in hits[:5]:
            tg_lines.append(
                f"• {to_display_pair(h['sym'])} `{_fmt_price(h['price'])}`"
                f" RSI={h['rsi']} ATR={h['atr_pct']}% RVol={h['rvol']}x Perf={h['perf']}"
            )
        if len(hits) > 5:
            tg_lines.append(f"  (+{len(hits)-5} pair lainnya → lihat tes6.txt)")
        send_telegram('\n'.join(tg_lines), parse_mode='Markdown')
        log(f"[S6] {len(hits)} pair ditulis ke tes6.txt dan dikirim Telegram.")

    except Exception as e:
        log(f"WARN [S6] check_strat6_4h error: {e}")


def run_thread_strat6():
    """Thread T-Strat6: scan Strategi #6 tiap STRAT6_SCAN_INTERVAL detik."""
    time.sleep(60)   # delay startup agar T1 selesai dulu
    while True:
        try:
            check_strat6_4h()
        except Exception as e:
            log(f"WARN T-Strat6 error: {e}")
        time.sleep(STRAT6_SCAN_INTERVAL)

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGI #7: Hunting 4h 
# ══════════════════════════════════════════════════════════════════════════════
def check_hunting_strategy(df, r, config, blocker_counts=None):
    """
    Strategi Hunting-4h: cari koin yang baru saja breakout tipis di atas EMA zona kompresi.
    Syarat wajib:
      - Quote USDT, base bukan fiat
      - price dalam pita EMA20 -HUNTING_EMA20_BAND_PCT% s/d +HUNTING_EMA20_BAND_PCT% (01/09/2026,
        dulu satu sisi 0-0.75%, lihat komentar konstantanya)
    Syarat opsional (via config):
      - EMA20 < EMA50, jarak 0-1.5%              (hunting_ema_gap)
      - price_change% antara 0%-2.0%              (hunting_price_change)
      - price > EMA20, jarak 0-0.75%              (hunting_above_ema50)
      - bullish candlestick: Hammer OR Strong Bull OR Bullish Engulfing OR Doji Bullish (hunting_uptrend)
    """
    symbol = r.get("symbol", "")

    def reject(label):
        count_blocker(blocker_counts, label, True)
        return None

    # --- Syarat wajib 1: USDT pair, base bukan fiat ---
    FIAT_LIST = {"USDT", "BUSD", "USDC", "TUSD", "DAI", "EUR", "GBP", "BRL", "RUB", "TRY", "AUD"}
    if not symbol.endswith("USDT"):
        return reject("Pair bukan USDT")
    base = symbol[:-4]
    if base in FIAT_LIST:
        return reject("Base asset fiat")

    # --- Ambil nilai indikator dari df ---
    if df is None or len(df) < 51:
        return reject("Data candle kurang")

    close  = float(df["close"].iloc[-1])
    ema20  = float(df["close"].ewm(span=20, adjust=False).mean().iloc[-1])
    ema50  = float(df["close"].ewm(span=50, adjust=False).mean().iloc[-1])

    # Supertrend dir (pakai kolom st_dir kalau ada, kalau tidak hitung inline)
    if "st_dir" in df.columns:
        st_dir = int(df["st_dir"].iloc[-1])
    else:
        import pandas_ta as _pta
        _st = _pta.supertrend(df["high"], df["low"], df["close"], length=10, multiplier=3)
        st_dir = int(_st.iloc[-1, 1]) if _st is not None and len(_st) > 0 else 0

    # RSI
    if "rsi" in df.columns:
        rsi = float(df["rsi"].iloc[-1])
    else:
        delta = df["close"].diff()
        gain  = delta.clip(lower=0).rolling(14).mean()
        loss  = (-delta.clip(upper=0)).rolling(14).mean()
        rs    = gain / loss.replace(0, float("nan"))
        rsi   = float((100 - 100 / (1 + rs)).iloc[-1])

    # ATR%
    if "atr_pct" in df.columns:
        atr_pct = float(df["atr_pct"].iloc[-1])
    else:
        prev_c  = df["close"].shift(1)
        tr      = ((df["high"] - df["low"])
                   .combine(abs(df["high"] - prev_c), max)
                   .combine(abs(df["low"]  - prev_c), max))
        atr_pct = float(tr.rolling(14).mean().iloc[-1] / close * 100) if close > 0 else 0.0

    # Price change% (close[-1] vs close[-2])
    prev_close = float(df["close"].iloc[-2])
    price_change_pct = ((close - prev_close) / prev_close * 100) if prev_close > 0 else 0.0

    # Jarak relatif (%)
    dist_ema20 = ((close - ema20) / ema20 * 100) if ema20 > 0 else None
    dist_ema50 = ((close - ema50) / ema50 * 100) if ema50 > 0 else None
    ema_gap    = ((ema50 - ema20) / ema50 * 100) if ema50 > 0 else None  # positif = EMA20 < EMA50

    # Uptrend proxy: close sekarang > close 5 candle lalu
    uptrend = (close > float(df["close"].iloc[-6])) if len(df) >= 6 else False

    # --- Syarat WAJIB 4: price dalam pita EMA20 -HUNTING_EMA20_BAND_PCT% s/d +HUNTING_EMA20_BAND_PCT% ---
    if dist_ema20 is None or not (-HUNTING_EMA20_BAND_PCT <= dist_ema20 <= HUNTING_EMA20_BAND_PCT):
        return reject("Jarak harga ke EMA20")

    # --- Tolak hanya Supertrend bearish; +1 bullish dan 0 transisi diterima ---
    if st_dir == -1:
        return reject("Supertrend bearish")

    # --- Syarat WAJIB: RSI < HUNTING_RSI_MAX (backtest: ST+1+RSI<60 = +0.714% vs baseline) ---
    rsi_max = config.get("rsi_max_override", HUNTING_RSI_MAX)
    if not (isinstance(rsi, float) and not (rsi != rsi)) and rsi >= rsi_max:
        return reject("RSI maksimum")

    # --- Syarat WAJIB: ATR% minimum (filter koin stagnan seperti PAX 0.02%) ---
    if atr_pct < HUNTING_MIN_ATR_PCT:
        return reject("ATR minimum")

    # --- Syarat OPSIONAL 2: EMA20 < EMA50, jarak 0-2.5% ---
    if config.get("hunting_ema_gap", True):
        if ema_gap is None or not (0.0 <= ema_gap <= 2.5):
            return reject("EMA20-EMA50 gap")

    # --- Syarat OPSIONAL 3: price_change% antara 0%-2.0% ---
    if config.get("hunting_price_change", True):
        if not (0.0 < price_change_pct <= 2.0):
            return reject("Perubahan harga")

    # --- Syarat OPSIONAL 5: price di atas EMA50, jarak 0-3% ---
    if config.get("hunting_above_ema50", True):
        if dist_ema50 is None or not (0.0 <= dist_ema50 <= 3.0):
            return reject("Jarak harga ke EMA50")

    # --- Syarat OPSIONAL 6: bullish candlestick — Hammer OR Strong Bull ---
    if config.get("hunting_uptrend", True):
        _open  = float(df["open"].iloc[-1])
        _high  = float(df["high"].iloc[-1])
        _low   = float(df["low"].iloc[-1])
        _range = _high - _low
        _body  = abs(close - _open)
        _lower_wick = min(close, _open) - _low
        # Candle n-2 (prev) untuk Bullish Engulfing
        _prev_open  = float(df["open"].iloc[-2])  if len(df) >= 2 else _open
        _prev_close = float(df["close"].iloc[-2]) if len(df) >= 2 else close
        _prev_body  = abs(_prev_close - _prev_open)
        # Pattern 1: Strong Bull — body mendominasi range (>50%)
        strong_bull = (_body / _range > 0.5) if _range > 0 else False
        # Pattern 2: Hammer — lower wick >= 1.5× body DAN close > open
        hammer = (_lower_wick >= 1.5 * _body) and (close > _open) if _body > 0 else False
        # Pattern 3: Bullish Engulfing — n-1 hijau, n-2 merah, body n-1 > body n-2
        bullish_engulfing = (close > _open) and (_prev_close < _prev_open) and (_body > _prev_body)
        # Pattern 4: Doji Bullish — body sangat kecil (<20% range), candle hijau
        doji_bullish = (_body / _range < 0.2) and (close > _open) if _range > 0 else False
        if not (strong_bull or hammer or bullish_engulfing or doji_bullish):
            return reject("Pola candle bullish")

    # --- Lolos semua syarat ---
    result = {
        "symbol":           symbol,
        "close":            close,
        "ema20":            round(ema20, 6),
        "ema50":            round(ema50, 6),
        "dist_ema20_pct":   round(dist_ema20, 2),
        "dist_ema50_pct":   round(dist_ema50, 2) if dist_ema50 is not None else None,
        "ema_gap_pct":      round(ema_gap, 2) if ema_gap is not None else None,
        "price_change_pct": round(price_change_pct, 2),
        "uptrend":          uptrend,
        "st_dir":           st_dir,
        "rsi":              round(rsi, 1) if isinstance(rsi, float) and rsi == rsi else None,
        "atr_pct":          round(atr_pct, 2),
        "strategy":         "hunting-4h",
    }
    return result

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGI #6 Hunting-4h — AUTO OPEN LONG + DEAL COUNTER
# ══════════════════════════════════════════════════════════════════════════════
def active_deal_count_hunting() -> int:
    """Jumlah deal aktif strategi hunting_4h."""
    with active_deals_lock:
        return sum(1 for d in active_deals.values() if d.get("strategy") == "hunting_4h")


def _try_swap_hunting_slot(incoming_symbol: str) -> bool:
    """
    Slot hunting penuh → cari deal hunting yang:
    1. trailing_armed = True (sudah arm)
    2. upnl_pct positif (profit)
    3. profit positif terendah (paling tipis)
    Kalau ditemukan → close deal itu sekarang → buka slot untuk incoming_symbol.
    Return True jika berhasil swap, False jika tidak ada kandidat.
    """
    with active_deals_lock:
        hunting_deals = {
            sym: d for sym, d in active_deals.items()
            if d.get("strategy") == "hunting_4h"
            and d.get("trailing_armed", False)
        }

    # Filter yang profit positif
    positive_armed = {}
    for sym, d in hunting_deals.items():
        ep = d.get("entry_price", 0)
        lp = d.get("last_price", ep)
        if ep > 0 and lp > ep:
            upnl_pct = (lp / ep - 1) * 100 - FEE_ROUND_TRIP_PCT
            if upnl_pct > 0:
                positive_armed[sym] = upnl_pct

    if not positive_armed:
        log(f"[HUNT-SWAP] {incoming_symbol}: slot penuh tapi tidak ada deal hunting armed+profit — skip swap")
        return False

    # Pilih yang profit positif TERENDAH
    swap_sym = min(positive_armed, key=lambda s: positive_armed[s])
    swap_pct  = positive_armed[swap_sym]
    log(f"[HUNT-SWAP] {incoming_symbol}: swap slot — close {swap_sym} (armed, profit +{swap_pct:.2f}%) → buka untuk {incoming_symbol}")

    # Close deal swap_sym via send_close_long
    price_now = get_price_now(swap_sym)
    if price_now <= 0:
        log(f"[HUNT-SWAP] gagal: price {swap_sym} tidak terbaca")
        return False

    reason = f"slot swap → buka {to_display_pair(incoming_symbol)} (profit +{swap_pct:.2f}%)"
    send_close_long(swap_sym, "hunting_4h")

    with active_deals_lock:
        d_swap = active_deals.pop(swap_sym, {})
    save_active_deals()
    record_closed(swap_sym)

    entry = d_swap.get("entry_price", price_now)
    prof  = (price_now / entry - 1) * 100 - FEE_ROUND_TRIP_PCT if entry > 0 else 0
    csv_log_close(to_display_pair(swap_sym),
                  now_wib().strftime("%Y-%m-%d %H:%M:%S"),
                  price_now, round(prof, 2), reason)
    send_telegram(
        f"Hunting-4h | SLOT SWAP\n"
        f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
        f"Close : {to_display_pair(swap_sym)} @ {_fmt_price(price_now)}\n"
        f"Profit: {prof:+.2f}% (armed)\n"
        f"Alasan: buka slot untuk {to_display_pair(incoming_symbol)}"
    )
    return True


def check_hunting_candidate(sym_info: dict, df, cfg: dict, notify: bool = True):
    """
    Babak 1 Hunting-4h (03/09/2026, pola 2-babak): evaluasi 6 syarat teknikal + AI
    individual. Return dict data kandidat kalau lolos (BELUM dibuka -- lihat
    open_hunting_candidate() utk babak 2), None kalau gagal di syarat manapun.
    Di-split dari open_hunting_if_signal() lama supaya kandidat bisa ditahan dulu
    (ditampung di pool) sebelum dibandingkan sekaligus oleh AI di babak 2, alih-alih
    langsung dibuka begitu lolos AI individual (first-come-first-served lama).
    Guard slot/swap SENGAJA tidak di sini -- itu dicek ulang fresh di babak 2 (open
    loop), sama seperti strategi lain, krn kondisi slot bisa berubah antar babak.
    notify: diteruskan ke ai_decision_open() -- pemanggil OTOMATIS (thread1d_scan_4h,
    scan_hunting_signals_only) HARUS pass notify=False (babak-1, jangan spam Telegram
    per kandidat). Default True supaya pemanggil MANUAL (open_hunting_if_signal, tombol
    "Open Long" dashboard) tetap dapat notifikasi OPEN/SKIP spt perilaku asli sebelum
    fungsi ini di-split -- jangan diubah ke False tanpa juga mengubah caller manualnya.
    """
    symbol = sym_info.get("symbol", "")

    with active_deals_lock:
        if symbol in active_deals:
            return None
    if symbol in SYMBOL_BLACKLIST:
        return None
    if is_cooldown_enabled('hunting_4h') and cooldown_remaining(symbol) > 0:
        return None
    # notify=False = pemanggil OTOMATIS (babak-1) -- pemanggil MANUAL (notify=True, tombol
    # dashboard) TIDAK kena cooldown ini, krn itu 1 keputusan eksplisit per klik (mis. Mas
    # Budi baru topup saldo terus langsung coba manual -- jangan diblokir tanpa alasan jelas).
    if not notify and sizing_fail_remaining(symbol) > 0:
        return None

    # ── Data indikator ────────────────────────────────────────────────────────
    if df is None or len(df) < 51:
        return None
    # df yang masuk cuma OHLCV mentah — lengkapi dulu (rsi/stoch/macd/bb/williams/cci/obv/
    # ema20/st_dir) supaya field itu terisi asli, bukan "—" terus di notif OPEN.
    df = compute_indicators_4h(df)
    r = df.iloc[-1]

    # +1 bullish dan 0 transisi diterima; hanya -1 bearish yang ditolak.
    try:
        import pandas_ta as _pta
        _st = _pta.supertrend(df["high"], df["low"], df["close"], length=10, multiplier=3)
        _st_dir_col = [column for column in _st.columns if "SUPERTd" in column]
        if _st_dir_col and int(_st[_st_dir_col[0]].iloc[-1]) == -1:
            return None
    except Exception:
        pass

    close      = float(df["close"].iloc[-1])
    ema20      = float(df["close"].ewm(span=20, adjust=False).mean().iloc[-1])
    ema50      = float(df["close"].ewm(span=50, adjust=False).mean().iloc[-1])
    prev_close = float(df["close"].iloc[-2])
    price_change_pct = ((close - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
    dist_ema20 = ((close - ema20) / ema20 * 100) if ema20 > 0 else None
    dist_ema50 = ((close - ema50) / ema50 * 100) if ema50 > 0 else None
    ema_gap    = ((ema50 - ema20) / ema50 * 100) if ema50 > 0 else None
    uptrend    = (close > float(df["close"].iloc[-6])) if len(df) >= 6 else False

    # ── Syarat WAJIB (tidak bisa dibypass) ───────────────────────────────────
    # 1. Quote USDT, base bukan fiat
    FIAT_LIST = {"USDT","BUSD","USDC","TUSD","DAI","EUR","GBP","BRL","RUB","TRY","AUD"}
    if not symbol.endswith("USDT"):
        return None
    if symbol[:-4] in FIAT_LIST:
        return None

    # 4. Price > EMA20, jarak 0-0.75% (WAJIB) — backtest_hunting_sweep optimal
    if dist_ema20 is None or not (0.0 <= dist_ema20 <= 0.75):
        return None

    # ── Syarat OPSIONAL — diabaikan jika cfg[key] = False ────────────────────
    # 2. EMA20 < EMA50, gap 0-2.5%
    if cfg.get("hunting_ema_gap", True):
        if ema_gap is None or not (0.0 <= ema_gap <= 2.5):
            return None

    # 3. Price change 0%-2.0% (dilonggarkan 18/08/2026)
    if cfg.get("hunting_price_change", True):
        if not (0.0 < price_change_pct <= 2.0):
            return None

    # 5. Price > EMA50, jarak 0-3%
    if cfg.get("hunting_above_ema50", True):
        if dist_ema50 is None or not (0.0 <= dist_ema50 <= 3.0):
            return None

    # 6. Bullish candlestick: Hammer OR Strong Bull OR Bullish Engulfing OR Doji Bullish
    if cfg.get("hunting_uptrend", True):
        try:
            _open  = float(df["open"].iloc[-1])
            _high  = float(df["high"].iloc[-1])
            _low   = float(df["low"].iloc[-1])
            _range = _high - _low
            _body  = abs(close - _open)
            _lower_wick = min(close, _open) - _low
            _prev_open  = float(df["open"].iloc[-2])  if len(df) >= 2 else _open
            _prev_close = float(df["close"].iloc[-2]) if len(df) >= 2 else close
            _prev_body  = abs(_prev_close - _prev_open)
            strong_bull       = (_body / _range > 0.5) if _range > 0 else False
            hammer            = (_lower_wick >= 1.5 * _body) and (close > _open) if _body > 0 else False
            bullish_engulfing = (close > _open) and (_prev_close < _prev_open) and (_body > _prev_body)
            doji_bullish      = (_body / _range < 0.2) and (close > _open) if _range > 0 else False
            if not (strong_bull or hammer or bullish_engulfing or doji_bullish):
                return None
        except Exception:
            return None

    # ── Semua syarat teknikal lolos ───────────────────────────────────────────
    try:
        atrp = float(df["atr_pct"].iloc[-1]) if "atr_pct" in df.columns else float(
            (df["high"] - df["low"]).rolling(14).mean().iloc[-1] / close * 100
        )
        if atrp != atrp or atrp <= 0:
            atrp = 3.0
    except Exception:
        atrp = 3.0

    score = 1
    # Cek estimasi saldo sebelum open — cegah ghost deal karena insufficient balance
    if not has_enough_balance_for_hunting(HUNTING_ORDER_VOLUME, HUNTING_CAPITAL_USD):
        log(f"[T1d-HUNT] {symbol} skip: estimasi saldo tidak cukup untuk open ${HUNTING_ORDER_VOLUME}")
        return None

    _rsi_hunt_val = None
    if is_ai_call_open_enabled('hunting_4h'):
        _ai_ind = {'atr_pct': f"{atrp:.2f}%"}
        try:
            if 'rsi' in df.columns:
                _rsi_hunt = df['rsi'].iloc[-1]
            else:
                _rsi_hunt = ta.rsi(df['close'], length=14).iloc[-1]
            if not pd.isna(_rsi_hunt):
                _rsi_hunt_val = float(_rsi_hunt)
                _ai_ind['rsi'] = f"{_rsi_hunt_val:.1f}"
        except Exception:
            pass
        if not ai_decision_open(symbol, 'Hunting-4h', _ai_ind, active_deal_count(), notify=notify):
            log(f"[T1d-HUNT] {symbol} " + ("babak-1 di-skip oleh AI individual" if not notify else "OPEN di-skip oleh AI decision"))
            return None

    _bb_hunt  = float(r.get('bb_pct')) if 'bb_pct' in r.index and not pd.isna(r.get('bb_pct')) else 0.0
    _gap_hunt = dist_ema20 if dist_ema20 is not None else 0.0
    return {
        'symbol': symbol, 'df': df, 'r': r, 'close': close, 'atrp': atrp, 'score': score,
        'dist_ema20': dist_ema20, 'dist_ema50': dist_ema50, 'ema_gap': ema_gap,
        'price_change_pct': price_change_pct, 'uptrend': uptrend,
        'detail': {'atr_pct': atrp, 'rvol': 0.0, 'bb_pct': _bb_hunt,
                   'gap_ema20_pct': _gap_hunt, 'rsi': _rsi_hunt_val or 0.0},
    }

def open_hunting_candidate(held: dict) -> bool:
    """
    Babak 2 Hunting-4h (03/09/2026, pola 2-babak): eksekusi open long utk kandidat yg
    sudah lolos check_hunting_candidate() DAN diloloskan ai_decision_batch_rank().
    Guard slot/swap dicek fresh di sini (bukan babak 1) krn kondisi slot bisa berubah
    antar babak. Sisanya sama persis dgn open_hunting_if_signal() lama.
    """
    symbol = held['symbol']; df = held['df']; r = held['r']
    close = held['close']; atrp = held['atrp']; score = held['score']
    dist_ema20 = held['dist_ema20']; dist_ema50 = held['dist_ema50']; ema_gap = held['ema_gap']
    price_change_pct = held['price_change_pct']; uptrend = held['uptrend']

    with active_deals_lock:
        if symbol in active_deals:
            return False
    if active_deal_count_hunting() >= HUNTING_MAX_DEALS:
        # Slot penuh — coba swap: close deal hunting yang sudah arm dan profit positif terendah
        _swapped = _try_swap_hunting_slot(symbol)
        if not _swapped:
            return False

    ok, target_usd, add_usd = open_deal_with_sizing(symbol, score, strategy="hunting_4h")
    if not ok:
        log(f"[T1d-HUNT] {symbol} " + ("Binance order ditolak" if USE_BINANCE_DIRECT else "3Commas TOLAK") + " open hunting")
        return False

    try:
        ticker_now  = _binance_get("/api/v3/ticker/price", {"symbol": symbol})
        entry_price = float(ticker_now["price"]) if ticker_now else close
    except Exception:
        entry_price = close

    slip_pct = (entry_price / close - 1) * 100 if close > 0 else 0.0
    candle_open_ms = (int(time.time() * 1000) // (STRAT4H_SECONDS * 1000)) * (STRAT4H_SECONDS * 1000)

    # Ambil stoch/st_dir dengan safe fallback — tidak boleh gagalkan add_to_active_deals
    try:
        _sk_open = float(r.get("stoch_k", float("nan"))) if "stoch_k" in r.index and not pd.isna(r.get("stoch_k", float("nan"))) else None
        _sd_open = float(r.get("stoch_d", float("nan"))) if "stoch_d" in r.index and not pd.isna(r.get("stoch_d", float("nan"))) else None
        _st_open = int(r.get("st_dir", 0)) if "st_dir" in r.index and not pd.isna(r.get("st_dir", 0)) else None
        _rsi_open  = float(r.get("rsi",       float("nan"))) if "rsi"       in r.index and not pd.isna(r.get("rsi",       float("nan"))) else None
        _mh_open   = float(r.get("macd_hist", float("nan"))) if "macd_hist" in r.index and not pd.isna(r.get("macd_hist", float("nan"))) else None
        _bb_open   = float(r.get("bb_pct",    float("nan"))) if "bb_pct"    in r.index and not pd.isna(r.get("bb_pct",    float("nan"))) else None
        _wr_open   = float(r.get("williams_r",float("nan"))) if "williams_r" in r.index and not pd.isna(r.get("williams_r",float("nan"))) else None
        _cci_open  = float(r.get("cci",       float("nan"))) if "cci"       in r.index and not pd.isna(r.get("cci",       float("nan"))) else None
        _ema20_open= float(r.get("ema20",     float("nan"))) if "ema20"     in r.index and not pd.isna(r.get("ema20",     float("nan"))) else None
        _obv_open  = float(r.get("obv",       float("nan"))) if "obv"       in r.index and not pd.isna(r.get("obv",       float("nan"))) else None
    except Exception:
        _sk_open = _sd_open = _st_open = _rsi_open = _mh_open = _bb_open = _wr_open = _cci_open = _ema20_open = _obv_open = None

    add_to_active_deals(symbol, {
        "strategy":         "hunting_4h",
        "entry_price":      entry_price,
        "signal_price":     close,
        "peak":             entry_price,
        "atr_pct":          atrp,
        "score":            score,
        "target_usd":       target_usd,
        "add_usd":          add_usd,
        "opened_ts":        time.time(),
        "opened_candle_ts": int(candle_open_ms),
        "opened_at_wib":    now_wib().strftime('%d/%m/%Y %H:%M'),
        "trailing_armed":   False,
        "tf":               "4h",
        "stoch_k_open":     _sk_open,
        "stoch_d_open":     _sd_open,
        "st_dir_open":      _st_open,
        "rsi_open":         _rsi_open,
        "macd_hist_open":   _mh_open,
        "bb_pct_open":      _bb_open,
        "williams_r_open":  _wr_open,
        "cci_open":         _cci_open,
        "ema20_open":       _ema20_open,
        "obv_open":         _obv_open,
    })

    trail_arm = get_arm_pct(atrp)
    trail_d   = trailing_dist(atrp)
    dist_ema50_str = f"{dist_ema50:.2f}" if dist_ema50 is not None else "—"
    ema_gap_str    = f"{ema_gap:.2f}"    if ema_gap    is not None else "—"
    sk_open = float(r.get("stoch_k", float("nan"))) if "stoch_k" in r.index and not pd.isna(r.get("stoch_k", float("nan"))) else None
    sd_open = float(r.get("stoch_d", float("nan"))) if "stoch_d" in r.index and not pd.isna(r.get("stoch_d", float("nan"))) else None
    st_open = int(r.get("st_dir", 0)) if "st_dir" in r.index and not pd.isna(r.get("st_dir", 0)) else None
    stoch_str = f"Stoch %K={sk_open:.1f} %D={sd_open:.1f}" if sk_open is not None and sd_open is not None else ""
    st_str    = ("Uptrend" if st_open == 1 else "Downtrend") if st_open is not None else ""

    msg = (
        f"\U0001f3af Hunting-4h | OPEN LONG\n"
        f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
        f"Pair       : {to_display_pair(symbol)}\n"
        f"Entry pasar: {_fmt_price(entry_price)}  |  Sinyal: {_fmt_price(close)}\n"
        f"Slippage   : {slip_pct:+.2f}%\n"
        f"\u0394ema20={dist_ema20:.2f}%  \u0394ema50={dist_ema50_str}%\n"
        f"gap={ema_gap_str}%  chg={price_change_pct:.2f}%  "
        f"{'\u2191uptrend' if uptrend else '\u2193'}\n"
        f"ST 4h: {st_str}  |  {stoch_str}\n"
        f"ATR%={atrp:.2f}  trailing {trail_d}% stlh +{trail_arm}%\n"
        f"Modal: ${target_usd:.0f}"
        + (f" (+add ${add_usd:.0f})" if add_usd > 0 else "")
        + f"\nSlot hunting: {active_deal_count_hunting()}/{HUNTING_MAX_DEALS}"
    )
    send_telegram(msg)
    # Cuma masukkan field yang benar-benar ada nilainya — kalau None, biarkan absen
    # supaya mekanisme auto-lengkapi di log_oac() (fetch+hitung ulang) yang isi, bukan
    # ketiban placeholder "—" duluan (placeholder bikin auto-lengkapi di-skip).
    _oac_fields = {
        'entry_price':  _fmt_price(entry_price),
        'slip_pct':     f"{slip_pct:+.2f}%",
        'atr_pct':      f"{atrp:.2f}%",
        'trail_arm':    f"+{trail_arm}%",
        'trail_dist':   f"{trail_d}%",
        'score':        score,
        'modal_usd':    f"${target_usd:.0f}",
        'dist_ema20':   f"{dist_ema20:.2f}%",
        'ema_gap':      f"{ema_gap_str}%",
        'chg':          f"{price_change_pct:.2f}%",
        'uptrend':      str(uptrend),
    }
    if _rsi_open   is not None: _oac_fields['rsi']        = f"{_rsi_open:.1f}"
    if _sk_open    is not None: _oac_fields['stoch_k']    = f"{_sk_open:.1f}"
    if _sd_open    is not None: _oac_fields['stoch_d']    = f"{_sd_open:.1f}"
    if _mh_open    is not None: _oac_fields['macd_hist']  = f"{_mh_open:.5f}"
    if _bb_open    is not None: _oac_fields['bb_pct']     = f"{_bb_open:.3f}"
    if _wr_open    is not None: _oac_fields['williams_r'] = f"{_wr_open:.1f}"
    if _cci_open   is not None: _oac_fields['cci']        = f"{_cci_open:.1f}"
    if _obv_open   is not None: _oac_fields['obv']        = f"{_obv_open:.0f}"
    if _ema20_open is not None: _oac_fields['ema20']      = _fmt_price(_ema20_open)
    log_oac('OPEN', symbol, 'hunting-4h', _oac_fields)

    csv_log_open({
        'open_time_wib':  now_wib().strftime('%Y-%m-%d %H:%M:%S'),
        'symbol':         to_display_pair(symbol),
        'signal_price':   _fmt_price(close),
        'entry_price':    _fmt_price(entry_price),
        'slip_pct':       f"{slip_pct:+.2f}",
        'atr_pct':        f"{atrp:.2f}",
        'trail_dist_pct': f"{trail_d}",
        'base_usd':       HUNTING_ORDER_VOLUME,
        'rsi_open':       f"{_rsi_open:.1f}" if _rsi_open is not None else '',
        'score':          score,
        'strategy':       'hunting_4h',
    })
    deal_log_write({
        'timestamp_wib': now_wib().strftime('%Y-%m-%d %H:%M:%S'),
        'event_type':    'OPEN',
        'strategy':      'hunting_4h',
        'symbol':        to_display_pair(symbol),
        'thread':        'T1d-HUNT',
        'signal_price':  _fmt_price(close),
        'entry_price':   _fmt_price(entry_price),
        'slip_pct':      f"{slip_pct:+.2f}",
        'score':         score,
        'base_usd':      HUNTING_ORDER_VOLUME,
        'add_usd':       add_usd,
        'total_usd':     target_usd,
        'atr_pct':       f"{atrp:.2f}",
    })
    log(f"[T1d-HUNT] OPEN {symbol} @ {_fmt_price(entry_price)} (hunting-4h, slip {slip_pct:+.2f}%)")
    # Verifikasi deal masuk active_deals — kalau tidak, kirim alert
    with active_deals_lock:
        if symbol not in active_deals:
            send_telegram(
                f"⚠️ GHOST DEAL WARNING\n"
                f"{to_display_pair(symbol)} — open_long terkirim ke 3Commas\n"
                f"tapi tidak masuk active_deals. Cek 3Commas manual!",
                parse_mode=None
            )
            log(f"WARN [T1d-HUNT] {symbol} ghost deal — tidak masuk active_deals!")
    return True

def open_hunting_if_signal(sym_info: dict, df, cfg: dict) -> bool:
    """
    Evaluasi 6 syarat Hunting-4h lalu langsung open long jika lolos -- SATU langkah,
    TANPA babak 2 (tidak ditahan/dibandingkan AI dgn kandidat lain). Dipertahankan
    persis spt semula (03/09/2026: sekarang tinggal wrapper tipis di atas
    check_hunting_candidate()+open_hunting_candidate()) KHUSUS utk pemanggil manual
    (tombol "Open Long" dashboard, /manual_open) -- itu sudah 1 keputusan eksplisit
    dari Mas Budi sendiri per klik, tidak perlu lewat pola 2-babak. Pemanggil
    OTOMATIS (thread1d_scan_4h, scan_hunting_signals_only) TIDAK lagi lewat sini --
    mereka panggil check_hunting_candidate()/open_hunting_candidate() langsung supaya
    bisa ditampung ke pool & dibandingkan AI sekaligus (lihat komentar di masing2).
    Return True jika deal berhasil dibuka.
    """
    held = check_hunting_candidate(sym_info, df, cfg)
    if held is None:
        return False
    return open_hunting_candidate(held)

# ══════════════════════════════════════════════════════════════════════════════
# WEB DASHBOARD — Flask server untuk monitoring dan kontrol deal
# ══════════════════════════════════════════════════════════════════════════════

DEAL_OVERRIDES_FILE = "/data/deal_overrides.json"
OAC_LOG_FILE  = "/data/open-arm-close.txt"
STRAT6_LOG_FILE = "/data/tes6.txt"
WEB_PORT = int(os.environ.get("PORT", 8080))

_dashboard_state = {
    "near_miss": {
        "brkX2-12h": [],
        "Reversal-8h": [],
        "brkX2-4h": [],
        "CrossEMA-4h": [],
        "Akumulasi-4h": [],
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
        parsed = []
        for item in items:
            sym    = item[1] if len(item) > 1 and isinstance(item[1], str) else (item[0] if isinstance(item[0], str) else "?")
            n_pass = item[0] if isinstance(item[0], int) else 0
            total  = item[3] if len(item) > 3 else 9
            fails  = item[2] if len(item) > 2 else []
            # item[4]: vol_ratio (brkX2) atau sideways_start (Akumulasi)
            extra4 = item[4] if len(item) > 4 else None
            support    = float(item[5]) if len(item) > 5 else 0
            resistance = float(item[6]) if len(item) > 6 else 0
            if strategi == "Akumulasi-4h":
                vol_ratio      = None
                sideways_start = extra4 if isinstance(extra4, str) else None
                weighted_score = item[7] if len(item) > 7 else 0
                gating_ok      = item[8] if len(item) > 8 else False
                close          = float(item[9]) if len(item) > 9 else 0.0
                support_zone_low      = float(item[10]) if len(item) > 10 else 0.0
                support_zone_high     = float(item[11]) if len(item) > 11 else 0.0
                resistance_zone_low   = float(item[12]) if len(item) > 12 else 0.0
                resistance_zone_high  = float(item[13]) if len(item) > 13 else 0.0
                support_zone_touches  = int(item[14]) if len(item) > 14 else 0
                resistance_zone_touches = int(item[15]) if len(item) > 15 else 0
            else:
                vol_ratio      = extra4 if isinstance(extra4, (int, float)) else None
                sideways_start = None
                weighted_score = 0
                gating_ok      = False
                close          = 0.0
                support_zone_low = 0.0
                support_zone_high = 0.0
                resistance_zone_low = 0.0
                resistance_zone_high = 0.0
                support_zone_touches = 0
                resistance_zone_touches = 0
            sl_a = round(support * 0.995, 8) if support > 0 else 0
            sl_b = round(resistance * 0.995, 8) if resistance > 0 else 0
            parsed.append({
                "sym":            sym,
                "n_pass":         n_pass,
                "total":          total,
                "fails":          fails,
                "vol_ratio":      vol_ratio,
                "sideways_start": sideways_start,
                "support":        support,
                "resistance":     resistance,
                "close_fmt":      _fmt_price(close) if close > 0 else "-",
                "pct_vs_support":  round((close - support) / support * 100, 2) if support > 0 else None,
                "pct_vs_resist":   round((close - resistance) / resistance * 100, 2) if resistance > 0 else None,
                "posisi":         "Dekat Support" if (support > 0 and resistance > 0 and abs(close - support) <= abs(close - resistance)) else "Dekat Resist",
                "support_fmt":    _fmt_price(support) if support > 0 else "-",
                "resistance_fmt": _fmt_price(resistance) if resistance > 0 else "-",
                "support_zone_fmt": (
                    f"{_fmt_price(support_zone_low)} - {_fmt_price(support_zone_high)}"
                    if support_zone_low > 0 and support_zone_high > 0 else "-"
                ),
                "resistance_zone_fmt": (
                    f"{_fmt_price(resistance_zone_low)} - {_fmt_price(resistance_zone_high)}"
                    if resistance_zone_low > 0 and resistance_zone_high > 0 else "-"
                ),
                "support_zone_touches": support_zone_touches,
                "resistance_zone_touches": resistance_zone_touches,
                "sl_a_fmt":       _fmt_price(sl_a) if sl_a > 0 else "-",
                "sl_b_fmt":       _fmt_price(sl_b) if sl_b > 0 else "-",
                "weighted_score": weighted_score,
                "gating_ok":      gating_ok,
            })
        if strategi == "Akumulasi-4h":
            # Urutkan berdasarkan jarak persen absolut terkecil ke garis terdekat
            # (support atau resistance), agar pair yang paling "nempel" tampil dulu.
            def _nearest_line_pct(item):
                dists = []
                if item.get("pct_vs_support") is not None:
                    dists.append(abs(item["pct_vs_support"]))
                if item.get("pct_vs_resist") is not None:
                    dists.append(abs(item["pct_vs_resist"]))
                return min(dists) if dists else float('inf')

            parsed.sort(key=lambda x: (_nearest_line_pct(x), -x["weighted_score"], -x["n_pass"]))
        else:
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
_DASH_JS = 'var _refreshTimer=null;\nvar _curStrat=\'brkX2-12h\';\nfunction startRefresh(){if(_refreshTimer)return;_refreshTimer=setInterval(function(){window.location.reload();},30000);}\nfunction stopRefresh(){if(_refreshTimer){clearInterval(_refreshTimer);_refreshTimer=null;}}\nfunction isPauseChecked(){var cb=document.getElementById(\'cb-pause-refresh\');return cb&&cb.checked;}\nfunction pauseRefresh(){stopRefresh();}\nfunction resumeRefresh(){if(!isPauseChecked())startRefresh();}\nfunction onPauseRefreshToggle(checked){if(checked){stopRefresh();}else{startRefresh();}}\nfunction togglePauseRefresh(checked){var a=document.getElementById(\'cb-pause-refresh\');var b=document.getElementById(\'cb-pause-refresh-float\');if(a)a.checked=checked;if(b)b.checked=checked;onPauseRefreshToggle(checked);}\n\n// Definisi secondary per strategi\nvar STRAT_SECONDARY={\n  \'brkX2-12h\':[\n    {key:\'vol\',label:\'Vol 0.6x--5.0xMA\'},{key:\'rsi\',label:\'RSI<60\'},\n    {key:\'stoch\',label:\'Stoch%K<70\'},{key:\'atr\',label:\'ATR%<9%\'},\n    {key:\'htf\',label:\'HTF 3D vol>0.7xMA\'},{key:\'perf\',label:\'Perf>=0.5\'},{key:\'bull3\',label:\'3bar bullish\'}\n  ],\n  \'Reversal-8h T1\':[\n    {key:\'ha_bull\',label:\'c+1 HA bullish\'},{key:\'cross\',label:\'cross-up EMA20\'},\n    {key:\'perf\',label:\'Perf>=0.5\'},{key:\'vol24\',label:\'Vol24h>=$1.5jt\'}\n  ],\n  \'Reversal-8h T3-REV\':[\n    {key:\'elapsed\',label:\'Elapsed 5%-50%\'},{key:\'cross_live\',label:\'price_now>EMA20\'},\n    {key:\'perf\',label:\'Perf>=0.5\'},{key:\'vol24\',label:\'Vol24h>=$1.5jt\'}\n  ],\n  \'brkX2-4h\':[\n    {key:\'vol\',label:\'Vol>=0.25xMA\'},{key:\'rsi\',label:\'RSI<60\'},{key:\'stoch\',label:\'Stoch%K<80\'},\n    {key:\'htf\',label:\'12h candle bullish\'},{key:\'perf\',label:\'Perf>=0.5\'}\n  ],\n  \'CrossEMA-4h\':[\n    {key:\'vol\',label:\'Vol>=0.25xMA\'},{key:\'htf\',label:\'HTF12h vol>1.0xMA\'},\n    {key:\'vol24\',label:\'Vol24h>=$1.0jt\'}\n  ],\n  \'Akumulasi-4h\':[\n    {key:\'vol_asim\',label:\'Vol hijau>merah\'},{key:\'rsi\',label:\'RSI 30-56\'},\n    {key:\'macd_flat\',label:\'MACD flat≈0\'},{key:\'body_ratio\',label:\'Body ratio<0.57\'}\n  ]\n};\n\nfunction onStratSelect(strat){\n  _curStrat=strat;\n  // Update dropdown kandidat\n  var opts=document.querySelectorAll(\'.nm-opt\');\n  var count=0;\n  opts.forEach(function(o){\n    var show=o.getAttribute(\'data-strat\')===strat;\n    o.style.display=show?\'\':\'none\';\n    if(show)count++;\n  });\n  document.getElementById(\'nm-count\').textContent=\'(\'+count+\' kandidat dari scan terakhir)\';\n  // Reset pair select\n  var sel=document.getElementById(\'pair-select\');if(sel)sel.value=\'\';\n  // Reset panel\n  var panel=document.getElementById(\'pair-detail\');if(panel)panel.style.display=\'none\';\n  // Update secondary grid\n  renderSecondaryGrid(strat);\n  // Reset primary status\n  var ps=document.getElementById(\'primary-status\');\n  if(ps)ps.innerHTML=\'<span style="color:var(--muted)">-- pilih pair untuk lihat nilai aktual --</span>\';\n}\n\nfunction renderSecondaryGrid(strat){\n  var grid=document.getElementById(\'secondary-grid\');\n  if(!grid)return;\n  var defs=STRAT_SECONDARY[strat]||[];\n  grid.innerHTML=defs.map(function(d){\n    return \'<div class="sec-item" data-key="\'+d.key+\'"><label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:11px"><input type="checkbox" class="sec-cb" data-key="\'+d.key+\'" checked style="cursor:pointer"><span class="sec-label">\'+d.label+\'</span><span class="sec-actual" style="color:var(--muted)">--</span><span class="sec-status">--</span></label></div>\';\n  }).join(\'\');\n  // Re-attach event listeners\n  grid.querySelectorAll(\'.sec-cb\').forEach(function(cb){\n    cb.addEventListener(\'change\',function(){\n      fetch(\'/manual_filter\',{method:\'POST\',headers:{\'Content-Type\':\'application/x-www-form-urlencoded\'},body:\'key=\'+this.dataset.key+\'&value=\'+this.checked});\n    });\n  });\n}\n\ndocument.addEventListener(\'DOMContentLoaded\',function(){\n  startRefresh();\n  onStratSelect(\'brkX2-12h\');\n});\n\nfunction onPairSelect(sym){\n  var panel=document.getElementById(\'pair-detail\');\n  if(!sym){panel.style.display=\'none\';return;}\n  panel.style.display=\'block\';\n  panel.innerHTML=\'Mengambil data \'+sym.replace(\'USDT\',\'/USDT\')+\'...\';\n  pauseRefresh();\n  fetch(\'/api/strategy_detail?sym=\'+encodeURIComponent(sym)+\'&strat=\'+encodeURIComponent(_curStrat))\n    .then(function(r){return r.json();})\n    .then(function(d){\n      resumeRefresh();\n      if(d.error){panel.innerHTML=\'Error: \'+d.error;return;}\n      // Update primary\n      var ps=document.getElementById(\'primary-status\');\n      ps.innerHTML=d.primary.map(function(p){return badge(p.ok,p.label+\' (\'+p.actual+\')\');}).join(\' \');\n      // Update secondary\n      d.secondary.forEach(function(s){updateSec(s.key,s.actual,s.ok);});\n      // Panel ringkasan\n      var allP=d.primary_ok;\n      panel.innerHTML=\'<b style="color:\'+(allP?\'var(--green)\':\'var(--red)\')+\'">\'+sym.replace(\'USDT\',\'/USDT\')+\'</b> | \'+\n        d.primary.map(function(p){return (p.ok?\'<span style="color:var(--green)">\':\'<span style="color:var(--red)">\') + p.label+\': \'+p.actual+\'</span>\';}).join(\' | \')+\n        \' | \'+(allP?\'<span style="color:var(--green)">Primary OK</span>\':\'<span style="color:var(--red)">Primary GAGAL</span>\');\n    })\n    .catch(function(e){resumeRefresh();panel.innerHTML=\'Error: \'+e;});\n}\n\nfunction updateSec(key,actual,ok){\n  document.querySelectorAll(\'.sec-item[data-key="\'+key+\'"]\').forEach(function(item){\n    var a=item.querySelector(\'.sec-actual\'),s=item.querySelector(\'.sec-status\');\n    if(a)a.textContent=\'(skrg \'+actual+\')\';\n    if(s)s.innerHTML=ok?\'<span style="color:var(--green)">OK</span>\':\'<span style="color:var(--red)">X</span>\';\n  });\n}\n\nfunction doManualScan(){\n  var btn=document.getElementById(\'btn-scan\'),st=document.getElementById(\'scan-status\');\n  btn.disabled=true;btn.textContent=\'Scanning...\';\n  st.textContent=\'Sedang scan semua pair... (30-60 detik)\';\n  pauseRefresh();\n  fetch(\'/manual_scan\',{method:\'POST\'}).then(function(r){return r.json();}).then(function(data){\n    btn.disabled=false;btn.textContent=\'Scan Sekarang\';\n    st.textContent=\'Selesai \'+data.ts+\' -- \'+data.pairs.length+\' pair dievaluasi\';\n    renderResults(data.pairs);resumeRefresh();\n  }).catch(function(e){btn.disabled=false;btn.textContent=\'Scan Sekarang\';st.textContent=\'Error: \'+e;resumeRefresh();});\n}\n\nfunction promptOpenLong(){\n  var sel=document.getElementById(\'pair-select\');\n  var sym=sel?sel.value:\'\';\n  if(!sym){alert(\'Pilih pair dari dropdown dulu.\');return;}\n  var ss=document.getElementById(\'strat-select\');var strat=ss?ss.value:\'brkX2-12h\';\n  if(!confirm(\'Open Long [\'+strat+\']: \'+sym.replace(\'USDT\',\'/USDT\')+\'?\'))return;\n  execOpenLong(sym,strat);\n}\n\nfunction execOpenLong(sym,strat){\n  var fd=new FormData();fd.append(\'sym\',sym);fd.append(\'strat\',strat||\"brkX2-12h\");\n  var st=document.getElementById(\'scan-status\');\n  if(st)st.textContent=\'Membuka deal \'+sym+\'...\';\n  pauseRefresh();\n  fetch(\'/manual_open\',{method:\'POST\',body:fd}).then(function(r){return r.json();}).then(function(data){\n    resumeRefresh();\n    var msg=data.ok?(\'BERHASIL: \'+sym+\' Score=\'+data.score+\' Target=$\'+data.target_usd):(\'GAGAL: \'+data.error);\n    if(st)st.textContent=msg;alert(msg);\n  }).catch(function(e){resumeRefresh();alert(\'Error: \'+e);});\n}\n\nfunction renderResults(pairs){\n  var el=document.getElementById(\'scan-results\');\n  var sample=pairs.find(function(p){return p.primary_ok;})||pairs[0];\n  if(sample){\n    document.getElementById(\'primary-status\').innerHTML=\n      sample.secondaries?sample.secondaries.map(function(s){return badge(s.ok,s.key+\':\'+s.actual);}).join(\' \'):\'\';\n    if(sample.secondaries)sample.secondaries.forEach(function(s){updateSec(s.key,s.actual,s.ok);});\n  }\n  var cands=pairs.filter(function(p){return p.primary_ok;}).slice(0,20);\n  if(cands.length===0){el.innerHTML=\'<div class="empty">Tidak ada pair lolos syarat primary.</div>\';return;}\n  var rows=cands.map(function(p){\n    var sb=p.secondaries.map(function(s){return \'<span style="color:\'+(s.ok?\'var(--green)\':\'var(--red)\')+\';font-size:10px">\'+s.key+\':\'+s.actual+\'</span>\';}).join(\' \');\n    var ab=p.all_ok?\'<span style="color:var(--green);font-weight:600">LOLOS</span>\':\'<span style="color:var(--yellow)">primary OK</span>\';\n    var ob=\'<button onclick="execOpenLong(this.dataset.sym)" data-sym="\'+p.sym+\'" style="background:\'+(p.all_ok?\'var(--green)\':\'var(--yellow)\')+\';color:#000;border:none;border-radius:3px;padding:3px 8px;font-size:10px;cursor:pointer">\'+(p.all_ok?\'Open Sekarang\':\'Open & Bypass\')+\'</button>\';\n    return \'<tr><td class="sym">\'+p.sym.replace(\'USDT\',\'/USDT\')+\'</td><td>\'+ab+\'</td><td style="font-size:10px">\'+sb+\'</td><td>\'+ob+\'</td></tr>\';\n  }).join(\'\');\n  el.innerHTML=\'<table><thead><tr><th>Pair</th><th>isArmed</th><th>Secondary</th><th>Aksi</th></tr></thead><tbody>\'+rows+\'</tbody></table>\';\n}\n\nfunction badge(ok,label){return \'<span style="color:\'+(ok?\'var(--green)\':\'var(--red)\')+\';font-size:11px">[\'+(ok?\'OK\':\'X\')+\'] \'+label+\'</span>\';}\nfunction fmt(v){\n  if(v===undefined||v===null)return \'?\';\n  if(v>=1000)return v.toFixed(0);\n  if(v>=1)return v.toFixed(4);\n  if(v>=0.01)return v.toFixed(6);\n  if(v>=0.0001)return v.toFixed(8);\n  // harga sangat kecil seperti SHIB: pakai fixed decimal\n  var s=v.toFixed(10);\n  // hapus trailing zeros berlebihan tapi sisakan min 2 significant digits\n  return parseFloat(s).toPrecision(4);\n}\nfunction doOpenLong(sym){execOpenLong(sym);}\n\nfunction _setCookie(k,v){document.cookie=k+\'=\'+v+\';path=/;max-age=2592000\';}\n\nfunction _getCookie(k){var m=document.cookie.match(\'(^|;) ?\'+k+\'=([^;]*)(;|$)\');return m?m[2]:null;}\n\nfunction toggleCard(header){var card=header.parentElement;var name=\'c_\'+(card.querySelector(\'h2\').textContent.trim().replace(/[^a-zA-Z0-9]/g,\'_\').substring(0,20));card.classList.toggle(\'collapsed\');var collapsed=card.classList.contains(\'collapsed\');_setCookie(name,collapsed?\'1\':\'0\');}\n\nfunction restoreCards(){document.querySelectorAll(\'.card\').forEach(function(card){var h=card.querySelector(\'h2\');if(!h)return;var name=\'c_\'+(h.textContent.trim().replace(/[^a-zA-Z0-9]/g,\'_\').substring(0,20));if(_getCookie(name)===\'1\')card.classList.add(\'collapsed\');});}\n\nfunction editEntry(sym,curVal){\n  var v=prompt(\'Edit entry price untuk \'+sym.replace(\'USDT\',\'/USDT\')+\':\\n(harga aktual dari 3Commas)\',curVal);\n  if(v===null)return;\n  v=parseFloat(v);\n  if(isNaN(v)||v<=0){alert(\'Nilai tidak valid\');return;}\n  if(!confirm(\'Set entry \'+sym.replace(\'USDT\',\'/USDT\')+\' = \'+v+\'?\'))return;\n  var fd=new FormData();fd.append(\'sym\',sym);fd.append(\'field\',\'entry_price\');fd.append(\'value\',v);\n  pauseRefresh();\n  fetch(\'/edit_deal\',{method:\'POST\',body:fd})\n    .then(function(r){return r.json();})\n    .then(function(data){\n      resumeRefresh();\n      if(data.ok){\n        var el=document.getElementById(\'ep-\'+sym);\n        if(el)el.textContent=v;\n        alert(\'Entry \'+sym.replace(\'USDT\',\'/USDT\')+\' diupdate ke \'+v);\n      } else {\n        alert(\'Gagal: \'+data.error);\n      }\n    })\n    .catch(function(e){resumeRefresh();alert(\'Error: \'+e);});\n}\n\nfunction promptCloseDeal(){\n  var deals=document.querySelectorAll(\'#active-deals-body tr\');\n  var syms=[];\n  deals.forEach(function(tr){\n    var td=tr.querySelector(\'td.sym\');\n    if(td)syms.push(td.textContent.replace(\'/USDT\',\'USDT\').trim());\n  });\n  if(syms.length===0){alert(\'Tidak ada deal aktif saat ini.\');return;}\n  var sym=syms.length===1?syms[0]:prompt(\'Pilih pair yang mau di-close:\\n\'+syms.map(function(s){return s.replace(\'USDT\',\'/USDT\');}).join(\'\\n\')+\'\\n\\nKetik simbol (contoh: PUMP atau PUMPUSDT):\');\n  if(!sym)return;\n  sym=sym.trim().toUpperCase();\n  if(!sym.endsWith(\'USDT\'))sym=sym+\'USDT\';\n  if(!confirm(\'CLOSE DEAL \'+sym.replace(\'USDT\',\'/USDT\')+\'?\\n\\nIni akan kirim sinyal close ke 3Commas sekarang.\'))return;\n  var st=document.getElementById(\'scan-status\');\n  if(st)st.textContent=\'Menutup deal \'+sym+\'...\';\n  pauseRefresh();\n  var fd=new FormData();fd.append(\'sym\',sym);\n  fetch(\'/manual_close\',{method:\'POST\',body:fd})\n    .then(function(r){return r.json();})\n    .then(function(data){\n      resumeRefresh();\n      if(data.ok){\n        var msg=\'Close \'+sym.replace(\'USDT\',\'/USDT\')+\' BERHASIL! Price=\'+data.price+\' Profit=\'+data.profit_pct+\'%\';\n        if(st)st.textContent=msg;\n        alert(msg);\n        setTimeout(function(){window.location.reload();},2000);\n      } else {\n        if(st)st.textContent=\'Gagal: \'+data.error;\n        alert(\'Close GAGAL: \'+data.error);\n      }\n    })\n    .catch(function(e){resumeRefresh();alert(\'Error: \'+e);});\n}\n\nfunction promptAddFund(){\n  var deals=document.querySelectorAll(\'#active-deals-body tr\');\n  var syms=[];\n  deals.forEach(function(tr){\n    var td=tr.querySelector(\'td.sym\');\n    if(td)syms.push(td.textContent.replace(\'/USDT\',\'USDT\').trim());\n  });\n  if(syms.length===0){alert(\'Tidak ada deal aktif saat ini.\');return;}\n  var sym=syms.length===1?syms[0]:prompt(\'Pilih pair untuk Add Fund:\\n\'+syms.map(function(s){return s.replace(\'USDT\',\'/USDT\');}).join(\'\\n\')+\'\\n\\nKetik simbol:\');\n  if(!sym)return;\n  sym=sym.trim().toUpperCase();\n  if(!sym.endsWith(\'USDT\'))sym=sym+\'USDT\';\n  if(!confirm(\'ADD FUND untuk \'+sym.replace(\'USDT\',\'/USDT\')+\'?\\n\\nNominal sesuai sizing saat open deal.\\nAverage price akan diupdate otomatis.\'))return;\n  var st=document.getElementById(\'scan-status\');\n  if(st)st.textContent=\'Mengirim add fund \'+sym+\'...\';\n  pauseRefresh();\n  var amount=prompt(\'Nominal add fund (USDT):\',\'0\');\n  if(!amount||isNaN(parseFloat(amount))||parseFloat(amount)<=0){resumeRefresh();if(st)st.textContent=\'\';return;}\n  var fd=new FormData();fd.append(\'sym\',sym);fd.append(\'amount\',amount);\n  fetch(\'/manual_addfund\',{method:\'POST\',body:fd})\n    .then(function(r){return r.json();})\n    .then(function(data){\n      resumeRefresh();\n      if(data.ok){\n        var msg=\'Add Fund \'+sym.replace(\'USDT\',\'/USDT\')+\' BERHASIL! +$\'+data.add_usd+\' @ \'+data.price+\' | Avg=\'+data.avg_price;\n        if(st)st.textContent=msg;\n        alert(msg);\n        setTimeout(function(){window.location.reload();},2000);\n      } else {\n        if(st)st.textContent=\'Gagal: \'+data.error;\n        alert(\'Add Fund GAGAL: \'+data.error);\n      }\n    })\n    .catch(function(e){resumeRefresh();alert(\'Error: \'+e);});\n}\n'

def _fmt_price(v):
    """Format harga agar tidak pakai notasi scientific (e-06 dll)."""
    if v is None or v != v: return "-"
    if v >= 1000: return f"{v:.2f}"
    if v >= 1:    return f"{v:.4f}"
    if v >= 0.01: return f"{v:.6f}"
    if v >= 0.0001: return f"{v:.8f}"
    # harga sangat kecil (SHIB, FLOKI, dll)
    return f"{v:.10f}".rstrip('0').rstrip('.')

DASHBOARD_HTML = '''
<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<!-- auto-refresh dikendalikan via JS -->
<title>Bot Dashboard</title>
<style>
  :root{--bg:#0f1117;--surface:#1a1d2e;--border:#2a2d3e;--accent:#4f9eff;--green:#00c896;--red:#ff4f6a;--yellow:#ffb84f;--text:#e2e8f0;--muted:#8892a4;--font:'SF Mono','Fira Code',monospace}
  *{box-sizing:border-box;margin:0;padding:0}
    body{background:var(--bg);color:var(--text);font-family:var(--font);font-size:13px;overflow-x:hidden}
    .header{max-width:1200px;width:calc(100% - 40px);margin:0 auto;background:var(--surface);border:1px solid var(--border);border-top:none;border-radius:0 0 8px 8px;padding:12px 20px;display:flex;align-items:center;gap:16px}
  .header h1{font-size:15px;color:var(--accent);letter-spacing:.05em}
  .header .status{font-size:11px;color:var(--muted)}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;margin-right:6px;animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
    .container{max-width:1200px;margin:0 auto;padding:12px 20px}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:16px}
    .container > .card:last-child{margin-bottom:0}
    .container + .card{margin-top:0 !important}
  .card-header{padding:10px 16px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;cursor:pointer;user-select:none}
  .card-header:hover{background:rgba(255,255,255,0.03)}
  .card-toggle{font-size:12px;color:var(--muted);margin-left:8px;transition:transform 0.2s}
  .card.collapsed .card-body{display:none}
    .card.collapsed > :not(.card-header){display:none}
  .card.collapsed .card-toggle{transform:rotate(-90deg)}
  .card.collapsed .card-header{border-bottom:none}
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
    .strategy-tabs{display:flex;gap:6px;overflow-x:auto;margin-bottom:10px;padding-bottom:2px}
    .strategy-tab{flex:0 0 auto;background:transparent;color:var(--muted);border:1px solid var(--border);border-radius:4px;padding:7px 10px;font:inherit;font-size:11px;cursor:pointer;white-space:nowrap}
    .strategy-tab:hover{color:var(--text);border-color:var(--accent)}
    .strategy-tab.active{background:rgba(79,158,255,.14);color:var(--accent);border-color:var(--accent)}
    .strategy-panel{display:none}
    .strategy-panel.active{display:block}
  @media(max-width:768px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body onload="restoreCards()">
<div class="header">
  <span class="dot"></span>
  <h1>TRADING BOT DASHBOARD</h1>
    <span class="status">Refresh dalam <span id="cd">30</span>s &nbsp;|&nbsp; <label style="font-size:11px;cursor:pointer;display:inline-flex;align-items:center;gap:4px"><input type="checkbox" id="cb-pause-refresh" onchange="togglePauseRefresh(this.checked)" style="cursor:pointer"> Pause</label> &nbsp;|&nbsp; {{ now }}</span>
<script>
  var s=30;setInterval(function(){if(typeof _refreshTimer!=='undefined'&&_refreshTimer){s--;if(s<0)s=30;}document.getElementById('cd').textContent=s;},1000);
</script>
</div>
<div id="float-pause-widget" style="position:fixed;bottom:18px;right:18px;z-index:999;background:var(--card);border:1px solid var(--border);border-radius:20px;padding:7px 14px;font-size:11px;box-shadow:0 2px 10px rgba(0,0,0,.4)">
  <label style="cursor:pointer;display:flex;align-items:center;gap:6px;white-space:nowrap"><input type="checkbox" id="cb-pause-refresh-float" onchange="togglePauseRefresh(this.checked)" style="cursor:pointer"> Pause refresh</label>
</div>

<!-- ═══════════════ DASHBOARD TABS ═══════════════ -->
<div class="dash-tabs" style="display:flex;gap:6px;flex-wrap:wrap;position:sticky;top:0;z-index:50;background:var(--bg);border-bottom:1px solid var(--border);max-width:1200px;width:calc(100% - 40px);margin:0 auto;padding:10px 20px">
  <button type="button" class="dash-tab-btn" data-tab="strategies" onclick="selectDashTab('strategies')" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 14px;font-size:12px;cursor:pointer;font-family:var(--font)">STRATEGIES</button>
  <button type="button" class="dash-tab-btn" data-tab="monitor" onclick="selectDashTab('monitor')" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 14px;font-size:12px;cursor:pointer;font-family:var(--font)">MONITOR</button>
  <button type="button" class="dash-tab-btn" data-tab="history" onclick="selectDashTab('history')" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 14px;font-size:12px;cursor:pointer;font-family:var(--font)">HISTORY</button>
  <button type="button" class="dash-tab-btn" data-tab="control" onclick="selectDashTab('control')" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 14px;font-size:12px;cursor:pointer;font-family:var(--font)">CONTROL</button>
  <button type="button" class="dash-tab-btn" data-tab="tools" onclick="selectDashTab('tools')" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 14px;font-size:12px;cursor:pointer;font-family:var(--font)">TOOLS</button>
</div>
<script>
function selectDashTab(tab) {
    var starts = document.querySelectorAll('.dash-section-start');
    starts.forEach(function(startEl, idx) {
        var markerTab = startEl.getAttribute('data-tab');
        try {
            var show = markerTab === tab;
            var el = startEl;
            var guard = 0;
            while (el && guard < 3000) {
                guard++;
                if (el !== startEl && el.classList && el.classList.contains('dash-section-start')) break;
                el.style.display = show ? '' : 'none';
                el = el.nextElementSibling;
            }
        } catch (e) {}
    });
    document.querySelectorAll('.dash-tab-btn').forEach(function(btn) {
        var active = btn.getAttribute('data-tab') === tab;
        btn.style.background = active ? 'var(--accent)' : 'var(--surface)';
        btn.style.color = active ? '#000' : 'var(--text)';
    });
    try { _setCookie('dash_active_tab', tab); } catch (e) {}
}
document.addEventListener('DOMContentLoaded', function() {
    var saved = 'strategies';
    try { saved = _getCookie('dash_active_tab') || 'strategies'; } catch (e) {}
    selectDashTab(saved);
});
</script>

<!-- ═══════════════ STRATEGY CONTROL ═══════════════ -->
<div class="container dash-section-start" data-tab="control" style="margin-bottom:16px">
    <div class="card">
        <div class="card-header" onclick="toggleCard(this)"><h2>AI DECISION PROVIDER <span class="card-toggle">&#9660;</span></h2></div>
        <div class="card-body" style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;font-size:11px">
            <label style="display:flex;align-items:center;gap:6px;color:var(--muted)">
                <span>Mode:</span>
                <select id="ai-provider-mode" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 8px;font-size:11px">
                    <option value="anthropic_gemini">Anthropic → Gemini otomatis</option>
                    <option value="anthropic_only">Anthropic saja</option>
                    <option value="gemini_only">Gemini AI Studio saja</option>
                    <option value="rule_based">Rule-based Python saja</option>
                </select>
            </label>
            <button type="button" onclick="event.stopPropagation();saveAIProviderConfig()" style="background:var(--accent);color:#000;border:none;border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer;font-weight:600">SAVE</button>
            <span id="ai-provider-status" style="color:var(--muted)">Loading...</span>
        </div>
    </div>
  <div class="card">
    <div class="card-header" onclick="toggleCard(this)" style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
      <h2 style="margin:0;font-size:13px;letter-spacing:.08em;color:var(--accent)">STRATEGY CONTROL</h2>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <label style="display:flex;align-items:center;gap:6px;font-size:11px;color:var(--muted)">
          <span>Strategy:</span>
          <select id="sc-strategy-select" onclick="event.stopPropagation()" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 8px;font-size:11px;min-width:160px"></select>
        </label>
        <button onclick="event.stopPropagation();resetStrategyConfig()" style="background:rgba(255,100,100,.15);color:var(--red);border:1px solid var(--red);border-radius:4px;padding:4px 12px;font-size:11px;cursor:pointer">RESET</button>
      </div>
    </div>
    <div class="card-body">
    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:14px;padding-bottom:14px;border-bottom:1px solid var(--border);font-size:11px">
        <label style="display:flex;align-items:center;gap:6px;color:var(--muted)" title="Circuit breaker lintas semua strategi: dihitung terus-menerus dari P&amp;L hari ini (realized + unrealized deal aktif) vs modal total. Begitu tersulut, cuma blokir OPEN DEAL BARU (deal aktif tetap jalan normal) -- otomatis lepas lagi begitu P&amp;L balik naik.">
            <span>Batas Rugi Harian (%):</span>
            <input type="number" id="dll-pct" min="0" step="0.1" value="3" style="width:70px;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 8px;font-size:11px">
        </label>
        <button type="button" onclick="event.stopPropagation();saveDailyLossLimit()" style="background:var(--accent);color:#000;border:none;border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer;font-weight:600">SAVE</button>
        <span id="dll-status" style="color:var(--muted)">Memuat...</span>
    </div>
    <div style="overflow-x:auto;-webkit-overflow-scrolling:touch">
    <table style="width:100%;min-width:760px;border-collapse:collapse;font-size:11px" id="sc-table">
        <thead><tr style="color:var(--muted);border-bottom:1px solid var(--border)">
          <th style="text-align:left;padding:5px 8px">Strategi</th>
          <th style="text-align:center;padding:5px 8px" title="Jumlah maksimum deal aktif bersamaan untuk strategi ini">Max Deals</th>
          <th style="text-align:center;padding:5px 8px">Izinkan Open Long</th>
          <th style="text-align:center;padding:5px 8px">AI Call on Open</th>
          <th style="text-align:center;padding:5px 8px">Gunakan Setting Modal</th>
          <th style="text-align:center;padding:5px 8px">Base Order (USDT)</th>
          <th style="text-align:center;padding:5px 8px">Add Fund (USDT)</th>
          <th style="text-align:center;padding:5px 8px">Cooldown Re-entry</th>
                    <th style="text-align:center;padding:5px 8px">Action</th>
        </tr></thead>
                <tbody id="sc-body"><tr><td colspan="9" style="color:var(--muted);padding:8px">Loading...</td></tr></tbody>
    </table>
    </div>
    </div>
  </div>
</div>
<!-- SC JS loaded via dash.js -->

<div class="container dash-section-start" data-tab="monitor">
  <div class="card">
        <div class="card-header" onclick="toggleCard(this)">
            <h2>Active Deals ({{ active_count }}) <span class="card-toggle">&#9660;</span></h2>
            {% if closest_to_close %}
            <span class="scan-time" title="Estimasi heuristik (timeout/trailing/hard-stop), BUKAN prediksi harga pasti">Estimasi closing tercepat: <b>{{ closest_to_close.sym.replace("USDT","/USDT") }}</b> <span style="color:var(--muted)">({{ closest_to_close.note }})</span></span>
            {% endif %}
            <span class="scan-time">Sisa USDT (blm terpakai): <b style="color:var(--accent)">${{ "%.2f"|format(free_usdt) }}</b></span>
    </div>
    <div class="card-body">
    {% if active_deals %}
    <div style="overflow-x:auto;-webkit-overflow-scrolling:touch">
        <table style="min-width:1280px">
            <thead><tr><th>Pair</th><th>Strategi</th><th>Opened</th><th>Chart</th><th>Entry / Average</th><th>U/PnL ($)<br><span style="font-size:9px;font-weight:normal;color:var(--muted)">modal terpakai</span></th><th>Profit<br><span style="font-size:9px;font-weight:normal;color:var(--muted)">net -0.2% fee</span></th><th>Cancel<br><span style="font-size:9px;font-weight:normal;color:var(--muted)">stop track, koin tetap</span></th><th>Auto TP<br><span style="font-size:9px;font-weight:normal;color:var(--muted)">close jika target tercapai ($/%/harga)</span></th><th>TP Target</th><th>Harga Skrg<br><span style="font-size:9px;font-weight:normal;color:var(--muted)">estd qty koin</span></th><th>isArmed</th><th>Arm Trailing</th><th>Auto Avg Down</th><th>Auto Close</th><th>AI Call</th><th>Report</th></tr></thead>
      <tbody id="active-deals-body">
      {% for sym, d in active_deals.items() %}
      <tr>
        <td class="sym">{{ sym.replace("USDT","/USDT") }}</td>
        <td>{% set _sm = {"brkX2":"brkX2-12h","brkX2_4h":"brkX2-4h","brkX2_crossema":"CrossEMA-4h","reversal":"Reversal-8h","trend_confirm_4h":"TrenKonfirmasi-4h"} %}{{ _sm.get(d.get("strategy",""),d.get("strategy","-")) }}</td>
        <td style="font-size:10px;color:var(--muted);white-space:nowrap">{{ d.get("opened_at","")[:16] if d.get("opened_at") else "-" }}</td>
        <td><button type="button" onclick="openTradingViewChart('{{ sym }}','{{ _sm.get(d.get('strategy',''),d.get('strategy','-')) }}')" style="background:#2962ff;color:#fff;border:none;border-radius:4px;padding:4px 8px;font-size:10px;cursor:pointer;font-family:var(--font);white-space:nowrap">Open Chart</button></td>
        <td>
          <div style="font-size:9px;color:var(--muted);margin-bottom:2px">{{ "Average" if d.get("add_fund_sent") else "Entry" }}</div>
          <span id="ep-{{ sym }}" style="cursor:pointer;text-decoration:underline dotted" title="Klik untuk edit" onclick="editEntry('{{ sym }}','{{ fmt_price(d.get(\"entry_price\",0)) }}')">{{ fmt_price(d.get("entry_price",0)) }}</span>
        </td>
        <td class="{{ "profit-pos" if d.get("upnl_usd",0) > 0 else "profit-neg" }}" style="white-space:nowrap">
          <div>{{ "%+.2f"|format(d.get("upnl_usd",0)) }}</div>
          <div style="font-size:9px;color:var(--muted)">${{ "%.0f"|format(d.get("total_usd_display",0)) }}</div>
        </td>
        <td class="{{ "profit-pos" if d.get("upnl_pct",0) > 0 else "profit-neg" }}">{{ "%+.2f"|format(d.get("upnl_pct",0)) }}%</td>
        <td>
          <form method="POST" action="/cancel_deal" style="display:inline" onsubmit="return confirm('Cancel deal {{ sym.replace(\"USDT\",\"/USDT\") }}?\n\nBot berhenti kelola pair ini (auto add fund/TP/close berhenti). Koin yang sudah dibeli TETAP di wallet, TIDAK dijual.');">
            <input type="hidden" name="sym" value="{{ sym }}">
            <button type="submit" style="background:#ef4444;color:#fff;border:none;border-radius:4px;padding:4px 8px;font-size:10px;cursor:pointer;font-family:var(--font);white-space:nowrap">Cancel</button>
          </form>
          {% set _hns_active = d.get("hold_no_sell_active") %}
          <form method="POST" action="/toggle" style="display:inline-block;margin-left:4px;{{ '' if _hns_active else 'opacity:0.4' }}" title="{{ 'Deal ini sudah dekat hard-stop/timeout-rugi -- checkbox aktif.' if _hns_active else 'Belum dekat hard-stop atau timeout-rugi -- checkbox terkunci, otomatis aktif begitu mendekat.' }} Tercentang = kalau hard-stop ATAU timeout (batas candle) kesulut sementara deal lagi rugi, koin di-HOLD (tidak dijual), catat sebagai loss, cooldown 96j. Timeout yg closing untung tetap dijual normal. Uncheck = tetap dijual seperti biasa.">
            <input type="hidden" name="sym" value="{{ sym }}">
            <input type="hidden" name="key" value="hold_no_sell">
            <input type="checkbox" name="value" onchange="this.form.submit()" {{ "checked" if overrides.get(sym,{}).get("hold_no_sell",True) else "" }} {{ "" if _hns_active else "disabled" }} style="width:16px;height:16px;cursor:{{ 'pointer' if _hns_active else 'not-allowed' }}">
            <div style="font-size:8px;color:var(--muted);white-space:nowrap">Hold, jgn jual</div>
          </form>
          {% if d.get("needs_reconcile") %}
          <form method="POST" action="/reconcile_deal" style="display:inline;margin-left:4px" title="Terdeteksi: saldo wallet asset ini jauh di bawah qty yang seharusnya -- koin kemungkinan sudah terjual di luar jalur normal bot (mis. lewat webhook TradingView). Klik utk catat ke Closed Trades pakai harga sekarang & hapus dari list." onsubmit="return confirm('Reconcile deal {{ sym.replace(\"USDT\",\"/USDT\") }}?\n\nTerdeteksi saldo wallet-nya jauh di bawah qty yang seharusnya -- koin kemungkinan sudah terjual di luar bot. Akan dicatat ke Closed Trades pakai harga SEKARANG sbg estimasi exit, lalu dihapus dari Active Deals.\n\nKalau ternyata koin belum dijual, batalkan ini dan pakai Cancel.');">
            <input type="hidden" name="sym" value="{{ sym }}">
            <button type="submit" style="background:#78716c;color:#fff;border:none;border-radius:4px;padding:4px 8px;font-size:10px;cursor:pointer;font-family:var(--font);white-space:nowrap">Reconcile</button>
          </form>
          {% endif %}
        </td>
        <td>
          <form method="POST" action="/toggle" style="display:inline">
            <input type="hidden" name="sym" value="{{ sym }}">
            <input type="hidden" name="key" value="tp1usd">
            <input type="checkbox" name="value" onchange="this.form.submit()" {{ "checked" if overrides.get(sym,{}).get("tp1usd",False) else "" }}>
          </form>
        </td>
        <td>
          {% set _tpmode = overrides.get(sym,{}).get("tp1_target_mode","usd") %}
          <form method="POST" action="/set_tp_target" style="display:inline-flex;gap:4px;align-items:center;flex-wrap:wrap">
            <input type="hidden" name="sym" value="{{ sym }}">
            <select name="mode" onchange="var i=this.closest('form').querySelector('input[name=value]'); i.value = this.value==='pct' ? i.dataset.pct : (this.value==='price' ? i.dataset.price : i.dataset.usd);" style="background:#0f1117;color:#e2e8f0;border:1px solid var(--border);border-radius:4px;padding:3px 3px;font-size:10px;font-family:var(--font)">
              <option value="usd" {{ "selected" if _tpmode=="usd" else "" }}>$</option>
              <option value="pct" {{ "selected" if _tpmode=="pct" else "" }}>%</option>
              <option value="price" {{ "selected" if _tpmode=="price" else "" }}>Harga</option>
            </select>
            <input type="text" inputmode="decimal" name="value" data-usd="{{ overrides.get(sym,{}).get("tp1_target_usd",1.0) }}" data-pct="{{ overrides.get(sym,{}).get("tp1_target_pct",1.0) }}" data-price="{{ overrides.get(sym,{}).get("tp1_target_price", d.get("entry_price",0)) }}" oninput="this.value = this.value.replace(/,/g,'.').replace(/[^0-9.]/g,'').replace(/(\\.[^.]*)\\./g, '$1');" style="width:74px;background:#0f1117;color:#e2e8f0;border:1px solid var(--border);border-radius:4px;padding:3px 5px;font-size:11px;font-family:var(--font)" value="{{ overrides.get(sym,{}).get('tp1_target_price', d.get('entry_price',0)) if _tpmode=='price' else (overrides.get(sym,{}).get('tp1_target_pct',1.0) if _tpmode=='pct' else overrides.get(sym,{}).get('tp1_target_usd',1.0)) }}">
            <button type="submit" style="background:var(--accent);color:#000;border:none;border-radius:4px;padding:3px 8px;font-size:10px;cursor:pointer;font-family:var(--font);white-space:nowrap">Save</button>
          </form>
        </td>
        <td style="white-space:nowrap">
          <div>{{ fmt_price(d.get("last_price",0)) if d.get("last_price") else "-" }}</div>
          {% if d.get("entry_price",0) > 0 and d.get("total_usd_display",0) > 0 %}
          <div style="font-size:9px;color:var(--muted)">
            estd {{ "%.2f"|format(d.get("total_usd_display",0) / d.get("entry_price",1)) }} {{ sym.replace("USDT","") }}
          </div>
          {% endif %}
        </td>
        <td>{% if d.get("strategy","") in ("akum_entry_a","akum_entry_b") %}<span class="badge" style="background:#444;color:#888">N/A</span>{% elif d.get("trailing_armed") %}<span class="badge badge-armed">Yes</span>{% else %}<span class="badge badge-wait">Wait</span>{% endif %}</td>
        <td>
          {% if d.get("strategy","") in ("akum_entry_a","akum_entry_b") %}
          <span class="badge" style="background:#444;color:#888">N/A</span>
          {% else %}
          <form method="POST" action="/toggle" style="display:inline" title="Uncheck = trailing TIDAK akan pernah di-arm buat deal ini, walau profit sudah lewat ambang arm. Deal tetap dipantau normal (hard-stop/manual close tetap jalan), cuma nggak ada auto-lock profit via trailing.">
            <input type="hidden" name="sym" value="{{ sym }}">
            <input type="hidden" name="key" value="arm_trailing_enabled">
            <input type="checkbox" name="value" onchange="this.form.submit()" {{ "checked" if overrides.get(sym,{}).get("arm_trailing_enabled",True) else "" }} style="width:16px;height:16px;cursor:pointer">
          </form>
          {% endif %}
        </td>
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
        <td>
          <form method="POST" action="/toggle" style="display:inline">
            <input type="hidden" name="sym" value="{{ sym }}">
            <input type="hidden" name="key" value="ai_call">
            <input type="checkbox" name="value" onchange="this.form.submit()" {{ "checked" if overrides.get(sym,{}).get("ai_call",True) else "" }}>
          </form>
        </td>
                <td><button type="button" onclick="resendOpenNotification('{{ sym }}')" style="background:var(--accent);color:#000;border:none;border-radius:4px;padding:3px 7px;font-size:10px;cursor:pointer">Resend OPEN</button></td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
        </div>
    {% else %}
    <div class="empty">Tidak ada deal aktif saat ini</div>
    {% endif %}
    </div>
  </div>
</div>
<div class="container dash-section-start" data-tab="monitor" style="margin-top:12px">
    <div class="card">
        <div class="card-header" onclick="toggleCard(this)"><h2>AUTO SELL ASSET <span class="card-toggle">&#9660;</span></h2></div>
        <div class="card-body" style="font-size:11px">
            <div style="color:var(--muted);margin-bottom:8px">Tiap asset dipantau &amp; dieksekusi independen — begitu 1 asset crossing naik lewat target, cuma asset itu yang dijual (95% saldo bebas) &amp; nonaktif; asset lain di daftar tetap jalan.</div>
            <div style="overflow-x:auto">
            <table style="width:100%;border-collapse:collapse">
                <thead><tr style="text-align:left;color:var(--muted);border-bottom:1px solid var(--border)">
                    <th style="padding:5px 6px">Asset</th>
                    <th style="padding:5px 6px">Avg beli (USDT/coin)</th>
                    <th style="padding:5px 6px">Harga sekarang</th>
                    <th style="padding:5px 6px">vs Avg</th>
                    <th style="padding:5px 6px">Target jual (USDT)</th>
                    <th style="padding:5px 6px">Jarak ke target</th>
                    <th style="padding:5px 6px">Status</th>
                    <th style="padding:5px 6px">Aktif</th>
                    <th style="padding:5px 6px" title="Sisa ~5% yg nggak ikut terjual otomatis dikonversi jadi BNB (buat diskon fee trading 25%)">Sisa→BNB</th>
                    <th style="padding:5px 6px"></th>
                </tr></thead>
                <tbody id="auto-sell-tbody"><tr><td colspan="10" style="padding:8px;color:var(--muted)">Memuat...</td></tr></tbody>
            </table>
            </div>
            <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px;border-top:1px solid var(--border);padding-top:10px">
                <label>+ Tambah asset <select id="auto-sell-new-asset" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 6px"><option>Memuat...</option></select></label>
                <label>Avg beli, USDT per coin (opsional) <input id="auto-sell-new-avg" type="number" min="0" step="0.00000001" placeholder="kalau tahu" style="width:110px;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 6px"><div id="auto-sell-new-avgsrc" style="color:var(--muted);font-size:9px"></div></label>
                <label>Harga trigger jual (USDT) <input id="auto-sell-new-threshold" type="number" min="0" step="0.00000001" value="0" style="width:110px;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 6px"></label>
                <label title="Sisa ~5% yg nggak ikut terjual otomatis dikonversi jadi BNB"><input id="auto-sell-new-bnb" type="checkbox" checked> Convert sisa ke BNB</label>
                <button type="button" onclick="addAutoSellAsset()" style="background:var(--red);color:#fff;border:none;border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer;font-weight:600">+ TAMBAH</button>
                <button type="button" onclick="loadAutoSellConfig()" style="background:var(--accent);color:#000;border:none;border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer">Refresh saldo</button>
            </div>
        </div>
    </div>
</div>
<!-- ═══ Convert Aset (04/09/2026): jual asset stagnan/rugi lalu beli koin lain yang
     jarak-ke-resistance-nya >= magnitude kerugian DAN ada momentum riil. Modal ini
     SENGAJA ditaruh DI LUAR .card manapun (04/09/2026 fix) -- .card punya
     overflow:hidden, yang MEMOTONG rendering position:fixed di dalamnya (dibuktikan
     lewat screenshot Mas Budi: modal kepotong kecil, numpuk sama isi card lain,
     bukan menutup layar penuh spt seharusnya). Diletakkan sejajar/sibling dgn
     .container, bukan nested di dalamnya, supaya position:fixed;inset:0 beneran full-screen. -->
<div id="convert-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:999;align-items:center;justify-content:center">
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 20px;max-width:640px;width:92%;max-height:80vh;overflow-y:auto">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
            <h3 id="convert-modal-title" style="margin:0;font-size:13px;color:var(--accent)">Convert Aset</h3>
            <button type="button" onclick="closeConvertModal()" style="background:none;border:none;color:var(--muted);font-size:16px;cursor:pointer">✕</button>
        </div>
        <div id="convert-modal-body" style="font-size:11px;color:var(--muted)">Memuat...</div>
    </div>
</div>
  <!-- ═══════════════ MANUAL SCAN ═══════════════ -->
<div class="container dash-section-start" data-tab="tools">
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
          <option value="Akumulasi-4h">Akumulasi-4h ⭐</option>
          <option value="Hunting-4h">Hunting-4h 🎯</option>
        </select>
      </div>
      <!-- Dropdown kandidat dinamis per strategi -->
      {% set all_nm = {
        "brkX2-12h": near_miss.get("brkX2-12h", []),
        "Reversal-8h T1": near_miss.get("Reversal-8h", []),
        "Reversal-8h T3-REV": near_miss.get("Reversal-8h", []),
        "brkX2-4h": near_miss.get("brkX2-4h", []),
        "CrossEMA-4h": near_miss.get("CrossEMA-4h", []),
        "Akumulasi-4h": near_miss.get("Akumulasi-4h", []),
        "Hunting-4h": near_miss.get("Hunting-4h", [])
      } %}
      <div style="margin-bottom:12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <label style="font-size:11px;color:var(--muted)">Pilih item hasil scan:</label>
        <select id="pair-select" onchange="onPairSelect(this.value)" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:5px 10px;font-size:12px;font-family:var(--font);cursor:pointer;min-width:160px">
          <option value="">-- pilih pair --</option>
          {% for stk, kandidat in all_nm.items() %}{% for item in kandidat %}
          <option value="{{ item.sym }}" class="nm-opt" data-strat="{{ stk }}" style="display:none">{{ item.sym.replace("USDT","/USDT") }} ({{ item.n_pass }}/{{ item.total }})</option>
          {% endfor %}{% endfor %}
        </select>
        <span id="nm-count" style="font-size:10px;color:var(--muted)"></span>
                <span style="font-size:10px;color:var(--muted)">(daftar ringkas yang ditampilkan)</span>
                <button type="button" id="btn-open-chart" onclick="openSelectedTradingViewChart()" disabled style="background:#2962ff;color:#fff;border:none;border-radius:4px;padding:6px 12px;font-size:11px;cursor:pointer;font-family:var(--font);opacity:.55">Open Chart</button>
      </div>
            <script>
            function openTradingViewChart(pair, strategy) {
                if (!pair) { return; }
                strategy = strategy || '';
                var interval = strategy.indexOf('12h') >= 0 ? '720' : strategy.indexOf('8h') >= 0 ? '480' : '240';
                window.open('https://www.tradingview.com/chart/?symbol=BINANCE%3A' + encodeURIComponent(pair) + '&interval=' + interval, '_blank', 'noopener');
            }
            function openSelectedTradingViewChart() {
                var pair = document.getElementById('pair-select').value;
                if (!pair) { return; }
                var strategyElement = document.getElementById('strat-select');
                var strategy = strategyElement ? strategyElement.value : '';
                openTradingViewChart(pair, strategy);
            }
            document.getElementById('pair-select').addEventListener('change', function() {
                var button = document.getElementById('btn-open-chart');
                button.disabled = !this.value;
                button.style.opacity = this.value ? '1' : '.55';
            });
            </script>
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
        <button id="btn-addfund" onclick="promptAddFund()" style="background:#ff9f43;color:#000;border:none;border-radius:4px;padding:7px 18px;font-size:12px;cursor:pointer;font-family:var(--font)">Add Fund</button>
        <button id="btn-close-deal" onclick="promptCloseDeal()" style="background:var(--red);color:#fff;border:none;border-radius:4px;padding:7px 18px;font-size:12px;cursor:pointer;font-family:var(--font)">Close Deal</button>
        <span id="scan-status" style="font-size:11px;color:var(--muted)"></span>

      </div>
      <!-- Scan results table -->
      <div id="scan-results"></div>
    </div>
  </div>
</div>

  <!-- ═══════════════ AKUMULASI DETECTOR ═══════════════ -->
<div class="container dash-section-start" data-tab="strategies">
  <div class="section-title">⭐ Strategi #5 — Akumulasi Detector (4h)</div>
  <div class="card" style="margin-bottom:16px">
    <div class="card-header" onclick="toggleCard(this)">
      <h2>Akumulasi-4h <span class="card-toggle">&#9660;</span>&nbsp;<span style="font-size:10px;color:var(--muted);text-transform:none;font-weight:400">Fase sideways post-downtrend | TF 4h | Maks 5 pair</span></h2>
      <span class="scan-time">Scan: {{ last_scan.get("Akumulasi-4h","-") }}</span>
    </div>
    <div class="card-body">
    {% set akum_items = near_miss.get("Akumulasi-4h", []) %}
    {% if akum_items %}
        <div style="margin-bottom:8px;color:var(--muted);font-size:10px">
            Jendela sideways dihitung dari 180 candle 4h terakhir (sekitar 30 hari).
        </div>
    <!-- Legend indikator -->
    <div style="margin-bottom:10px;display:flex;flex-wrap:wrap;gap:10px;font-size:10px">
      <span style="color:var(--muted)">GATING (wajib): </span>
      <span style="color:var(--accent)">P3 OBV↑ (25)</span>
      <span style="color:var(--accent)">P4 ATR↓≥25% (20)</span>
    <span style="color:var(--muted);margin-left:8px">BOBOT: </span>
      <span style="color:var(--accent)">P1 Range≤18% (15)</span>
      <span style="color:var(--yellow)">S1 Vol G>R (15)</span>
      <span style="color:var(--accent)">P2 EMAGap≤6% (10)</span>
      <span style="color:var(--yellow)">S2 RSI 30-56 (5)</span>
      <span style="color:var(--yellow)">S3 MACD flat (5)</span>
      <span style="color:var(--yellow)">S4 Body ratio kecil (5)</span>
      <span style="color:#f85149;margin-left:8px">P5 Slope EMA20≤4% (filter)</span>
      <span style="color:#f85149">P6 Drift close≤7% (filter)</span>
      <span style="color:#f85149">P7 DistRatio≤2.5x (filter)</span>
    </div>
        <div style="margin-bottom:10px;color:var(--muted);font-size:10px">
            Level yang ditampilkan: Extreme line (wick min/max 180 candle) + Structural zone (cluster test berulang ala Wyckoff).
        </div>
    <div style="overflow-x:auto;-webkit-overflow-scrolling:touch">
        <table style="width:100%;min-width:1780px;border-collapse:collapse">
      <thead>
        <tr>
          <th>Pair</th>
          <th>Skor /100</th>
          <th>Gating</th>
          <th>Primary</th>
          <th>Secondary</th>
          <th>Belum lolos</th>
          <th>Sideways sejak</th>
          <th>Current price</th>
          <th>Posisi</th>
          <th>% vs Support</th>
          <th>% vs Resist</th>
                    <th>Support (extreme)</th>
                    <th>Resistance (extreme)</th>
                    <th>Support Zone (struct)</th>
                    <th>Resistance Zone (struct)</th>
                    <th>Touch S/R zone</th>
          <th style="color:#f85149">SL-A est.</th>
          <th style="color:#f85149">SL-B est.</th>
        </tr>
      </thead>
      <tbody>
      {% for item in akum_items %}
      <tr>
        <td class="sym" style="white-space:nowrap">{{ item.sym.replace("USDT","/USDT") }}</td>
        <td style="font-weight:600;text-align:center">
          {% set ws = item.get("weighted_score", 0) %}
          <span style="color:{% if ws >= 70 %}var(--green){% elif ws >= 45 %}var(--yellow){% else %}var(--muted){% endif %}">
            {{ ws }}
          </span>
        </td>
        <td style="text-align:center;font-size:11px">
          {% if item.get("gating_ok") %}
            <span style="color:var(--green)">✓</span>
          {% else %}
            <span style="color:var(--red)">✗</span>
          {% endif %}
        </td>
        <td style="font-size:10px">
          <span style="color:{% if item.n_pass==4 %}var(--green){% elif item.n_pass==3 %}var(--yellow){% else %}var(--muted){% endif %}">
            {{ item.n_pass }}/4
          </span>
        </td>
        <td style="font-size:10px;color:var(--muted)">≥2 req</td>
        <td class="fails" style="font-size:10px;line-height:1.5;max-width:200px">
          {% if item.fails %}
            {{ ("; ".join(item.fails[:3]))|e }}{% if item.fails|length > 3 %} +{{ item.fails|length - 3 }} lagi{% endif %}
          {% else %}
            <span style="color:var(--green)">✓ Semua lolos</span>
          {% endif %}
        </td>
        <td style="font-size:10px;color:var(--muted);white-space:nowrap">
          {{ item.get("sideways_start", "-") if item.get("sideways_start") else "-" }}
        </td>
        <td style="font-size:10px;color:var(--text);white-space:nowrap;font-weight:600">{{ item.get("close_fmt", "-") }}</td>
        <td style="font-size:10px;white-space:nowrap;color:{% if item.get('posisi')=='Dekat Support' %}var(--green){% else %}var(--yellow){% endif %}">
          {{ item.get("posisi", "-") }}
        </td>
        <td style="font-size:10px;white-space:nowrap;color:{% if item.get('pct_vs_support') is not none and item.get('pct_vs_support') >= 0 %}var(--green){% else %}var(--red){% endif %}">
          {% if item.get('pct_vs_support') is not none %}{{ '%+.2f'|format(item.get('pct_vs_support')) }}%{% else %}-{% endif %}
        </td>
        <td style="font-size:10px;white-space:nowrap;color:{% if item.get('pct_vs_resist') is not none and item.get('pct_vs_resist') >= 0 %}var(--green){% else %}var(--red){% endif %}">
          {% if item.get('pct_vs_resist') is not none %}{{ '%+.2f'|format(item.get('pct_vs_resist')) }}%{% else %}-{% endif %}
        </td>
        <td style="font-size:10px;color:var(--green);white-space:nowrap">{{ item.get("support_fmt", "-") }}</td>
        <td style="font-size:10px;color:var(--yellow);white-space:nowrap">{{ item.get("resistance_fmt", "-") }}</td>
                <td style="font-size:10px;color:var(--green);white-space:nowrap">{{ item.get("support_zone_fmt", "-") }}</td>
                <td style="font-size:10px;color:var(--yellow);white-space:nowrap">{{ item.get("resistance_zone_fmt", "-") }}</td>
                <td style="font-size:10px;color:var(--muted);white-space:nowrap">{{ item.get("support_zone_touches", 0) }}/{{ item.get("resistance_zone_touches", 0) }}</td>
        <td style="font-size:10px;color:#f85149;white-space:nowrap">{{ item.get("sl_a_fmt", "-") }}</td>
        <td style="font-size:10px;color:#f85149;white-space:nowrap">{{ item.get("sl_b_fmt", "-") }}</td>
      </tr>
      {% endfor %}
      </tbody>
    </table>
    </div>
    <div style="margin-top:10px;font-size:10px;color:var(--muted)">
      Scan otomatis tiap 30 menit. Gating wajib: OBV↑ + ATR↓≥25%. Urut skor tertinggi. Skor maks = 100.
    </div>

    <!-- ── ENTRY A / B STATUS ── -->
    <div style="margin-top:16px;border-top:1px solid var(--border);padding-top:12px">
      <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px">
        Entry Status (scan tiap 15 menit)
        <span style="color:var(--accent);margin-left:8px">A = Spring/Fakeout</span>
        <span style="color:var(--yellow);margin-left:8px">B = Breakout+Retest</span>
      </div>
    <div style="overflow-x:auto;-webkit-overflow-scrolling:touch">
    <table style="width:100%;min-width:520px;border-collapse:collapse">
        <thead>
          <tr>
            <th>Pair</th>
            <th>Entry A (Spring)</th>
            <th>Entry B (Breakout)</th>
            <th title="Progress terjauh antara Entry A dan Entry B, 1 bintang = 20%">Rating</th>
            <th>Update</th>
          </tr>
        </thead>
        <tbody>
        {% for item in akum_items %}
        {% set es = akum_entry_status.get(item.sym, {}) %}
        <tr>
          <td class="sym" style="white-space:nowrap">{{ item.sym.replace("USDT","/USDT") }}</td>
          <td style="text-align:left;max-width:260px">
            {% if not es %}
              <span style="color:var(--muted);font-size:10px">menunggu scan</span>
            {% elif es.entry_a %}
              <span style="color:var(--green);font-weight:600">✓ SIAP</span>
            {% else %}
              <span style="color:var(--red)">✗</span> <span style="color:var(--muted);font-size:10px">{{ es.get("reason_a", "belum") }}</span>
            {% endif %}
          </td>
          <td style="text-align:left;max-width:260px">
            {% if not es %}
              <span style="color:var(--muted);font-size:10px">menunggu scan</span>
            {% elif es.entry_b %}
              <span style="color:var(--green);font-weight:600">✓ SIAP</span>
            {% else %}
              <span style="color:var(--red)">✗</span> <span style="color:var(--muted);font-size:10px">{{ es.get("reason_b", "belum") }}</span>
            {% endif %}
          </td>
          <td style="text-align:center;white-space:nowrap">
            {% if es %}
              {% set stars_filled = (es.get("rating_pct",0) / 20) | round | int %}
              <span style="color:var(--accent);letter-spacing:1px" title="{{ es.get('rating_pct',0) }}%">{{ '★' * stars_filled }}{{ '☆' * (5 - stars_filled) }}</span>
            {% else %}
              <span style="color:var(--muted);font-size:10px">-</span>
            {% endif %}
          </td>
          <td style="font-size:10px;color:var(--muted);white-space:nowrap">{{ es.get("ts", "-") if es else "-" }}</td>
        </tr>
        {% endfor %}
        </tbody>
    </table>
    </div>
    </div>

    {% else %}
    <div class="empty">{{ window_info.get("Akumulasi-4h", "Belum ada data scan (maks 30 menit setelah bot start).")|e }}</div>
    {% endif %}
    </div>
  </div>
</div>

<div class="container dash-section-start" data-tab="strategies">
  <div class="section-title">Kandidat Terdekat per Strategi</div>
    <div class="strategy-tabs" role="tablist" aria-label="Kandidat strategi">
    {% set strategy_tab_index = namespace(value=0) %}
    {% for strategi, items in near_miss.items() %}{% if strategi != "Akumulasi-4h" %}
    <button type="button" class="strategy-tab{% if strategy_tab_index.value == 0 %} active{% endif %}" role="tab" aria-selected="{{ 'true' if strategy_tab_index.value == 0 else 'false' }}" data-strategy-tab="strategy-panel-{{ strategy_tab_index.value }}" onclick="selectStrategyTab(this)">{{ strategi }}</button>
    {% set strategy_tab_index.value = strategy_tab_index.value + 1 %}
    {% endif %}{% endfor %}
    </div>
    <div>
    {% set strategy_panel_index = namespace(value=0) %}
    {% for strategi, items in near_miss.items() %}{% if strategi != "Akumulasi-4h" %}
    <div id="strategy-panel-{{ strategy_panel_index.value }}" class="strategy-panel{% if strategy_panel_index.value == 0 %} active{% endif %}" role="tabpanel">
    <div class="card">
    <div class="card-header" onclick="toggleCard(this)">
      <h2>{{ strategi }} <span class="card-toggle">&#9660;</span></h2>
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
    </div>
    {% set strategy_panel_index.value = strategy_panel_index.value + 1 %}
    {% endif %}{% endfor %}
  </div>
    <script>
    function selectStrategyTab(tab) {
        document.querySelectorAll('.strategy-tab').forEach(function(button) {
            var active = button === tab;
            button.classList.toggle('active', active);
            button.setAttribute('aria-selected', active ? 'true' : 'false');
        });
        document.querySelectorAll('.strategy-panel').forEach(function(panel) {
            panel.classList.toggle('active', panel.id === tab.getAttribute('data-strategy-tab'));
        });
    }
    </script>
</div>


  <!-- ═══════════════ HUNTING-4H ═══════════════ -->
<div class="container dash-section-start" data-tab="strategies">
  <div class="section-title">🎯 Strategi #7 — Hunting (4h)</div>
  <div class="card" style="margin-bottom:16px">
    <div class="card-header" onclick="toggleCard(this)">
      <h2>Hunting-4h <span class="card-toggle">&#9660;</span>&nbsp;<span style="font-size:10px;color:var(--muted);text-transform:none;font-weight:400">EMA kompresi tipis, harga baru breakout | TF 4h</span></h2>
      <span class="scan-time" id="hunting-scan-time">Scan: —</span>
    </div>
    <div class="card-body">
      <div style="display:flex;flex-wrap:wrap;gap:12px;font-size:11px;margin-bottom:10px">
        <label title="EMA20 harus di bawah EMA50, jarak 0-1%">
          <input type="checkbox" id="chk_hunting_ema_gap" checked onchange="updateHuntingConfig()">
          EMA20&lt;EMA50 gap&le;1%
        </label>
        <label title="Price change 0%-5% vs candle sebelumnya">
          <input type="checkbox" id="chk_hunting_price_change" checked onchange="updateHuntingConfig()">
          Price Change 0–5%
        </label>
        <label title="Harga di atas EMA50, jarak 0-3%">
          <input type="checkbox" id="chk_hunting_above_ema50" checked onchange="updateHuntingConfig()">
          Price&gt;EMA50 (0–3%)
        </label>
        <label title="Candlestick bullish: Hammer, Strong Bull, Bullish Engulfing, atau Doji Bullish">
          <input type="checkbox" id="chk_hunting_uptrend" checked onchange="updateHuntingConfig()">
          Bullish Pattern
        </label>
      </div>
      <div id="hunting-signals"><em style="color:var(--muted);font-size:11px">Belum ada sinyal.</em></div>
    </div>
  </div>

<script>
function updateHuntingConfig() {
  fetch("/api/hunting_config", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({
      hunting_ema_gap:      document.getElementById("chk_hunting_ema_gap").checked,
      hunting_price_change: document.getElementById("chk_hunting_price_change").checked,
      hunting_above_ema50:  document.getElementById("chk_hunting_above_ema50").checked,
      hunting_uptrend:      document.getElementById("chk_hunting_uptrend").checked,
    })
  });
}
function refreshHuntingSignals() {
  fetch("/api/hunting_signals")
    .then(function(r){ return r.json(); })
    .then(function(data) {
      var el = document.getElementById("hunting-signals");
      var ts = document.getElementById("hunting-scan-time");
      if(ts) ts.textContent = "Scan: " + (data.scan_ts || "—");
      if (!data.signals || !data.signals.length) {
        el.innerHTML = "<em style='color:var(--muted);font-size:11px'>Belum ada sinyal.</em>";
        return;
      }
      el.innerHTML = data.signals.map(function(s){
        return '<div style="font-size:11px;padding:4px 0;border-bottom:1px solid var(--border)">'
          + '<b>' + s.symbol + '</b>'
          + ' &nbsp;close=<b>' + s.close + '</b>'
          + ' &nbsp;&Delta;ema20=<b style="color:#3fb950">' + s.dist_ema20_pct + '%</b>'
          + ' &nbsp;&Delta;ema50=<b>' + (s.dist_ema50_pct !== null ? s.dist_ema50_pct : '—') + '%</b>'
          + ' &nbsp;gap=<b>' + (s.ema_gap_pct !== null ? s.ema_gap_pct : '—') + '%</b>'
          + ' &nbsp;chg=<b>' + s.price_change_pct + '%</b>'
          + ' &nbsp;' + (s.uptrend ? '↑<span style="color:#3fb950">up</span>' : '↓')
          + '</div>';
      }).join("");
    })
    .catch(function(){});
}
setInterval(refreshHuntingSignals, 30000);
refreshHuntingSignals();
</script>
</div>

<script src="/dash.js?v=1786097338"></script>
<script>
// Inject Hunting-4h ke STRAT_SECONDARY setelah dash.js selesai load.
// Guard ini penting supaya modal tidak crash jika script dipanggil sebelum DOM siap.
var SC_LABELS = {
    brkX2: 'brkX2-12h',
    reversal: 'Reversal-8h',
    brkX2_4h: 'brkX2-4h',
    brkX2_crossema: 'CrossEMA-4h',
    akum_entry_a: 'Akumulasi-4h',
    hunting_4h: 'Hunting-4h',
    trend_confirm_4h: 'TrenKonfirmasi-4h'
};
var SC_HAS_ADDFUND = {brkX2: true, brkX2_4h: true, trend_confirm_4h: true};
var SC_ADDFUND_LABEL = {brkX2: 'auto (score-based)'};
var _scData = {};

function buildStrategySelect() {
    var keys = Object.keys(SC_LABELS);
    var select = document.getElementById('sc-strategy-select');
    if (!select) return;
    select.innerHTML = '';
    for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        var opt = document.createElement('option');
        opt.value = k;
        opt.textContent = SC_LABELS[k];
        select.appendChild(opt);
    }
    var allOpt = document.createElement('option');
    allOpt.value = '__all__';
    allOpt.textContent = 'All';
    select.appendChild(allOpt);
    select.onchange = function() {
        loadStrategyConfig();
    };
}

function loadStrategyConfig() {
    fetch('/api/strategy_config')
        .then(function(r){ return r.json(); })
        .then(function(d) {
            _scData = d || {};
            var select = document.getElementById('sc-strategy-select');
            var selectedKey = select && select.value ? select.value : '__all__';
            buildStrategySelect();
            if (select && select.options.length) {
                if (selectedKey === '__all__' || (selectedKey && SC_LABELS[selectedKey])) {
                    select.value = selectedKey;
                } else {
                    selectedKey = select.options[0].value;
                }
            }
            var rows = '';
            var keys = selectedKey === '__all__' ? Object.keys(SC_LABELS) : (selectedKey ? [selectedKey] : Object.keys(SC_LABELS));
            var tbody = document.getElementById('sc-body');
            if (!tbody) return;
            if (!keys.length || !Object.keys(d || {}).length) {
                tbody.innerHTML = '<tr><td colspan="9" style="color:var(--red);padding:8px">Error: data kosong</td></tr>';
                return;
            }
            for (var i = 0; i < keys.length; i++) {
                var k = keys[i];
                var cfg = d[k] || {strategy_enabled: true, sizing_enabled: true, base_usd: 8, add_usd: null, cooldown_enabled: true, ai_call_open: false};
                var strategyEnabled = cfg.strategy_enabled !== false;
                var sizingEnabled = cfg.sizing_enabled !== false;
                var cooldownEnabled = cfg.cooldown_enabled !== false;
                var aiCallOpenEnabled = cfg.ai_call_open === true;
                var dim = sizingEnabled ? '' : 'opacity:0.35;pointer-events:none';
                var saveButton = '<button type="button" onclick="saveStrategyConfig(this)" style="background:var(--accent);color:#000;border:none;border-radius:4px;padding:3px 10px;font-size:11px;cursor:pointer;font-weight:600">SAVE</button>';
                var addFundCell = '';
                if (SC_HAS_ADDFUND[k]) {
                    if (SC_ADDFUND_LABEL[k]) {
                        addFundCell = '<td style="text-align:center;padding:5px 8px;white-space:nowrap"><span style="' + dim + ';color:var(--muted);font-style:italic">' + SC_ADDFUND_LABEL[k] + '</span></td>';
                    } else {
                        addFundCell = '<td style="text-align:center;padding:5px 8px;white-space:nowrap"><input type="number" id="sc-add-' + k + '" value="' + (cfg.add_usd || 0) + '" min="0" step="1" style="width:60px;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px;' + dim + '"></td>';
                    }
                } else {
                    addFundCell = '<td style="text-align:center;padding:5px 8px;color:var(--muted);white-space:nowrap">—</td>';
                }
                rows += '<tr data-strategy="' + k + '" style="border-bottom:1px solid rgba(255,255,255,0.04)">'
                    + '<td style="padding:5px 8px;font-weight:600">' + SC_LABELS[k] + '</td>'
                    + '<td style="text-align:center;padding:5px 8px"><input type="number" id="sc-maxdeals-' + k + '" value="' + (cfg.max_deals || 2) + '" min="1" step="1" style="width:50px;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px" title="Jumlah maksimum deal aktif bersamaan untuk strategi ini"></td>'
                    + '<td style="text-align:center;padding:5px 8px"><input type="checkbox" id="sc-run-' + k + '" ' + (strategyEnabled ? 'checked' : '') + ' style="width:16px;height:16px;cursor:pointer"></td>'
                    + '<td style="text-align:center;padding:5px 8px"><input type="checkbox" id="sc-aicall-' + k + '" ' + (aiCallOpenEnabled ? 'checked' : '') + ' style="width:16px;height:16px;cursor:pointer" title="Kandidat OPEN yang lolos semua filter rule-based masih dikonsultasikan ke AI dulu sebelum dibuka. Default OFF -- ini nggak bisa di-backtest kayak parameter lain."></td>'
                    + '<td style="text-align:center;padding:5px 8px"><input type="checkbox" id="sc-size-' + k + '" ' + (sizingEnabled ? 'checked' : '') + ' style="width:16px;height:16px;cursor:pointer"></td>'
                    + '<td style="text-align:center;padding:5px 8px"><input type="number" id="sc-base-' + k + '" value="' + (cfg.base_usd || 8) + '" min="1" step="1" style="width:60px;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px;' + dim + '"></td>'
                    + addFundCell
                    + '<td style="text-align:center;padding:5px 8px"><input type="checkbox" id="sc-cooldown-' + k + '" ' + (cooldownEnabled ? 'checked' : '') + ' style="width:16px;height:16px;cursor:pointer" title="Skip re-entry pair yang sama selama masih cooldown"></td>'
                    + '<td style="text-align:center;padding:5px 8px">' + saveButton + '</td>'
                    + '</tr>';
            }
            tbody.innerHTML = rows;
            tbody.querySelectorAll('[id^="sc-run-"], [id^="sc-size-"]').forEach(function(el) {
                el.addEventListener('change', function() {
                    onScToggle(this.id.slice(7));
                });
            });
        })
        .catch(function(e) {
            var tbody = document.getElementById('sc-body');
            if (tbody) {
                tbody.innerHTML = '<tr><td colspan="9" style="color:var(--red);padding:8px">Error fetch: ' + e + '</td></tr>';
            }
        });
}

function onScToggle(k) {
    var sizingEnabled = document.getElementById('sc-size-' + k).checked;
    var baseEl = document.getElementById('sc-base-' + k);
    if (baseEl) { baseEl.style.opacity = sizingEnabled ? '1' : '0.35'; baseEl.style.pointerEvents = sizingEnabled ? '' : 'none'; }
    var addEl = document.getElementById('sc-add-' + k);
    if (addEl) { addEl.style.opacity = sizingEnabled ? '1' : '0.35'; addEl.style.pointerEvents = sizingEnabled ? '' : 'none'; }
}

function saveStrategyConfig(button) {
    var select = document.getElementById('sc-strategy-select');
    var row = button && button.closest ? button.closest('tr') : null;
    var key = row ? row.getAttribute('data-strategy') : (select ? select.value : 'brkX2');
    if (key === '__all__') return;
    var data = {};
    var baseEl = document.getElementById('sc-base-' + key);
    var addEl = document.getElementById('sc-add-' + key);
    var maxDealsEl = document.getElementById('sc-maxdeals-' + key);
    var strategyEnabledEl = document.getElementById('sc-run-' + key);
    var sizingEnabledEl = document.getElementById('sc-size-' + key);
    var cooldownEnabledEl = document.getElementById('sc-cooldown-' + key);
    var aiCallOpenEl = document.getElementById('sc-aicall-' + key);
    data[key] = {
        strategy_enabled: strategyEnabledEl ? strategyEnabledEl.checked : true,
        sizing_enabled: sizingEnabledEl ? sizingEnabledEl.checked : true,
        cooldown_enabled: cooldownEnabledEl ? cooldownEnabledEl.checked : true,
        ai_call_open: aiCallOpenEl ? aiCallOpenEl.checked : false,
        base_usd: parseFloat(baseEl ? baseEl.value : 8) || 8,
        max_deals: parseInt(maxDealsEl ? maxDealsEl.value : 2) || 2,
    };
    if (SC_HAS_ADDFUND[key]) {
        data[key].add_usd = parseFloat(addEl ? addEl.value : 0) || 0;
    }
    fetch('/api/strategy_config', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data)})
        .then(function(r){ return r.json(); })
        .then(function(d){ if (d && d.ok) loadStrategyConfig(); });
}

function resetStrategyConfig() {
    if (!confirm('Reset semua nilai ke default?')) return;
    fetch('/api/strategy_config/reset', {method: 'POST'})
        .then(function(r){ return r.json(); })
        .then(function(){ loadStrategyConfig(); });
}

document.addEventListener('DOMContentLoaded', function() {
    setTimeout(loadStrategyConfig, 300);
    loadAIProviderConfig();
    loadAutoSellConfig();
    loadDailyLossLimit();
    setInterval(loadDailyLossLimit, 30000);
});

function loadAIProviderConfig() {
    fetch('/api/ai_provider_config').then(function(r){ return r.json(); }).then(function(d) {
        var mode = document.getElementById('ai-provider-mode');
        var status = document.getElementById('ai-provider-status');
        if (mode) mode.value = d.mode || 'anthropic_gemini';
        if (status) status.textContent = 'Mode aktif: ' + (mode && mode.options[mode.selectedIndex] ? mode.options[mode.selectedIndex].text : 'Anthropic → Gemini otomatis') + ' | Anthropic: ' + (d.anthropic_configured ? 'siap' : 'belum') + ' | Gemini: ' + (d.gemini_configured ? 'siap' : 'belum') + ' | Terakhir: ' + (d.last_provider || 'Belum ada keputusan AI');
    });
}

function saveAIProviderConfig() {
    var mode = document.getElementById('ai-provider-mode').value;
    fetch('/api/ai_provider_config', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({mode:mode})})
        .then(function(){ loadAIProviderConfig(); });
}

function loadDailyLossLimit() {
    fetch('/api/daily_loss_limit').then(function(r){ return r.json(); }).then(function(d) {
        if (!d.ok) return;
        var input = document.getElementById('dll-pct');
        var status = document.getElementById('dll-status');
        if (input && document.activeElement !== input) input.value = d.limit_pct;
        if (status) {
            var pnlColor = d.pnl_usd >= 0 ? 'var(--green)' : 'var(--red)';
            status.innerHTML = 'P&amp;L hari ini: <b style="color:' + pnlColor + '">' + (d.pnl_usd>=0?'+':'') + d.pnl_usd.toFixed(2) + ' USD (' + (d.pnl_pct>=0?'+':'') + d.pnl_pct.toFixed(2) + '%)</b> dari modal $' + d.capital_usd.toFixed(2) +
                (d.breached ? ' <b style="color:var(--red)">— TERSULUT, open deal baru sedang diblokir</b>' : ' — normal');
        }
    }).catch(function(){});
}
function saveDailyLossLimit() {
    var v = parseFloat(document.getElementById('dll-pct').value);
    if (isNaN(v) || v < 0) { alert('Isi angka batas yang valid.'); return; }
    fetch('/api/daily_loss_limit', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({limit_pct:v})})
        .then(function(r){ return r.json(); }).then(function(d){
            if (!d.ok) { alert('Error: ' + d.error); return; }
            loadDailyLossLimit();
        });
}

function resendOpenNotification(sym) {
    if (!confirm('Kirim ulang laporan OPEN LONG untuk ' + sym + '?')) return;
    fetch('/resend_open_notification', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({symbol: sym})
    }).then(function(r){ return r.json(); }).then(function(d) {
        alert(d.ok ? 'Laporan OPEN LONG sudah dikirim ke Telegram.' : 'Gagal: ' + d.error);
    }).catch(function(e){ alert('Gagal mengirim laporan: ' + e); });
}

function loadAutoSellConfig() {
        Promise.all([
            fetch('/api/auto_sell_config').then(function(r){ return r.json(); }),
            fetch('/api/binance_spot_assets').then(function(r){
                return r.json().then(function(a){ if (!r.ok || !a.ok) throw new Error(a.error || 'Gagal membaca aset Binance'); return a; });
            })
        ]).then(function(results) {
            var cfg = results[0], freeAssets = results[1].assets || [];
            var sel = document.getElementById('auto-sell-new-asset');
            if (sel) {
                sel.innerHTML = '';
                if (!freeAssets.length) {
                    sel.innerHTML = '<option value="">Tidak ada aset bebas</option>';
                } else {
                    freeAssets.forEach(function(item) {
                        var option = document.createElement('option');
                        option.value = item.asset;
                        option.textContent = item.asset + ' (' + item.free + ')';
                        sel.appendChild(option);
                    });
                }
                sel.onchange = suggestNewAssetAvg;
                suggestNewAssetAvg();
            }
            renderAutoSellTable(cfg.assets || {});
        }).catch(function(e){
            var tbody = document.getElementById('auto-sell-tbody');
            if (tbody) tbody.innerHTML = '<tr><td colspan="10" style="color:var(--red);padding:8px">Gagal memuat: ' + e + '</td></tr>';
        });
    }

var autoSellCurrentAssets = [];
function renderAutoSellTable(assets) {
    var tbody = document.getElementById('auto-sell-tbody');
    if (!tbody) return;
    var names = Object.keys(assets);
    autoSellCurrentAssets = names;
    if (!names.length) {
        tbody.innerHTML = '<tr><td colspan="10" style="color:var(--muted);padding:8px">Belum ada asset auto-sell. Tambah lewat form di bawah.</td></tr>';
        return;
    }
    tbody.innerHTML = '';
    names.forEach(function(asset) {
        var cfg = assets[asset];
        var tr = document.createElement('tr');
        tr.id = 'auto-sell-row-' + asset;
        tr.style.borderBottom = '1px solid var(--border)';
        tr.innerHTML =
            '<td style="padding:5px 6px;font-weight:600">' + asset + '</td>' +
            '<td style="padding:5px 6px"><input type="number" min="0" step="0.00000001" value="' + (cfg.avg_price || '') + '" placeholder="belum diketahui" id="auto-sell-avg-' + asset + '" style="width:100px;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:3px 5px"><div id="auto-sell-avgsrc-' + asset + '" style="color:var(--muted);font-size:9px"></div></td>' +
            '<td style="padding:5px 6px" id="auto-sell-price-' + asset + '">memuat...</td>' +
            '<td style="padding:5px 6px" id="auto-sell-avggap-' + asset + '">-</td>' +
            '<td style="padding:5px 6px"><input type="number" min="0" step="0.00000001" value="' + cfg.threshold_usdt + '" id="auto-sell-thr-' + asset + '" style="width:100px;background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:3px 5px"><div id="auto-sell-breakeven-' + asset + '" style="color:var(--muted);font-size:9px"></div></td>' +
            '<td style="padding:5px 6px" id="auto-sell-gap-' + asset + '">-</td>' +
            '<td style="padding:5px 6px;color:var(--muted)">' + (cfg.enabled ? 'Aktif: menunggu crossing naik' : 'Nonaktif') + '</td>' +
            '<td style="padding:5px 6px"><input type="checkbox" ' + (cfg.enabled ? 'checked' : '') + ' id="auto-sell-chk-' + asset + '"></td>' +
            '<td style="padding:5px 6px" title="Sisa ~5% yg nggak ikut terjual otomatis dikonversi jadi BNB (buat diskon fee trading 25%)"><input type="checkbox" ' + (cfg.convert_leftover_bnb ? 'checked' : '') + ' id="auto-sell-bnb-' + asset + '"></td>' +
            '<td style="padding:5px 6px;white-space:nowrap">' +
                '<button type="button" onclick="saveAutoSellRow(\\'' + asset + '\\')" style="background:var(--accent);color:#000;border:none;border-radius:4px;padding:3px 8px;font-size:10px;cursor:pointer;margin-right:4px;font-weight:600">SAVE</button>' +
                '<button type="button" onclick="openConvertModal(\\'' + asset + '\\')" title="Cari koin lain buat convert (jual asset ini, beli koin lain yang lebih bertenaga)" style="background:#7c5cff;color:#fff;border:none;border-radius:4px;padding:3px 8px;font-size:10px;cursor:pointer;margin-right:4px;font-weight:600">CONVERT</button>' +
                '<button type="button" onclick="removeAutoSellRow(\\'' + asset + '\\')" style="background:var(--red);color:#fff;border:none;border-radius:4px;padding:3px 8px;font-size:10px;cursor:pointer">Hapus</button>' +
            '</td>';
        tbody.appendChild(tr);
        refreshAutoSellRowPrice(asset);
    });
}

function refreshAutoSellRowPrice(asset) {
    fetch('/api/auto_sell_price?asset=' + encodeURIComponent(asset)).then(function(r){ return r.json(); }).then(function(d) {
        var priceEl = document.getElementById('auto-sell-price-' + asset);
        var gapEl = document.getElementById('auto-sell-gap-' + asset);
        var avgGapEl = document.getElementById('auto-sell-avggap-' + asset);
        var avgSrcEl = document.getElementById('auto-sell-avgsrc-' + asset);
        var breakevenEl = document.getElementById('auto-sell-breakeven-' + asset);
        var avgEl = document.getElementById('auto-sell-avg-' + asset);
        if (!d.ok) { if (priceEl) priceEl.textContent = 'tidak tersedia'; return; }
        if (priceEl) priceEl.textContent = d.price + ' USDT';
        if (gapEl) gapEl.innerHTML = d.gap_pct + '%' + (d.est_profit_usd ? '<br><span style="color:var(--muted);font-size:9px">≈ ' + d.est_profit_usd + ' USDT kalau target kena</span>' : '');
        if (avgGapEl) avgGapEl.textContent = d.avg_gap_pct ? (d.avg_gap_pct + '%') : '-';
        if (avgSrcEl) avgSrcEl.textContent = d.avg_price_source === 'active_deal' ? '(deal aktif bot saat ini)' : (d.avg_price_source === 'auto' ? '(otomatis dari bot)' : (d.avg_price_source === 'manual' ? '(input manual)' : (d.avg_price_source === 'binance_history' ? '(perkiraan dari riwayat Binance)' : '')));
        if (breakevenEl) {
            var beTxt = d.breakeven ? ('breakeven ~' + d.breakeven) : '';
            if (d.htf_trend) beTxt += (beTxt ? ' | ' : '') + 'HTF ' + d.htf_trend;
            if (d.resistance) beTxt += (beTxt ? ' | ' : '') + 'resistance terdekat ~' + d.resistance;
            breakevenEl.innerHTML = beTxt;
            if (d.breakeven_warning) breakevenEl.innerHTML += '<br><span style="color:var(--red)">⚠ ' + d.breakeven_warning + '</span>';
        }
        // Jangan timpa avg_price kalau user lagi ngetik / sudah keisi lokal, cuma isi kalau field masih kosong & ada suggestion (auto atau perkiraan riwayat Binance)
        if (avgEl && !avgEl.value && d.avg_price && (d.avg_price_source === 'active_deal' || d.avg_price_source === 'auto' || d.avg_price_source === 'binance_history')) avgEl.value = d.avg_price;
    }).catch(function(){});
}

function saveAutoSellRow(asset) {
    var thrEl = document.getElementById('auto-sell-thr-' + asset);
    var chkEl = document.getElementById('auto-sell-chk-' + asset);
    var avgEl = document.getElementById('auto-sell-avg-' + asset);
    var bnbEl = document.getElementById('auto-sell-bnb-' + asset);
    var enabled = chkEl ? chkEl.checked : false;
    var threshold = parseFloat(thrEl ? thrEl.value : 0) || 0;
    var avgPrice = avgEl && avgEl.value !== '' ? parseFloat(avgEl.value) : null;
    if (enabled && threshold <= 0) { alert('Isi threshold harga yang valid.'); return; }
    var body = {asset:asset, enabled:enabled, threshold_usdt:threshold, convert_leftover_bnb: bnbEl ? bnbEl.checked : false};
    if (avgPrice !== null) body.avg_price = avgPrice;
    fetch('/api/auto_sell_config', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)})
        .then(function(r){ return r.json(); }).then(function(d){
            if (!d.ok) { alert('Error: ' + d.error); return; }
            renderAutoSellTable(d.assets || {});
        });
}

function removeAutoSellRow(asset) {
    if (!confirm('Hapus auto-sell untuk ' + asset + '?')) return;
    fetch('/api/auto_sell_config', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({asset:asset, remove:true})})
        .then(function(r){ return r.json(); }).then(function(d){
            if (!d.ok) { alert('Error: ' + d.error); return; }
            renderAutoSellTable(d.assets || {});
        });
}

function suggestNewAssetAvg() {
    var sel = document.getElementById('auto-sell-new-asset');
    var avgEl = document.getElementById('auto-sell-new-avg');
    var srcEl = document.getElementById('auto-sell-new-avgsrc');
    var asset = sel ? sel.value : '';
    if (avgEl) avgEl.value = '';
    if (srcEl) srcEl.textContent = '';
    if (!asset) return;
    if (avgEl) avgEl.placeholder = 'memuat...';
    fetch('/api/auto_sell_price?asset=' + encodeURIComponent(asset)).then(function(r){ return r.json(); }).then(function(d) {
        if (avgEl) avgEl.placeholder = 'kalau tahu';
        if (!d.ok || !d.avg_price) return;
        if (avgEl) avgEl.value = d.avg_price;
        if (srcEl) srcEl.textContent = d.avg_price_source === 'active_deal' ? '(deal aktif bot saat ini)' : (d.avg_price_source === 'auto' ? '(otomatis dari bot)' : (d.avg_price_source === 'binance_history' ? '(perkiraan dari riwayat Binance)' : ''));
    }).catch(function(){ if (avgEl) avgEl.placeholder = 'kalau tahu'; });
}

function addAutoSellAsset() {
    var sel = document.getElementById('auto-sell-new-asset');
    var thrEl = document.getElementById('auto-sell-new-threshold');
    var avgEl = document.getElementById('auto-sell-new-avg');
    var bnbEl = document.getElementById('auto-sell-new-bnb');
    var asset = sel ? sel.value : '';
    var threshold = parseFloat(thrEl ? thrEl.value : 0) || 0;
    var avgPrice = avgEl && avgEl.value !== '' ? parseFloat(avgEl.value) : null;
    if (!asset) { alert('Pilih asset dulu.'); return; }
    if (threshold <= 0) { alert('Isi threshold harga yang valid.'); return; }
    var body = {asset:asset, enabled:false, threshold_usdt:threshold, convert_leftover_bnb: bnbEl ? bnbEl.checked : false};
    if (avgPrice !== null) body.avg_price = avgPrice;
    fetch('/api/auto_sell_config', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)})
        .then(function(r){ return r.json(); }).then(function(d){
            if (!d.ok) { alert('Error: ' + d.error); return; }
            if (thrEl) thrEl.value = 0;
            if (avgEl) avgEl.value = '';
            var srcEl = document.getElementById('auto-sell-new-avgsrc');
            if (srcEl) srcEl.textContent = '';
            renderAutoSellTable(d.assets || {});
        });
}
function openConvertModal(asset) {
    var modal = document.getElementById('convert-modal');
    var body = document.getElementById('convert-modal-body');
    var title = document.getElementById('convert-modal-title');
    title.textContent = 'Convert Aset — ' + asset;
    body.innerHTML = 'Mencari kandidat (scan pasar, bisa 1-2 menit)...';
    modal.style.display = 'flex';
    fetch('/api/convert_candidates?asset=' + encodeURIComponent(asset)).then(function(r){ return r.json(); }).then(function(d) {
        if (!d.ok) { body.innerHTML = '<span style="color:var(--red)">Gagal: ' + (d.error || 'tidak diketahui') + '</span>'; return; }
        var html = '<div style="margin-bottom:8px">Kerugian ' + asset + ' saat ini: <b>' + d.loss_pct + '%</b> — kandidat butuh jarak ke resistance minimal <b>' + d.min_gap_pct + '%</b> DAN momentum riil (RVOL≥1, harga>EMA20, ATR% ≥ median sendiri).</div>';
        if (!d.candidates || !d.candidates.length) {
            html += '<div style="color:var(--muted)">Tidak ada kandidat yang lolos kedua syarat saat ini. Coba lagi nanti — syarat momentum sengaja tidak dilonggarkan.</div>';
        } else {
            html += '<table style="width:100%;border-collapse:collapse;font-size:10px"><thead><tr style="text-align:left;color:var(--muted);border-bottom:1px solid var(--border)">' +
                '<th style="padding:4px">Koin</th><th style="padding:4px">Harga</th><th style="padding:4px">Resistance</th><th style="padding:4px">Gap%</th><th style="padding:4px">RVOL</th><th style="padding:4px">RSI</th><th style="padding:4px"></th></tr></thead><tbody>';
            d.candidates.forEach(function(c) {
                html += '<tr style="border-bottom:1px solid var(--border)">' +
                    '<td style="padding:4px;font-weight:600">' + c.asset + '</td>' +
                    '<td style="padding:4px">' + c.price + '</td>' +
                    '<td style="padding:4px">' + c.resistance + '</td>' +
                    '<td style="padding:4px;color:var(--accent)">+' + c.gap_pct + '%</td>' +
                    '<td style="padding:4px">' + (c.rvol != null ? c.rvol : '-') + '</td>' +
                    '<td style="padding:4px">' + (c.rsi != null ? c.rsi : '-') + '</td>' +
                    '<td style="padding:4px"><button type="button" onclick="runConvert(\\'' + asset + '\\',\\'' + c.symbol + '\\')" style="background:#7c5cff;color:#fff;border:none;border-radius:4px;padding:3px 8px;font-size:10px;cursor:pointer;font-weight:600">Convert</button></td>' +
                '</tr>';
            });
            html += '</tbody></table>';
        }
        body.innerHTML = html;
    }).catch(function(e){ body.innerHTML = '<span style="color:var(--red)">Gagal: ' + e + '</span>'; });
}

function closeConvertModal() {
    document.getElementById('convert-modal').style.display = 'none';
}

function runConvert(sourceAsset, targetSymbol) {
    if (!confirm('Yakin JUAL ' + sourceAsset + ' (semua saldo bebas) lalu BELI ' + targetSymbol + ' sekarang? Order market dikirim langsung ke Binance, tidak bisa dibatalkan.')) return;
    var body = document.getElementById('convert-modal-body');
    body.innerHTML = 'Mengeksekusi convert (jual lalu beli, market order)...';
    fetch('/api/convert_execute', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({source_asset:sourceAsset, target_symbol:targetSymbol})})
        .then(function(r){ return r.json(); }).then(function(d) {
            if (!d.ok) { alert('Gagal convert: ' + (d.error || 'tidak diketahui')); closeConvertModal(); return; }
            alert('Convert berhasil: ' + sourceAsset + ' → ' + targetSymbol.replace('USDT','') + '\\nTarget jual baru: ' + (d.target_final || '-'));
            closeConvertModal();
            loadAutoSellConfig();
        }).catch(function(e){ alert('Gagal convert: ' + e); closeConvertModal(); });
}

if (typeof STRAT_SECONDARY !== 'undefined') {
    STRAT_SECONDARY['Hunting-4h'] = [{key: 'rsi', label: 'RSI<60'}];
}
setInterval(function(){ autoSellCurrentAssets.forEach(refreshAutoSellRowPrice); }, 30000);
</script>

<!-- ═══════════════ CLOSED TRADES ═══════════════ -->
<div class="container dash-section-start" data-tab="history" style="margin-top:12px">
<div class="card" style="background:var(--card);border:1px solid var(--border);border-radius:10px;padding:18px 22px;margin-top:0">
    <div class="card-header" onclick="toggleCard(this)" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
        <h2 style="margin:0;font-size:13px;letter-spacing:.08em;color:var(--accent)">CLOSED TRADES <span class="card-toggle">&#9660;</span></h2>
    <div style="display:flex;gap:8px;align-items:center">
    <select id="ct-filter-strat" onclick="event.stopPropagation()" onchange="loadClosedTrades()" style="background:var(--bg);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px">
        <option value="">Semua strategi</option>
        <option value="brkX2">brkX2-12h</option>
        <option value="brkX2_4h">brkX2-4h</option>
        <option value="reversal">Reversal-8h</option>
        <option value="hunting_4h">Hunting-4h</option>
        <option value="brkX2_crossema">CrossEMA-4h</option>
        <option value="akumulasi">Akumulasi-4h</option>
        <option value="trend_confirm_4h">TrenKonfirmasi-4h</option>
        <option value="__exclude_hardstop__">Semua strategi, exclude hardstop volatilitas</option>
      </select>
    <select id="ct-filter-pair" onclick="event.stopPropagation()" onchange="loadClosedTrades()" style="background:var(--bg);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px">
        <option value="">Semua pair</option>
      </select>
    <select id="ct-filter-time" onclick="event.stopPropagation()" onchange="onCtTimeFilterChange()" style="background:var(--bg);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px">
        <option value="">Semua waktu</option>
        <option value="today">Today</option>
        <option value="week">This week</option>
        <option value="month">This month</option>
        <option value="custom">Custom</option>
      </select>
    <span id="ct-filter-custom-range" onclick="event.stopPropagation()" style="display:none;gap:4px;align-items:center">
        <input type="date" id="ct-filter-from" onchange="loadClosedTrades()" style="background:var(--bg);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px">
        <span style="color:var(--muted);font-size:11px">s/d</span>
        <input type="date" id="ct-filter-to" onchange="loadClosedTrades()" style="background:var(--bg);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px">
      </span>
    <button onclick="event.stopPropagation();loadClosedTrades()" style="background:var(--accent);color:#000;border:none;border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer">Refresh</button>
    <button onclick="event.stopPropagation();resetCtFilters()" style="background:var(--surface);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer">Reset</button>
    </div>
  </div>
  <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:12px">
    <select id="ct-filter-outcome" onchange="loadClosedTrades()" style="background:var(--bg);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px">
        <option value="">Semua hasil</option>
        <option value="win">Profit saja</option>
        <option value="loss">Loss saja</option>
      </select>
    <select id="ct-filter-reason" onchange="loadClosedTrades()" style="background:var(--bg);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px">
        <option value="">Semua alasan close</option>
        <option value="Take Profit (custom)">Take Profit (custom)</option>
        <option value="Trailing">Trailing</option>
        <option value="Hard Stop">Hard Stop</option>
        <option value="Timeout (candle limit)">Timeout (candle limit)</option>
        <option value="Manual Reconcile">Manual Reconcile</option>
        <option value="Lainnya">Lainnya</option>
      </select>
    <input type="text" id="ct-filter-search" oninput="renderClosedTradesRows()" placeholder="cari pair..." style="background:var(--bg);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:3px 6px;font-size:11px;width:110px">
    <button onclick="exportCtCsv()" style="background:var(--surface);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer">Export CSV</button>
  </div>
  <div id="ct-stats" style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:10px;font-size:11px"></div>
  <div id="ct-summary" style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px;font-size:10px"></div>
  <div id="ct-equity" style="margin-bottom:14px"></div>
  <div style="overflow-x:auto">
    <table id="ct-table" style="width:100%;border-collapse:collapse;font-size:11px">
      <thead><tr style="color:var(--muted);border-bottom:1px solid var(--border)">
        <th data-sort-key="open_time" onclick="sortClosedTrades('open_time')" style="text-align:left;padding:5px 8px;cursor:pointer">Opened</th>
        <th data-sort-key="close_time" onclick="sortClosedTrades('close_time')" style="text-align:left;padding:5px 8px;cursor:pointer">Close</th>
        <th data-sort-key="symbol" onclick="sortClosedTrades('symbol')" style="text-align:left;padding:5px 8px;cursor:pointer">Pair</th>
        <th data-sort-key="strategy" onclick="sortClosedTrades('strategy')" style="text-align:left;padding:5px 8px;cursor:pointer">Strategi</th>
        <th style="text-align:right;padding:5px 8px">Entry</th>
        <th data-sort-key="rsi_open" onclick="sortClosedTrades('rsi_open')" style="text-align:right;padding:5px 8px;cursor:pointer" title="RSI(14) saat open long">RSI@Open</th>
        <th style="text-align:right;padding:5px 8px">Exit</th>
        <th data-sort-key="profit_pct" onclick="sortClosedTrades('profit_pct')" style="text-align:right;padding:5px 8px;cursor:pointer">Profit%</th>
        <th data-sort-key="profit_usd" onclick="sortClosedTrades('profit_usd')" style="text-align:right;padding:5px 8px;cursor:pointer">Profit$</th>
        <th style="text-align:right;padding:5px 8px">Modal</th>
        <th data-sort-key="duration" onclick="sortClosedTrades('duration')" style="text-align:right;padding:5px 8px;cursor:pointer">Durasi</th>
        <th style="text-align:left;padding:5px 8px">Alasan</th>
      </tr></thead>
      <tbody id="ct-body"><tr><td colspan="12" style="color:var(--muted);padding:12px 8px;text-align:center">Klik Refresh untuk muat data</td></tr></tbody>
    </table>
  </div>
</div>
</div>
<script>
var closedTradesRows = [];
var closedTradesSortKey = 'close_time';
var closedTradesSortDirection = -1;

function sortClosedTrades(key) {
    if (closedTradesSortKey === key) closedTradesSortDirection *= -1;
    else { closedTradesSortKey = key; closedTradesSortDirection = 1; }
    renderClosedTradesRows();
}

function renderClosedTradesRows() {
    var rows = closedTradesRows.slice();
    var strat_map = {brkX2:'brkX2-12h',brkX2_4h:'brkX2-4h',reversal:'Reversal-8h',hunting_4h:'Hunting-4h',brkX2_crossema:'CrossEMA-4h',akum_entry_a:'Akumulasi Entry A',akum_entry_b:'Akumulasi Entry B',trend_confirm_4h:'TrenKonfirmasi-4h'};
    var searchBox = document.getElementById('ct-filter-search');
    var searchTerm = searchBox ? searchBox.value.trim().toUpperCase() : '';
    if (searchTerm) {
        rows = rows.filter(function(r) { return (r.symbol||'').toUpperCase().indexOf(searchTerm) !== -1; });
    }
    if (closedTradesSortKey) {
        rows.sort(function(a,b) {
            var av = a[closedTradesSortKey] || '';
            var bv = b[closedTradesSortKey] || '';
            if (closedTradesSortKey === 'profit_pct' || closedTradesSortKey === 'profit_usd') {
                av = parseFloat(av || 0); bv = parseFloat(bv || 0);
            } else if (closedTradesSortKey === 'strategy') {
                av = strat_map[av] || av; bv = strat_map[bv] || bv;
            } else if (closedTradesSortKey === 'duration') {
                var durationMinutes = function(value) {
                      var match = String(value).match(/([0-9]+)j[ ]*([0-9]+)m|^([0-9]+)m$/);
                    return match ? (match[1] ? parseInt(match[1]) * 1440 + parseInt(match[2]) : parseInt(match[3])) : -1;
                };
                av = durationMinutes(av); bv = durationMinutes(bv);
            }
            if (av < bv) return -1 * closedTradesSortDirection;
            if (av > bv) return 1 * closedTradesSortDirection;
            return 0;
        });
    }
    document.querySelectorAll('#ct-table th[data-sort-key]').forEach(function(th) {
        var label = th.getAttribute('data-sort-key');
        var text = {open_time:'Opened',close_time:'Close',symbol:'Pair',strategy:'Strategi',rsi_open:'RSI@Open',profit_pct:'Profit%',profit_usd:'Profit$',duration:'Durasi'}[label];
        th.textContent = text + (closedTradesSortKey === label ? (closedTradesSortDirection === 1 ? ' ▲' : ' ▼') : '');
    });
    window._ctFilteredRows = rows;
    renderCtSummary(rows);
    renderCtEquityCurve(rows);
    var tbody = document.getElementById('ct-body');
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="12" style="color:var(--muted);padding:12px 8px;text-align:center">Belum ada data closed trades</td></tr>'; return; }
    tbody.innerHTML = rows.map(function(r) {
            var pct = parseFloat(r.profit_pct||0);
            var usd = parseFloat(r.profit_usd||0);
            var clr = pct>=0?'var(--green)':'var(--red)';
            return '<tr style="border-bottom:1px solid rgba(255,255,255,0.04)">' +
                '<td style="padding:5px 8px;color:var(--muted);white-space:nowrap">' + (r.open_time ? r.open_time.substring(0,16) : '-') + '</td>' +
                '<td style="padding:5px 8px;color:var(--muted);white-space:nowrap">' + (r.close_time||'').substring(0,16) + '</td>' +
                '<td style="padding:5px 8px;font-weight:600">' + (r.symbol ? ('<a href="javascript:void(0)" onclick="openTradingViewChart(\\'' + r.symbol.replace('/','') + '\\',\\'' + (strat_map[r.strategy]||r.strategy||'') + '\\')" style="color:#2962ff;text-decoration:none;cursor:pointer" title="Buka chart TradingView">' + r.symbol + '</a>') : '-') + '</td>' +
                '<td style="padding:5px 8px;color:var(--muted)">' + (strat_map[r.strategy]||r.strategy||'-') + '</td>' +
                '<td style="padding:5px 8px;text-align:right;font-size:10px">' + (r.entry_price||'-') + '</td>' +
                '<td style="padding:5px 8px;text-align:right;font-size:10px;color:var(--muted)">' + (r.rsi_open||'-') + '</td>' +
                '<td style="padding:5px 8px;text-align:right;font-size:10px">' + (r.exit_price||'-') + '</td>' +
                '<td style="padding:5px 8px;text-align:right;color:' + clr + '">' + (pct>=0?'+':'') + pct.toFixed(2) + '%</td>' +
                '<td style="padding:5px 8px;text-align:right;color:' + clr + '">' + (usd>=0?'+':'') + usd.toFixed(2) + '</td>' +
                '<td style="padding:5px 8px;text-align:right;color:var(--muted)">$' + (r.base_usd||'-') + '</td>' +
                '<td style="padding:5px 8px;text-align:right;color:var(--muted)">' + (r.duration||'-') + '</td>' +
                '<td style="padding:5px 8px;color:var(--muted);font-size:10px;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + (r.exit_reason||'') + '">' + (r.exit_reason||'-') + '</td>' +
            '</tr>';
    }).join('');
}

function renderCtSummary(rows) {
    var el = document.getElementById('ct-summary');
    if (!el) return;
    if (!rows.length) { el.innerHTML = ''; return; }
    var strat_map = {brkX2:'brkX2-12h',brkX2_4h:'brkX2-4h',reversal:'Reversal-8h',hunting_4h:'Hunting-4h',brkX2_crossema:'CrossEMA-4h',akum_entry_a:'Akumulasi Entry A',akum_entry_b:'Akumulasi Entry B',trend_confirm_4h:'TrenKonfirmasi-4h'};
    var groups = {};
    rows.forEach(function(r) {
        var key = r.strategy || 'brkX2';
        if (!groups[key]) groups[key] = {n:0, wins:0, pnlPct:0, pnlUsd:0};
        var g = groups[key];
        var pct = parseFloat(r.profit_pct||0), usd = parseFloat(r.profit_usd||0);
        g.n++;
        if (pct > 0) g.wins++;
        g.pnlPct += pct;
        g.pnlUsd += usd;
    });
    el.innerHTML = Object.keys(groups).sort().map(function(key) {
        var g = groups[key];
        var wr = (g.wins / g.n * 100).toFixed(0);
        var clr = g.pnlUsd >= 0 ? 'var(--green)' : 'var(--red)';
        return '<span style="background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:3px 8px">' +
            '<b>' + (strat_map[key]||key) + '</b>: ' + g.n + ' trade, WR ' + wr + '%, ' +
            '<span style="color:' + clr + '">' + (g.pnlUsd>=0?'+':'') + g.pnlUsd.toFixed(2) + ' USD (' + (g.pnlPct>=0?'+':'') + g.pnlPct.toFixed(1) + '%)</span></span>';
    }).join('');
}

function renderCtEquityCurve(rows) {
    var el = document.getElementById('ct-equity');
    if (!el) return;
    if (rows.length < 2) { el.innerHTML = ''; return; }
    var sorted = rows.slice().sort(function(a,b) { return (a.close_time||'').localeCompare(b.close_time||''); });
    var cum = 0;
    var points = sorted.map(function(r) { cum += parseFloat(r.profit_pct||0); return cum; });
    var minV = Math.min(0, Math.min.apply(null, points));
    var maxV = Math.max(0, Math.max.apply(null, points));
    var range = (maxV - minV) || 1;
    var W = 1000, H = 80, pad = 4;
    var stepX = points.length > 1 ? (W - pad*2) / (points.length - 1) : 0;
    var toY = function(v) { return H - pad - ((v - minV) / range) * (H - pad*2); };
    var pathPts = points.map(function(v, i) { return (pad + i*stepX).toFixed(1) + ',' + toY(v).toFixed(1); }).join(' ');
    var zeroY = toY(0).toFixed(1);
    var lineColor = points[points.length-1] >= 0 ? '#3fb950' : '#f85149';
    el.innerHTML =
        '<div style="font-size:9px;color:var(--muted);margin-bottom:3px">Equity curve (PnL% kumulatif, ' + points.length + ' trade, terurut waktu)</div>' +
        '<svg viewBox="0 0 ' + W + ' ' + H + '" preserveAspectRatio="none" style="width:100%;height:70px;background:var(--bg);border:1px solid var(--border);border-radius:4px">' +
            '<line x1="0" y1="' + zeroY + '" x2="' + W + '" y2="' + zeroY + '" stroke="var(--border)" stroke-dasharray="4,3" stroke-width="1"></line>' +
            '<polyline points="' + pathPts + '" fill="none" stroke="' + lineColor + '" stroke-width="2" vector-effect="non-scaling-stroke"></polyline>' +
        '</svg>';
}

function exportCtCsv() {
    var rows = window._ctFilteredRows || [];
    if (!rows.length) { alert('Tidak ada data untuk di-export.'); return; }
    var strat_map = {brkX2:'brkX2-12h',brkX2_4h:'brkX2-4h',reversal:'Reversal-8h',hunting_4h:'Hunting-4h',brkX2_crossema:'CrossEMA-4h',akum_entry_a:'Akumulasi Entry A',akum_entry_b:'Akumulasi Entry B',trend_confirm_4h:'TrenKonfirmasi-4h'};
    var headers = ['Opened','Close','Pair','Strategi','Entry','RSI@Open','Exit','Profit%','Profit$','Modal','Durasi','Alasan'];
    var csvEsc = function(v) {
        v = String(v === undefined || v === null ? '' : v);
        if (v.indexOf(',') !== -1 || v.indexOf('"') !== -1 || v.indexOf('\\n') !== -1) { v = '"' + v.replace(/"/g, '""') + '"'; }
        return v;
    };
    var lines = [headers.join(',')];
    rows.forEach(function(r) {
        lines.push([r.open_time||'', r.close_time||'', r.symbol||'', strat_map[r.strategy]||r.strategy||'',
            r.entry_price||'', r.rsi_open||'', r.exit_price||'', r.profit_pct||'', r.profit_usd||'', r.base_usd||'',
            r.duration||'', r.exit_reason||''].map(csvEsc).join(','));
    });
    var blob = new Blob([lines.join('\\n')], {type: 'text/csv;charset=utf-8;'});
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = 'closed_trades_' + (new Date().toISOString().slice(0,10)) + '.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

function onCtTimeFilterChange() {
  var sel = document.getElementById('ct-filter-time');
  var box = document.getElementById('ct-filter-custom-range');
  if (box) box.style.display = (sel && sel.value === 'custom') ? 'inline-flex' : 'none';
  loadClosedTrades();
}

function _ctDateStr(d) {
  var yyyy = d.getFullYear();
  var mm = String(d.getMonth()+1).padStart(2,'0');
  var dd = String(d.getDate()).padStart(2,'0');
  return yyyy + '-' + mm + '-' + dd;
}

function ctTimeFilterRange() {
  var sel = document.getElementById('ct-filter-time');
  var mode = sel ? sel.value : '';
  if (!mode) return null;
  var now = new Date();
  if (mode === 'today') {
    var s = _ctDateStr(now);
    return {from: s, to: s};
  }
  if (mode === 'week') {
    var dow = now.getDay();               // 0=Minggu..6=Sabtu
    var diffToMon = (dow === 0) ? 6 : dow - 1;
    var monday = new Date(now); monday.setDate(now.getDate() - diffToMon);
    return {from: _ctDateStr(monday), to: _ctDateStr(now)};
  }
  if (mode === 'month') {
    var first = new Date(now.getFullYear(), now.getMonth(), 1);
    return {from: _ctDateStr(first), to: _ctDateStr(now)};
  }
  if (mode === 'custom') {
    var f = document.getElementById('ct-filter-from');
    var t = document.getElementById('ct-filter-to');
    var fv = f ? f.value : '', tv = t ? t.value : '';
    if (!fv && !tv) return null;
    return {from: fv || null, to: tv || null};
  }
  return null;
}

function loadClosedTrades() {
  var strat = document.getElementById('ct-filter-strat').value;
  var pairSel = document.getElementById('ct-filter-pair');
  var pair = pairSel ? pairSel.value : '';
  var params = [];
  if (strat === '__exclude_hardstop__') params.push('exclude_hardstop=1');
  else if (strat) params.push('strategy=' + strat);
  if (pair) params.push('pair=' + encodeURIComponent(pair));
  var outcomeSel = document.getElementById('ct-filter-outcome');
  if (outcomeSel && outcomeSel.value) params.push('outcome=' + outcomeSel.value);
  var reasonSel = document.getElementById('ct-filter-reason');
  if (reasonSel && reasonSel.value) params.push('reason_category=' + encodeURIComponent(reasonSel.value));
  var range = ctTimeFilterRange();
  if (range) {
    if (range.from) params.push('date_from=' + range.from);
    if (range.to) params.push('date_to=' + range.to);
  }
  // Simpan pilihan filter ke cookie biar SELAMAT dari reload penuh (auto-refresh 30d / manual refresh) --
  // sebelumnya semua dropdown balik ke default "Semua..." tiap kali halaman reload.
  try {
    var timeSel = document.getElementById('ct-filter-time');
    var fromEl  = document.getElementById('ct-filter-from');
    var toEl    = document.getElementById('ct-filter-to');
    var searchEl= document.getElementById('ct-filter-search');
    _setCookie('ct_strat', strat);
    _setCookie('ct_pair', pair);
    _setCookie('ct_outcome', outcomeSel ? outcomeSel.value : '');
    _setCookie('ct_reason', reasonSel ? reasonSel.value : '');
    _setCookie('ct_time', timeSel ? timeSel.value : '');
    _setCookie('ct_from', fromEl ? fromEl.value : '');
    _setCookie('ct_to', toEl ? toEl.value : '');
    _setCookie('ct_search', searchEl ? searchEl.value : '');
  } catch (e) {}
  var url = '/api/closed_trades' + (params.length ? '?' + params.join('&') : '');
  fetch(url).then(function(r){return r.json();}).then(function(d){
    if (pairSel && pairSel.options.length <= 1 && (d.all_pairs || []).length) {
        d.all_pairs.forEach(function(item) {
            var option = document.createElement('option');
            option.value = item.symbol;
            option.textContent = item.display;
            pairSel.appendChild(option);
        });
        var savedPair = pair;
        try { savedPair = pair || _getCookie('ct_pair') || ''; } catch (e) {}
        pairSel.value = savedPair;
    }
    var stats = d.stats || {};
    var sEl = document.getElementById('ct-stats');
    var pnl_pct = parseFloat(stats.total_pnl_pct||0);
    var pnl_usd = parseFloat(stats.total_pnl_usd||0);
    sEl.innerHTML = [
      'Total: <b>' + (stats.total||0) + '</b>',
      '<span style="color:var(--green)">W: ' + (stats.wins||0) + '</span>',
      '<span style="color:var(--red)">L: ' + (stats.losses||0) + '</span>',
      'WR: <b>' + (stats.wr||'0') + '%</b>',
      'PnL%: <b style="color:' + (pnl_pct>=0?'var(--green)':'var(--red)') + '">' + (pnl_pct>=0?'+':'') + pnl_pct.toFixed(1) + '%</b>',
      'PnL$: <b style="color:' + (pnl_usd>=0?'var(--green)':'var(--red)') + '">' + (pnl_usd>=0?'+':'') + pnl_usd.toFixed(2) + ' USD</b>',
    ].join('<span style="color:var(--border);margin:0 4px">|</span>');
        closedTradesRows = d.trades || [];
        renderClosedTradesRows();
  }).catch(function(e){ document.getElementById('ct-body').innerHTML = '<tr><td colspan="11" style="color:var(--red);padding:8px">Error: ' + e + '</td></tr>'; });
}
function resetCtFilters() {
    var stratSel  = document.getElementById('ct-filter-strat');
    var pairSel   = document.getElementById('ct-filter-pair');
    var timeSel   = document.getElementById('ct-filter-time');
    var fromEl    = document.getElementById('ct-filter-from');
    var toEl      = document.getElementById('ct-filter-to');
    var outcomeSel= document.getElementById('ct-filter-outcome');
    var reasonSel = document.getElementById('ct-filter-reason');
    var searchEl  = document.getElementById('ct-filter-search');
    if (stratSel)   stratSel.value = '';
    if (pairSel)    pairSel.value = '';
    if (timeSel)    timeSel.value = '';
    if (fromEl)     fromEl.value = '';
    if (toEl)       toEl.value = '';
    if (outcomeSel) outcomeSel.value = '';
    if (reasonSel)  reasonSel.value = '';
    if (searchEl)   searchEl.value = '';
    var box = document.getElementById('ct-filter-custom-range');
    if (box) box.style.display = 'none';
    loadClosedTrades();   // langsung nge-save cookie kosong & muat ulang "Semua..."
}

function restoreCtFilters() {
    try {
        var stratSel  = document.getElementById('ct-filter-strat');
        var outcomeSel= document.getElementById('ct-filter-outcome');
        var reasonSel = document.getElementById('ct-filter-reason');
        var timeSel   = document.getElementById('ct-filter-time');
        var fromEl    = document.getElementById('ct-filter-from');
        var toEl      = document.getElementById('ct-filter-to');
        var searchEl  = document.getElementById('ct-filter-search');
        var sv;
        sv = _getCookie('ct_strat');   if (stratSel && sv)   stratSel.value = sv;
        sv = _getCookie('ct_outcome'); if (outcomeSel && sv) outcomeSel.value = sv;
        sv = _getCookie('ct_reason');  if (reasonSel && sv)  reasonSel.value = sv;
        sv = _getCookie('ct_time');    if (timeSel && sv)    timeSel.value = sv;
        sv = _getCookie('ct_from');    if (fromEl && sv)     fromEl.value = sv;
        sv = _getCookie('ct_to');      if (toEl && sv)       toEl.value = sv;
        sv = _getCookie('ct_search');  if (searchEl && sv)   searchEl.value = sv;
        var box = document.getElementById('ct-filter-custom-range');
        if (box) box.style.display = (timeSel && timeSel.value === 'custom') ? 'inline-flex' : 'none';
    } catch (e) {}
}
window.addEventListener('load', function(){ restoreCtFilters(); loadClosedTrades(); });
</script>
<div class="container dash-section-start" data-tab="tools" style="margin-top:12px">
    <div class="card">
        <div class="card-header" onclick="toggleCard(this)"><h2>SCAN BLOCKERS <span class="card-toggle">&#9660;</span></h2></div>
        <div class="card-body" style="font-size:11px">
            <div id="scan-blockers-status" style="color:var(--muted)">Menunggu snapshot scan berikutnya...</div>
            <div style="overflow-x:auto;-webkit-overflow-scrolling:touch;margin-top:10px">
                <table id="scan-blockers-table" style="width:100%;min-width:620px;border-collapse:collapse;display:none">
                    <thead><tr style="color:var(--muted);border-bottom:1px solid var(--border)">
                        <th style="text-align:left;padding:5px 8px">Strategi</th>
                        <th style="text-align:right;padding:5px 8px">Pair scan</th>
                        <th style="text-align:right;padding:5px 8px">Jumlah hasil</th>
                        <th style="text-align:left;padding:5px 8px">Blocker terbanyak</th>
                    </tr></thead>
                    <tbody id="scan-blockers-body"></tbody>
                </table>
            </div>
        </div>
    </div>
</div>
<div class="container dash-section-start" data-tab="control" style="margin-top:12px">
    <div class="card">
        <div class="card-header" onclick="toggleCard(this)"><h2>BLOCK PAIRS <span class="card-toggle">&#9660;</span></h2></div>
        <div class="card-body" style="font-size:11px">
            <div style="color:var(--muted);margin-bottom:8px">Pair yang diblok tidak akan di-scan/dibuka deal baru sama sekali (lintas semua strategi). Deal yang KEBETULAN sudah aktif di pair itu dibiarkan jalan normal sampai closed sendiri, tidak dipaksa tutup.</div>
            <div style="overflow-x:auto">
            <table style="width:100%;border-collapse:collapse">
                <thead><tr style="text-align:left;color:var(--muted);border-bottom:1px solid var(--border)">
                    <th style="padding:5px 6px">Pair</th>
                    <th style="padding:5px 6px">Diblok sejak</th>
                    <th style="padding:5px 6px"></th>
                </tr></thead>
                <tbody id="block-pair-tbody"><tr><td colspan="3" style="padding:8px;color:var(--muted)">Memuat...</td></tr></tbody>
            </table>
            </div>
            <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:12px;border-top:1px solid var(--border);padding-top:10px">
                <label>+ Block pair <select id="block-pair-new" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:4px 6px"><option>Memuat...</option></select></label>
                <button type="button" onclick="addBlockPair()" style="background:var(--red);color:#fff;border:none;border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer;font-weight:600">+ BLOCK</button>
                <button type="button" onclick="loadBlockedPairs()" style="background:var(--accent);color:#000;border:none;border-radius:4px;padding:4px 10px;font-size:11px;cursor:pointer">Refresh</button>
            </div>
        </div>
    </div>
</div>
<script>
function loadBlockedPairs() {
    fetch('/api/blocked_pairs').then(function(r){ return r.json(); }).then(function(d) {
        if (!d.ok) return;
        var tbody = document.getElementById('block-pair-tbody');
        var blocked = d.blocked || [];
        if (!blocked.length) {
            tbody.innerHTML = '<tr><td colspan="3" style="color:var(--muted);padding:8px">Belum ada pair yang diblok.</td></tr>';
        } else {
            tbody.innerHTML = '';
            blocked.forEach(function(item) {
                var tr = document.createElement('tr');
                tr.style.borderBottom = '1px solid var(--border)';
                var actionCell = item.hardcoded
                    ? '<span style="color:var(--muted);font-size:10px" title="Diblok permanen oleh developer, tidak bisa di-unblock lewat menu ini">(sistem)</span>'
                    : '<button type="button" onclick="removeBlockPair(\\'' + item.symbol + '\\')" style="background:var(--accent);color:#000;border:none;border-radius:4px;padding:3px 8px;font-size:10px;cursor:pointer">Unblock</button>';
                tr.innerHTML =
                    '<td style="padding:5px 6px;font-weight:600">' + item.display + '</td>' +
                    '<td style="padding:5px 6px;color:var(--muted)">' + (item.blocked_at || '-') + '</td>' +
                    '<td style="padding:5px 6px">' + actionCell + '</td>';
                tbody.appendChild(tr);
            });
        }
        var sel = document.getElementById('block-pair-new');
        if (sel) {
            var avail = d.available || [];
            sel.innerHTML = avail.length ? '' : '<option value="">Tidak ada pair tersedia</option>';
            avail.forEach(function(item) {
                var option = document.createElement('option');
                option.value = item.symbol;
                option.textContent = item.display;
                sel.appendChild(option);
            });
        }
    }).catch(function(){});
}
function addBlockPair() {
    var sel = document.getElementById('block-pair-new');
    var symbol = sel ? sel.value : '';
    if (!symbol) { alert('Pilih pair dulu.'); return; }
    fetch('/api/blocked_pairs', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({action:'block', symbol:symbol})})
        .then(function(r){ return r.json(); }).then(function(d){
            if (!d.ok) { alert('Error: ' + d.error); return; }
            loadBlockedPairs();
        });
}
function removeBlockPair(symbol) {
    fetch('/api/blocked_pairs', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({action:'unblock', symbol:symbol})})
        .then(function(r){ return r.json(); }).then(function(d){
            if (!d.ok) { alert('Error: ' + d.error); return; }
            loadBlockedPairs();
        });
}
window.addEventListener('load', function(){ loadBlockedPairs(); });
</script>
<script>
var SCAN_BLOCKERS_SCHEDULE = {
    'brkX2-12h':   'Scan tiap loop (~1-3 menit), tidak dibatasi jendela waktu',
    'brkX2-4h':    'Hanya scan di menit ke-5 s/d 60 tiap candle 4h',
    'CrossEMA-4h': 'Hanya scan di menit ke-~12 s/d 36 tiap candle 4h',
    'Reversal-8h': 'Hanya scan sekali saat candle 8h baru tertutup',
    'Hunting-4h':  'Scan tiap loop (~1-3 menit), tidak dibatasi jendela waktu',
    'Akumulasi-4h':'Scan periodik (~tiap beberapa menit)'
};
function loadScanBlockers() {
    fetch('/api/scan_blockers').then(function(r){ return r.json(); }).then(function(d) {
        var scans = d.scans || {};
        var status = document.getElementById('scan-blockers-status');
        var table = document.getElementById('scan-blockers-table');
        var body = document.getElementById('scan-blockers-body');
        var allKeys = Object.keys(SCAN_BLOCKERS_SCHEDULE);
        Object.keys(scans).forEach(function(k){ if (allKeys.indexOf(k) === -1) allKeys.push(k); });
        body.innerHTML = allKeys.map(function(k) {
            var s = scans[k];
            if (!s) {
                return '<tr style="border-bottom:1px solid rgba(255,255,255,0.04)">' +
                    '<td style="padding:5px 8px;font-weight:600;color:var(--muted)">' + k + '</td>' +
                    '<td style="padding:5px 8px;text-align:right;color:var(--muted)">-</td>' +
                    '<td style="padding:5px 8px;text-align:right;color:var(--muted)">-</td>' +
                    '<td style="padding:5px 8px;color:var(--muted);font-style:italic">Belum pernah ada snapshot scan &mdash; ' + (SCAN_BLOCKERS_SCHEDULE[k] || 'jadwal tidak diketahui') + '</td></tr>';
            }
            var b = s.blockers || {}, names = Object.keys(b).sort(function(a, z){ return (Number(b[z]) || 0) - (Number(b[a]) || 0); });
            var top = names.slice(0, 3).map(function(n){ return n + ': ' + b[n]; }).join(' | ') || 'Tidak ada blocker';
            var resultLabel = k === 'Akumulasi-4h' ? 'hasil detector' : 'kandidat valid';
            return '<tr style="border-bottom:1px solid rgba(255,255,255,0.04)"><td style="padding:5px 8px;font-weight:600">' + k + '</td><td style="padding:5px 8px;text-align:right">' + s.total_scanned + '</td><td style="padding:5px 8px;text-align:right">' + s.candidates + '<br><small style="color:var(--muted)">' + resultLabel + '</small></td><td style="padding:5px 8px;color:var(--muted)">' + top + '<br><small>Scan: ' + s.scan_time + '</small></td></tr>';
        }).join('');
        table.style.display = 'table';
        status.textContent = 'Snapshot blocker per scan terakhir. Satu pair dapat gagal pada beberapa indikator. Strategi berjadwal (4h/8h) baru terisi saat window scan-nya aktif.';
    }).catch(function(e){ document.getElementById('scan-blockers-status').textContent = 'Gagal memuat blocker: ' + e; });
}
document.addEventListener('DOMContentLoaded', function(){ loadScanBlockers(); setInterval(loadScanBlockers, 30000); });
</script>
</body>
</html>
'''

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGI #5: Akumulasi Detector — deteksi fase akumulasi (sideways post-downtrend)
# Thread: T_AKUM, scan tiap 30 menit, output maks 5 pair
# PRIMARY  : range sideways, EMA konvergen, OBV slope+, ATR turun
# SECONDARY: volume asimetri, RSI 30-56, MACD flat, body ratio kecil
# ══════════════════════════════════════════════════════════════════════════════

def compute_indicators_akum(df):
    """Hitung indikator khusus Akumulasi Detector pada dataframe 4h."""
    close, high, low, vol = df['close'], df['high'], df['low'], df['vol']
    df['ema20']  = ta.ema(close, length=20)
    df['ema50']  = ta.ema(close, length=50)
    df['ema200'] = ta.ema(close, length=200)
    df['atr']    = ta.atr(high, low, close, length=14)
    df['rsi']    = ta.rsi(close, length=14)
    _macd = ta.macd(close, fast=12, slow=26, signal=9)
    hist_col = [c for c in _macd.columns if 'MACDh' in c]
    df['macd_hist'] = _macd[hist_col[0]] if hist_col else float('nan')
    # OBV manual
    obv = [0.0]
    cv = close.values; vv = vol.values
    for i in range(1, len(cv)):
        if cv[i] > cv[i-1]:   obv.append(obv[-1] + vv[i])
        elif cv[i] < cv[i-1]: obv.append(obv[-1] - vv[i])
        else:                  obv.append(obv[-1])
    df['obv'] = obv
    # body ratio
    rng = (high - low).replace(0, float('nan'))
    df['body_ratio'] = (close - df['open']).abs() / rng
    return df

def score_akumulasi(df, sym: str) -> dict:
    """
    Hitung skor akumulasi untuk 1 symbol. Return dict dengan detail atau None kalau data kurang.
    Skor 0-8: 4 primary (bobot 2) + 4 secondary (bobot 1).
    Ambil N = AKUM_SIDEWAYS_CANDLES candle terakhir sebagai jendela sideways.
    """
    try:
        if len(df) < AKUM_SIDEWAYS_CANDLES + 50:
            return None
        # Candle berjalan dibuang, pakai yang sudah tutup
        if df['ct'].iloc[-1] >= int(time.time() * 1000):
            df = df.iloc[:-1]
        if len(df) < AKUM_SIDEWAYS_CANDLES + 50:
            return None
        df = compute_indicators_akum(df)
        # Jendela sideways = N candle terakhir
        win = df.iloc[-AKUM_SIDEWAYS_CANDLES:]
        # Ambil timestamp candle pertama jendela sideways
        # get_ohlcv pakai kolom 'ot', get_ohlcv_4h pakai 'ts'
        _ts_col = 'ot' if 'ot' in win.columns else ('ts' if 'ts' in win.columns else None)
        _ts0 = int(win[_ts_col].iloc[0]) if _ts_col else None
        row = df.iloc[-1]

        close_now = float(row['close'])
        if close_now <= 0:
            return None

        # ── PRIMARY ────────────────────────────────────────────────────────────
        # P1: range sideways ≤ AKUM_RANGE_PCT
        hi_max  = float(win['high'].max())
        lo_min  = float(win['low'].min())
        range_pct = (hi_max - lo_min) / close_now if close_now > 0 else 99
        p1_ok = range_pct <= AKUM_RANGE_PCT
        p1_val = f"{range_pct*100:.1f}%"

        # Structural zone (Wyckoff-like): area test berulang, bukan wick ekstrem tunggal.
        zone_half_band = max((hi_max - lo_min) * 0.06, close_now * 0.004)
        lows = win['low'].dropna().astype(float)
        highs = win['high'].dropna().astype(float)

        sup_seed = float(np.nanpercentile(lows, 18))
        res_seed = float(np.nanpercentile(highs, 82))

        sup_cluster = lows[(lows >= sup_seed - zone_half_band) & (lows <= sup_seed + zone_half_band)]
        res_cluster = highs[(highs >= res_seed - zone_half_band) & (highs <= res_seed + zone_half_band)]

        sup_center = float(np.median(sup_cluster)) if len(sup_cluster) >= 3 else sup_seed
        res_center = float(np.median(res_cluster)) if len(res_cluster) >= 3 else res_seed

        support_zone_low = max(lo_min, sup_center - zone_half_band)
        support_zone_high = min(hi_max, sup_center + zone_half_band)
        resistance_zone_low = max(lo_min, res_center - zone_half_band)
        resistance_zone_high = min(hi_max, res_center + zone_half_band)

        if support_zone_high >= resistance_zone_low:
            mid = (sup_center + res_center) / 2.0
            support_zone_high = min(support_zone_high, mid)
            resistance_zone_low = max(resistance_zone_low, mid)

        if support_zone_low > support_zone_high:
            support_zone_low = support_zone_high = sup_center
        if resistance_zone_low > resistance_zone_high:
            resistance_zone_low = resistance_zone_high = res_center

        # P2: EMA20 vs EMA200 konvergen & datar
        ema20_now  = float(row['ema20'])  if not pd.isna(row.get('ema20'))  else None
        ema200_now = float(row['ema200']) if not pd.isna(row.get('ema200')) else None
        if ema20_now is None or ema200_now is None or ema200_now == 0:
            return None
        ema_gap = abs(ema20_now - ema200_now) / ema200_now
        p2_ok  = ema_gap <= AKUM_EMA_GAP_PCT
        p2_val = f"{ema_gap*100:.1f}%"

        # P3: OBV slope positif (linear regression atas N/2 candle terakhir)
        obv_win = win['obv'].values
        n_obv   = len(obv_win)
        if n_obv < 10:
            return None
        x = np.arange(n_obv)
        try:
            obv_slope = float(np.polyfit(x, obv_win, 1)[0])
        except Exception:
            obv_slope = 0.0
        p3_ok  = obv_slope > 0
        p3_val = f"slope {obv_slope:+.0f}"

        # P4: ATR sekarang <= (1 - AKUM_ATR_DROP_PCT) x ATR puncak dari lookback
        atr_now = float(row['atr']) if not pd.isna(row.get('atr')) else None
        # Ambil ATR dari periode sebelum jendela sideways (atau seluruh df kalau tidak cukup)
        pre_window = df['atr'].iloc[:-AKUM_SIDEWAYS_CANDLES] if len(df) > AKUM_SIDEWAYS_CANDLES + 10 else df['atr']
        atr_history = pre_window.dropna()
        if atr_now is None or len(atr_history) < 5:
            p4_ok  = False
            p4_val = "n/a"
        else:
            atr_peak = float(atr_history.max())
            atr_drop = 1 - (atr_now / atr_peak) if atr_peak > 0 else 0
            p4_ok  = atr_drop >= AKUM_ATR_DROP_PCT
            p4_val = f"turun {atr_drop*100:.0f}%"

        # P5: EMA20 slope filter — pastikan EMA20 tidak turun signifikan (bukan downtrend)
        ema20_start = float(win['ema20'].iloc[0]) if not pd.isna(win['ema20'].iloc[0]) else None
        ema20_end   = float(win['ema20'].iloc[-1]) if not pd.isna(win['ema20'].iloc[-1]) else None
        if ema20_start is not None and ema20_end is not None and ema20_start > 0:
            ema_slope_drop = (ema20_start - ema20_end) / ema20_start  # positif = turun
            p5_ok  = ema_slope_drop <= AKUM_EMA_SLOPE_MAX
            p5_val = f"EMA20 {'turun' if ema_slope_drop > 0 else 'naik'} {abs(ema_slope_drop)*100:.1f}%"
        else:
            p5_ok  = True   # data tidak cukup, tidak di-disqualify
            p5_val = "n/a"

        # P6: Close drift — selisih close awal vs close akhir jendela tidak boleh > AKUM_CLOSE_DRIFT_MAX
        close_start = float(win['close'].iloc[0])
        close_end   = float(win['close'].iloc[-1])
        if close_start > 0:
            close_drift = abs(close_end - close_start) / close_start
            p6_ok  = close_drift <= AKUM_CLOSE_DRIFT_MAX
            p6_val = f"drift {'naik' if close_end >= close_start else 'turun'} {close_drift*100:.1f}%"
        else:
            p6_ok  = True
            p6_val = "n/a"

        # P7: Range distribution — bagi jendela 3 bagian, cek apakah volatilitas merata
        seg = len(win) // 3
        if seg >= 10:
            def _seg_range(df_seg):
                lo = float(df_seg['low'].min()); hi = float(df_seg['high'].max())
                mid = float(df_seg['close'].mean())
                return (hi - lo) / mid if mid > 0 else 0
            r1 = _seg_range(win.iloc[:seg])
            r2 = _seg_range(win.iloc[seg:seg*2])
            r3 = _seg_range(win.iloc[seg*2:])
            ranges = [r for r in [r1, r2, r3] if r > 0]
            if len(ranges) >= 2:
                dist_ratio = max(ranges) / min(ranges)
                p7_ok  = dist_ratio <= AKUM_RANGE_DIST_MAX
                p7_val = f"dist {dist_ratio:.1f}x (maks {AKUM_RANGE_DIST_MAX}x)"
            else:
                p7_ok  = True
                p7_val = "n/a"
        else:
            p7_ok  = True
            p7_val = "n/a"

        primary_ok    = p1_ok and p2_ok and p3_ok and p4_ok and p5_ok and p6_ok and p7_ok
        primary_score = sum([p1_ok, p2_ok, p3_ok, p4_ok])  # P5/P6/P7 filter saja, tidak masuk skor

        # ── SECONDARY ──────────────────────────────────────────────────────────
        # S1: volume candle hijau > merah (asimetri)
        green_vol = float(win.loc[win['close'] >= win['open'], 'vol'].sum())
        red_vol   = float(win.loc[win['close']  < win['open'], 'vol'].sum())
        s1_ok  = green_vol > red_vol
        s1_val = f"G:{green_vol/max(red_vol,1):.2f}x"

        # S2: RSI 30-56
        rsi_now = float(row['rsi']) if not pd.isna(row.get('rsi')) else None
        s2_ok   = rsi_now is not None and 30 <= rsi_now <= 56
        s2_val  = f"{rsi_now:.1f}" if rsi_now is not None else "n/a"

        # S3: MACD histogram flat dekat nol
        macd_now = float(row['macd_hist']) if not pd.isna(row.get('macd_hist')) else None
        s3_ok    = macd_now is not None and abs(macd_now) < AKUM_MACD_FLAT_PCT * close_now
        s3_val   = f"{macd_now:.5f}" if macd_now is not None else "n/a"

        # S4: rata-rata body/range kecil (konsolidasi)
        avg_body = float(win['body_ratio'].mean(skipna=True))
        s4_ok    = avg_body < AKUM_BODY_RATIO_MAX
        s4_val   = f"{avg_body:.2f}"

        secondary_score = sum([s1_ok, s2_ok, s3_ok, s4_ok])
        secondary_ok    = secondary_score >= AKUM_MIN_SECONDARY
        total_score     = primary_score * 2 + secondary_score  # max 12

        # Ranking berbobot (total 100) — gating wajib: OBV slope+ DAN ATR turun ≥25%
        gating_ok = p3_ok and p4_ok
        weighted_score = 0
        if gating_ok:
            weighted_score = (
                (AKUM_W_OBV    if p3_ok else 0) +
                (AKUM_W_ATR    if p4_ok else 0) +
                (AKUM_W_RANGE  if p1_ok else 0) +
                (AKUM_W_VOL    if s1_ok else 0) +
                (AKUM_W_EMAGAP if p2_ok else 0) +
                (AKUM_W_RSI    if s2_ok else 0) +
                (AKUM_W_MACD   if s3_ok else 0) +
                (AKUM_W_BODY   if s4_ok else 0)
            )

        # Kumpulkan fails
        fails = []
        if not p1_ok: fails.append(f"Range {p1_val} >18%")
        if not p2_ok: fails.append(f"EMAGap {p2_val} >6%")
        if not p3_ok: fails.append(f"OBV {p3_val} ↓")
        if not p4_ok: fails.append(f"ATR {p4_val} <25%")
        if not p5_ok: fails.append(f"Slope: {p5_val} >4% (downtrend)")
        if not p6_ok: fails.append(f"Drift: {p6_val} >{AKUM_CLOSE_DRIFT_MAX*100:.0f}% (bukan sideways)")
        if not p7_ok: fails.append(f"Dist: {p7_val} (volatilitas tdk merata)")
        if not s1_ok: fails.append(f"Vol asimetri {s1_val}")
        if not s2_ok: fails.append(f"RSI {s2_val} OOB")
        if not s3_ok: fails.append(f"MACD {s3_val} tdk flat")
        if not s4_ok: fails.append(f"Body {s4_val} >{AKUM_BODY_RATIO_MAX}")

        # Cari kapan harga mulai bergerak dalam range ini (scan mundur, dibatasi ke jendela win)
        _ts_col2 = 'ot' if 'ot' in df.columns else ('ts' if 'ts' in df.columns else None)
        _lo_tol = float(lo_min) * 0.98
        _hi_tol = float(hi_max) * 1.02
        _win_start_idx = len(df) - AKUM_SIDEWAYS_CANDLES  # batas awal jendela win
        _sw_start_ts = None
        for _i in range(len(df) - 1, _win_start_idx - 1, -1):
            _r = df.iloc[_i]
            if float(_r['high']) > _hi_tol or float(_r['low']) < _lo_tol:
                if _i + 1 < len(df) and _ts_col2:
                    _sw_start_ts = int(df.iloc[_i + 1][_ts_col2])
                break

        if _sw_start_ts is None:
            # Seluruh window 180 candle stabil dalam range akumulasi.
            # Cari candle pertama (dari awal window) di mana indikator koin ini
            # masuk zona konsolidasi: ATR% sudah di bawah ATR puncak * (1-AKUM_ATR_DROP_PCT)
            # dan EMA20 slope tidak terlalu curam (abs slope < AKUM_EMA_SLOPE_MAX * close).
            # Ini menghasilkan timestamp unik per-koin, bukan nilai seragam.
            try:
                _atr_col = 'atr_pct' if 'atr_pct' in df.columns else None
                _ema_col = 'ema20' if 'ema20' in df.columns else None
                _atr_peak = float(df['atr_pct'].iloc[max(0, _win_start_idx - AKUM_ATR_LOOKBACK):_win_start_idx + 1].max()) if _atr_col else None
                _consol_start = None
                for _j in range(_win_start_idx, len(df)):
                    _rj = df.iloc[_j]
                    _atr_ok = True
                    _slope_ok = True
                    if _atr_col and _atr_peak and _atr_peak > 0:
                        _atr_ok = float(_rj['atr_pct']) <= _atr_peak * (1 - AKUM_ATR_DROP_PCT)
                    if _ema_col and _j > 0:
                        _ema_prev = df.iloc[_j - 1].get('ema20')
                        _ema_now2 = _rj.get('ema20')
                        if _ema_prev and _ema_now2 and float(_ema_prev) > 0:
                            _slope_ok = abs(float(_ema_now2) / float(_ema_prev) - 1) < AKUM_EMA_SLOPE_MAX / len(win)
                    if _atr_ok and _slope_ok and _ts_col2:
                        _consol_start = int(_rj[_ts_col2])
                        break
                _sw_start_ts = _consol_start if _consol_start else (int(_ts0) if _ts0 is not None else None)
            except Exception:
                _sw_start_ts = int(_ts0) if _ts0 is not None else None
        
        _sideways_start_str = (
            (pd.Timestamp(_sw_start_ts, unit='ms') + pd.Timedelta(hours=7)).strftime("%d/%m %H:%M")
            if _sw_start_ts and _sw_start_ts > 0 else "-"
        )

        return {
            "sym":            sym,
            "close":          close_now,
            "primary_ok":     primary_ok,
            "secondary_ok":   secondary_ok,
            "total_score":    total_score,
            "primary_score":  primary_score,
            "secondary_score":secondary_score,
            "fails":          fails,
            # nilai indikator utk display
            "range_pct":      p1_val,
            "ema_gap":        p2_val,
            "obv_slope":      p3_val,
            "atr_drop":       p4_val,
            "ema_slope":      p5_val,
            "close_drift":    p6_val,
            "range_dist":     p7_val,
            "vol_asim":       s1_val,
            "rsi":            s2_val,
            "macd_flat":      s3_val,
            "body_ratio":     s4_val,
            "p1_ok": p1_ok, "p2_ok": p2_ok, "p3_ok": p3_ok, "p4_ok": p4_ok,
            "p5_ok": p5_ok, "p6_ok": p6_ok, "p7_ok": p7_ok,
            "s1_ok": s1_ok, "s2_ok": s2_ok, "s3_ok": s3_ok, "s4_ok": s4_ok,
            "support":        round(float(lo_min), 8),
            "resistance":     round(float(hi_max), 8),
            "support_zone_low": round(float(support_zone_low), 8),
            "support_zone_high": round(float(support_zone_high), 8),
            "resistance_zone_low": round(float(resistance_zone_low), 8),
            "resistance_zone_high": round(float(resistance_zone_high), 8),
            "support_zone_touches": int(len(sup_cluster)),
            "resistance_zone_touches": int(len(res_cluster)),
            "sideways_start": _sideways_start_str,
            "weighted_score": weighted_score,
            "gating_ok":      gating_ok,
        }
    except Exception as e:
        log(f"  [AKUM] score error {sym}: {e}")
        return None


def get_akum_swing_high(df, n: int = 30) -> float:
    """
    Swing high lokal dari N candle 4h terakhir (tidak termasuk candle live).
    Dipakai sebagai TP realistis untuk Akumulasi — lebih dekat dari resistance 180-candle.
    Return 0.0 kalau data kurang.
    """
    try:
        window = df.iloc[-n-1:-1] if len(df) >= n + 1 else df.iloc[:-1]
        if len(window) < 5:
            return 0.0
        return float(window['high'].max())
    except Exception:
        return 0.0


def detect_entry_a_spring(df, support: float, support_reentry: float | None = None) -> dict | None:
    """
    Deteksi Entry A — Spring/Fakeout Wyckoff (TF 4h).
    Kondisi:
    1. Harga tembus support (low < support) lalu close kembali di atas dalam 1-3 candle
    2. Volume candle breakdown > AKUM_A_VOL_SPIKE_MULT × vol MA
    3. OBV tidak turun saat harga turun (divergensi bullish)
    4. RSI sempat < AKUM_A_RSI_MIN lalu naik kembali < AKUM_A_RSI_MAX_ENTRY
    Return: dict info atau None kalau tidak ada sinyal.
    """
    if len(df) < 20: return None
    if support_reentry is None:
        support_reentry = support
    try:
        import pandas_ta as _pta
        df = df.copy()
        df['vol_ma'] = df['vol'].rolling(20).mean()
        df['rsi'] = _pta.rsi(df['close'], length=14)
        obv = [0.0]
        for i in range(1, len(df)):
            if df['close'].iloc[i] > df['close'].iloc[i-1]:
                obv.append(obv[-1] + df['vol'].iloc[i])
            elif df['close'].iloc[i] < df['close'].iloc[i-1]:
                obv.append(obv[-1] - df['vol'].iloc[i])
            else:
                obv.append(obv[-1])
        df['obv'] = obv

        # Cari candle Spring: low < support dan vol spike
        for lookback in range(3, min(AKUM_A_REENTRY_CANDLES + 4, len(df) - 1)):
            spring_idx = len(df) - 1 - lookback
            if spring_idx < 5: break
            row_s = df.iloc[spring_idx]
            if row_s['low'] >= support * (1 + AKUM_A_SUPPORT_TOUCH_BUFFER): continue
            # Cek vol spike
            vol_ma = row_s.get('vol_ma', 0)
            if pd.isna(vol_ma) or vol_ma <= 0: continue
            if row_s['vol'] < AKUM_A_VOL_SPIKE_MULT * vol_ma: continue
            # Cek re-entry ke atas support_reentry dalam 1-3 candle setelah spring
            reentry_ok = False
            for k in range(1, AKUM_A_REENTRY_CANDLES + 1):
                if spring_idx + k >= len(df): break
                if df.iloc[spring_idx + k]['close'] > support_reentry:
                    reentry_ok = True; break
            if not reentry_ok: continue
            # Cek RSI: sempat rendah
            rsi_window = df['rsi'].iloc[max(0, spring_idx-3):spring_idx+1]
            if rsi_window.min() >= AKUM_A_RSI_MIN: continue
            # Cek RSI sekarang sudah naik
            rsi_now = df['rsi'].iloc[-1]
            if pd.isna(rsi_now) or rsi_now >= AKUM_A_RSI_MAX_ENTRY: continue
            # Cek OBV divergensi: OBV slope positif di 5 candle terakhir
            obv_recent = df['obv'].iloc[-AKUM_A_OBV_SLOPE_CANDLES:]
            obv_slope = (obv_recent.iloc[-1] - obv_recent.iloc[0])
            if obv_slope <= 0: continue
            # Spring valid
            spring_low = float(row_s['low'])
            sl_anchor = min(spring_low, support)
            sl_price   = round(sl_anchor * (1 - AKUM_ENTRY_SL_BUFFER), 8)
            return {
                'type':       'A',
                'spring_low': spring_low,
                'sl_price':   sl_price,
                'support':    support,
                'support_reentry': support_reentry,
                'rsi_now':    round(float(rsi_now), 1),
                'vol_ratio':  round(float(row_s['vol']) / float(vol_ma), 2),
                'obv_slope':  round(obv_slope, 2),
            }
    except Exception as e:
        log(f"[AKUM-ENTRY-A] error: {e}")
    return None


def detect_entry_b_breakout(df, resistance: float, support: float, resistance_retest_low: float | None = None) -> dict | None:
    """
    Deteksi Entry B — Breakout + Retest Resistance Wyckoff (TF 4h).
    Kondisi:
    1. Ada candle yang close > resistance dengan vol > AKUM_B_VOL_BREAKOUT_MULT × vol MA
    2. Setelah breakout, harga turun retest ke resistance ± AKUM_B_RETEST_TOL_PCT
    3. Retest tidak tembus resistance (low >= resistance * (1 - AKUM_B_RETEST_TOL_PCT))
    4. Volume retest < AKUM_B_RETEST_VOL_MAX × volume candle breakout
    5. EMA20 > EMA50 (konfirmasi uptrend)
    Return: dict info atau None.
    """
    if len(df) < 20: return None
    if resistance_retest_low is None:
        resistance_retest_low = resistance
    try:
        import pandas_ta as _pta
        df = df.copy()
        df['vol_ma'] = df['vol'].rolling(20).mean()
        df['ema20']  = _pta.ema(df['close'], length=20)
        df['ema50']  = _pta.ema(df['close'], length=50)

        # Cek EMA konfirmasi
        last = df.iloc[-1]
        if pd.isna(last.get('ema20')) or pd.isna(last.get('ema50')): return None
        if last['ema20'] <= last['ema50']: return None

        # Cari candle breakout: close > resistance + vol tinggi
        breakout_idx = None
        breakout_vol = 0.0
        for i in range(len(df) - 10, len(df) - 1):
            if i < 5: continue
            r = df.iloc[i]
            vol_ma = r.get('vol_ma', 0)
            if pd.isna(vol_ma) or vol_ma <= 0: continue
            if r['close'] > resistance and r['vol'] >= AKUM_B_VOL_BREAKOUT_MULT * vol_ma:
                breakout_idx = i
                breakout_vol = float(r['vol'])
                break
        if breakout_idx is None: return None

        # Cari candle retest setelah breakout
        retest_low = resistance_retest_low * (1 - AKUM_B_RETEST_TOL_PCT)
        retest_high = resistance * (1 + AKUM_B_RETEST_TOL_PCT)
        for j in range(breakout_idx + 1, len(df)):
            r = df.iloc[j]
            # Harga turun ke zona resistance (structural/exreme ref)
            if r['low'] <= retest_high and r['low'] >= retest_low:
                # Tidak tembus ke bawah (support tetap aman)
                if r['low'] < retest_low: continue
                # Volume retest lebih rendah dari breakout
                if breakout_vol > 0 and r['vol'] >= AKUM_B_RETEST_VOL_MAX * breakout_vol: continue
                # Retest valid — candle terakhir sudah kembali di atas resistance
                if df.iloc[-1]['close'] < resistance: continue
                sl_price = round(resistance_retest_low * (1 - AKUM_ENTRY_SL_BUFFER), 8)
                return {
                    'type':          'B',
                    'resistance':    resistance,
                    'resistance_retest_low': resistance_retest_low,
                    'support':       support,
                    'sl_price':      sl_price,
                    'breakout_idx':  breakout_idx,
                    'ema20':         round(float(last['ema20']), 6),
                    'ema50':         round(float(last['ema50']), 6),
                    'retest_low':    round(float(r['low']), 8),
                    'vol_ratio_bo':  round(float(df.iloc[breakout_idx]['vol']) /
                                          float(df.iloc[breakout_idx]['vol_ma']), 2),
                }
    except Exception as e:
        log(f"[AKUM-ENTRY-B] error: {e}")
    return None


def diagnose_entry_a_spring(df, support: float, support_reentry: float | None = None) -> tuple:
    """Diagnosa (bukan trigger) -- jelasin syarat MANA yang masih ganjal buat Entry A,
    dipakai buat tampilan dashboard "Entry Status" biar informatif (bukan cuma 'X belum').
    Nyari kandidat spring yang paling JAUH kepenuhan syaratnya, lalu laporkan penghalangnya.
    Return (pct, msg): pct 0-100 = seberapa banyak dari 5 syarat sudah kepenuhan (20% per syarat)."""
    if support_reentry is None:
        support_reentry = support
    if len(df) < 20:
        return (0, "data candle kurang")
    try:
        import pandas_ta as _pta
        df = df.copy()
        df['vol_ma'] = df['vol'].rolling(20).mean()
        df['rsi'] = _pta.rsi(df['close'], length=14)
        obv = [0.0]
        for i in range(1, len(df)):
            if df['close'].iloc[i] > df['close'].iloc[i-1]:
                obv.append(obv[-1] + df['vol'].iloc[i])
            elif df['close'].iloc[i] < df['close'].iloc[i-1]:
                obv.append(obv[-1] - df['vol'].iloc[i])
            else:
                obv.append(obv[-1])
        df['obv'] = obv

        best = None  # (stage_reached, message)
        for lookback in range(3, min(AKUM_A_REENTRY_CANDLES + 4, len(df) - 1)):
            spring_idx = len(df) - 1 - lookback
            if spring_idx < 5: break
            row_s = df.iloc[spring_idx]
            if row_s['low'] >= support * (1 + AKUM_A_SUPPORT_TOUCH_BUFFER):
                continue
            vol_ma = row_s.get('vol_ma', 0)
            if pd.isna(vol_ma) or vol_ma <= 0:
                continue
            vol_ratio = float(row_s['vol']) / float(vol_ma)
            if row_s['vol'] < AKUM_A_VOL_SPIKE_MULT * vol_ma:
                cand = (1, f"low tembus support, vol cuma {vol_ratio:.2f}x (butuh >={AKUM_A_VOL_SPIKE_MULT}x)")
                if best is None or cand[0] > best[0]: best = cand
                continue
            reentry_ok = False
            for k in range(1, AKUM_A_REENTRY_CANDLES + 1):
                if spring_idx + k >= len(df): break
                if df.iloc[spring_idx + k]['close'] > support_reentry:
                    reentry_ok = True; break
            if not reentry_ok:
                cand = (2, f"spring vol {vol_ratio:.2f}x OK, belum reclaim balik atas support")
                if best is None or cand[0] > best[0]: best = cand
                continue
            rsi_window = df['rsi'].iloc[max(0, spring_idx-3):spring_idx+1]
            if pd.isna(rsi_window.min()) or rsi_window.min() >= AKUM_A_RSI_MIN:
                cand = (3, f"spring+reclaim OK, RSI nggak sempat <{AKUM_A_RSI_MIN}")
                if best is None or cand[0] > best[0]: best = cand
                continue
            rsi_now = df['rsi'].iloc[-1]
            if pd.isna(rsi_now) or rsi_now >= AKUM_A_RSI_MAX_ENTRY:
                _rn = f"{rsi_now:.1f}" if not pd.isna(rsi_now) else "n/a"
                cand = (4, f"tinggal RSI turun -- sekarang {_rn}, butuh <{AKUM_A_RSI_MAX_ENTRY}")
                if best is None or cand[0] > best[0]: best = cand
                continue
            obv_recent = df['obv'].iloc[-AKUM_A_OBV_SLOPE_CANDLES:]
            obv_slope = obv_recent.iloc[-1] - obv_recent.iloc[0]
            if obv_slope <= 0:
                cand = (5, "tinggal OBV slope positif -- masih negatif/flat")
                if best is None or cand[0] > best[0]: best = cand
                continue
            return (100, "siap trigger")
        if best is None:
            return (0, "belum ada candle nembus support")
        return ((best[0] - 1) * 20, best[1])
    except Exception as e:
        return (0, f"error diagnosa: {e}")


def diagnose_entry_b_breakout(df, resistance: float, support: float, resistance_retest_low: float | None = None) -> tuple:
    """Diagnosa (bukan trigger) -- jelasin syarat MANA yang masih ganjal buat Entry B.
    Return (pct, msg): pct 0-100 = seberapa banyak dari 4 syarat sudah kepenuhan (25% per syarat)."""
    if resistance_retest_low is None:
        resistance_retest_low = resistance
    if len(df) < 20:
        return (0, "data candle kurang")
    try:
        import pandas_ta as _pta
        df = df.copy()
        df['vol_ma'] = df['vol'].rolling(20).mean()
        df['ema20']  = _pta.ema(df['close'], length=20)
        df['ema50']  = _pta.ema(df['close'], length=50)
        last = df.iloc[-1]
        if pd.isna(last.get('ema20')) or pd.isna(last.get('ema50')):
            return (0, "data EMA kurang")
        if last['ema20'] <= last['ema50']:
            gap = (float(last['ema20'])/float(last['ema50'])-1)*100
            return (0, f"EMA20 masih di bawah EMA50 ({gap:+.2f}%), belum uptrend confirm")

        breakout_idx = None; breakout_vol = 0.0; best_vol_ratio = None
        for i in range(len(df) - 10, len(df) - 1):
            if i < 5: continue
            r = df.iloc[i]
            vol_ma = r.get('vol_ma', 0)
            if pd.isna(vol_ma) or vol_ma <= 0: continue
            if r['close'] > resistance:
                vol_ratio = float(r['vol']) / float(vol_ma)
                if vol_ratio >= AKUM_B_VOL_BREAKOUT_MULT:
                    breakout_idx = i; breakout_vol = float(r['vol']); break
                elif best_vol_ratio is None or vol_ratio > best_vol_ratio:
                    best_vol_ratio = vol_ratio
        if breakout_idx is None:
            price_now = float(df.iloc[-1]['close'])
            gap_pct = (price_now / resistance - 1) * 100
            if best_vol_ratio is not None:
                return (25, f"pernah tembus resistance tp vol cuma {best_vol_ratio:.2f}x (butuh >={AKUM_B_VOL_BREAKOUT_MULT}x), harga skrg {gap_pct:+.2f}% dari resistance")
            return (25, f"belum breakout, harga skrg {gap_pct:+.2f}% dari resistance")

        retest_low = resistance_retest_low * (1 - AKUM_B_RETEST_TOL_PCT)
        retest_high = resistance * (1 + AKUM_B_RETEST_TOL_PCT)
        for j in range(breakout_idx + 1, len(df)):
            r = df.iloc[j]
            if r['low'] <= retest_high and r['low'] >= retest_low:
                if r['low'] < retest_low: continue
                if breakout_vol > 0 and r['vol'] >= AKUM_B_RETEST_VOL_MAX * breakout_vol:
                    return (75, "breakout + retest ketemu, tp volume retest kegedean")
                if df.iloc[-1]['close'] < resistance:
                    gap_pct = (float(df.iloc[-1]['close']) / resistance - 1) * 100
                    return (75, f"breakout+retest OK, tinggal harga balik di atas resistance ({gap_pct:+.2f}%)")
                return (100, "siap trigger")
        return (50, "sudah breakout, nunggu retest ke zona resistance")
    except Exception as e:
        return (0, f"error diagnosa: {e}")


def thread_akum_scan():
    """Scan Akumulasi Detector: cari pair dalam fase akumulasi (sideways post-downtrend)."""
    global _akum_near_miss, _akum_last_scan_ts
    if not STRAT_AKUM_ENABLED:
        return
    log("[T_AKUM] Scan Akumulasi Detector (4h)...")
    try:
        pairs = get_usdt_spot_pairs()
        if not pairs:
            log("[T_AKUM] Gagal ambil pair."); return
        ticker = get_ticker_24h()
        volmap = {}
        for t in (ticker or []):
            try: volmap[t['symbol']] = float(t.get('quoteVolume', 0))
            except: pass
        universe = [p for p in pairs
                    if volmap.get(p, 0) >= AKUM_MIN_VOL_USD
                    and p not in SYMBOL_BLACKLIST]

        results = []
        akum_blockers = {}
        for sym in universe:
            try:
                df = get_ohlcv(sym, interval=AKUM_TIMEFRAME, limit=AKUM_CANDLE_LIMIT)
                if df is None or len(df) < AKUM_SIDEWAYS_CANDLES + 50:
                    count_blocker(akum_blockers, "Data candle kurang", True)
                    continue
                res = score_akumulasi(df, sym)
                if res is None:
                    count_blocker(akum_blockers, "Detector tidak dapat dinilai", True)
                    continue
                for flag, label in (
                    ("p1_ok", "Range sideways"),
                    ("p2_ok", "EMA20-EMA200 gap"),
                    ("p3_ok", "OBV slope positif"),
                    ("p4_ok", "ATR turun"),
                    ("p5_ok", "EMA20 slope"),
                    ("p6_ok", "Close drift"),
                    ("p7_ok", "Distribusi range"),
                    ("s1_ok", "Volume asimetri"),
                    ("s2_ok", "RSI 30-56"),
                    ("s3_ok", "MACD flat"),
                    ("s4_ok", "Body ratio"),
                ):
                    count_blocker(akum_blockers, label, not res.get(flag, True))
                # Hanya simpan yang lolos minimal 3 dari 4 primary
                if res['primary_score'] >= 3:
                    results.append(res)
            except Exception as e:
                log(f"  [T_AKUM] error {sym}: {e}")

        # Sort: gating_ok + primary_ok dulu, lalu weighted_score descending
        results.sort(key=lambda x: (not x['gating_ok'], not x['primary_ok'], -x['weighted_score']))
        top = results[:AKUM_MAX_RESULTS]

        record_scan_blockers("Akumulasi-4h", len(universe), len(results), akum_blockers)
        ts = now_wib().strftime("%H:%M:%S")
        with _akum_lock:
            _akum_near_miss    = top
            _akum_last_scan_ts = ts

        # Format near_miss untuk update_dashboard_near_miss
        # Struktur: (n_pass, sym, fails, total_syarat)  — total 8 (4P×2 + 4S×1 = 12 → pakai skor mentah)
        nm_items = [
            (res['primary_score'], res['sym'], res['fails'], 4,
             res.get('sideways_start',''),
             res.get('support', 0), res.get('resistance', 0),
             res.get('weighted_score', 0), res.get('gating_ok', False),
             res.get('close', 0),
             res.get('support_zone_low', 0), res.get('support_zone_high', 0),
             res.get('resistance_zone_low', 0), res.get('resistance_zone_high', 0),
             res.get('support_zone_touches', 0), res.get('resistance_zone_touches', 0))
            for res in top
        ]
        update_dashboard_near_miss("Akumulasi-4h", nm_items)

        # Log near_miss ke file (sama dengan strategi lain)
        akum_near_miss_log = [
            (res['primary_score'], res['sym'], res['fails'], 4)
            for res in top
        ]
        log_near_miss("Akumulasi-4h", akum_near_miss_log, 4)

        n_full = sum(1 for r in top if r['primary_ok'] and r['secondary_ok'])
        log(f"[T_AKUM] {len(universe)} pair discan → {len(results)} kandidat, "
            f"{n_full} lolos penuh. Top {len(top)} ditampilkan.")

        # Simpan top 5 untuk General heartbeat
        global _akum_top5
        _akum_top5 = top[:5]

        # Kirim notif Telegram detail (format asli)
        if top:
            ts_str = now_wib().strftime("%H:%M:%S") + " WIB"
            tg_lines = [f"AKUMULASI TERDETEKSI (Strategi #5)\n{ts_str}"]
            for r in top[:5]:
                gate_str = "✓ Gating OK" if r.get('gating_ok') else "⚠ Gating BELUM"
                tg_lines.append(
                    f"• {to_display_pair(r['sym'])} | Skor {r.get('weighted_score',0)}/100 ({gate_str})\n"
                    f"Current Price: {_fmt_price(r.get('close',0))}\n"
                    f"Sideways sejak: {r.get('sideways_start','-')}\n"
                    f"Support: {_fmt_price(r.get('support',0))} | Resistance: {_fmt_price(r.get('resistance',0))}\n"
                    f"SL-A est: {_fmt_price(r.get('sl_a',0))} | SL-B est: {_fmt_price(r.get('sl_b',0))}\n"
                    f"Range {r.get('range_pct','-')} | EMAGap {r.get('ema_gap_pct','-')} | OBV slope {r.get('obv_slope','-')}\n"
                    f"ATR turun {r.get('atr_drop','—')} | RSI {float(r.get('rsi',0)):.1f} | Vol G:{float(r.get('vol_ratio',0)):.2f}x"
                )
            if HEARTBEAT_TELEGRAM_ENABLED:
                send_telegram('\n'.join(tg_lines), parse_mode=None)
            log(f"[T_AKUM] Notif Akumulasi terkirim: {n_full} lolos penuh")

    except Exception as e:
        log(f"WARN [T_AKUM] scan error: {e}")


def run_thread_akum():
    """Thread T_AKUM: scan akumulasi tiap AKUM_SCAN_INTERVAL detik."""
    log("[T_AKUM] Thread akumulasi dimulai.")
    while True:
        try:
            thread_akum_scan()
        except Exception as e:
            log(f"WARN [T_AKUM] thread error: {e}")
        time.sleep(AKUM_SCAN_INTERVAL)


def thread_akum_entry_scan():
    """
    Scan Entry A (Spring) dan Entry B (Breakout Retest) untuk pair yang
    sudah terdeteksi dalam fase akumulasi oleh T_AKUM.
    Jalan tiap AKUM_ENTRY_SCAN_INTERVAL detik.
    """
    with _akum_lock:
        kandidat = list(_akum_near_miss)
    if not kandidat:
        log("[T_AKUM_ENTRY] Tidak ada kandidat akumulasi, skip.")
        return

    # Pass 1: Update status Entry A/B untuk semua kandidat (tanpa guard slot)
    log(f"[T_AKUM_ENTRY] Pass 1: update status {len(kandidat)} kandidat")
    for item in kandidat:
        sym        = item.get('sym', '')
        support    = item.get('support')
        resistance = item.get('resistance')
        support_zone_low = item.get('support_zone_low')
        support_zone_high = item.get('support_zone_high')
        resistance_zone_low = item.get('resistance_zone_low')
        resistance_zone_high = item.get('resistance_zone_high')
        if not sym or support is None or resistance is None: continue
        if sym in SYMBOL_BLACKLIST: continue
        try:
            df = get_ohlcv_4h(sym, limit=AKUM_CANDLE_LIMIT)
            if df is None or len(df) < AKUM_SIDEWAYS_CANDLES + 50:
                log(f"[T_AKUM_ENTRY] {sym}: data kurang ({len(df) if df is not None else 'None'}), skip status")
                continue
            if df['ct'].iloc[-1] >= int(time.time() * 1000):
                df = df.iloc[:-1]
            if len(df) < AKUM_SIDEWAYS_CANDLES + 10: continue
            has_struct = (
                support_zone_low is not None and support_zone_high is not None and
                resistance_zone_low is not None and resistance_zone_high is not None and
                float(support_zone_low) > 0 and float(support_zone_high) > 0 and
                float(resistance_zone_low) > 0 and float(resistance_zone_high) > 0 and
                float(support_zone_low) <= float(support_zone_high) <= float(resistance_zone_low) <= float(resistance_zone_high)
            )
            support_break_ref = float(support_zone_low) if has_struct else float(support)
            support_reentry_ref = float(support_zone_high) if has_struct else float(support)
            resistance_break_ref = float(resistance_zone_high) if has_struct else float(resistance)
            resistance_retest_low_ref = float(resistance_zone_low) if has_struct else float(resistance)

            sig_a = detect_entry_a_spring(df, support_break_ref, support_reentry_ref)
            sig_b = detect_entry_b_breakout(df, resistance_break_ref, support_break_ref, resistance_retest_low_ref)
            # Diagnosa (bukan trigger) -- jelasin syarat mana yg masih ganjal + persentase progress,
            # ditampilkan di dashboard "Entry Status" biar informatif ketimbang cuma "X belum" polos.
            pct_a, reason_a = (100, "siap trigger") if sig_a is not None else diagnose_entry_a_spring(df, support_break_ref, support_reentry_ref)
            pct_b, reason_b = (100, "siap trigger") if sig_b is not None else diagnose_entry_b_breakout(df, resistance_break_ref, support_break_ref, resistance_retest_low_ref)
            rating_pct = max(pct_a, pct_b)
            ts_entry = now_wib().strftime("%H:%M")
            with _akum_lock:
                _akum_entry_status[sym] = {
                    "entry_a": sig_a is not None,
                    "entry_b": sig_b is not None,
                    "reason_a": reason_a,
                    "reason_b": reason_b,
                    "pct_a": pct_a,
                    "pct_b": pct_b,
                    "rating_pct": rating_pct,
                    "ts": ts_entry,
                }
            log(f"[T_AKUM_ENTRY] {sym}: A={'✓' if sig_a else '✗'} B={'✓' if sig_b else '✗'}")
        except Exception as e:
            log(f"[T_AKUM_ENTRY] error status {sym}: {e}")

    # Log near_miss Entry A/B ke file
    entry_near_miss_log = []
    with _akum_lock:
        status_now = dict(_akum_entry_status)
    for sym, es in status_now.items():
        fails = []
        if not es.get('entry_a'): fails.append("Entry A (Spring): belum terpenuhi")
        if not es.get('entry_b'): fails.append("Entry B (Breakout): belum terpenuhi")
        if fails:
            entry_near_miss_log.append(("A=✗B=✗" if not es.get('entry_a') and not es.get('entry_b') else "partial", sym, fails, 2))
    if entry_near_miss_log:
        log_near_miss("Akumulasi-4h Entry", entry_near_miss_log, 2)

    # Pass 2: Open deal (dengan guard slot dan active_deals)
    n_akum = active_deal_count_akum()
    if n_akum >= AKUM_ENTRY_MAX_DEALS:
        log(f"[T_AKUM_ENTRY] Slot penuh {n_akum}/{AKUM_ENTRY_MAX_DEALS}, skip open.")
        return

    held_akum = {}   # 03/09/2026 pola 2-babak: symbol -> data kandidat yg lolos babak 1
    for item in kandidat:
        if len(held_akum) >= AI_BATCH_POOL_SIZE:
            log(f"[T_AKUM_ENTRY] Kolam babak-1 penuh ({AI_BATCH_POOL_SIZE}), berhenti kumpulkan kandidat baru siklus ini.")
            break
        sym   = item.get('sym', '')
        score = item.get('weighted_score') or item.get('total_score') or item.get('score', 0)
        support    = item.get('support')
        resistance = item.get('resistance')
        support_zone_low = item.get('support_zone_low')
        support_zone_high = item.get('support_zone_high')
        resistance_zone_low = item.get('resistance_zone_low')
        resistance_zone_high = item.get('resistance_zone_high')
        if not sym or support is None or resistance is None: continue

        with active_deals_lock:
            if sym in active_deals: continue
        if sym in SYMBOL_BLACKLIST: continue

        try:
            df = get_ohlcv_4h(sym, limit=AKUM_CANDLE_LIMIT)
            if df is None or len(df) < AKUM_SIDEWAYS_CANDLES + 50: continue
            if df['ct'].iloc[-1] >= int(time.time() * 1000):
                df = df.iloc[:-1]
            if len(df) < AKUM_SIDEWAYS_CANDLES + 10: continue

            has_struct = (
                support_zone_low is not None and support_zone_high is not None and
                resistance_zone_low is not None and resistance_zone_high is not None and
                float(support_zone_low) > 0 and float(support_zone_high) > 0 and
                float(resistance_zone_low) > 0 and float(resistance_zone_high) > 0 and
                float(support_zone_low) <= float(support_zone_high) <= float(resistance_zone_low) <= float(resistance_zone_high)
            )
            support_break_ref = float(support_zone_low) if has_struct else float(support)
            support_reentry_ref = float(support_zone_high) if has_struct else float(support)
            resistance_break_ref = float(resistance_zone_high) if has_struct else float(resistance)
            resistance_retest_low_ref = float(resistance_zone_low) if has_struct else float(resistance)

            # Coba Entry A dulu untuk open deal
            sig = detect_entry_a_spring(df, support_break_ref, support_reentry_ref)
            entry_type = 'A'
            if sig is None:
                sig = detect_entry_b_breakout(df, resistance_break_ref, support_break_ref, resistance_retest_low_ref)
                entry_type = 'B'
            if sig is None: continue

            # Open deal
            price_now = get_price_now(sym)
            if price_now <= 0: continue
            strat_key = f'akum_entry_{entry_type.lower()}'
            if is_cooldown_enabled(strat_key) and cooldown_remaining(sym) > 0:
                log(f"[T_AKUM_ENTRY] {sym} cooldown {cooldown_remaining(sym)/3600:.1f}j — skip")
                continue
            if sizing_fail_remaining(sym) > 0:
                log(f"[T_AKUM_ENTRY] {sym} baru gagal sizing/open (sisa {sizing_fail_remaining(sym)/60:.0f} menit) — skip, jangan coba lagi dulu.")
                continue
            # RSI di titik sinyal ini (01/09/2026) -- df['close'] sudah tersedia dari scan
            # score_akumulasi() di atas, cukup buat RSI(14) langsung, dipakai utk konteks AI +
            # log CSV permanen (Akumulasi sebelumnya TIDAK PERNAH tercatat rsi_open sama sekali).
            try:
                _rsi_akum = float(ta.rsi(df['close'], length=14).iloc[-1])
                if pd.isna(_rsi_akum): _rsi_akum = None
            except Exception:
                _rsi_akum = None

            if is_ai_call_open_enabled('akum_entry_a'):
                _ai_ind = {'entry_type': entry_type, 'price_now': _fmt_price(price_now),
                           'atr_pct': f"{item.get('atr_pct', 3.0):.2f}%"}
                if _rsi_akum is not None: _ai_ind['rsi'] = f"{_rsi_akum:.1f}"
                if not ai_decision_open(sym, f'Akumulasi-4h Entry {entry_type}', _ai_ind, active_deal_count(), notify=False):
                    log(f"[T_AKUM_ENTRY] {sym} babak-1 di-skip oleh AI individual")
                    continue

            held_akum[sym] = {
                'entry_type': entry_type, 'strat_key': strat_key, 'sig': sig, 'score': score,
                'support': support, 'resistance': resistance,
                'support_zone_low': support_zone_low, 'support_zone_high': support_zone_high,
                'resistance_zone_low': resistance_zone_low, 'resistance_zone_high': resistance_zone_high,
                'support_reentry_ref': support_reentry_ref, 'resistance_break_ref': resistance_break_ref,
                'has_struct': has_struct, 'atrp_item': item.get('atr_pct', 3.0),
                'price_now': price_now, 'rsi_akum': _rsi_akum, 'df_ct_last': int(df['ct'].iloc[-1]),
                'detail': {'atr_pct': item.get('atr_pct', 3.0), 'rvol': 0.0, 'bb_pct': 0.0,
                           'gap_ema20_pct': 0.0, 'rsi': _rsi_akum or 0.0},
            }
        except Exception as e:
            log(f"[T_AKUM_ENTRY] error {sym}: {e}")

    # ============ BABAK 2: AI bandingkan SEMUA yg lolos babak 1 sekaligus ============
    approved_akum = []
    if held_akum:
        log(f"[T_AKUM_ENTRY] Babak 1 selesai: {len(held_akum)} lolos AI individual. Lanjut babak 2 (AI batch re-analysis)...")
        _lolos_disp = [f"{to_display_pair(s)} ({v['entry_type']})" for s, v in held_akum.items()]
        log_ai_babak1('Akumulasi-4h', _lolos_disp, AI_BATCH_MAX_APPROVE)
        batch_input_akum = [{'symbol': s, 'strategy': v['strat_key'], 'score': v['score'], 'detail': v['detail']}
                             for s, v in held_akum.items()]
        approved_akum = ai_decision_batch_rank(
            batch_input_akum, strategy_label='Akumulasi-4h', max_approve=AI_BATCH_MAX_APPROVE,
            criteria_note="Kriteria: skor, ATR%, RSI (Akumulasi-4h tidak hitung RVOL/BB%b/EMA20 -- basis Wyckoff S/R, bukan EMA)",
        )

    for sym in approved_akum:
        n_akum = active_deal_count_akum()
        if n_akum >= AKUM_ENTRY_MAX_DEALS:
            log(f"[T_AKUM_ENTRY] Slot penuh {n_akum}/{AKUM_ENTRY_MAX_DEALS}, sisa kandidat tidak dibuka.")
            break
        with active_deals_lock:
            if sym in active_deals: continue
        v = held_akum[sym]
        entry_type = v['entry_type']; strat_key = v['strat_key']; sig = v['sig']; score = v['score']
        support = v['support']; resistance = v['resistance']
        support_zone_low = v['support_zone_low']; support_zone_high = v['support_zone_high']
        resistance_zone_low = v['resistance_zone_low']; resistance_zone_high = v['resistance_zone_high']
        support_reentry_ref = v['support_reentry_ref']; resistance_break_ref = v['resistance_break_ref']
        has_struct = v['has_struct']; price_now = v['price_now']; _rsi_akum = v['rsi_akum']
        _atrp_item = v['atrp_item']

        try:
            ok, target_usd, add_usd = open_deal_with_sizing(sym, score, strat_key)
            if not ok:
                reject_reason = "Binance order ditolak (cek saldo/lot size)" if USE_BINANCE_DIRECT else "3Commas tolak (cek saldo/slot)"
                log(f"[T_AKUM_ENTRY] {sym} Entry {entry_type}: {reject_reason}")
                send_telegram(
                    f"⚠️ Akumulasi-4h | GAGAL OPEN\n"
                    f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                    f"Pair  : {to_display_pair(sym)}\n"
                    f"Entry : {entry_type} | Score: {score}\n"
                    f"Harga : {_fmt_price(price_now)}\n"
                    f"Alasan: {reject_reason}",
                    parse_mode=None
                )
                continue

            ts = now_wib().strftime('%Y-%m-%d %H:%M:%S')
            add_to_active_deals(sym, {
                'strategy':        strat_key,
                'entry_price':     price_now,
                'peak':            price_now,
                'signal_price':    price_now,
                'sl_price':        sig['sl_price'],
                'atr_pct':         _atrp_item,
                'opened_candle_ts': v['df_ct_last'],
                'trailing_armed':  False,
                'opened_at':       ts,
                'target_usd':      target_usd,
                'add_usd':         add_usd,
                'tf':              AKUM_TIMEFRAME,
                'akum_score':      score,
                'entry_type':      entry_type,
                'support':         support,
                'resistance':      resistance,
                'support_zone_low': support_zone_low,
                'support_zone_high': support_zone_high,
                'resistance_zone_low': resistance_zone_low,
                'resistance_zone_high': resistance_zone_high,
                'support_ref':     support_reentry_ref,
                'resistance_ref':  resistance_break_ref,
                'timeout_candles': AKUM_ENTRY_TIMEOUT,
                'rsi_open':        _rsi_akum,
            })
            csv_log_open({
                'open_time_wib':  ts,
                'symbol':         to_display_pair(sym),
                'signal_price':   f"{_fmt_price(price_now)}",
                'entry_price':    f"{_fmt_price(price_now)}",
                'slip_pct':       '0.00',
                'atr_pct':        f"{_atrp_item:.2f}",
                'trail_dist_pct': f"{trailing_dist(_atrp_item)}",
                'base_usd':       target_usd,
                'score':          score,
                'strategy':       strat_key,
                'rsi_open':       f"{_rsi_akum:.1f}" if _rsi_akum is not None else '',
            })
            log(f"[T_AKUM_ENTRY] {sym} OPEN Entry {entry_type} @ {_fmt_price(price_now)} "
                f"SL={_fmt_price(sig['sl_price'])} score={score}")
            send_telegram(
                f"Akumulasi-4h | OPEN LONG Entry {entry_type}\n"
                f"{ts} WIB\n"
                f"Pair      : {to_display_pair(sym)}\n"
                f"Entry     : {_fmt_price(price_now)}\n"
                f"SL        : {_fmt_price(sig['sl_price'])}\n"
                f"Support   : {_fmt_price(support)}\n"
                f"Resistance: {_fmt_price(resistance)}\n"
                f"Ref S/R   : {_fmt_price(support_reentry_ref)} / {_fmt_price(resistance_break_ref)}"
                + (f" (struct zone)\n" if has_struct else " (extreme fallback)\n")
                + f"Score     : {score} | Target: ${target_usd}"
            )
            log_oac('OPEN', sym, f'Akumulasi-4h Entry {entry_type}', {
                'entry_price':  _fmt_price(price_now),
                'sl_price':     _fmt_price(sig['sl_price']),
                'support':      _fmt_price(support),
                'resistance':   _fmt_price(resistance),
                'support_ref':  _fmt_price(support_reentry_ref),
                'resistance_ref': _fmt_price(resistance_break_ref),
                'ref_basis':    'struct_zone' if has_struct else 'extreme_fallback',
                'score':        score,
                'target_usd':   f"${target_usd}",
            })

        except Exception as e:
            log(f"[T_AKUM_ENTRY] error {sym}: {e}")


def run_thread_akum_entry():
    """Thread T_AKUM_ENTRY: scan entry A/B tiap AKUM_ENTRY_SCAN_INTERVAL detik."""
    log("[T_AKUM_ENTRY] Thread entry akumulasi dimulai.")
    time.sleep(120)  # tunggu 2 menit agar T_AKUM sempat selesai scan pertama
    while True:
        try:
            thread_akum_entry_scan()
        except Exception as e:
            log(f"WARN [T_AKUM_ENTRY] thread error: {e}")
        time.sleep(AKUM_ENTRY_SCAN_INTERVAL)



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
            from flask import Flask, render_template_string, request, redirect, jsonify, session
        except ImportError:
            import subprocess
            subprocess.run(["pip", "install", "flask", "--quiet", "--break-system-packages"],
                          capture_output=True)
            from flask import Flask, render_template_string, request, redirect, jsonify, session

        app = Flask(__name__)
        app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32)
        app.config.update(
            SESSION_COOKIE_SECURE=True,
            SESSION_COOKIE_HTTPONLY=True,
            SESSION_COOKIE_SAMESITE="Lax",
        )
        dashboard_username = os.environ.get("DASHBOARD_USERNAME", "")
        dashboard_password_hash = os.environ.get("DASHBOARD_PASSWORD_HASH", "")
        dashboard_totp_secret = os.environ.get("DASHBOARD_TOTP_SECRET", "").replace(" ", "").upper()

        def dashboard_login_required():
            if not dashboard_username or not dashboard_password_hash:
                return None
            if session.get("dashboard_authenticated"):
                return None
            if request.path in {"/login", "/logout", "/dash.js", "/tradingview_webhook"} or request.method == "OPTIONS":
                return None
            if request.path.startswith("/api/") or request.is_json:
                return jsonify({"ok": False, "error": "login diperlukan"}), 401
            return redirect("/login")

        @app.before_request
        def require_dashboard_login():
            return dashboard_login_required()

        @app.route("/login", methods=["GET", "POST"])
        def dashboard_login():
            error = ""
            if request.method == "POST":
                from werkzeug.security import check_password_hash
                username = request.form.get("username", "")
                password = request.form.get("password", "")
                otp = request.form.get("otp", "").replace(" ", "")
                password_ok = username == dashboard_username and check_password_hash(dashboard_password_hash, password)
                totp_ok = True
                if dashboard_totp_secret:
                    try:
                        import pyotp
                        totp_ok = pyotp.TOTP(dashboard_totp_secret).verify(otp, valid_window=1)
                    except Exception:
                        totp_ok = False
                if password_ok and totp_ok:
                    session.clear()
                    session["dashboard_authenticated"] = True
                    return redirect("/")
                error = "Username, password, atau kode authenticator salah."
            return render_template_string("""
<!doctype html><html lang="id"><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Dashboard Login</title>
<style>body{background:#0f1117;color:#e2e8f0;font:14px monospace;display:grid;place-items:center;min-height:100vh}form{background:#1a1d2e;border:1px solid #2a2d3e;border-radius:8px;padding:24px;width:min(340px,calc(100% - 40px))}h1{color:#4f9eff;font-size:16px}label{display:block;margin:14px 0 6px;color:#8892a4}input{width:100%;box-sizing:border-box;padding:9px;background:#0f1117;color:#e2e8f0;border:1px solid #2a2d3e;border-radius:4px}button{margin-top:18px;width:100%;padding:9px;background:#4f9eff;border:0;border-radius:4px;font-weight:bold}p{color:#ff4f6a}</style></head>
<body><form method="post"><h1>TRADING BOT DASHBOARD</h1>{% if error %}<p>{{ error }}</p>{% endif %}<label>Username</label><input name="username" autocomplete="username" required><label>Password</label><input type="password" name="password" autocomplete="current-password" required>{% if totp_enabled %}<label>Authenticator code</label><input name="otp" inputmode="numeric" autocomplete="one-time-code" pattern="[0-9]{6}" maxlength="6" required>{% endif %}<button type="submit">LOGIN</button></form></body></html>
""", error=error, totp_enabled=bool(dashboard_totp_secret))

        @app.route("/logout")
        def dashboard_logout():
            session.clear()
            return redirect("/login")

        @app.route("/")
        def index():
            with active_deals_lock:
                deals = dict(active_deals)
            # Saldo wallet SEKALI aja (bukan per-deal) -- buat deteksi deal yg "needs_reconcile"
            # (koinnya udah kejual di luar jalur normal tapi masih nyangkut di active_deals).
            _wallet_balances = {}
            if USE_BINANCE_DIRECT and deals:
                try:
                    _acct = _binance_trading_request("GET", "/api/v3/account", {})
                    for _b in (_acct or {}).get("balances", []):
                        _wallet_balances[_b["asset"]] = float(_b.get("free", 0) or 0)
                except Exception as _e:
                    log(f"WARN [DASHBOARD] gagal baca saldo wallet utk needs_reconcile: {_e}")
            deals_display = {}
            for sym, d in deals.items():
                dd = dict(d)
                ep = dd.get("entry_price", 0)
                lp = dd.get("last_price", ep)
                dd["upnl_pct"] = (lp/ep - 1)*100 - FEE_ROUND_TRIP_PCT if ep > 0 else 0
                # Modality real position: prefer actual qty * entry_price di Binance direct mode,
                # karena beberapa deal punya sequence buy multipel yang tidak ditangkap oleh base_usd statis $8.
                total_usd = estimate_deal_total_usd(dd)
                dd["total_usd_display"] = total_usd
                dd["upnl_usd"] = round(dd["upnl_pct"] / 100 * total_usd, 2)
                # Ambang checkbox "hold, jangan jual": 85% dari ambang hard-stop tier deal ini.
                # Akumulasi dikecualikan -- exit-nya via SL/TP sendiri, bukan hard_stop_pct().
                _near_hardstop = False
                if dd.get("strategy", "") not in ("akum_entry_a", "akum_entry_b"):
                    _hs_lbl, _hs_bs, _hs_full = hard_stop_pct(dd.get("atr_pct", 3.0))
                    dd["hardstop_full_pct"] = _hs_full
                    dd["hardstop_warn_pct"] = round(_hs_full * HOLD_NO_SELL_WARN_RATIO, 4)
                    _near_hardstop = dd["upnl_pct"] <= -1 * dd["hardstop_warn_pct"]
                # Checkbox "hold, jangan jual" (30/08/2026): SELALU tampil di dashboard, tapi
                # di-disable (dimmed) sampai deal ini beneran dekat bahaya -- dekat hard-stop, ATAU
                # tinggal <=1 candle lagi menuju timeout SAMBIL lagi rugi (upnl<0). Di luar itu,
                # checkbox terkunci (nggak bisa diklik) supaya nggak salah kesan "wajib diisi
                # sekarang" padahal belum relevan.
                _near_timeout_loss = False
                _opened_ts_s = (dd.get("opened_candle_ts", 0) or 0) / 1000.0
                if _opened_ts_s > 0 and dd["upnl_pct"] < 0:
                    _strat_d = dd.get("strategy", "brkX2")
                    if _strat_d == "reversal":
                        _hold_limit_sec_d = REVERSAL_MAX_HOLD_CANDLES * REVERSAL_SECONDS_PER_CANDLE
                    elif _strat_d == "brkX2_4h":
                        _hold_limit_sec_d = STRAT4H_MAX_HOLD_CANDLES * STRAT4H_SECONDS
                    elif _strat_d in ("brkX2_crossema", "hunting_4h"):
                        _hold_limit_sec_d = HUNTING_MAX_HOLD_CANDLES * STRAT4H_SECONDS
                    elif _strat_d in ("akum_entry_a", "akum_entry_b"):
                        _hold_limit_sec_d = (dd.get("timeout_candles", AKUM_ENTRY_TIMEOUT) or AKUM_ENTRY_TIMEOUT) * STRAT4H_SECONDS
                    else:
                        _hold_limit_sec_d = MAX_HOLD_DAYS * SECONDS_PER_CANDLE
                    _candle_sec_d = _candle_seconds_for_strategy(_strat_d)
                    _remaining_sec_d = _hold_limit_sec_d - (time.time() - _opened_ts_s)
                    _near_timeout_loss = 0 <= _remaining_sec_d <= _candle_sec_d
                dd["hold_no_sell_active"] = bool(_near_hardstop or _near_timeout_loss)
                # needs_reconcile: qty_coin tercatat (atau estimasi dari modal/entry_price kalau
                # qty_coin nggak ada -- kasus deal dibuka lewat webhook lama) jauh lebih besar
                # dari saldo wallet SEKARANG -> pertanda koin sudah kejual di luar jalur normal.
                if _wallet_balances:
                    _asset = sym.replace("USDT", "")
                    _qty_expected = float(dd.get("qty_coin", 0) or 0)
                    if _qty_expected <= 0 and ep > 0:
                        _qty_expected = total_usd / ep
                    _wallet_qty = _wallet_balances.get(_asset)
                    dd["needs_reconcile"] = bool(_wallet_qty is not None and _qty_expected > 0 and _wallet_qty <= _qty_expected * 0.05)
                deals_display[sym] = dd
            closest_to_close = estimate_closest_deal_to_close(deals_display)
            with _dashboard_lock:
                nm = dict(_dashboard_state["near_miss"])
                ls = dict(_dashboard_state["last_scan"])
            # Sisipkan hunting signals ke nm agar muncul di dropdown Manual Scan
            with _hunting_lock:
                nm["Hunting-4h"] = [
                    {"sym": s["symbol"], "n_pass": 4, "total": 4,
                     "dist_ema20": s.get("dist_ema20_pct", 0),
                     "fails": []}
                    for s in _hunting_signals
                ]
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
                "Akumulasi-4h": (
                    f"Scan tiap 30 menit. Deteksi fase akumulasi (sideways post-downtrend) TF 4h. "
                    f"Scan terakhir: {_akum_last_scan_ts or 'belum ada'}."
                ),
                "Hunting-4h": (
                    f"Scan Hunting terakhir: {_hunting_scan_ts}. "
                    "Belum ada kandidat yang memenuhi seluruh syarat saat scan terakhir."
                    if _hunting_scan_ts != "-" else
                    "Belum ada scan Hunting-4h."
                ),
            }
            with _akum_lock:
                akum_entry_status = dict(_akum_entry_status)
            try:
                free_usdt, _locked_usdt = get_usdt_balance() if USE_BINANCE_DIRECT else (0.0, 0.0)
            except Exception:
                free_usdt = 0.0
            return render_template_string(
                DASHBOARD_HTML,
                active_deals=deals_display,
                active_count=len(deals_display),
                closest_to_close=closest_to_close,
                near_miss=nm,
                last_scan=ls,
                overrides=overrides,
                now=now_wib().strftime("%d/%m %H:%M:%S WIB"),
                window_info=window_info,
                fmt_price=_fmt_price,
                akum_entry_status=akum_entry_status,
                free_usdt=free_usdt,
            )

        @app.route("/manual_addfund", methods=["POST"])
        def manual_addfund():
            sym = request.form.get("sym", "").upper().strip()
            if not sym:
                return jsonify({"ok": False, "error": "sym kosong"})
            # Normalize: SNXUSDT / SNX/USDT / snxusdt → SNXUSDT
            sym = sym.replace("/", "").replace("-", "")
            if not sym.endswith("USDT"):
                sym = sym + "USDT"
            with active_deals_lock:
                if sym not in active_deals:
                    return jsonify({"ok": False, "error": f"{sym} tidak ada di active_deals"})
                d = dict(active_deals[sym])
            add_usd = d.get('add_usd', 0)
            # Strategi tanpa add_usd (Akumulasi, brkX2_4h dll): pakai nominal dari form atau BASE_ORDER_VOLUME
            manual_amount = request.form.get("amount", "")
            if add_usd <= 0:
                try:
                    add_usd = float(manual_amount) if manual_amount else float(BASE_ORDER_VOLUME)
                except (ValueError, TypeError):
                    add_usd = float(BASE_ORDER_VOLUME)
            if d.get('add_fund_sent'):
                return jsonify({"ok": False, "error": "Add fund sudah pernah dikirim sebelumnya."})
            strat = d.get('strategy', 'brkX2')
            try:
                price_now = get_price_now(sym)
                if price_now <= 0:
                    return jsonify({"ok": False, "error": "Gagal ambil harga live"})
                ok = send_add_funds(sym, add_usd, strat)
                if ok:
                    # Hitung average price baru
                    entry_price = d.get('entry_price', price_now)
                    base_usd    = BASE_ORDER_VOLUME
                    avg_price   = (base_usd * entry_price + add_usd * price_now) / (base_usd + add_usd)
                    ts = now_wib().strftime('%Y-%m-%d %H:%M:%S')
                    with active_deals_lock:
                        if sym in active_deals:
                            active_deals[sym]['add_fund_sent'] = True
                            active_deals[sym]['entry_price']   = avg_price
                            active_deals[sym]['peak']          = max(active_deals[sym].get('peak', avg_price), avg_price)
                            active_deals[sym]['trailing_armed'] = False
                    # Simpan ke file
                    with active_deals_lock:
                        d2 = dict(active_deals)
                    try:
                        import json as _j
                        with open(ACTIVE_DEALS_FILE, 'w') as f:
                            _j.dump(d2, f)
                    except Exception as e:
                        log(f"[MANUAL-ADDFUND] Gagal simpan file: {e}")
                    log(f"[MANUAL-ADDFUND] {sym} add ${add_usd} @ {price_now:.6g} | avg={avg_price:.6g}")
                    send_telegram(
                        f"Bot | ADD FUND MANUAL\n"
                        f"{ts} WIB\n"
                        f"Pair     : {to_display_pair(sym)}\n"
                        f"Add fund : ${add_usd}\n"
                        f"Harga    : {_fmt_price(price_now)}\n"
                        f"Avg price: {_fmt_price(avg_price)}\n"
                        f"Strategi : {strat}"
                    )
                    return jsonify({"ok": True, "sym": sym, "add_usd": add_usd,
                                    "price": price_now, "avg_price": round(avg_price, 8)})
                else:
                    return jsonify({"ok": False, "error": "3Commas menolak add fund"})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)})

        @app.route("/manual_close", methods=["POST"])
        def manual_close():
            sym = request.form.get("sym", "").upper().strip()
            if not sym:
                return jsonify({"ok": False, "error": "sym kosong"})
            if not sym.endswith("USDT"):
                sym = sym + "USDT"
            with active_deals_lock:
                if sym not in active_deals:
                    return jsonify({"ok": False, "error": f"{sym} tidak ada di active_deals"})
                d = dict(active_deals[sym])
            strat = d.get('strategy', 'brkX2')
            try:
                price_now = get_price_now(sym)
                if price_now <= 0:
                    return jsonify({"ok": False, "error": "Gagal ambil harga live"})
                entry = d.get('entry_price', price_now)
                prof = (price_now / entry - 1) * 100 if entry > 0 else 0
                reason = "manual_close (dashboard)"
                if send_close_long(sym, strat):
                    ts = now_wib().strftime('%Y-%m-%d %H:%M:%S')
                    total_usd = estimate_deal_total_usd(d)
                    csv_log_close(to_display_pair(sym), ts, price_now, prof, reason, strategy=strat, base_usd=total_usd)
                    _opened_ts = (d.get('opened_candle_ts', 0) or 0) / 1000.0
                    _hold_c = round((time.time() - _opened_ts) / _candle_seconds_for_strategy(strat)) if _opened_ts > 0 else ''
                    deal_log_write({
                        'timestamp_wib': ts,
                        'event_type':    'CLOSE',
                        'strategy':      strat,
                        'symbol':        to_display_pair(sym),
                        'thread':        'MANUAL',
                        'entry_price':   _fmt_price(entry) if entry else '',
                        'exit_price':    _fmt_price(price_now),
                        'profit_pct':    f"{prof:.2f}",
                        'exit_reason':   reason,
                        'trailing_armed':str(d.get('trailing_armed', False)),
                        'hold_candles':  str(_hold_c),
                        'atr_pct':       f"{d.get('atr_pct', ''):.2f}" if d.get('atr_pct') else '',
                        'score':         d.get('score', ''),
                        'total_usd':     d.get('target_usd', ''),
                    })
                    remove_from_active_deals(sym)
                    if strat in ('brkX2', 'hunting_4h', 'reversal', 'brkX2_4h', 'brkX2_crossema', 'akum_entry_a', 'akum_entry_b', 'trend_confirm_4h'): record_closed(sym)
                    if strat == 'brkX2_4h' and d.get('quick_reentry_open'): record_quick_reentry_close(sym, prof)
                    log(f"[MANUAL-CLOSE] {sym} @ {price_now:.6g} profit={prof:.2f}%")
                    send_telegram(
                        f"CLOSE MANUAL (dashboard)\n"
                        f"{ts} WIB\n"
                        f"Pair  : {to_display_pair(sym)}\n"
                        f"Exit  : {_fmt_price(price_now)} | Profit: {prof:+.2f}%\n"
                        f"Strategi: {strat}"
                    )
                    return jsonify({"ok": True, "sym": sym, "price": price_now, "profit_pct": round(prof, 2)})
                else:
                    return jsonify({"ok": False, "error": "3Commas menolak close"})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)})

        @app.route("/cancel_deal", methods=["POST"])
        def cancel_deal():
            """Cancel deal (dashboard button). Sama konsepnya dengan tombol Cancel di
            3Commas: berhenti nge-track & mengelola deal ini (auto add fund/TP/close
            berhenti jalan), TAPI TIDAK menjual apa pun — base currency yang sudah
            dibeli tetap utuh di wallet Binance. Bot ini murni market-order (tidak
            pernah nyisain order pending di exchange), jadi tidak ada perintah apa pun
            yang dikirim ke Binance di sini — murni penghapusan tracking internal."""
            sym = request.form.get("sym", "").upper().strip()
            if sym and not sym.endswith("USDT"):
                sym = sym + "USDT"
            with active_deals_lock:
                if sym not in active_deals:
                    return redirect("/")
                removed_deal = dict(active_deals[sym])
                del active_deals[sym]
            save_active_deals()
            try:
                base_usd_file = os.path.join(DATA_DIR, "deal_base_usd.json")
                if os.path.exists(base_usd_file):
                    with open(base_usd_file, "r") as f:
                        existing = json.load(f)
                    existing.pop(sym, None)
                    with open(base_usd_file, "w") as f:
                        json.dump(existing, f)
            except Exception:
                pass
            log(f"[CANCEL] {sym} dibatalkan dari tracking (koin tetap di wallet, tidak dijual)")
            send_telegram(
                f"CANCEL DEAL (dashboard)\n"
                f"Pair : {to_display_pair(sym)}\n"
                f"Strategi: {removed_deal.get('strategy','-')}\n"
                f"Bot berhenti kelola pair ini. Koin TIDAK dijual, tetap di wallet."
            )
            return redirect("/")

        @app.route("/reconcile_deal", methods=["POST"])
        def reconcile_deal():
            """Bersihkan deal yang NYANGKUT di active_deals padahal koinnya sudah kejual
            di luar jalur normal bot (mis. lewat webhook TradingView close_deal versi lama
            sebelum di-fix 29/08/2026 -- order kejual beneran tapi bookkeeping internal
            nggak ke-update). BEDA dari Cancel: ini khusus buat kasus koin SUDAH TERJUAL
            (saldo wallet <=5% dari qty seharusnya), jadi ditulis ke Closed Trades pakai
            harga sekarang sbg estimasi exit, bukan cuma dihapus diam-diam. Guard: nolak
            kalau saldo wallet masih signifikan (>5% dari qty_coin tercatat, atau estimasi
            modal/entry_price kalau qty_coin nggak ada -- kasus deal dibuka lewat webhook
            lama) -- itu tandanya koin BELUM kejual, harusnya pakai Cancel, bukan Reconcile.
            Threshold & fallback qty di sini SAMA PERSIS dengan yg dipakai buat nentuin
            kapan tombol Reconcile muncul di dashboard (lihat index(), 'needs_reconcile')."""
            sym = request.form.get("sym", "").upper().strip()
            if sym and not sym.endswith("USDT"):
                sym = sym + "USDT"
            with active_deals_lock:
                deal = dict(active_deals.get(sym, {}))
            if not deal:
                return redirect("/")
            asset = sym.replace("USDT", "")
            try:
                wallet_qty = binance_get_asset_qty(asset)
            except Exception:
                wallet_qty = 0.0
            entry_price_chk = float(deal.get('entry_price', 0) or 0)
            qty_expected = float(deal.get("qty_coin", 0) or 0)
            if qty_expected <= 0 and entry_price_chk > 0:
                qty_expected = estimate_deal_total_usd(deal) / entry_price_chk
            if qty_expected > 0 and wallet_qty > qty_expected * 0.05:
                log(f"[RECONCILE] {sym} ditolak: saldo wallet {wallet_qty} masih signifikan vs qty seharusnya {qty_expected} -- koin sepertinya belum terjual, pakai Cancel kalau memang mau berhenti track.")
                return redirect("/")
            price = get_price_now(sym)
            entry_price = float(deal.get('entry_price', 0) or 0)
            prof_pct = ((price / entry_price) - 1) * 100 if entry_price > 0 and price > 0 else 0.0
            total_usd = estimate_deal_total_usd(deal)
            strat = deal.get('strategy', 'brkX2')
            csv_log_close(sym, now_wib().strftime('%Y-%m-%d %H:%M:%S'), price, prof_pct,
                          "manual reconcile (koin sudah terjual di luar jalur normal)",
                          strategy=strat, base_usd=total_usd)
            remove_from_active_deals(sym)
            if strat in ('brkX2', 'hunting_4h', 'reversal', 'brkX2_4h', 'brkX2_crossema', 'akum_entry_a', 'akum_entry_b', 'trend_confirm_4h'):
                record_closed(sym)
            log(f"[RECONCILE] {sym} dibersihkan dari active_deals + dicatat ke Closed Trades (harga estimasi {price}, profit {prof_pct:+.2f}%)")
            send_telegram(
                f"RECONCILE DEAL (dashboard)\n"
                f"Pair   : {to_display_pair(sym)}\n"
                f"Strategi: {strat}\n"
                f"Exit (estimasi harga sekarang): {_fmt_price(price)}\n"
                f"Profit : {prof_pct:+.2f}%\n"
                f"Koin sudah terjual di luar jalur normal bot, sekarang disinkronkan ke Closed Trades."
            )
            return redirect("/")

        @app.route("/resend_open_notification", methods=["POST"])
        def resend_open_notification():
            symbol = str((request.get_json(force=True, silent=True) or {}).get("symbol", "")).upper().replace("/", "")
            with active_deals_lock:
                deal = dict(active_deals.get(symbol, {}))
            if not deal:
                return jsonify({"ok": False, "error": f"{symbol} tidak ada di active deals"}), 404
            strategy = deal.get("strategy", "brkX2")
            _STRAT_DISPLAY = {
                "brkX2": "brkX2-12h", "reversal": "Reversal-8h",
                "brkX2_4h": "brkX2-4h", "brkX2_crossema": "CrossEMA-4h",
                "hunting_4h": "Hunting-4h",
                "akum_entry_a": "Akumulasi-4h", "akum_entry_b": "Akumulasi-4h",
                "trend_confirm_4h": "TrenKonfirmasi-4h",
            }
            strategy_display = _STRAT_DISPLAY.get(strategy, strategy)
            opened_at = deal.get("opened_at") or deal.get("opened_at_wib") or "-"
            entry_price = float(deal.get("entry_price", 0) or 0)
            peak = float(deal.get("peak", entry_price) or entry_price)
            price = float(deal.get("last_price", entry_price) or entry_price)
            profit_pct = (price / entry_price - 1) * 100 - FEE_ROUND_TRIP_PCT if entry_price > 0 else 0
            fmt_indicator = lambda key, digits: (f"{float(deal[key]):.{digits}f}" if deal.get(key) is not None else "—")
            atr_pct = float(deal.get("atr_pct", 0) or 0)
            # Hitung hold candles dari opened_candle_ts (lebih akurat dari stored counter)
            _opened_ts = deal.get("opened_candle_ts")
            if _opened_ts:
                _candle_secs = {
                    "reversal": 8 * 3600,
                    "brkX2_4h": 4 * 3600, "brkX2_crossema": 4 * 3600,
                    "hunting_4h": 4 * 3600, "akum_entry_a": 4 * 3600, "akum_entry_b": 4 * 3600,
                    "trend_confirm_4h": 4 * 3600,
                }.get(strategy, 12 * 3600)
                hold_candles = int((time.time() - float(_opened_ts) / 1000) / _candle_secs)
            else:
                hold_candles = deal.get("hold_candle_count", "—")
            message = (
                f"OPEN LONG REPORT TERLAMBAT / RESEND\n"
                f"Waktu open: {opened_at}\n"
                f"Pair: {to_display_pair(symbol)}\n"
                f"Strategi: {strategy_display}\n"
                f"Entry: {_fmt_price(entry_price)}\n"
                f"Harga terakhir: {_fmt_price(price)}\n"
                f"Peak: {_fmt_price(peak)}\n"
                f"Profit estimasi: {profit_pct:+.2f}%\n"
                f"Armed: {bool(deal.get('trailing_armed', False))}\n"
                f"ATR%: {atr_pct:.2f}%\n"
                f"Hold candles: {hold_candles}\n"
                f"RSI: {fmt_indicator('rsi_open', 1)}\n"
                f"Stoch K: {fmt_indicator('stoch_k_open', 1)}\n"
                f"Stoch D: {fmt_indicator('stoch_d_open', 1)}\n"
                f"MACD hist: {fmt_indicator('macd_hist_open', 5)}\n"
                f"BB%: {fmt_indicator('bb_pct_open', 3)}\n"
                f"Williams R: {fmt_indicator('williams_r_open', 1)}\n"
                f"CCI: {fmt_indicator('cci_open', 1)}\n"
                f"OBV: {fmt_indicator('obv_open', 0)}\n"
                f"Modal: ${estimate_deal_total_usd(deal):.2f}\n"
                "Catatan: laporan dikirim ulang; tidak membuka deal baru."
            )
            send_telegram(message, parse_mode=None)
            log(f"[REPORT] resend OPEN notification {symbol} ({strategy})")
            return jsonify({"ok": True, "symbol": symbol, "strategy": strategy})

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
                    # Reset peak dan trailing_armed saat entry dikoreksi
                    active_deals[sym]["peak"] = val
                    active_deals[sym]["trailing_armed"] = False
                    log(f"[EDIT_DEAL] {sym} peak dan trailing_armed direset")
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

        @app.route("/set_tp_target", methods=["POST"])
        def set_tp_target():
            sym = request.form.get("sym", "")
            mode = request.form.get("mode", "usd")
            if mode not in ("usd", "pct", "price"):
                mode = "usd"
            try:
                val = float(request.form.get("value", "1"))
            except (TypeError, ValueError):
                val = 1.0
            # Mode "price" = harga koin asli (bisa banyak desimal, mis. 0.00001234) -- jangan
            # dibulatkan ke 2 desimal kayak $/% (bisa nge-nolkan harga koin kecil).
            val = max(val, 0.00000001) if mode == "price" else round(max(val, 0.01), 2)
            if sym:
                overrides = load_deal_overrides()
                if sym not in overrides:
                    overrides[sym] = {}
                overrides[sym]['tp1_target_mode'] = mode
                if mode == "pct":
                    overrides[sym]['tp1_target_pct'] = val
                elif mode == "price":
                    overrides[sym]['tp1_target_price'] = val
                else:
                    overrides[sym]['tp1_target_usd'] = val
                save_deal_overrides(overrides)
            return redirect("/")

        @app.route("/api/strategy_config", methods=["GET", "POST"])
        def api_strat_config():
            if request.method == "GET":
                return jsonify(load_strategy_config())
            save_strategy_config(request.get_json(force=True, silent=True) or {})
            sync_max_deals_globals()  # biar perubahan max_deals langsung kepakai tanpa restart
            return jsonify({"ok": True})

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
                            {"label":f"close vs EMA20 {ema20:.4g}","ok":p[1],"actual":f"{close:.4g} ({(close/ema20-1)*100:+.2f}%)"},
                            {"label":f"close vs EMA50 {ema50:.4g}","ok":p[2],"actual":f"{close:.4g} ({(close/ema50-1)*100:+.2f}%)"},
                            {"label":f"close vs HH3 {hh:.4g}","ok":p[3],"actual":f"{close:.4g} ({(close/hh-1)*100:+.2f}%)"},
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
                    open_c3 = float(df.iloc[im3]['open']); close_c1 = float(df.iloc[im1]['close'])
                    drop = (close_c1/open_c3-1)*100 if open_c3>0 else 0
                    n_red = sum(1 for i in (im3,im2,im1) if df.iloc[i]['close']<df.iloc[i]['open'])
                    doji_ok = float(c0.get('body_ratio',1)) < REVERSAL_DOJI_MAX
                    below_ema = float(c0['close'])<float(c0['ema_fast']) and float(c0['close'])<float(c0['ema_slow'])
                    p1_ok = n_red >= 2 and drop<=-5.0
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
                            {"label":f"minimal 2/3 candle merah+turun>={REVERSAL_DROP_MIN_PCT:.0f}%","ok":p1_ok,"actual":f"{n_red}/3 merah, turun {drop:.1f}%"},
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
                    ema20  = float(row['ema20']) if not pd.isna(row.get('ema20')) and row['ema20']>0 else None
                    gap_ema20 = ((float(row['close'])/ema20-1)*100) if ema20 else None
                    ema_band_ok = gap_ema20 is not None and 0 <= gap_ema20 <= STRAT4H_EMA20_BAND_MAX_PCT
                    p = [st_dir==1, macd_h is not None and macd_h>0, atr_pct is not None and atr_pct>=STRAT4H_ATR_MIN_PCT, ema_band_ok]
                    return jsonify(_s({"strat": strat, "sym": sym,
                        "primary": [
                            {"label":"ST=+1","ok":p[0],"actual":f"ST={st_dir}"},
                            {"label":"MACD hist>0","ok":p[1],"actual":f"{macd_h:.4f}" if macd_h else "n/a"},
                            {"label":f"Harga 0%-{STRAT4H_EMA20_BAND_MAX_PCT}% di atas EMA20 {ema20:.4g}" if ema20 else "Harga vs EMA20","ok":p[3],"actual":f"{gap_ema20:+.2f}%" if gap_ema20 is not None else "n/a"},
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
                    st_dir = int(row['st_dir_cx']) if not pd.isna(row.get('st_dir_cx')) else 0
                    close  = float(row['close']); ema20 = float(row['ema20'])
                    vol_ma = float(row['vol_ma']) if not pd.isna(row.get('vol_ma')) and row['vol_ma']>0 else 1
                    vol_ratio= float(row['vol'])/vol_ma
                    price_now= get_price_now(sym)
                    cross_ok = price_now>0 and price_now > ema20 * (1 - STRAT_CROSSEMA_CROSS_TOL_PCT / 100)
                    htf_r  = htf_vol_ratio(sym, STRAT4H_HTF_TF, STRAT4H_HTF_LIMIT, STRAT4H_HTF_VOL_MA)
                    vol24  = float(row.get('vol24h_usd',0)) if 'vol24h_usd' in row else 0
                    p = [st_dir==-1, close <= ema20 * (1 + STRAT_CROSSEMA_EMA20_TOL_PCT / 100), cross_ok]
                    return jsonify(_s({"strat": strat, "sym": sym,
                        "primary": [
                            {"label":"ST=-1 (downtrend)","ok":p[0],"actual":f"ST={st_dir}"},
                            {"label":f"close < EMA20 {ema20:.4g}","ok":p[1],"actual":f"{close:.4g} ({(close/ema20-1)*100:+.2f}%)"},
                            {"label":f"price_now>EMA20 (cross)","ok":p[2],"actual":f"{price_now:.4g} ({(price_now/ema20-1)*100:+.2f}%)" if price_now>0 else "n/a"},
                        ],
                        "secondary": [
                            {"key":"vol","label":f"Vol>={STRAT_CROSSEMA_VOLUME_MULT}xMA","actual":f"{vol_ratio:.2f}x","ok":vol_ratio>=STRAT_CROSSEMA_VOLUME_MULT,"thr":f"{STRAT_CROSSEMA_VOLUME_MULT}x"},
                            {"key":"htf","label":f"HTF12h vol>{STRAT_CROSSEMA_HTF_VOL_MULT}xMA","actual":f"{htf_r:.2f}x" if htf_r>=0 else "n/a","ok":htf_r>=STRAT_CROSSEMA_HTF_VOL_MULT,"thr":f"{STRAT_CROSSEMA_HTF_VOL_MULT}x"},
                            {"key":"vol24","label":f"Vol24h>=${STRAT_CROSSEMA_MIN_VOL_USD/1e6:.1f}jt","actual":f"${vol24/1e6:.2f}jt","ok":vol24>=STRAT_CROSSEMA_MIN_VOL_USD,"thr":f"${STRAT_CROSSEMA_MIN_VOL_USD/1e6:.1f}jt"},
                        ],
                        "primary_ok": all(p),
                    }))

                elif strat in ("Akumulasi-4h", "Akumulasi-4h Entry A", "Akumulasi-4h Entry B"):
                    df = get_ohlcv_4h(sym, limit=AKUM_CANDLE_LIMIT)
                    if df is None: return jsonify(_s({"error": "Gagal ambil OHLCV 4h"}))
                    if df['ct'].iloc[-1] >= int(time.time()*1000): df = df.iloc[:-1]
                    if len(df) < AKUM_SIDEWAYS_CANDLES + 50:
                        return jsonify(_s({"error": f"Data kurang ({len(df)} candle)"}))
                    res = score_akumulasi(df, sym)
                    if res is None:
                        return jsonify(_s({"error": "Gagal hitung skor akumulasi"}))
                    # Hitung support/resistance dari jendela sideways
                    window = df.iloc[-(AKUM_SIDEWAYS_CANDLES):]
                    support    = float(window['low'].min())
                    resistance = float(window['high'].max())
                    support_zone_low = float(res.get('support_zone_low', 0) or 0)
                    support_zone_high = float(res.get('support_zone_high', 0) or 0)
                    resistance_zone_low = float(res.get('resistance_zone_low', 0) or 0)
                    resistance_zone_high = float(res.get('resistance_zone_high', 0) or 0)
                    has_struct = (
                        support_zone_low > 0 and support_zone_high > 0 and
                        resistance_zone_low > 0 and resistance_zone_high > 0 and
                        support_zone_low <= support_zone_high <= resistance_zone_low <= resistance_zone_high
                    )
                    support_break_ref = support_zone_low if has_struct else support
                    support_reentry_ref = support_zone_high if has_struct else support
                    resistance_break_ref = resistance_zone_high if has_struct else resistance
                    resistance_retest_low_ref = resistance_zone_low if has_struct else resistance
                    p = [
                        bool(res.get('p1_ok')),
                        bool(res.get('p2_ok')),
                        bool(res.get('p3_ok')),
                        bool(res.get('p4_ok')),
                    ]
                    primary_labels = [
                        {"label": f"Range sideways <{AKUM_RANGE_PCT*100:.0f}%",
                         "ok": bool(res.get('p1_ok')), "actual": res.get('range_pct','?')},
                        {"label": f"EMA konvergen <{AKUM_EMA_GAP_PCT*100:.0f}%",
                         "ok": bool(res.get('p2_ok')), "actual": res.get('ema_gap','?')},
                        {"label": "OBV slope positif",
                         "ok": bool(res.get('p3_ok')), "actual": res.get('obv_slope','?')},
                        {"label": f"ATR turun >{AKUM_ATR_DROP_PCT*100:.0f}%",
                         "ok": bool(res.get('p4_ok')), "actual": res.get('atr_drop','?')},
                        {"label": f"EMA20 slope turun <{AKUM_EMA_SLOPE_MAX*100:.0f}%",
                         "ok": bool(res.get('p5_ok')), "actual": res.get('ema_slope','?')},
                        {"label": f"Close drift <{AKUM_CLOSE_DRIFT_MAX*100:.0f}%",
                         "ok": bool(res.get('p6_ok')), "actual": res.get('close_drift','?')},
                        {"label": f"Distribusi range <{AKUM_RANGE_DIST_MAX}x",
                         "ok": bool(res.get('p7_ok')), "actual": res.get('range_dist','?')},
                    ]
                    if strat == "Akumulasi-4h":
                        secondary_labels = [
                            {"key":"vol_asim","label":"Vol hijau>merah",
                             "actual": res.get('vol_asim','?'), "ok": bool(res.get('s1_ok'))},
                            {"key":"rsi","label":"RSI 30-56",
                             "actual": res.get('rsi','?'), "ok": bool(res.get('s2_ok'))},
                            {"key":"macd_flat","label":"MACD flat≈0",
                             "actual": res.get('macd_flat','?'), "ok": bool(res.get('s3_ok'))},
                            {"key":"body_ratio","label":f"Body ratio<{AKUM_BODY_RATIO_MAX}",
                             "actual": res.get('body_ratio','?'), "ok": bool(res.get('s4_ok'))},
                        ]
                        extra = {}
                    elif strat == "Akumulasi-4h Entry A":
                        sig = detect_entry_a_spring(df, support_break_ref, support_reentry_ref)
                        row_last = df.iloc[-1]
                        vol_ma = float(row_last.get('vol_ma', 0)) if not pd.isna(row_last.get('vol_ma', 0)) else 0
                        vol_ratio = float(row_last['vol'])/vol_ma if vol_ma > 0 else 0
                        rsi_now = float(row_last['rsi']) if not pd.isna(row_last.get('rsi')) else None
                        secondary_labels = [
                            {"key":"vol_spike","label":f"Vol spike>{AKUM_A_VOL_SPIKE_MULT}xMA",
                             "actual": f"{vol_ratio:.2f}x", "ok": vol_ratio >= AKUM_A_VOL_SPIKE_MULT},
                            {"key":"rsi_low","label":f"RSI sempat<{AKUM_A_RSI_MIN}",
                             "actual": f"{rsi_now:.1f}" if rsi_now else "n/a",
                             "ok": rsi_now is not None and rsi_now < AKUM_A_RSI_MAX_ENTRY},
                            {"key":"obv_div","label":"OBV divergensi",
                             "actual": "Terdeteksi" if sig and sig.get('obv_slope',0) > 0 else "Belum",
                             "ok": bool(sig and sig.get('obv_slope',0) > 0)},
                            {"key":"reentry","label":"Close kembali>support ref",
                             "actual": "Ya" if sig else "Belum",
                             "ok": bool(sig)},
                        ]
                        extra = {
                            "support": _fmt_price(support),
                            "resistance": _fmt_price(resistance),
                            "support_ref": _fmt_price(support_reentry_ref),
                            "resistance_ref": _fmt_price(resistance_break_ref),
                            "ref_basis": "struct_zone" if has_struct else "extreme_fallback",
                            "entry_signal": sig if sig else None,
                            "signal_detected": sig is not None,
                        }
                    elif strat == "Akumulasi-4h Entry B":
                        sig = detect_entry_b_breakout(df, resistance_break_ref, support_break_ref, resistance_retest_low_ref)
                        row_last = df.iloc[-1]
                        ema20 = float(row_last.get('ema20', 0)) if not pd.isna(row_last.get('ema20', 0)) else 0
                        ema50 = float(row_last.get('ema50', 0)) if not pd.isna(row_last.get('ema50', 0)) else 0
                        close_last = float(row_last['close'])
                        ema_cross = ema20 > ema50
                        secondary_labels = [
                            {"key":"breakout","label":f"Close>resistance ref+vol",
                             "actual": f"{_fmt_price(close_last)} vs {_fmt_price(resistance_break_ref)}",
                             "ok": close_last > resistance_break_ref},
                            {"key":"retest","label":"Retest tdk jebol",
                             "actual": "Terdeteksi" if sig else "Belum",
                             "ok": bool(sig)},
                            {"key":"vol_retest","label":"Vol retest<80% breakout",
                             "actual": "OK" if sig else "Belum",
                             "ok": bool(sig)},
                            {"key":"ema_cross","label":"EMA20>EMA50",
                             "actual": f"{_fmt_price(ema20)} vs {_fmt_price(ema50)}",
                             "ok": ema_cross},
                        ]
                        extra = {
                            "support": _fmt_price(support),
                            "resistance": _fmt_price(resistance),
                            "support_ref": _fmt_price(support_reentry_ref),
                            "resistance_ref": _fmt_price(resistance_break_ref),
                            "ref_basis": "struct_zone" if has_struct else "extreme_fallback",
                            "entry_signal": sig if sig else None,
                            "signal_detected": sig is not None,
                        }
                    else:
                        secondary_labels = []
                        extra = {}
                    return jsonify(_s({
                        "strat": strat, "sym": sym,
                        "primary": primary_labels,
                        "secondary": secondary_labels,
                        "primary_ok": res['primary_ok'],
                        "akum_score": res['total_score'],
                        **extra,
                    }))

                elif strat == "Hunting-4h":
                    df = get_ohlcv_4h(sym, limit=100)
                    if df is None:
                        return jsonify(_s({"error": "Gagal ambil OHLCV 4h"}))
                    with _hunting_lock:
                        cfg = dict(_hunting_config)
                    hit = check_hunting_strategy(df, {"symbol": sym}, cfg)
                    close  = float(df["close"].iloc[-1])
                    ema20  = float(df["close"].ewm(span=20, adjust=False).mean().iloc[-1])
                    ema50  = float(df["close"].ewm(span=50, adjust=False).mean().iloc[-1])
                    dist20 = round((close - ema20) / ema20 * 100, 2) if ema20 > 0 else 0
                    dist50 = round((close - ema50) / ema50 * 100, 2) if ema50 > 0 else 0
                    gap    = round((ema50 - ema20) / ema50 * 100, 2) if ema50 > 0 else 0
                    chg    = round((close - float(df["close"].iloc[-2])) / float(df["close"].iloc[-2]) * 100, 2)
                    uptrend = close > float(df["close"].iloc[-6]) if len(df) >= 6 else False
                    rsi_val = round(hit['rsi'], 1) if hit and hit.get('rsi') else None
                    primary_labels = [
                        {"key": "ema20",    "label": "Δema20 0-3%",      "actual": f"{dist20:.2f}%", "ok": 0 <= dist20 <= 3},
                        {"key": "ema_gap",  "label": "EMA gap 0-1%",     "actual": f"{gap:.2f}%",   "ok": 0 <= gap <= 1},
                        {"key": "ema50",    "label": "Δema50 0-3%",      "actual": f"{dist50:.2f}%","ok": 0 <= dist50 <= 3},
                        {"key": "chg",      "label": "Price chg 0-5%",   "actual": f"{chg:.2f}%",   "ok": 0 < chg <= 5},
                        {"key": "uptrend",  "label": "Uptrend",           "actual": "↑" if uptrend else "↓", "ok": uptrend},
                        {"key": "st",       "label": "ST+1",              "actual": f"{hit['st_dir'] if hit else '?'}", "ok": bool(hit and hit.get('st_dir') == 1)},
                    ]
                    secondary_labels = [
                        {"key": "rsi", "label": f"RSI<{HUNTING_RSI_MAX}",
                         "actual": f"{rsi_val}" if rsi_val else "?",
                         "ok": bool(rsi_val and rsi_val < HUNTING_RSI_MAX)},
                    ]
                    return jsonify(_s({
                        "strat": strat, "sym": sym,
                        "primary": primary_labels,
                        "secondary": secondary_labels,
                        "primary_ok": hit is not None,
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

        @app.route("/fix_deal_usd", methods=["POST"])
        def fix_deal_usd():
            """Patch base_usd di active_deals.json untuk fix kalkulasi locked balance.
            Juga reset add_usd=0 dan add_fund_sent=False agar modal terpakai akurat."""
            sym = request.form.get("symbol", "").upper().replace("/", "")
            try:
                usd = float(request.form.get("base_usd", 0))
            except ValueError:
                return f"ERROR: base_usd harus angka", 400
            if not sym:
                return "ERROR: symbol required", 400
            with active_deals_lock:
                if sym not in active_deals:
                    return f"ERROR: {sym} tidak ada di active_deals", 404
                active_deals[sym]["base_usd"]       = usd
                active_deals[sym]["add_usd"]        = 0
                active_deals[sym]["add_fund_sent"]  = False
            save_active_deals()
            # Persist ke deal_base_usd.json agar survive redeploy
            try:
                base_usd_file = os.path.join(DATA_DIR, "deal_base_usd.json")
                existing = {}
                if os.path.exists(base_usd_file):
                    with open(base_usd_file, "r") as f:
                        existing = json.load(f)
                existing[sym] = usd
                with open(base_usd_file, "w") as f:
                    json.dump(existing, f)
            except Exception as e:
                log(f"WARN [fix_deal_usd] persist gagal: {e}")
            log(f"[FIX] {sym} base_usd={usd}, add_usd=0, add_fund_sent=False via /fix_deal_usd")
            return f"OK: {sym} base_usd={usd} — modal terpakai akan tampil ${usd:.0f}"

        @app.route("/inject_deal", methods=["POST"])
        def inject_deal():
            """Inject deal yang ada di 3Commas tapi tidak ada di active_deals bot.
            Params: symbol, entry_price, base_usd, strategy (opsional, default hunting_4h)"""
            sym      = request.form.get("symbol", "").upper().replace("/", "")
            strategy = request.form.get("strategy", "hunting_4h")
            try:
                entry_price = float(request.form.get("entry_price", 0))
                base_usd    = float(request.form.get("base_usd", 20))
            except ValueError:
                return "ERROR: entry_price dan base_usd harus angka", 400
            if not sym or entry_price <= 0:
                return "ERROR: symbol dan entry_price required", 400
            with active_deals_lock:
                if sym in active_deals:
                    return f"ERROR: {sym} sudah ada di active_deals", 400
            add_to_active_deals(sym, {
                "strategy":         strategy,
                "entry_price":      entry_price,
                "signal_price":     entry_price,
                "peak":             entry_price,
                "atr_pct":          3.0,
                "score":            1,
                "target_usd":       base_usd,
                "base_usd":         base_usd,
                "add_usd":          0,
                "opened_ts":        time.time(),
                "opened_candle_ts": int(time.time() * 1000),
                "opened_at_wib":    now_wib().strftime('%d/%m/%Y %H:%M') + " (inject manual)",
                "trailing_armed":   False,
                "tf":               "4h",
            })
            # Persist ke deal_base_usd.json
            try:
                base_usd_file = os.path.join(DATA_DIR, "deal_base_usd.json")
                existing = {}
                if os.path.exists(base_usd_file):
                    with open(base_usd_file, "r") as f:
                        existing = json.load(f)
                existing[sym] = base_usd
                with open(base_usd_file, "w") as f:
                    json.dump(existing, f)
            except Exception as e:
                log(f"WARN [inject_deal] persist gagal: {e}")
            log(f"[INJECT] {sym} diinjeksi ke active_deals: entry={entry_price} base_usd={base_usd} strategy={strategy}")
            return f"OK: {sym} berhasil diinjeksi ke active_deals — entry={entry_price} base_usd={base_usd}"

        @app.route("/admin/remove_deal", methods=["POST"])
        def admin_remove_deal():
            data = request.get_json(force=True, silent=True) or {}
            sym  = data.get("symbol", "").upper().replace("/","")
            if not sym:
                return jsonify({"ok": False, "error": "symbol required"})
            with active_deals_lock:
                if sym == "ALL":
                    removed = list(active_deals.keys())
                    active_deals.clear()
                elif sym in active_deals:
                    removed = [sym]
                    del active_deals[sym]
                else:
                    return jsonify({"ok": False, "error": f"{sym} tidak ada di active_deals"})
                # Simpan langsung ke file dalam lock
                with open("/data/active_deals.json", "w") as _f:
                    import json as _json
                    _json.dump(dict(active_deals), _f)
            log(f"[ADMIN] remove_deal: {removed}")
            # Cleanup dari deal_base_usd.json
            try:
                base_usd_file = os.path.join(DATA_DIR, "deal_base_usd.json")
                if os.path.exists(base_usd_file):
                    with open(base_usd_file, "r") as f:
                        existing = json.load(f)
                    for s in removed:
                        existing.pop(s, None)
                    with open(base_usd_file, "w") as f:
                        json.dump(existing, f)
            except Exception: pass
            return jsonify({"ok": True, "removed": removed, "remaining": list(active_deals.keys())})

        @app.route("/admin/delete_trade", methods=["POST"])
        def admin_delete_trade():
            """
            Hapus 1 baris dari trades_forwardtest.csv berdasarkan symbol + strategy.
            Jika ada lebih dari 1 baris dengan symbol+strategy yang sama, hapus yang paling baru (close_time_wib terbesar).
            Body JSON: {"symbol": "WLDUSDT", "strategy": "brkX2_4h"}
            """
            data     = request.get_json(force=True, silent=True) or {}
            sym      = data.get("symbol", "").upper().replace("/", "")
            strategy = data.get("strategy", "")
            if not sym:
                return jsonify({"ok": False, "error": "symbol required"})
            try:
                with trades_csv_lock:
                    if not os.path.exists(TRADES_CSV):
                        return jsonify({"ok": False, "error": "CSV tidak ditemukan"})
                    with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        rows = list(reader)
                    # Filter baris yang cocok — support WLD/USDT dan WLDUSDT
                    sym_raw = sym.replace("/", "")
                    matches = [
                        (i, r) for i, r in enumerate(rows)
                        if r.get('symbol','').upper().replace("/","") == sym_raw
                        and (not strategy or r.get('strategy','') == strategy)
                    ]
                    if not matches:
                        return jsonify({"ok": False, "error": f"Tidak ada baris {sym} {strategy} di CSV"})
                    # Hapus yang paling baru (index terakhir dari matches)
                    del_idx, del_row = matches[-1]
                    rows.pop(del_idx)
                    # Tulis ulang CSV
                    with open(TRADES_CSV, 'w', newline='', encoding='utf-8') as f:
                        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                        w.writeheader()
                        w.writerows(rows)
                log(f"[ADMIN] delete_trade: hapus {sym} {strategy} | profit={del_row.get('profit_pct')} | sisa {len(rows)} baris")
                return jsonify({
                    "ok":      True,
                    "deleted": del_row,
                    "remaining_rows": len(rows)
                })
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)})

        @app.route("/admin/backfill_rsi_open", methods=["POST"])
        def admin_backfill_rsi_open():
            """
            One-off backfill: isi kolom rsi_open utk baris trades_forwardtest.csv
            yang dibuka SEBELUM fitur persistensi RSI di-deploy (commit a318b5e,
            01/09/2026 23:42 WIB) sehingga kolomnya masih kosong.

            RSI(14) dihitung ULANG dari candle historis Binance asli (endTime =
            open_time_wib trade - 1 detik, biar candle yg baru saja closed saat
            entry yang kepilih, bukan candle berikutnya yg baru mulai), interval
            sesuai timeframe strategi masing2. Ini rekonstruksi, bukan nilai live
            otentik dari momen keputusan -- tapi metode sama persis dgn yg dipakai
            utk laporan manual RSI 12 coin rugi minggu lalu.

            Idempotent: baris yg rsi_open-nya sudah terisi DILEWATI -- aman
            dipanggil ulang berkali-kali (mis. kalau sebagian gagal krn rate
            limit, tinggal panggil lagi utk melengkapi sisanya).
            """
            STRAT_INTERVAL = {
                'brkX2': '12h', 'brkX2_4h': '4h', 'reversal': '8h',
                'hunting_4h': '4h', 'brkX2_crossema': '4h',
                'akum_entry_a': '4h', 'akum_entry_b': '4h',
                'trend_confirm_4h': '4h',
            }
            filled, skipped, errors = 0, 0, 0
            skip_reasons = {}
            try:
                with trades_csv_lock:
                    if not os.path.exists(TRADES_CSV):
                        return jsonify({"ok": False, "error": "CSV tidak ditemukan"})
                    with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                        rows = list(csv.DictReader(f))

                    for row in rows:
                        if str(row.get('rsi_open', '')).strip():
                            continue  # sudah terisi -- idempotent skip
                        strat    = row.get('strategy', '')
                        interval = STRAT_INTERVAL.get(strat)
                        open_wib = str(row.get('open_time_wib', '')).strip()
                        sym_disp = row.get('symbol', '')
                        if not interval or not open_wib or not sym_disp:
                            skipped += 1
                            skip_reasons[strat or '?'] = skip_reasons.get(strat or '?', 0) + 1
                            continue
                        sym_raw = sym_disp.replace('/', '').upper()
                        try:
                            dt_wib = datetime.strptime(open_wib, '%Y-%m-%d %H:%M:%S')
                            dt_utc = dt_wib - timedelta(hours=7)
                            end_ms = int(dt_utc.timestamp() * 1000) - 1000
                            r = _binance_get("/api/v3/klines", params={
                                'symbol': sym_raw, 'interval': interval,
                                'endTime': end_ms, 'limit': 100,
                            }, timeout=15)
                            if r is None or r.status_code != 200:
                                errors += 1; continue
                            d = r.json()
                            if not isinstance(d, list) or len(d) < 20:
                                errors += 1; continue
                            closes = [float(k[4]) for k in d]
                            rsi_series = ta.rsi(pd.Series(closes), length=14)
                            rsi_val = rsi_series.iloc[-1]
                            if pd.isna(rsi_val):
                                errors += 1; continue
                            row['rsi_open'] = f"{float(rsi_val):.1f}"
                            filled += 1
                        except Exception:
                            errors += 1
                            continue

                    with open(TRADES_CSV, 'w', newline='', encoding='utf-8') as f:
                        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                        w.writeheader()
                        w.writerows(rows)
                log(f"[ADMIN] backfill_rsi_open: filled={filled} skipped={skipped} errors={errors} skip_reasons={skip_reasons}")
                sync_trades_csv_to_drive()
                return jsonify({"ok": True, "filled": filled, "skipped": skipped,
                                 "errors": errors, "skip_reasons": skip_reasons,
                                 "total_rows": len(rows)})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)})

        @app.route("/admin/test_buy", methods=["POST"])
        def admin_test_buy():
            """
            Test beli MARKET via Binance direct API.
            Body JSON: {"symbol": "BTCUSDT", "usdt": 1.0}
            HANYA untuk validasi koneksi — gunakan nominal kecil ($1-$2).
            """
            data   = request.get_json(force=True, silent=True) or {}
            symbol = data.get("symbol", "").upper().replace("/", "")
            usdt   = float(data.get("usdt", 0))
            if not symbol or usdt <= 0:
                return jsonify({"ok": False, "error": "symbol dan usdt diperlukan"})
            try:
                result = binance_buy_market(symbol, usdt)
                return jsonify({"ok": True, "result": result})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)})

        @app.route("/admin/test_sell", methods=["POST"])
        def admin_test_sell():
            """
            Test jual MARKET via Binance direct API.
            Body JSON: {"symbol": "BTCUSDT", "qty": 0.00001}
            """
            data   = request.get_json(force=True, silent=True) or {}
            symbol = data.get("symbol", "").upper().replace("/", "")
            qty    = float(data.get("qty", 0))
            if not symbol or qty <= 0:
                return jsonify({"ok": False, "error": "symbol dan qty diperlukan"})
            try:
                result = binance_sell_market(symbol, qty)
                return jsonify({"ok": True, "result": result})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)})

        @app.route("/admin/test_balance", methods=["GET"])
        def admin_test_balance():
            """Test baca saldo via BINANCE_TRADING_KEY.
            ?asset=DOGE untuk cek asset tertentu, tanpa parameter return USDT + semua non-zero."""
            try:
                data   = _binance_trading_request("GET", "/api/v3/account", {})
                asset  = request.args.get("asset", "").upper()
                if asset:
                    for b in data.get("balances", []):
                        if b["asset"] == asset:
                            return jsonify({"ok": True, asset: float(b["free"]), "locked": float(b["locked"])})
                    return jsonify({"ok": True, asset: 0.0, "locked": 0.0})
                # Return semua non-zero
                nonzero = {b["asset"]: {"free": float(b["free"]), "locked": float(b["locked"])}
                           for b in data.get("balances", [])
                           if float(b["free"]) > 0 or float(b["locked"]) > 0}
                return jsonify({"ok": True, "balances": nonzero})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)})

        @app.route("/admin/add_closed_trade", methods=["POST"])
        def admin_add_closed_trade():
            """Inject baris CLOSED manual ke CSV untuk deal yang close tanpa baris OPEN."""
            data    = request.get_json(force=True, silent=True) or {}
            sym     = data.get("symbol", "")
            strat   = data.get("strategy", "hunting_4h")
            pct     = float(data.get("profit_pct", 0))
            ep      = data.get("exit_price", "0")
            open_t  = data.get("open_time", "")
            close_t = data.get("close_time", now_wib().strftime('%Y-%m-%d %H:%M:%S'))
            reason  = data.get("exit_reason", "manual_inject")
            if not sym:
                return jsonify({"ok": False, "error": "symbol required"})
            try:
                csv_log_close(sym, close_t, float(ep), pct, reason, strategy=strat)
                if open_t:
                    with trades_csv_lock:
                        with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                            rows = list(csv.DictReader(f))
                        for r in reversed(rows):
                            if r.get('symbol') == sym and r.get('status') == 'CLOSED' and not r.get('open_time_wib'):
                                r['open_time_wib'] = open_t; break
                        with open(TRADES_CSV, 'w', newline='', encoding='utf-8') as f:
                            w = csv.DictWriter(f, fieldnames=CSV_FIELDS); w.writeheader(); w.writerows(rows)
                log(f"[ADMIN] add_closed_trade: {sym} {strat} {pct:+.2f}%")
                return jsonify({"ok": True, "symbol": sym, "profit_pct": pct})
            except Exception as e:
                return jsonify({"ok": False, "error": str(e)})

        def manual_scan_endpoint():
            result = run_manual_scan()
            return jsonify(result)

        @app.route("/manual_open", methods=["POST"])
        def manual_open():
            sym   = request.form.get("sym", "").upper().strip()
            strat = request.form.get("strat", "brkX2-12h")
            if not sym:
                return jsonify({"ok": False, "error": "sym kosong"})
            if not sym.endswith("USDT"):
                sym = sym + "USDT"
            with active_deals_lock:
                if sym in active_deals:
                    return jsonify({"ok": False, "error": f"{sym} sudah ada di active_deals"})

            # Tentukan strategi, timeframe, dan bot berdasarkan strat
            if strat in ("brkX2-4h", "CrossEMA-4h"):
                strat_key = "brkX2_4h" if strat == "brkX2-4h" else "brkX2_crossema"
                tf_label  = "4h"
                get_df    = lambda: get_ohlcv_4h(sym, limit=120)
                compute   = compute_indicators_4h
            elif strat == "Hunting-4h":
                # Hunting pakai open_hunting_if_signal — handle terpisah
                try:
                    df2 = get_ohlcv_4h(sym, limit=120)
                    if df2 is None:
                        return jsonify({"ok": False, "error": "Gagal ambil OHLCV 4h"})
                    with _hunting_lock:
                        cfg = dict(_hunting_config)
                    # Cek state checkbox RSI dari manual filters
                    with _manual_filters_lock:
                        rsi_filter_on = _manual_filters.get("rsi", True)
                    if not rsi_filter_on:
                        cfg["rsi_max_override"] = 999  # bypass RSI kalau checkbox dimatikan
                    ticker_item = {"symbol": sym}
                    ok2 = open_hunting_if_signal(ticker_item, df2, cfg)
                    if ok2:
                        return jsonify({"ok": True, "sym": sym, "score": 1, "target_usd": BASE_ORDER_VOLUME})
                    else:
                        return jsonify({"ok": False, "error": "Hunting open gagal atau syarat tidak terpenuhi"})
                except Exception as e:
                    return jsonify({"ok": False, "error": f"Hunting open error: {e}"})
            elif strat == "Reversal-8h T1":
                strat_key = "reversal"
                tf_label  = "8h"
                get_df    = lambda: get_ohlcv(sym, interval=REVERSAL_TIMEFRAME, limit=60)
                compute   = compute_indicators
            else:  # brkX2-12h (default)
                strat_key = "brkX2"
                tf_label  = "12h"
                get_df    = lambda: get_ohlcv(sym, limit=120)
                compute   = compute_indicators

            # Ambil OHLCV dan hitung indikator
            try:
                df = get_df()
                if df is None:
                    return jsonify({"ok": False, "error": "Gagal ambil OHLCV"})
                if df['ct'].iloc[-1] >= int(time.time() * 1000):
                    df = df.iloc[:-1]
                df = compute(df)
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

            # Guard bStocks: cek NYSE open
            if is_bstock_symbol(sym) and not is_nyse_open():
                return jsonify({"ok": False, "error": f"{sym} adalah tokenized stock (bStock). NYSE sedang tutup — open long tidak diizinkan. NYSE buka 21:30–04:00 WIB (Senin–Jumat)."})

            ok, target_usd, add_usd = open_deal_with_sizing(sym, sc, strat_key)
            if ok:
                entry_price = get_price_now(sym)
                if entry_price <= 0:
                    entry_price = close  # fallback ke close candle kalau gagal
                add_to_active_deals(sym, {
                    "strategy":       strat_key,
                    "entry_price":    entry_price,
                    "peak":           entry_price,
                    "signal_price":   close,
                    "atr_pct":        atr_pct or 3.0,
                    "opened_candle_ts": int(df['ct'].iloc[-1]),
                    "trailing_armed": False,
                    "opened_at":      now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                    "target_usd":     target_usd,
                    "add_usd":        add_usd,
                    "tf":             tf_label,
                    "manual":         True,
                })
                send_telegram(
                    f"OPEN LONG MANUAL ({strat})\n"
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

        @app.route("/tradingview_webhook", methods=["POST"])
        def tradingview_webhook():
            """Terima JSON TradingView; eksekusi hanya jika diaktifkan eksplisit."""
            webhook_secret = os.environ.get("TRADINGVIEW_WEBHOOK_SECRET", "")
            payload = request.get_json(force=True, silent=True) or {}
            supplied_secret = payload.get("secret", "") or request.headers.get("X-Webhook-Secret", "")
            if not webhook_secret or supplied_secret != webhook_secret:
                return jsonify({"ok": False, "error": "webhook tidak terautentikasi"}), 401
            if os.environ.get("TRADINGVIEW_WEBHOOK_ENABLED", "false").lower() != "true":
                return jsonify({"ok": False, "error": "webhook masih nonaktif"}), 403
            action = str(payload.get("action", "")).lower().strip()
            symbol = normalize_binance_symbol(payload.get("symbol", ""))
            strategy = str(payload.get("strategy", "brkX2")).lower().strip()
            strategy = {
                "brkx2-12h": "brkX2", "brkx2": "brkX2", "reversal-8h": "reversal",
                "brkx2-4h": "brkX2_4h", "crossema-4h": "brkX2_crossema",
                "hunting-4h": "hunting_4h",
                "akumulasi-4h-a": "akum_entry_a", "akumulasi-4h-b": "akum_entry_b",
                "trenkonfirmasi-4h": "trend_confirm_4h",
            }.get(strategy, strategy)
            if action not in {"open_long", "add_fund", "start_trailing", "close_deal"} or not symbol.endswith("USDT"):
                return jsonify({"ok": False, "error": "action atau symbol tidak valid"}), 400
            if action == "open_long":
                with active_deals_lock:
                    if symbol in active_deals:
                        return jsonify({"ok": False, "error": "deal sudah ada di active_deals"}), 409
                score = int(payload.get("score", 0) or 0)
                ok, target_usd, add_usd = open_deal_with_sizing(symbol, score, strategy)
                if ok:
                    entry_price = get_price_now(symbol)
                    if entry_price <= 0:
                        return jsonify({"ok": False, "error": "order berhasil tetapi harga entry tidak terbaca"}), 502
                    add_to_active_deals(symbol, {
                        "strategy": strategy, "entry_price": entry_price, "peak": entry_price,
                        "signal_price": entry_price, "atr_pct": 3.0,
                        "opened_candle_ts": int(time.time() * 1000), "trailing_armed": False,
                        "opened_at": now_wib().strftime("%Y-%m-%d %H:%M:%S"),
                        "target_usd": target_usd, "add_usd": add_usd, "source": "tradingview_webhook",
                    })
                    csv_log_open({
                        'open_time_wib': now_wib().strftime('%Y-%m-%d %H:%M:%S'),
                        'symbol': to_display_pair(symbol), 'strategy': strategy,
                        'signal_price': f"{_fmt_price(entry_price)}", 'entry_price': f"{_fmt_price(entry_price)}",
                        'base_usd': target_usd, 'score': score,
                    })
                    send_telegram(f"OPEN LONG TradingView\nPair: {to_display_pair(symbol)}\nStrategi: {strategy}\nEntry: {_fmt_price(entry_price)}\nModal: ${target_usd:.2f}")
                return jsonify({"ok": ok, "action": action, "symbol": symbol, "target_usd": target_usd, "add_usd": add_usd})
            with active_deals_lock:
                deal = dict(active_deals.get(symbol, {}))
            if not deal:
                return jsonify({"ok": False, "error": f"{symbol} tidak ada di active deals"}), 404
            if action == "add_fund":
                amount = float(payload.get("amount", deal.get("add_usd", 0)) or 0)
                ok = amount > 0 and send_add_funds(symbol, amount, strategy, delay=0)
            elif action == "start_trailing":
                ok = send_start_trailing(symbol, strategy)
            else:
                # close_deal -- jalankan pipeline closing LENGKAP (bukan cuma raw sell ke Binance),
                # biar active_deals/Closed Trades/cooldown ikut update sama kayak close via T2 internal.
                # Sebelumnya cuma send_close_long() doang: order kejual beneran tapi bot "lupa" deal-nya
                # sudah selesai -- nyangkut selamanya di Active Deals & nggak pernah muncul di Closed Trades.
                wh_price = get_price_now(symbol)
                wh_entry = float(deal.get('entry_price', 0) or 0)
                wh_prof_pct = ((wh_price / wh_entry) - 1) * 100 if wh_entry > 0 and wh_price > 0 else 0.0
                ok = send_close_long(symbol, strategy)
                if ok:
                    wh_total_usd = estimate_deal_total_usd(deal)
                    csv_log_close(symbol, now_wib().strftime('%Y-%m-%d %H:%M:%S'), wh_price, wh_prof_pct,
                                  "close via TradingView webhook", strategy=strategy, base_usd=wh_total_usd)
                    remove_from_active_deals(symbol)
                    if strategy in ('brkX2', 'hunting_4h', 'reversal', 'brkX2_4h', 'brkX2_crossema', 'akum_entry_a', 'akum_entry_b', 'trend_confirm_4h'):
                        record_closed(symbol)
                    send_telegram(
                        f"{strategy} | CLOSE LONG (TradingView webhook)\n"
                        f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                        f"Pair   : {to_display_pair(symbol)}\n"
                        f"Exit   : {_fmt_price(wh_price)}\n"
                        f"Profit : {wh_prof_pct:+.2f}%"
                    )
            if ok and action == "start_trailing":
                with active_deals_lock:
                    if symbol in active_deals:
                        active_deals[symbol]["trailing_armed"] = True
                save_active_deals()
            return jsonify({"ok": ok, "action": action, "symbol": symbol})

        # ── Hunting-4h API endpoints ──────────────────────────────────────
        @app.route("/api/strategy_config/reset", methods=["POST"])
        def api_strategy_config_reset():
            save_strategy_config({k: dict(v) for k, v in STRATEGY_CONFIG_DEFAULTS.items()})
            log("[STRATEGY_CONFIG] Reset ke default")
            return jsonify({"ok": True})

        @app.route("/api/ai_provider_config", methods=["GET", "POST"])
        def api_ai_provider_config():
            if request.method == "POST":
                try:
                    save_ai_provider_config((request.get_json(force=True, silent=True) or {}).get("mode", "anthropic_gemini"))
                except ValueError as error:
                    return jsonify({"ok": False, "error": str(error)}), 400
            return jsonify({"ok": True, **load_ai_provider_config()})

        @app.route("/api/simulate_balance_conversion", methods=["POST"])
        def api_simulate_balance_conversion():
            try:
                payload = request.get_json(force=True, silent=True) or {}
                threshold = float(payload.get("threshold", 0))
                requested = float(payload.get("amount", 0))
                asset = str(payload.get("asset", "BIDR")).upper()
                if threshold < 0 or requested < 0 or asset not in {"BIDR", "IDRT"}:
                    return jsonify({"ok": False, "error": "Parameter simulasi tidak valid"}), 400
                balance = get_binance_usdt_balance()
                locked = get_estimated_locked_usd()
                available = max(0.0, balance - locked)
                amount = min(requested, available) if requested > 0 else max(0.0, available - threshold)
                return jsonify({
                    "ok": True, "simulation_only": True, "asset": asset,
                    "usdt_balance": round(balance, 8), "locked_usdt": round(locked, 8),
                    "available_usdt": round(available, 8), "threshold_usdt": round(threshold, 8),
                    "would_trigger": balance >= threshold, "estimated_convert_usdt": round(amount, 8),
                    "message": "Simulasi saja; tidak ada order Binance yang dikirim.",
                })
            except Exception as error:
                return jsonify({"ok": False, "error": str(error)}), 500

        @app.route("/api/binance_spot_assets")
        def api_binance_spot_assets():
            try:
                return jsonify({"ok": True, "assets": get_binance_spot_assets()})
            except Exception as error:
                return jsonify({"ok": False, "assets": [], "error": str(error)}), 500

        @app.route("/api/auto_sell_price")
        def api_auto_sell_price():
            asset = str(request.args.get("asset", "")).upper().strip()
            if not asset.isalnum():
                return jsonify({"ok": False, "error": "Asset tidak valid"}), 400
            symbol = asset + "USDT"
            try:
                price = get_price_now(symbol)
                if price <= 0:
                    return jsonify({"ok": False, "error": "Harga tidak tersedia"}), 404
                config = load_auto_sell_config()
                threshold = float(config.get("assets", {}).get(asset, {}).get("threshold_usdt", 0) or 0)
                gap_pct = ((threshold / price) - 1) * 100 if threshold > 0 else 0
                avg_info = get_hold_no_sell_price(asset)
                avg_price = get_effective_avg_price(asset)
                cfg_manual = float(config.get("assets", {}).get(asset, {}).get("avg_price", 0) or 0)
                deal_entry = get_active_deal_avg_price(asset)
                if deal_entry > 0:
                    avg_source = "active_deal"
                elif avg_info and avg_info.get("entry_price", 0) > 0:
                    avg_source = "auto"
                elif cfg_manual > 0:
                    avg_source = "manual"
                elif avg_price > 0:
                    avg_source = "binance_history"
                else:
                    avg_source = ""
                avg_gap_pct = ((price / avg_price) - 1) * 100 if avg_price > 0 else None
                sugg = get_sell_suggestion(asset)
                # Estimasi profit USDT kalau target tercapai: (target - avg_beli) x qty yg
                # bakal dijual (95% saldo, sama persis logic eksekusi), dikurangi estimasi
                # fee jual ~0.1% (Binance spot taker default, blm termasuk diskon BNB).
                est_profit_usd = None
                try:
                    qty = binance_get_asset_qty(asset)
                    sell_qty = qty * 0.95
                    if threshold > 0 and avg_price > 0 and sell_qty > 0:
                        est_profit_usd = sell_qty * threshold * (1 - 0.001) - sell_qty * avg_price
                except Exception:
                    pass
                return jsonify({"ok": True, "asset": asset, "symbol": symbol,
                                "price": _fmt_price(price), "threshold": _fmt_price(threshold),
                                "gap_pct": f"{gap_pct:+.2f}",
                                "avg_price": _fmt_price(avg_price) if avg_price > 0 else "",
                                "avg_price_source": avg_source,
                                "avg_gap_pct": f"{avg_gap_pct:+.2f}" if avg_gap_pct is not None else "",
                                "breakeven": _fmt_price(sugg["breakeven"]) if sugg["breakeven"] > 0 else "",
                                "resistance": _fmt_price(sugg["resistance"]) if sugg["resistance"] else "",
                                "breakeven_warning": sugg["warning"] or "",
                                "htf_trend": sugg["htf_trend"] or "",
                                "est_profit_usd": f"{est_profit_usd:+.2f}" if est_profit_usd is not None else ""})
            except Exception as error:
                return jsonify({"ok": False, "error": str(error)}), 500

        @app.route("/api/auto_sell_config", methods=["GET", "POST"])
        def api_auto_sell_config():
            """GET: daftar semua asset auto-sell. POST: upsert (atau hapus) 1 entry asset,
            asset lain di daftar tidak terpengaruh."""
            try:
                if request.method == "POST":
                    payload = request.get_json(force=True, silent=True) or {}
                    asset = str(payload.get("asset", "")).upper().strip()
                    if not asset:
                        return jsonify({"ok": False, "error": "Asset wajib diisi"}), 400
                    if payload.get("remove"):
                        remove_auto_sell_asset(asset)
                    else:
                        enabled   = bool(payload.get("enabled", False))
                        threshold = float(payload.get("threshold_usdt", 0) or 0)
                        if enabled and threshold <= 0:
                            return jsonify({"ok": False, "error": "Threshold wajib diisi"}), 400
                        avg_price = payload.get("avg_price", None)
                        avg_price = float(avg_price) if avg_price not in (None, "") else None
                        convert_bnb = payload.get("convert_leftover_bnb", None)
                        convert_bnb = bool(convert_bnb) if convert_bnb is not None else None
                        upsert_auto_sell_asset(asset, enabled, threshold, avg_price, convert_bnb)
                return jsonify({"ok": True, **load_auto_sell_config()})
            except Exception as error:
                return jsonify({"ok": False, "error": str(error)}), 500

        @app.route("/api/convert_candidates")
        def api_convert_candidates():
            """Scan kandidat convert utk 1 asset hold_no_sell (04/09/2026, fitur Convert)."""
            asset = str(request.args.get("asset", "")).upper().strip()
            if not asset.isalnum():
                return jsonify({"ok": False, "error": "Asset tidak valid"}), 400
            try:
                data = find_convert_candidates(asset)
                status = 200 if data.get("ok") else 400
                return jsonify(data), status
            except Exception as error:
                return jsonify({"ok": False, "error": str(error), "candidates": []}), 500

        @app.route("/api/convert_execute", methods=["POST"])
        def api_convert_execute():
            """Eksekusi convert asset (jual source lalu beli target market, berurutan).
            Outward-facing / mengirim order Binance sungguhan -- trigger manual dari dashboard."""
            try:
                payload = request.get_json(force=True, silent=True) or {}
                source_asset = str(payload.get("source_asset", "")).upper().strip()
                target_symbol = str(payload.get("target_symbol", "")).upper().strip()
                if not source_asset or not target_symbol:
                    return jsonify({"ok": False, "error": "source_asset & target_symbol wajib diisi"}), 400
                data = execute_convert(source_asset, target_symbol)
                return jsonify(data), (200 if data.get("ok") else 400)
            except Exception as error:
                return jsonify({"ok": False, "error": str(error)}), 500

        @app.route("/api/hunting_config", methods=["POST"])
        def api_hunting_config():
            with _hunting_lock:
                _hunting_config.update(request.get_json(force=True, silent=True) or {})
            return jsonify({"ok": True})

        @app.route("/api/hunting_signals")
        def api_hunting_signals():
            with _hunting_lock:
                return jsonify({
                    "signals": list(_hunting_signals),
                    "scan_ts": _hunting_scan_ts,
                })

        def categorize_exit_reason(reason: str) -> str:
            """Kelompokkan exit_reason (teks bebas) jadi kategori tetap buat filter dashboard."""
            r = (reason or "").strip().lower()
            if r.startswith("tp "):
                return "Take Profit (custom)"
            if r.startswith("trailing"):
                return "Trailing"
            if r.startswith("hard stop volatilitas"):
                return "Hard Stop"
            if r.startswith("batas") and "candle" in r:
                return "Timeout (candle limit)"
            if r.startswith("manual reconcile"):
                return "Manual Reconcile"
            return "Lainnya"

        @app.route("/api/closed_trades")
        def api_closed_trades():
            """Baca trades_forwardtest.csv dan kembalikan semua CLOSED trades + stats."""
            try:
                import datetime as _dt
                strategy_filter = request.args.get("strategy", "")
                exclude_hardstop = request.args.get("exclude_hardstop", "") in ("1", "true", "True")
                pair_filter = request.args.get("pair", "").upper().replace("/", "")
                date_from = request.args.get("date_from", "").strip()  # "YYYY-MM-DD", filter by close_time_wib
                date_to   = request.args.get("date_to", "").strip()
                outcome_filter = request.args.get("outcome", "")  # "win" | "loss"
                reason_filter  = request.args.get("reason_category", "")
                rows = []
                if os.path.exists(TRADES_CSV):
                    with trades_csv_lock:
                        with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            rows = []
                            for r in reader:
                                if r.get('status') == 'CLOSED':
                                    r = repair_stale_ondo_base_usd(r)
                                    rows.append(r)
                phase_offsets = {
                    'brkX2': FWDTEST_BRKX2_PHASE_OFFSET,
                    'hunting_4h': HUNTING_FWDTEST_PHASE_OFFSET,
                }
                if strategy_filter:
                    if strategy_filter == 'akumulasi':
                        rows = [r for r in rows if (r.get('strategy') or 'brkX2') in ('akum_entry_a', 'akum_entry_b')]
                    else:
                        rows = [r for r in rows if (r.get('strategy') or 'brkX2') == strategy_filter]
                    offset = phase_offsets.get(strategy_filter, 0)
                    if offset:
                        rows = rows[offset:]
                else:
                    rows_by_strategy = {}
                    for row in rows:
                        key = row.get('strategy') or 'brkX2'
                        rows_by_strategy.setdefault(key, []).append(row)
                    rows = []
                    for key, strategy_rows in rows_by_strategy.items():
                        offset = phase_offsets.get(key, 0)
                        rows.extend(strategy_rows[offset:] if offset else strategy_rows)
                if exclude_hardstop:
                    rows = [r for r in rows if not (r.get('exit_reason') or '').lower().startswith('hard stop volatilitas')]
                if pair_filter:
                    rows = [r for r in rows if (r.get('symbol') or '').replace('/', '').upper() == pair_filter]
                if date_from:
                    rows = [r for r in rows if (r.get('close_time_wib') or '')[:10] >= date_from]
                if date_to:
                    rows = [r for r in rows if (r.get('close_time_wib') or '')[:10] <= date_to]
                if outcome_filter == "win":
                    rows = [r for r in rows if float(r.get('profit_pct') or 0) > 0]
                elif outcome_filter == "loss":
                    rows = [r for r in rows if float(r.get('profit_pct') or 0) <= 0]
                if reason_filter:
                    rows = [r for r in rows if categorize_exit_reason(r.get('exit_reason', '')) == reason_filter]
                # Sort terbaru dulu
                rows.sort(key=lambda r: r.get('close_time_wib',''), reverse=True)
                # Hitung stats dan profit$
                trades = []
                wins = losses = 0
                total_pnl_pct = total_pnl_usd = 0.0
                for r in rows:
                    pct    = float(r.get('profit_pct') or 0)
                    base   = float(r.get('base_usd') or 8)
                    p_usd  = round(pct / 100 * base, 2)
                    # Durasi
                    dur = '-'
                    try:
                        if r.get('open_time_wib') and r.get('close_time_wib'):
                            fmt = "%Y-%m-%d %H:%M:%S"
                            t0 = _dt.datetime.strptime(r['open_time_wib'][:19], fmt)
                            t1 = _dt.datetime.strptime(r['close_time_wib'][:19], fmt)
                            d  = t1 - t0
                            h, rem = divmod(int(d.total_seconds()), 3600)
                            m = rem // 60
                            dur = f"{h}j {m}m" if h else f"{m}m"
                    except Exception:
                        pass
                    if pct > 0: wins += 1
                    else: losses += 1
                    total_pnl_pct += pct
                    total_pnl_usd += p_usd
                    trades.append({
                        "open_time":    r.get('open_time_wib',''),
                        "close_time":   r.get('close_time_wib',''),
                        "symbol":       r.get('symbol',''),
                        "strategy":     r.get('strategy',''),
                        "entry_price":  r.get('entry_price',''),
                        "exit_price":   r.get('exit_price',''),
                        "profit_pct":   pct,
                        "profit_usd":   p_usd,
                        "base_usd":     base,
                        "exit_reason":  r.get('exit_reason',''),
                        "reason_category": categorize_exit_reason(r.get('exit_reason','')),
                        "duration":     dur,
                        "rsi_open":     r.get('rsi_open',''),
                    })
                total = wins + losses
                wr = round(wins / total * 100, 1) if total else 0
                all_pairs = [{"symbol": s, "display": to_display_pair(s)} for s in get_closed_trades_distinct_pairs()]
                return jsonify({
                    "trades": trades,
                    "all_pairs": all_pairs,
                    "stats": {
                        "total":         total,
                        "wins":          wins,
                        "losses":        losses,
                        "wr":            wr,
                        "total_pnl_pct": round(total_pnl_pct, 2),
                        "total_pnl_usd": round(total_pnl_usd, 2),
                    }
                })
            except Exception as e:
                return jsonify({"trades": [], "stats": {}, "error": str(e)})

        @app.route("/api/scan_blockers")
        def api_scan_blockers():
            return jsonify({"ok": True, "scans": get_scan_blockers()})

        @app.route("/api/blocked_pairs", methods=["GET", "POST"])
        def api_blocked_pairs():
            """GET: daftar pair yg diblok + daftar pair tersedia (dari Closed Trades, blm diblok).
            POST: {action: 'block'|'unblock', symbol: 'TRUMPUSDT'}."""
            try:
                if request.method == "POST":
                    payload = request.get_json(force=True, silent=True) or {}
                    action = str(payload.get("action", "")).lower()
                    symbol = str(payload.get("symbol", "")).upper().strip()
                    if not symbol.isalnum():
                        return jsonify({"ok": False, "error": "Symbol tidak valid"}), 400
                    if action == "block":
                        block_pair(symbol)
                    elif action == "unblock":
                        if not unblock_pair(symbol):
                            return jsonify({"ok": False, "error": "Pair ini dikunci developer, tidak bisa di-unblock lewat menu"}), 400
                    else:
                        return jsonify({"ok": False, "error": "action harus 'block' atau 'unblock'"}), 400
                with blocked_pairs_meta_lock:
                    meta = dict(blocked_pairs_meta)
                blocked = []
                # Manual (blok user lewat menu) di atas, sistem (hardcoded) di bawah -- masing-masing grup A-Z
                for sym in sorted(SYMBOL_BLACKLIST, key=lambda s: (s in SYMBOL_BLACKLIST_HARDCODED, s)):
                    blocked.append({
                        "symbol": sym,
                        "display": to_display_pair(sym),
                        "blocked_at": meta.get(sym, {}).get("blocked_at", ""),
                        "hardcoded": sym in SYMBOL_BLACKLIST_HARDCODED,
                    })
                available = [{"symbol": s, "display": to_display_pair(s)}
                             for s in get_closed_trades_distinct_pairs() if s not in SYMBOL_BLACKLIST]
                return jsonify({"ok": True, "blocked": blocked, "available": available})
            except Exception as error:
                return jsonify({"ok": False, "error": str(error)}), 500

        @app.route("/api/daily_loss_limit", methods=["GET", "POST"])
        def api_daily_loss_limit():
            """GET: status batas rugi harian (limit, P&L hari ini, tersulut atau tidak).
            POST: {limit_pct: 3.0} -- update angka batasnya."""
            try:
                if request.method == "POST":
                    payload = request.get_json(force=True, silent=True) or {}
                    try:
                        pct = float(payload.get("limit_pct", daily_loss_limit_pct))
                    except (TypeError, ValueError):
                        return jsonify({"ok": False, "error": "limit_pct tidak valid"}), 400
                    save_daily_loss_limit(pct)
                status = get_daily_loss_status()
                return jsonify({"ok": True, **status})
            except Exception as error:
                return jsonify({"ok": False, "error": str(error)}), 500

        log(f"[WEB] Dashboard jalan di port {WEB_PORT}")
        app.run(host="0.0.0.0", port=WEB_PORT, debug=False, use_reloader=False)
    except Exception as e:
        log(f"WARN web dashboard error: {e}")

# ============================================================
# AI DECISION ENGINE
# Dipanggil saat ai_call=True di deal_overrides untuk suatu pair.
# Menggunakan Anthropic API terlebih dahulu, lalu Gemini sebagai fallback:
#   - OPEN: buka deal atau skip (T1/T1b/T1d)
#   - ARMED: arm trailing sekarang atau tahan (T2)
#   - CLOSE: close deal sekarang atau hold (T2)
# ============================================================
import urllib.request as _urllib_req

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
AI_DECISION_MODEL  = "claude-sonnet-5"   # naik dari Haiku 4.5 (29/08/2026, maksimalkan kredit Anthropic yg jarang kepakai)
# 04/09/2026: premis 29/08 di atas ("kredit jarang kepakai") sudah nggak berlaku lagi --
# kredit Anthropic (dipakai lintas bot + Claude Code) terbukti terpakai deras, Sep 3 saja
# ~$7 dari ~$17 kredit awal. Babak 1 (ai_decision_open, saringan per-kandidat SEBELUM
# batch-rank) adalah kontributor volume panggilan AI TERBESAR (~18x lebih sering dari
# babak 2, dari data ai_decisions_log.txt riil: 273 vs 15 panggilan dlm 2.8 jam) sehingga
# jadi target pertama diturunkan. Haiku 4.5 harganya PERSIS setengah Sonnet 5 baik input
# maupun output ($1/$5 per MTok vs $2/$10) -- turunkan babak 1 saja memotong porsi
# terbesar biaya AI bot tanpa mengubah logika 2-babak yg baru dibangun. Babak 2
# (ai_decision_batch_rank) dan semua ai_decision_* aktif-deal (armed/add_fund/
# near_timeout/close) TETAP Sonnet 5 -- volume jauh lebih kecil, keputusannya lebih
# konsekuensial (langsung pengaruhi deal aktif), jadi belum diturunkan.
AI_DECISION_MODEL_BABAK1 = "claude-haiku-4-5-20251001"
GEMINI_API_KEY     = os.environ.get("GEMINI_API_KEY", "")
GEMINI_AI_MODEL    = os.environ.get("GEMINI_AI_MODEL", "gemini-flash-latest")
AI_DECISION_TIMEOUT = 25  # detik -- dinaikkan dari 10 (30/08/2026), seiringan sama max_tokens 200->1024:
# model butuh waktu proses lebih lama buat budget token yg lebih besar (apalagi kalau isi block "thinking"
# dulu), 10 detik kekecilan -> sering "Gagal: The read operation timed out" padahal API-nya sehat, cuma
# belum sempat selesai jawab dalam 10 detik.
AI_PROVIDER_CONFIG_FILE = os.path.join(DATA_DIR, "ai_provider_config.json")
AI_PRIMARY_PROVIDER = os.environ.get("AI_PRIMARY_PROVIDER", "anthropic").lower()
AI_FALLBACK_PROVIDER = os.environ.get("AI_FALLBACK_PROVIDER", "gemini").lower()
AI_LAST_PROVIDER = "Belum ada keputusan AI"

_ai_quota_notif_sent = False  # flag agar notif quota habis tidak berulang

def _anthropic_ai_call(prompt: str, model: str = None) -> str:
    """Panggil Anthropic sebagai provider utama.
    model: override model per-panggilan (04/09/2026, dipakai babak 1 -> Haiku 4.5
    utk hemat biaya) -- default None berarti pakai AI_DECISION_MODEL (Sonnet 5)."""
    global _ai_quota_notif_sent
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY belum di-set")
    try:
        import json as _json
        payload = _json.dumps({
            "model": model or AI_DECISION_MODEL,
            "max_tokens": 1024,  # dinaikkan dari 200 (30/08/2026) -- Sonnet 5 kadang isi block "thinking"
            # dulu sebelum jawaban teks; budget 200 abis semua kepakai thinking, jawaban teksnya sendiri
            # nggak pernah kebentuk (response cuma berisi block "thinking", 0 block "text").
            "output_config": {"effort": "low"},  # baru (01/09/2026) -- ROOT CAUSE asli: Sonnet 5 defaultnya
            # ADAPTIVE THINKING SELALU AKTIF walau parameter "thinking" nggak pernah dikirim sama sekali,
            # jadi kadang proses "mikir"-nya sendiri abisin seluruh max_tokens sebelum sempat nulis
            # jawaban teks (masih kejadian lagi walau max_tokens udah dinaikkan ke 1024). Fix resmi yg
            # direkomendasikan Anthropic: bukan matiin thinking total (bisa bikin model bocorin tag
            # internal ke teks visible), tapi batasi KEDALAMAN-nya lewat effort="low" -- thinking tetap
            # jalan tapi jauh lebih ringkas, cocok buat tugas sederhana kayak keputusan open/close ini.
            "messages": [{"role": "user", "content": prompt}]
        }).encode()
        req = _urllib_req.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            method="POST"
        )
        with _urllib_req.urlopen(req, timeout=AI_DECISION_TIMEOUT) as resp:
            body = _json.loads(resp.read())
            # content bisa berisi lebih dari satu block (mis. block "thinking" muncul
            # sebelum block "text" tergantung request-nya) -- jangan asumsikan block
            # pertama selalu berisi "text". Ini penyebab bug lama: KeyError('text')
            # intermiten yang salah dikira "Anthropic API down" (bolak-balik notif ON/OFF)
            # padahal response-nya sebenarnya valid, cuma block teksnya bukan di index 0.
            blocks = body.get("content", []) or []
            text = ""
            for block in blocks:
                if isinstance(block, dict) and block.get("type") == "text" and block.get("text"):
                    text = block["text"].strip()
                    break
            if not text:
                block_types = [b.get("type", "?") for b in blocks if isinstance(b, dict)]
                raise RuntimeError(f"Response tidak berisi block teks (block types: {block_types or 'kosong'})")
            # Reset flag kalau API kembali normal -- taruh SETELAH berhasil ambil teks,
            # supaya nggak kirim "kembali ON" untuk call yang ternyata masih gagal.
            if _ai_quota_notif_sent:
                _ai_quota_notif_sent = False
                _msg_ok = "✅ AI Decision kembali ON\nAnthropic API sudah normal kembali."
                send_telegram(_msg_ok, parse_mode=None)
                threading.Thread(
                    target=send_email_open_long,
                    args=("✅ AI Decision kembali ON", _msg_ok),
                    daemon=True
                ).start()
            return text
    except Exception:
        raise


def _gemini_ai_call(prompt: str) -> str:
    """Panggil Gemini sebagai fallback provider."""
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY belum di-set")
    import json as _json
    payload = _json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": 200},
    }).encode()
    endpoint = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        + GEMINI_AI_MODEL + ":generateContent?key=" + GEMINI_API_KEY
    )
    req = _urllib_req.Request(
        endpoint,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with _urllib_req.urlopen(req, timeout=AI_DECISION_TIMEOUT) as resp:
        body = _json.loads(resp.read())
    return body["candidates"][0]["content"]["parts"][0]["text"].strip()


def _ai_call(prompt: str, model: str = None) -> str:
    """Panggil provider terpilih lalu fallback provider dan rule-based.
    model: diteruskan ke _anthropic_ai_call (override per-panggilan, mis. babak 1
    pakai Haiku 4.5) -- tidak berlaku ke provider Gemini (model Gemini diatur
    terpisah lewat env var GEMINI_AI_MODEL, di luar cakupan optimasi ini)."""
    global _ai_quota_notif_sent, AI_LAST_PROVIDER
    mode = load_ai_provider_config().get("mode", "anthropic_gemini")
    if mode == "rule_based":
        AI_LAST_PROVIDER = "rule-based Python"
        return ""
    if mode == "anthropic_only":
        providers = ["anthropic"]
    elif mode == "gemini_only":
        providers = ["gemini"]
    else:
        providers = [AI_PRIMARY_PROVIDER, AI_FALLBACK_PROVIDER]
        providers = [provider for provider in providers if provider in {"anthropic", "gemini"}]
        if not providers:
            providers = ["anthropic", "gemini"]
        providers = list(dict.fromkeys(providers))
    provider_errors = {}
    for provider in providers:
        try:
            result = _anthropic_ai_call(prompt, model=model) if provider == "anthropic" else _gemini_ai_call(prompt)
            if result:
                AI_LAST_PROVIDER = "Anthropic" if provider == "anthropic" else "Gemini AI Studio"
                if provider == "gemini": log("[AI] Keputusan memakai Gemini fallback")
                return result
        except Exception as error:
            error_text = str(error)
            provider_errors[provider] = error_text
            log(f"WARN [AI] {provider} gagal: {error_text[:160]}")
    AI_LAST_PROVIDER = "rule-based Python"
    if not _ai_quota_notif_sent:
        _ai_quota_notif_sent = True
        _lines = ["AI Decision provider tidak tersedia."]
        for provider, error_text in provider_errors.items():
            _lines.append(f"{provider.capitalize()}: {_classify_ai_error(error_text)}")
        _lines.append("Bot memakai rule-based Python sebagai fallback terakhir.")
        send_telegram("\n".join(_lines), parse_mode=None)
        _err_summary = "; ".join(f"{p}={e[:80]}" for p, e in provider_errors.items())
        log(f"WARN [AI] fallback terakhir rule-based aktif; {_err_summary}")
    return ""


def _classify_ai_error(error_text: str) -> str:
    """Terjemahkan error HTTP mentah jadi penyebab yang jelas (bukan asumsi 'billing habis')."""
    t = error_text.lower()
    if "401" in t or "unauthorized" in t:
        return f"API key tidak valid/revoked (401 Unauthorized) — cek env var API key. [{error_text[:100]}]"
    if "403" in t or "forbidden" in t:
        return f"Akses ditolak (403 Forbidden) — cek permission/region key. [{error_text[:100]}]"
    if "429" in t or "rate limit" in t or "quota" in t:
        return f"Rate limit/quota terlampaui (429). [{error_text[:100]}]"
    if "404" in t:
        return f"Endpoint/model tidak ditemukan (404) — cek nama model atau API key. [{error_text[:100]}]"
    if "credit" in t or "billing" in t or "insufficient" in t:
        return f"Credit/billing habis. [{error_text[:100]}]"
    return f"Gagal: {error_text[:120]}"


def load_ai_provider_config() -> dict:
    """Muat mode provider dari file; environment variables menjadi default."""
    default = {"mode": "anthropic_gemini"}
    try:
        if os.path.exists(AI_PROVIDER_CONFIG_FILE):
            with open(AI_PROVIDER_CONFIG_FILE, encoding="utf-8") as file:
                mode = json.load(file).get("mode", default["mode"])
                if mode in {"anthropic_gemini", "anthropic_only", "gemini_only", "rule_based"}:
                    default["mode"] = mode
    except Exception as error:
        log(f"WARN load_ai_provider_config: {error}")
    return {**default, "primary": AI_PRIMARY_PROVIDER, "fallback": AI_FALLBACK_PROVIDER,
            "last_provider": AI_LAST_PROVIDER,
            "anthropic_configured": bool(ANTHROPIC_API_KEY),
            "gemini_configured": bool(GEMINI_API_KEY)}


def save_ai_provider_config(mode: str) -> None:
    if mode not in {"anthropic_gemini", "anthropic_only", "gemini_only", "rule_based"}:
        raise ValueError("mode provider tidak valid")
    with open(AI_PROVIDER_CONFIG_FILE, "w", encoding="utf-8") as file:
        json.dump({"mode": mode}, file, indent=2)


def ai_provider_note() -> str:
    return f"Provider: {AI_LAST_PROVIDER}"


def _tf_indicator_line_for_ai(symbol: str, tf_label: str, tf_binance: str, limit: int) -> str | None:
    """1 baris ringkasan indikator lengkap utk 1 timeframe (dipakai HTF maupun LTF).
    Return None kalau data nggak cukup / gagal fetch."""
    try:
        import pandas_ta as _pta
        df_htf = get_ohlcv_htf(symbol, interval=tf_binance, limit=limit)
        if df_htf is None or len(df_htf) < 14:
            return None
        c = df_htf["close"]; h = df_htf["high"]; l = df_htf["low"]
        ema20 = _pta.ema(c, length=20).iloc[-1]
        ema50 = _pta.ema(c, length=50).iloc[-1]
        ema200 = _pta.ema(c, length=200).iloc[-1]
        rsi = _pta.rsi(c, length=14).iloc[-1]
        atr = _pta.atr(h, l, c, length=14).iloc[-1]
        atr_pct = atr / c.iloc[-1] * 100 if c.iloc[-1] > 0 else None
        st = _pta.supertrend(h, l, c, length=7, multiplier=3.0)
        std_col = [col for col in st.columns if "SUPERTd" in col]
        st_dir = int(st[std_col[0]].iloc[-1]) if std_col else 0
        adx_df = _pta.adx(h, l, c, length=14)
        adx_col = [col for col in adx_df.columns if col.startswith("ADX_")]
        adx = adx_df[adx_col[0]].iloc[-1] if adx_col else None
        stoch = _pta.stoch(h, l, c, k=14, d=3, smooth_k=3)
        stoch_k_col = [col for col in stoch.columns if "STOCHk" in col]
        stoch_d_col = [col for col in stoch.columns if "STOCHd" in col]
        stoch_k = stoch[stoch_k_col[0]].iloc[-1] if stoch_k_col else None
        stoch_d = stoch[stoch_d_col[0]].iloc[-1] if stoch_d_col else None
        macd = _pta.macd(c, fast=12, slow=26, signal=9)
        macd_col = [col for col in macd.columns if "MACDh" in col]
        macd_hist = macd[macd_col[0]].iloc[-1] if macd_col else None
        bb = _pta.bbands(c, length=20, std=2)
        bb_col = [col for col in bb.columns if "BBP" in col]
        bb_pct = bb[bb_col[0]].iloc[-1] if bb_col else None
        willr = _pta.willr(h, l, c, length=14).iloc[-1]
        cci = _pta.cci(h, l, c, length=20).iloc[-1]
        obv = _pta.obv(c, df_htf["vol"]).iloc[-1]
        vol_ma = df_htf["vol"].rolling(20).mean().iloc[-1]
        vol_ratio = df_htf["vol"].iloc[-1] / vol_ma if vol_ma > 0 else None
        p20 = (c.iloc[-1] / ema20 - 1) * 100 if ema20 > 0 else None
        p50 = (c.iloc[-1] / ema50 - 1) * 100 if ema50 > 0 else None
        p200 = (c.iloc[-1] / ema200 - 1) * 100 if ema200 > 0 else None
        def fmt(label, value, spec):
            return f"{label}=" + (format(float(value), spec) if value is not None and not pd.isna(value) else "n/a")
        parts = [f"{tf_label}: ST={'Uptrend' if st_dir == 1 else 'Downtrend' if st_dir == -1 else 'Neutral'}",
                 fmt("RSI", rsi, ".1f"), fmt("ATR%", atr_pct, ".2f"), fmt("vs EMA20", p20, "+.2f"),
                 fmt("vs EMA50", p50, "+.2f"), fmt("vs EMA200", p200, "+.2f"), fmt("Stoch%K", stoch_k, ".1f"),
                 fmt("Stoch%D", stoch_d, ".1f"), fmt("MACD hist", macd_hist, "+.6f"), fmt("BB%b", bb_pct, ".2f"),
                 fmt("Williams%R", willr, ".1f"), fmt("CCI", cci, ".1f"), fmt("ADX", adx, ".1f"),
                 fmt("Vol", vol_ratio, ".2f"), fmt("OBV", obv, ".0f"),
                 f"Candle={'bullish' if c.iloc[-1] > df_htf['open'].iloc[-1] else 'bearish'}"]
        return " | ".join(parts)
    except Exception:
        return None


def fetch_htf_context_for_ai(symbol: str) -> str:
    """
    Fetch indikator HTF (1D, 3D, 1W) untuk konteks AI decision.
    Dipakai di OPEN dan ARMED. Return string siap masuk prompt.
    (29/08/2026: nambah 1D -- sebelumnya cuma 3D+1W. Juga fix bug lama:
    get_ohlcv_htf() dipanggil pakai keyword salah [tf= bukan interval=],
    jadi TypeError kena try/except tiap timeframe & context ini SELALU
    kosong dari awal dibuat -- AI nggak pernah beneran lihat data HTF ini.)
    """
    try:
        lines_out = []
        for tf_label, tf_binance, limit in [("HTF 1D", "1d", 250), ("HTF 3D", "3d", 250), ("HTF 1W", "1w", 250)]:
            line = _tf_indicator_line_for_ai(symbol, tf_label, tf_binance, limit)
            if line:
                lines_out.append(line)
        return "\n".join(lines_out) if lines_out else ""
    except Exception as e:
        log(f"WARN [AI HTF] fetch gagal: {e}")
        return ""


def fetch_ltf_context_for_ai(symbol: str) -> str:
    """Fetch indikator LTF (1h) untuk konteks AI decision OPEN -- momentum jangka
    pendek di bawah TF eksekusi strategi, biar AI juga lihat apa harga lagi
    'kehabisan tenaga' atau justru baru mulai jalan di skala kecil. 29/08/2026."""
    try:
        line = _tf_indicator_line_for_ai(symbol, "LTF 1h", "1h", 250)
        return line or ""
    except Exception as e:
        log(f"WARN [AI LTF] fetch gagal: {e}")
        return ""


def get_full_4h_indicator_context(symbol: str) -> str:
    """
    Fetch semua indicators 4h untuk konteks AI decision (OPEN, ARMED, ADD FUND).
    Mencakup: RSI, Stoch, MACD, BB%b, Williams%R, CCI, OBV, RVOL, EMA200, ADX, candlestick pattern.
    """
    try:
        import pandas_ta as _pta
        df4 = get_ohlcv_4h(symbol, limit=60)
        if df4 is None or len(df4) < 20:
            return ""
        df4 = compute_indicators_4h(df4)
        r4  = df4.iloc[-1]; r4p = df4.iloc[-2]
        c   = df4["close"]

        def _safe(key):
            v = r4.get(key, float("nan")) if key in r4.index else float("nan")
            return None if pd.isna(v) else float(v)

        parts = []
        st_dir  = int(r4.get("st_dir",0)) if "st_dir" in r4.index and not pd.isna(r4.get("st_dir")) else None
        rsi     = _safe("rsi");    sk = _safe("stoch_k"); sd = _safe("stoch_d")
        macd_h  = _safe("macd_hist"); bb_pct = _safe("bb_pct")
        wr      = _safe("williams_r"); cci = _safe("cci"); obv_now = _safe("obv")
        obv_prv = float(r4p.get("obv",float("nan"))) if "obv" in r4p.index and not pd.isna(r4p.get("obv")) else None
        vol     = float(r4.get("vol",0)) if "vol" in r4.index else 0
        vol_ma  = float(r4.get("vol_ma",0)) if "vol_ma" in r4.index else 0
        vol_rat = vol/vol_ma if vol_ma>0 else None
        try:
            rvol_ma = df4["vol"].rolling(20).mean().iloc[-1]
            rvol    = vol/rvol_ma if rvol_ma>0 else None
        except Exception:
            rvol = None
        try:
            ema200    = float(_pta.ema(c, length=200).iloc[-1])
            ema200_d  = (float(r4["close"])/ema200-1)*100 if ema200>0 else None
        except Exception:
            ema200_d = None
        try:
            adx_df  = _pta.adx(df4["high"],df4["low"],c,length=14)
            adx_col = [col for col in adx_df.columns if col.startswith("ADX_")]
            adx_val = float(adx_df[adx_col[0]].iloc[-1]) if adx_col else None
        except Exception:
            adx_val = None

        if st_dir    is not None: parts.append(f"ST={'Uptrend' if st_dir==1 else 'Downtrend'}")
        if rsi       is not None: parts.append(f"RSI={rsi:.1f}")
        if sk        is not None: parts.append(f"Stoch%K={sk:.1f}")
        if sd        is not None: parts.append(f"Stoch%D={sd:.1f}")
        if macd_h    is not None: parts.append(f"MACD hist={macd_h:+.6f}")
        if bb_pct    is not None: parts.append(f"BB%b={bb_pct:.2f}")
        if wr        is not None: parts.append(f"Williams%R={wr:.1f}")
        if cci       is not None: parts.append(f"CCI={cci:.1f}")
        if vol_rat   is not None: parts.append(f"Vol={vol_rat:.2f}xMA")
        if rvol      is not None: parts.append(f"RVOL={rvol:.2f}x")
        if adx_val   is not None: parts.append(f"ADX={adx_val:.1f}")
        if ema200_d  is not None: parts.append(f"vs EMA200={ema200_d:+.2f}%")
        if obv_now is not None and obv_prv is not None:
            parts.append(f"OBV={'↑' if obv_now>obv_prv else '↓'}")

        # Candlestick pattern
        try:
            c_n=float(r4["close"]); o_n=float(r4["open"])
            h_n=float(r4["high"]);  l_n=float(r4["low"])
            c_p=float(r4p["close"]); o_p=float(r4p["open"])
            body=abs(c_n-o_n); full_r=h_n-l_n if h_n>l_n else 1e-10; br=body/full_r
            uw=h_n-max(c_n,o_n); lw=min(c_n,o_n)-l_n
            pats=[]
            if br<0.15: pats.append("Doji")
            if c_n>o_n and o_n<c_p and c_n>c_p: pats.append("Bullish Engulfing")
            if c_n<o_n and o_n>c_p and c_n<c_p: pats.append("Bearish Engulfing")
            if uw>body*2: pats.append("Upper wick panjang")
            if lw>body*2: pats.append("Lower wick panjang")
            if c_n>c_p: pats.append("Candle bullish")
            elif c_n<c_p: pats.append("Candle bearish")
            if pats: parts.append("Pattern: "+", ".join(pats))
        except Exception:
            pass

        return "Indikator 4h: " + " | ".join(parts) if parts else ""
    except Exception as e:
        log(f"WARN [AI IND] get_full_4h gagal: {e}")
        return ""


def ai_decision_open(symbol: str, strategy: str, indicators: dict, n_active: int, notify: bool = True) -> bool:
    """
    AI decide: buka deal atau skip.
    Return True = buka, False = skip.
    Default True jika AI tidak tersedia / error.
    Fetch HTF 1D/3D/1W dan LTF 1h untuk konteks tambahan (tidak time-critical).
    (29/08/2026: nambah 1D + LTF 1h, plus fix bug lama HTF yg selalu kosong.)
    notify=False (03/09/2026, dipakai TrenKonfirmasi-4h babak 1/batch): skip kirim
    Telegram di sini -- dipakai saat keputusan ini BELUM final (masih akan direview
    ulang di babak 2/batch), supaya user tidak dapat notif "OPEN" yang lalu ternyata
    dibuang di babak berikutnya. Caller yang tanggung jawab kirim notif final sendiri.
    """
    ind_str  = "\n".join(f"- {k}: {v}" for k, v in indicators.items())
    htf_str  = fetch_htf_context_for_ai(symbol)
    ltf_str  = fetch_ltf_context_for_ai(symbol)
    ind4h_str = get_full_4h_indicator_context(symbol)
    htf_section  = f"\nKonteks HTF (1D/3D/1W):\n{htf_str}\n" if htf_str else ""
    ltf_section  = f"\nKonteks LTF (1h):\n{ltf_str}\n" if ltf_str else ""
    ind4h_section = f"\n{ind4h_str}\n" if ind4h_str else ""
    prompt = (
        f"Kamu adalah AI analyst trading crypto. Berikan analisis singkat.\n\n"
        f"Strategi: {strategy} | Pair: {to_display_pair(symbol)}\n"
        f"Deal aktif saat ini: {n_active}\n\n"
        f"Indikator saat sinyal:\n{ind_str}\n"
        f"{ind4h_section}"
        f"{htf_section}"
        f"{ltf_section}\n"
        f"Semua filter rule sudah lolos.\n\n"
        f"Format jawaban (ikuti persis):\n"
        f"OPEN atau SKIP\n"
        f"Target: estimasi target profit % dan harga resistance terdekat\n"
        f"Momentum: ringkasan kondisi momentum\n"
        f"Warning: risiko atau kondisi yang perlu diwaspadai (atau 'Tidak ada')\n"
        f"Trail: saran trailing (normal/ketat/longgar)\n\n"
        f"Baris pertama HARUS hanya kata OPEN atau SKIP."
    )
    # 04/09/2026: babak 1 pakai Haiku 4.5 (bukan AI_DECISION_MODEL/Sonnet 5 default) --
    # ini call terbanyak (per-kandidat, tiap siklus scan), jadi target pertama diturunkan
    # buat hemat biaya. Babak 2 (batch-rank) & semua ai_decision_* aktif-deal lain TETAP
    # Sonnet 5 lewat default _ai_call(prompt) tanpa param model.
    result = _ai_call(prompt, model=AI_DECISION_MODEL_BABAK1)
    if not result:
        # AI tidak tersedia sama sekali -- fail-open ke OPEN, tapi tetap dicatat (04/09/2026)
        # supaya kelihatan di riwayat kalau ada open yg terjadi TANPA analisis AI riil.
        log_ai_decision(
            f"[{now_wib().strftime('%Y-%m-%d %H:%M:%S')} WIB] OPEN-DECISION | {strategy} | "
            f"{to_display_pair(symbol)} | OPEN (fail-open, AI tidak tersedia) | notify={notify}\n"
            f"{'─'*36}\n"
        )
        return True
    lines = result.strip().split("\n")
    first_line = lines[0].strip().upper()
    decision = "OPEN" in first_line
    reasoning = "\n".join(lines[1:]).strip() if len(lines) > 1 else ""
    log(f"[AI] OPEN decision {symbol}: {first_line} → {'BUKA' if decision else 'SKIP'}")
    # Riwayat lengkap semua keputusan (OPEN maupun SKIP) ke ai_decisions_log.txt (04/09/2026,
    # permintaan Mas Budi -- utk investigasi/penyelidikan nanti, terlepas dari notify Telegram).
    log_ai_decision(
        f"[{now_wib().strftime('%Y-%m-%d %H:%M:%S')} WIB] OPEN-DECISION | {strategy} | "
        f"{to_display_pair(symbol)} | {'OPEN' if decision else 'SKIP'} | notify={notify}\n"
        f"{ind_str}\n"
        f"Alasan AI: {reasoning if reasoning else '(tidak ada)'}\n"
        f"{'─'*36}\n"
    )
    # SKIP TIDAK PERNAH kirim Telegram lagi (03/09/2026, permintaan Mas Budi, berlaku ke SEMUA
    # 7 strategi -- bukan cuma TrenKonfirmasi-4h): SKIP artinya tidak ada tindakan, notifnya jauh
    # lebih sering & kurang bernilai dibanding OPEN (yg berarti sesuatu benar2 terjadi), dan
    # volume-nya kebukti bikin spam. Tetap dicatat (log() + log_ai_decision() di atas) utk
    # audit/investigasi, cuma dihapus dari Telegram. OPEN tetap gated `notify` spt sebelumnya
    # (TrenKonfirmasi-4h babak 1 masih notify=False di situ, 6 strategi lain default notify=True).
    if notify and decision:
        if reasoning:
            send_telegram(
                f"🤖 AI Analysis | {to_display_pair(symbol)}\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"{ai_provider_note()}\n"
                f"Keputusan: ✅ OPEN\n"
                f"{reasoning}",
                parse_mode=None
            )
    return decision


def ai_decision_batch_rank(candidates: list, strategy_label: str, max_approve: int = 5,
                            extra_guidance: str = "", criteria_note: str = "") -> list:
    """
    Babak KEDUA (03/09/2026, permintaan Mas Budi -- awalnya khusus TrenKonfirmasi-4h,
    digeneralisasi 03/09/2026 supaya dipakai ULANG strategi lain juga tanpa duplikasi
    kode): dipanggil SETELAH sejumlah kandidat lolos AI individual (ai_decision_open,
    notify=False) dan ditahan -- di sini SEMUA kandidat yg ditahan itu dibandingkan
    bersamaan oleh AI, bukan dinilai satu-satu terpisah, supaya keputusan akhir "siapa
    yang benar2 dibuka" mempertimbangkan kandidat lain yg lolos di jendela yg sama
    (bukan cuma first-come-first-served).

    candidates: list of dict, masing2 minimal punya 'symbol','strategy','score','detail'
    (detail = {'atr_pct','rvol','bb_pct','gap_ema20_pct','rsi'}), sudah dalam URUTAN
    RANKING asli dari scan -- urutan ini dipakai sbg fallback kalau AI gagal/tidak
    tersedia.
    strategy_label: nama strategi utk ditampilkan di prompt & notifikasi (WAJIB diisi
    caller -- tidak ada default, supaya tidak ada strategi yg salah label krn lupa).
    extra_guidance: paragraf tambahan opsional di prompt (mis. panduan ATR khusus
    TrenKonfirmasi-4h) -- kosongkan kalau strategi tsb tidak butuh panduan tambahan.
    criteria_note: baris "Kriteria: ..." opsional di notifikasi Telegram, ringkasan
    dimensi yg dibandingkan -- kosongkan utk skip baris ini.

    Return: list of symbol (subset dari candidates, urutan = prioritas terbaik dulu),
    MAKS max_approve. Fail-open: kalau AI tidak tersedia/error/jawaban tidak bisa
    di-parse, fallback ke urutan ranking asli (bukan buka semua tanpa seleksi, dan
    bukan tolak semua) -- dipotong max_approve.
    """
    if not candidates:
        return []
    valid_symbols = {c['symbol'] for c in candidates}
    fallback = [c['symbol'] for c in candidates[:max_approve]]

    lines_desc = []
    for i, c in enumerate(candidates, 1):
        det = c.get('detail', {})
        lines_desc.append(
            f"{i}. {to_display_pair(c['symbol'])} | skor={c.get('score','?')} | "
            f"ATR%={det.get('atr_pct',0):.2f} | RVOL={det.get('rvol',0):.2f}x | "
            f"BB%b={det.get('bb_pct',0):.2f} | vs EMA20={det.get('gap_ema20_pct',0):+.2f}% | "
            f"RSI={det.get('rsi',0):.1f}"
        )
    daftar_str = "\n".join(lines_desc)
    guidance_section = f"{extra_guidance}\n\n" if extra_guidance else ""

    prompt = (
        f"Kamu adalah AI analyst trading crypto. Ada {len(candidates)} kandidat OPEN LONG "
        f"strategi {strategy_label} yang SEMUA sudah lolos syarat wajib teknikal DAN sudah "
        f"lolos penilaian AI individual masing-masing. Sekarang bandingkan SEMUA kandidat "
        f"ini satu sama lain dan pilih maksimal {max_approve} yang KUALITASNYA PALING BAIK "
        f"(bukan sekadar lolos, tapi paling meyakinkan dibanding yang lain di daftar ini) "
        f"-- kandidat yg lebih lemah dibuang meski tadinya lolos penilaian individual.\n\n"
        f"{guidance_section}"
        f"Daftar kandidat:\n{daftar_str}\n\n"
        f"Format jawaban (ikuti persis):\n"
        f"PILIH: <daftar pair dipisah koma, urutan dari paling bagus, mis. BTC/USDT, ETH/USDT>\n"
        f"Alasan: alasan singkat kenapa yang lain tidak dipilih\n\n"
        f"Baris pertama HARUS diawali 'PILIH:'. Kalau tidak ada yang layak sama sekali, "
        f"tulis 'PILIH: TIDAK ADA'."
    )
    result = _ai_call(prompt)
    if not result:
        log(f"[AI] BATCH rank {strategy_label}: AI tidak tersedia, fallback ke urutan ranking asli ({len(fallback)}/{len(candidates)})")
        log_ai_decision(
            f"[{now_wib().strftime('%Y-%m-%d %H:%M:%S')} WIB] AI-BATCH-ANALYSIS | {strategy_label}\n"
            f"Dievaluasi: {len(candidates)} kandidat | AI tidak tersedia -- fallback ke urutan ranking asli\n"
            f"Diloloskan (fallback): {', '.join(to_display_pair(s) for s in fallback)}\n"
            f"{'─'*36}\n"
        )
        return fallback

    lines = result.strip().split("\n")
    first_line = lines[0].strip()
    reasoning = "\n".join(lines[1:]).strip() if len(lines) > 1 else ""
    if not first_line.upper().startswith("PILIH"):
        log(f"[AI] BATCH rank {strategy_label}: format jawaban tidak dikenali, fallback ke urutan ranking asli")
        log_ai_decision(
            f"[{now_wib().strftime('%Y-%m-%d %H:%M:%S')} WIB] AI-BATCH-ANALYSIS | {strategy_label}\n"
            f"Dievaluasi: {len(candidates)} kandidat | Format jawaban AI tidak dikenali -- fallback ke urutan ranking asli\n"
            f"Diloloskan (fallback): {', '.join(to_display_pair(s) for s in fallback)}\n"
            f"Jawaban mentah AI: {result.strip()[:300]}\n"
            f"{'─'*36}\n"
        )
        return fallback

    criteria_line = f"{criteria_note}\n" if criteria_note else ""
    picked_raw = first_line.split(":", 1)[1].strip() if ":" in first_line else ""
    if not picked_raw or "TIDAK ADA" in picked_raw.upper():
        log(f"[AI] BATCH rank {strategy_label}: AI memilih TIDAK ADA dari {len(candidates)} kandidat")
        log_ai_decision(
            f"[{now_wib().strftime('%Y-%m-%d %H:%M:%S')} WIB] AI-BATCH-ANALYSIS | {strategy_label}\n"
            f"Dievaluasi: {len(candidates)} kandidat | Diloloskan: 0\n"
            f"{criteria_line}"
            f"Alasan AI: {reasoning if reasoning else '(tidak ada)'}\n"
            f"{'─'*36}\n"
        )
        return []

    picked_syms = []
    for tok in picked_raw.split(","):
        tok = tok.strip().upper().replace("/", "").replace("-", "")
        for sym in valid_symbols:
            if sym.upper() == tok or sym.upper().replace("USDT", "") == tok.replace("USDT", ""):
                if sym not in picked_syms:
                    picked_syms.append(sym)
                break
    if not picked_syms:
        log(f"[AI] BATCH rank {strategy_label}: gagal parse simbol dari jawaban AI, fallback ke urutan ranking asli")
        log_ai_decision(
            f"[{now_wib().strftime('%Y-%m-%d %H:%M:%S')} WIB] AI-BATCH-ANALYSIS | {strategy_label}\n"
            f"Dievaluasi: {len(candidates)} kandidat | Gagal parse simbol dari jawaban AI -- fallback ke urutan ranking asli\n"
            f"Diloloskan (fallback): {', '.join(to_display_pair(s) for s in fallback)}\n"
            f"Jawaban mentah AI: {result.strip()[:300]}\n"
            f"{'─'*36}\n"
        )
        return fallback
    picked_syms = picked_syms[:max_approve]

    log(f"[AI] BATCH rank {strategy_label}: {len(picked_syms)}/{len(candidates)} diloloskan → {picked_syms}")
    log_ai_decision(
        f"[{now_wib().strftime('%Y-%m-%d %H:%M:%S')} WIB] AI-BATCH-ANALYSIS | {strategy_label}\n"
        f"Dievaluasi: {len(candidates)} kandidat | Diloloskan: {len(picked_syms)}\n"
        f"Terpilih: {', '.join(to_display_pair(s) for s in picked_syms)}\n"
        f"{criteria_line}"
        f"Alasan AI: {reasoning if reasoning else '(tidak ada)'}\n"
        f"{'─'*36}\n"
    )
    return picked_syms


def ai_decision_armed(symbol: str, strategy: str, d: dict, price: float, peak: float) -> bool:
    """
    AI decide: arm trailing sekarang atau tahan.
    Return True = arm, False = tahan.
    Default True jika AI tidak tersedia / error.
    Fetch HTF 3D dan 1W untuk konteks tambahan (tidak time-critical).
    """
    entry  = d.get('entry_price', 0)
    atrp   = d.get('atr_pct', 0)
    profit_now  = (price/entry - 1)*100 if entry > 0 else 0
    profit_peak = (peak/entry - 1)*100  if entry > 0 else 0
    htf_str     = fetch_htf_context_for_ai(symbol)
    ind4h_str   = get_full_4h_indicator_context(symbol)
    htf_section  = f"\nKonteks HTF:\n{htf_str}\n" if htf_str else ""
    ind4h_section = f"\n{ind4h_str}\n" if ind4h_str else ""
    prompt = (
        f"Kamu adalah AI analyst trading crypto. Berikan analisis singkat.\n\n"
        f"Deal: {to_display_pair(symbol)} | Strategi: {strategy}\n"
        f"Entry: {_fmt_price(entry)} | Peak: {_fmt_price(peak)} | Harga skrg: {_fmt_price(price)}\n"
        f"Profit dari entry: {profit_now:+.2f}% | Profit peak: {profit_peak:+.2f}%\n"
        f"ATR%: {atrp:.2f}% | Arm threshold: {get_arm_pct(atrp):.1f}%\n"
        f"Hold candle ke-{d.get('hold_candle_count', '?')}\n"
        f"{ind4h_section}"
        f"{htf_section}\n"
        f"Trailing threshold tercapai.\n\n"
        f"Format jawaban (ikuti persis):\n"
        f"ARM atau HOLD\n"
        f"Momentum: kondisi momentum saat ini\n"
        f"Warning: risiko yang perlu diwaspadai (atau 'Tidak ada')\n\n"
        f"Baris pertama HARUS hanya kata ARM atau HOLD."
    )
    result = _ai_call(prompt)
    if not result:
        return True
    lines = result.strip().split("\n")
    first_line = lines[0].strip().upper()
    decision = "ARM" in first_line
    reasoning = "\n".join(lines[1:]).strip() if len(lines) > 1 else ""
    log(f"[AI] ARMED decision {symbol}: {first_line} → {'ARM' if decision else 'TAHAN'}")
    if not decision:
        send_telegram(
            f"🤖 AI Decision | {to_display_pair(symbol)}\n"
            f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
            f"{ai_provider_note()}\n"
            f"Strategi : {strategy}\n"
            f"Event    : ARMED trailing\n"
            f"Profit pk: {profit_peak:+.2f}%\n"
            f"Keputusan: ⏸ TAHAN\n"
            f"{reasoning}",
            parse_mode=None
        )
    else:
        if reasoning:
            send_telegram(
                f"🤖 AI Analysis | {to_display_pair(symbol)}\n"
                f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
                f"{ai_provider_note()}\n"
                f"Keputusan: ✅ ARM\n"
                f"{reasoning}",
                parse_mode=None
            )
    return decision


def ai_decision_add_fund(symbol: str, strategy: str, d: dict, add_usd: float, current_atr: float) -> bool:
    """
    AI decide: kirim add-fund sekarang atau tunda. Return True = kirim, False = tunda
    (dicoba lagi setelah cooldown 15 menit, lihat titik panggil di T2). Default True
    (fail-open) jika AI tidak tersedia / error -- sama spt semua ai_decision_* lain.

    Fase 2 TrenKonfirmasi-4h (03/09/2026, permintaan Mas Budi) -- HANYA dipakai strategi
    ini. 6 strategi lain TETAP add-fund otomatis murni (skor-based, tanpa AI-gate di
    titik ini), tidak diubah.
    """
    entry = d.get('entry_price', 0)
    price = get_price_now(symbol)
    profit_now = (price/entry - 1)*100 if entry > 0 and price > 0 else 0
    ind4h_str = get_full_4h_indicator_context(symbol)
    ind4h_section = f"\n{ind4h_str}\n" if ind4h_str else ""
    prompt = (
        f"Kamu adalah AI analyst trading crypto. Berikan analisis singkat.\n\n"
        f"Deal: {to_display_pair(symbol)} | Strategi: {strategy}\n"
        f"Entry: {_fmt_price(entry)} | Harga skrg: {_fmt_price(price)}\n"
        f"Profit dari entry: {profit_now:+.2f}%\n"
        f"ATR% skrg: {current_atr:.2f}%\n"
        f"Rencana tambah modal (add-fund): ${add_usd:.0f}\n"
        f"{ind4h_section}\n"
        f"Bot akan menambah modal ke posisi ini sekarang (bukan reaksi rugi -- ini murni\n"
        f"penambahan modal berbasis skor sinyal awal). Apakah kondisi pasar saat ini masih\n"
        f"mendukung penambahan modal, atau sebaiknya ditunda dulu?\n\n"
        f"Format jawaban (ikuti persis):\n"
        f"ADD atau TUNDA\n"
        f"Alasan: alasan singkat\n\n"
        f"Baris pertama HARUS hanya kata ADD atau TUNDA."
    )
    result = _ai_call(prompt)
    if not result:
        return True
    lines = result.strip().split("\n")
    first_line = lines[0].strip().upper()
    decision = "ADD" in first_line
    reasoning = "\n".join(lines[1:]).strip() if len(lines) > 1 else ""
    log(f"[AI] ADD-FUND decision {symbol}: {first_line} -> {'ADD' if decision else 'TUNDA'}")
    if not decision:
        send_telegram(
            f"🤖 AI Decision | {to_display_pair(symbol)}\n"
            f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
            f"{ai_provider_note()}\n"
            f"Strategi : {strategy}\n"
            f"Event    : ADD FUND\n"
            f"Profit   : {profit_now:+.2f}%\n"
            f"Keputusan: ⏸ TUNDA (dicoba lagi ~15 menit)\n"
            f"{reasoning}",
            parse_mode=None
        )
    return decision


def ai_decision_near_timeout(symbol: str, strategy: str, d: dict, price: float, peak: float,
                              hold_now: int, max_candle: int) -> bool:
    """
    AI decide saat deal mendekati timeout (sisa ≤1 candle).
    Return True = CLOSE sekarang, False = EXTEND (biarkan timeout alami).
    Default False jika AI tidak tersedia / error.
    Menggunakan indikator live 4h + HTF untuk keputusan lebih kaya.
    Fee round trip 0.3% sudah diperhitungkan dalam profit.
    """
    FEE_ROUND_TRIP = 0.3  # 0.15% buy + 0.15% sell (konservatif)
    entry  = d.get('entry_price', 0)
    atrp   = d.get('atr_pct', 0)
    armed  = d.get('trailing_armed', False)
    profit_gross    = (price/entry - 1)*100 if entry > 0 else 0
    profit_after_fee = profit_gross - FEE_ROUND_TRIP
    profit_peak     = (peak/entry - 1)*100  if entry > 0 else 0
    sisa            = max_candle - hold_now

    # Fetch live indicators 4h
    ind_live_str = ""
    candle_str   = ""
    try:
        import pandas_ta as _pta
        df4 = get_ohlcv_4h(symbol, limit=60)
        if df4 is not None and len(df4) >= 20:
            df4 = compute_indicators_4h(df4)
            r4  = df4.iloc[-1]
            r4p = df4.iloc[-2]
            rsi_live  = float(r4.get('rsi', float('nan'))) if 'rsi' in r4.index and not pd.isna(r4.get('rsi')) else None
            sk_live   = float(r4.get('stoch_k', float('nan'))) if 'stoch_k' in r4.index and not pd.isna(r4.get('stoch_k')) else None
            sd_live   = float(r4.get('stoch_d', float('nan'))) if 'stoch_d' in r4.index and not pd.isna(r4.get('stoch_d')) else None
            macd_h    = float(r4.get('macd_hist', float('nan'))) if 'macd_hist' in r4.index and not pd.isna(r4.get('macd_hist')) else None
            vol_ratio = float(r4.get('vol')/r4.get('vol_ma')) if 'vol' in r4.index and 'vol_ma' in r4.index and r4.get('vol_ma',0)>0 else None
            st_dir    = int(r4.get('st_dir', 0)) if 'st_dir' in r4.index and not pd.isna(r4.get('st_dir')) else None
            bb_pct    = float(r4.get('bb_pct', float('nan'))) if 'bb_pct' in r4.index and not pd.isna(r4.get('bb_pct')) else None
            wr_live   = float(r4.get('williams_r', float('nan'))) if 'williams_r' in r4.index and not pd.isna(r4.get('williams_r')) else None
            cci_live  = float(r4.get('cci', float('nan'))) if 'cci' in r4.index and not pd.isna(r4.get('cci')) else None
            obv_now   = float(r4.get('obv', float('nan'))) if 'obv' in r4.index and not pd.isna(r4.get('obv')) else None
            obv_prev  = float(r4p.get('obv', float('nan'))) if 'obv' in r4p.index and not pd.isna(r4p.get('obv')) else None

            ind_parts = []
            if rsi_live  is not None: ind_parts.append(f"RSI={rsi_live:.1f}")
            if sk_live   is not None: ind_parts.append(f"Stoch%K={sk_live:.1f}")
            if sd_live   is not None: ind_parts.append(f"Stoch%D={sd_live:.1f}")
            if macd_h    is not None: ind_parts.append(f"MACD hist={macd_h:+.6f}")
            if vol_ratio is not None: ind_parts.append(f"Vol={vol_ratio:.2f}xMA")
            if st_dir    is not None: ind_parts.append(f"ST={'Up' if st_dir==1 else 'Down'}trend")
            if bb_pct    is not None: ind_parts.append(f"BB%b={bb_pct:.2f}")
            if wr_live   is not None: ind_parts.append(f"Williams%R={wr_live:.1f}")
            if cci_live  is not None: ind_parts.append(f"CCI={cci_live:.1f}")
            if obv_now is not None and obv_prev is not None:
                obv_dir = "naik" if obv_now > obv_prev else "turun"
                ind_parts.append(f"OBV={obv_dir}")
            if ind_parts: ind_live_str = "Indikator 4h live: " + " | ".join(ind_parts)

            # Candlestick pattern detection
            c_now  = float(r4['close']); o_now = float(r4['open'])
            h_now  = float(r4['high']);  l_now = float(r4['low'])
            c_prev = float(r4p['close']); o_prev = float(r4p['open'])
            body   = abs(c_now - o_now)
            full_range = h_now - l_now if h_now > l_now else 1e-10
            body_ratio = body / full_range
            patterns = []
            if body_ratio < 0.15:
                patterns.append("Doji (indecision)")
            if c_now > o_now and c_now > c_prev:
                patterns.append("Candle hijau (bullish)")
            elif c_now < o_now and c_now < c_prev:
                patterns.append("Candle merah (bearish)")
            upper_wick = h_now - max(c_now, o_now)
            lower_wick = min(c_now, o_now) - l_now
            if upper_wick > body * 2:
                patterns.append("Upper shadow panjang (penolakan harga tinggi)")
            if lower_wick > body * 2:
                patterns.append("Lower shadow panjang (support kuat)")
            if c_now > o_prev and o_now < c_prev and c_now > c_prev:
                patterns.append("Bullish engulfing")
            if c_now < o_prev and o_now > c_prev and c_now < c_prev:
                patterns.append("Bearish engulfing")
            if patterns:
                candle_str = "Pattern candle: " + ", ".join(patterns)
    except Exception:
        pass

    # HTF context
    htf_str     = fetch_htf_context_for_ai(symbol)
    htf_section = f"\nKonteks HTF:\n{htf_str}" if htf_str else ""

    prompt = (
        f"Kamu adalah AI analyst trading crypto. Berikan analisis singkat.\n\n"
        f"Deal: {to_display_pair(symbol)} | Strategi: {strategy}\n"
        f"Entry: {_fmt_price(entry)} | Harga skrg: {_fmt_price(price)} | Peak: {_fmt_price(peak)}\n"
        f"Profit gross: {profit_gross:+.2f}% | Setelah fee (0.3%): {profit_after_fee:+.2f}% | Peak profit: {profit_peak:+.2f}%\n"
        f"ATR%: {atrp:.2f}% | Armed: {armed}\n"
        f"Hold: candle ke-{hold_now} dari maks {max_candle} (sisa {sisa} candle)\n"
        f"{ind_live_str}\n"
        f"{candle_str}\n"
        f"{htf_section}\n\n"
        f"Deal hampir timeout.\n\n"
        f"Format jawaban (ikuti persis):\n"
        f"CLOSE atau EXTEND\n"
        f"Alasan: penjelasan singkat keputusan berdasarkan semua indikator\n\n"
        f"Baris pertama HARUS hanya kata CLOSE atau EXTEND."
    )
    result = _ai_call(prompt)
    if not result:
        return False
    lines = result.strip().split("\n")
    first_line = lines[0].strip().upper()
    decision = "CLOSE" in first_line
    reasoning = "\n".join(lines[1:]).strip() if len(lines) > 1 else ""
    log(f"[AI] NEAR-TIMEOUT decision {symbol}: {first_line} → {'CLOSE' if decision else 'EXTEND'}")
    send_telegram(
        f"🤖 AI Decision | {to_display_pair(symbol)}\n"
        f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
        f"{ai_provider_note()}\n"
        f"Strategi : {strategy}\n"
        f"Event    : NEAR TIMEOUT (candle {hold_now}/{max_candle})\n"
        f"Profit   : {profit_after_fee:+.2f}% (setelah fee)\n"
        f"Keputusan: {'⚡ CLOSE' if decision else '▶️ EXTEND'}\n"
        f"{reasoning}",
        parse_mode=None
    )
    return decision


def ai_decision_close(symbol: str, strategy: str, d: dict, price: float, peak: float, reason: str) -> bool:
    """
    AI decide: close deal sekarang atau hold (override stop).
    Return True = close, False = hold.
    Default True jika AI tidak tersedia / error (fail-open: default close).
    """
    entry  = d.get('entry_price', 0)
    atrp   = d.get('atr_pct', 0)
    armed  = d.get('trailing_armed', False)
    profit_now  = (price/entry - 1)*100 if entry > 0 else 0
    profit_peak = (peak/entry - 1)*100  if entry > 0 else 0
    tdist  = trailing_dist_progressive(atrp, profit_peak)
    # Konteks indikator (termasuk RSI) -- sebelumnya fungsi ini kosong sama sekali dari
    # indikator, cuma harga/ATR%/reason (01/09/2026, permintaan Budi setelah kasus TLM/BICO).
    ind4h_str = get_full_4h_indicator_context(symbol)
    ind4h_section = f"\n{ind4h_str}\n" if ind4h_str else ""
    prompt = (
        f"Kamu adalah AI assistant untuk trading bot crypto.\n\n"
        f"Deal: {to_display_pair(symbol)} | Strategi: {strategy}\n"
        f"Entry: {_fmt_price(entry)} | Peak: {_fmt_price(peak)} | Harga skrg: {_fmt_price(price)}\n"
        f"Profit dari entry: {profit_now:+.2f}% | Profit dari peak: {(price/peak-1)*100:+.2f}%\n"
        f"ATR%: {atrp:.2f}% | Trail dist: {tdist:.2f}% | Armed: {armed}\n"
        f"{ind4h_section}"
        f"Alasan close: {reason}\n\n"
        f"Apakah CLOSE sekarang atau HOLD (tahan, skip close ini)?\n"
        f"Jawab hanya: CLOSE atau HOLD"
    )
    result = _ai_call(prompt)
    if not result:
        return True  # fail-open: default close
    decision = "CLOSE" in result
    log(f"[AI] CLOSE decision {symbol}: {result} → {'CLOSE' if decision else 'HOLD'}")
    if not decision:
        send_telegram(
            f"🤖 AI Decision | {to_display_pair(symbol)}\n"
            f"{now_wib().strftime('%d/%m/%Y %H:%M')} WIB\n"
            f"{ai_provider_note()}\n"
            f"Strategi : {strategy}\n"
            f"Event    : CLOSE ({reason[:40]})\n"
            f"Profit   : {profit_now:+.2f}%\n"
            f"Keputusan: ⏸ HOLD — close dibatalkan AI",
            parse_mode=None
        )
    return decision
if __name__ == '__main__':
    log("="*55)
    log("  BINANCE SCREENER -> 3COMMAS + TELEGRAM")
    log("  BUILD: 20260821-F (+ pindah SC JS ke dash.js area (hapus Jinja raw block) + fix base_usd → target_usd semua lokasi)")
    log("  STRATEGI: MOMENTUM BREAKOUT brkX2 (12h)")
    log("="*55)
    if USE_BINANCE_DIRECT:
        try:
            _usdt_free, _usdt_locked = get_usdt_balance()
            log(f"  Saldo USDT Spot wallet : free=${_usdt_free:.2f} locked=${_usdt_locked:.2f}")
        except Exception as _e:
            log(f"WARN gagal cek saldo USDT saat startup: {_e}")
    try:
        if os.path.exists(TRADES_CSV):
            with trades_csv_lock:
                with open(TRADES_CSV, 'r', newline='', encoding='utf-8') as _f:
                    _all_closed = [r for r in csv.DictReader(_f) if r.get('status') == 'CLOSED']
            _akum_rows = [r for r in _all_closed if r.get('strategy') in ('akum_entry_a', 'akum_entry_b')]
            _brk_rows  = [r for r in _all_closed if (r.get('strategy') or 'brkX2') == 'brkX2']
            log(f"  Histori CLOSED Akumulasi-4h: {len(_akum_rows)} deal")
            for _r in _akum_rows:
                log(f"    - {_r.get('symbol','?')} ({_r.get('strategy','?')}) "
                    f"profit={_r.get('profit_pct','?')}% closed={_r.get('close_time_wib','?')}")
            log(f"  Histori CLOSED brkX2-12h: {len(_brk_rows)} deal (termasuk sebelum phase offset={FWDTEST_BRKX2_PHASE_OFFSET})")
            for _r in _brk_rows:
                log(f"    - {_r.get('symbol','?')} profit={_r.get('profit_pct','?')}% closed={_r.get('close_time_wib','?')}")
    except Exception as _e:
        log(f"WARN gagal baca histori Akumulasi-4h/brkX2 saat startup: {_e}")
    log(f"  Timeframe        : {TIMEFRAME}")
    log(f"  Entry syarat     : ST-up, >EMA20, 3bar-bullish, vol>={VOLUME_MULT}xMA, RSI<{RSI_MAX}" + (f", Stoch<{STOCH_MAX}" if STOCH_MAX is not None else "") + (f", ATR<{ATR_MAX_PCT}%" if ATR_MAX_PCT is not None else "") + f", HTF3D>{HTF_VOL_MULT}xMA")
    log(f"  Exit             : trailing adaptif (arm +{TRAIL_ARM_PCT}%), batas {MAX_HOLD_DAYS} candle 12h (2.5 hari)")
    log(f"  Trailing FAKTOR  : {TRAILING_FAKTOR*100:.0f}% (jarak trailing = tabel ATR% x {TRAILING_FAKTOR})")
    log(f"  Base order       : ${BASE_ORDER_VOLUME} | Max deal total: {total_max_deals_all_strategies()}")
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
    log(f"  Progressive trail: {'ON' if PROG_TRAIL_ENABLED else 'OFF'} (thr={PROG_TRAIL_THRESHOLD}% stp={PROG_TRAIL_STEP}% red={PROG_TRAIL_REDUCE}% min={PROG_TRAIL_MIN}%)")
    log(f"  Cooldown internal: {COOLDOWN_SECONDS}s ({COOLDOWN_SECONDS/3600:.0f}j, brkX2) -- cegah kirim sinyal yg pasti ditolak 3Commas (deal hantu)")
    log(f"  Add fund auto    : {'ON' if ADD_FUND_AUTO else 'OFF (manual)'}")
    log(f"  Filter BTC L1&L2 : {'ON' if BTC_FILTER_ENABLED else 'OFF'}")
    log(f"  Filter HTF 3D    : {'ON' if HTF_FILTER_ENABLED else 'OFF'}"
        + (f" (price>EMA{HTF_EMA_SLOW} AND MACD>0 di {HTF_TIMEFRAME})" if HTF_FILTER_ENABLED else ""))
    log(f"  Min vol 24h      : ${MIN_VOLUME_USD:,}")
    log("  " + "-"*51)
    log(f"  STRATEGI #4 CrossEMA-4h: {'ON' if STRAT_CROSSEMA_ENABLED else 'OFF'} | TF 4h")
    if STRAT_CROSSEMA_ENABLED:
        log(f"  Entry: ST=-1 + close<EMA20 + vol>={STRAT_CROSSEMA_VOLUME_MULT}xMA + HTF 12h (lalu price cross EMA20 intrabar)")
        log(f"  Window: {int(STRAT_CROSSEMA_ENTRY_MIN*100*240/100)}-{int(STRAT_CROSSEMA_ENTRY_MAX*100*240/100)} menit ({STRAT_CROSSEMA_ENTRY_MIN*100:.0f}%-{STRAT_CROSSEMA_ENTRY_MAX*100:.0f}% elapsed), scan tiap {STRAT_CROSSEMA_SCAN_INTERVAL//60}m")
        log(f"  Slot: {STRAT_CROSSEMA_MAX_DEALS} | Target forward-test: {STRAT_CROSSEMA_FWDTEST} deal | Perf filter: OFF")
        log(f"  HTF 12h vol: >{STRAT_CROSSEMA_HTF_VOL_MULT}xMA | Vol candle: >={STRAT_CROSSEMA_VOLUME_MULT}xMA")
    if STRAT_AKUM_ENABLED:
        log("  " + "-"*51)
        log(f"  STRATEGI #5 Akumulasi-4h: ON | TF {AKUM_TIMEFRAME}")
        log(f"  PRIMARY : Range≤{int(AKUM_RANGE_PCT*100)}% | EMAGap≤{int(AKUM_EMA_GAP_PCT*100)}% | OBV↑ | ATR↓≥{int(AKUM_ATR_DROP_PCT*100)}%")
        log(f"  SECONDARY: Vol G>R | RSI 30-56 | MACD flat | Body ratio<{AKUM_BODY_RATIO_MAX}")
        log(f"  Scan tiap {AKUM_SCAN_INTERVAL//60} menit | Jendela {AKUM_SIDEWAYS_CANDLES} candle 4h | Tampil maks {AKUM_MAX_RESULTS} pair")
        log(f"  Timeout: {AKUM_ENTRY_TIMEOUT} candle 4h ({AKUM_ENTRY_TIMEOUT//6} hari) | TP: swing_high_{AKUM_TP_SWING_LOOKBACK}c / RSI>{AKUM_TP_RSI_OB} / Stoch>{AKUM_TP_STOCH_OB}")
    if REVERSAL_ENABLED:
        log("  " + "-"*51)
        log(f"  STRATEGI 2 REVERSAL: ON | TF {REVERSAL_TIMEFRAME}")
        log(f"  Setup: minimal 2/3 candle merah+turun>={REVERSAL_DROP_MIN_PCT:.0f}%, doji(<{int(REVERSAL_DOJI_MAX*100)}% body), 1 HA bull, cross-up EMA20")
        log(f"  Exit : trailing adaptif (sama brkX2) | add fund: {'ON' if REVERSAL_ADD_FUND else 'OFF'}")
        log(f"  Hold : maks {REVERSAL_MAX_HOLD_CANDLES} candle 8h")
        log(f"  Min vol reversal : ${REVERSAL_MIN_VOL_USD:,} (lebih luas dari brkX2 ${MIN_VOLUME_USD:,})")
    if STRAT4H_ENABLED:
        log("  " + "-"*51)
        log(f"  STRATEGI 3 brkX2-4h: ON | TF {STRAT4H_TIMEFRAME}")
        log(f"  Entry: ST+1 + MACD>0 + ATR>={STRAT4H_ATR_MIN_PCT}% + Vol>={STRAT4H_VOLUME_MULT}xMA + RSI {STRAT4H_RSI_MIN}-{STRAT4H_RSI_MAX} + Stoch<{STRAT4H_STOCH_MAX} + HTF {STRAT4H_HTF_TF} 3x candle bullish + cross EMA20 0-0.75%")
        log(f"  Intrabar: menit ke 5-60 (25% elapsed), scan tiap {STRAT4H_SCAN_INTERVAL}s")
        log(f"  Slot: {STRAT4H_MAX_DEALS} | Target forward-test: {STRAT4H_FWDTEST_TARGET} deal")
        log(f"  Bot : #{COMMAS_BOT_ID_4H}")
        log("  " + "-"*51)
        log("  STRATEGI #7 Hunting-4h: ON | TF 4h")
        log(f"  Entry: EMA20<EMA50 gap<=1.5% | price>EMA20 0-0.75% | chg 0-2.0% | Hammer OR StrongBull (semua opsional kecuali price>EMA20)")
        log(f"  Slot: {HUNTING_MAX_DEALS} | Base order: ${HUNTING_ORDER_VOLUME} | Target forward-test: {HUNTING_FWDTEST_TARGET} deal")
        log(f"  Bot : #{COMMAS_BOT_ID_HUNTING}")
    log("="*55)
    # Flush banner sebelum thread-thread mulai print
    time.sleep(2.0)

    load_active_deals()
    load_last_closed()
    load_hold_no_sell_closed()
    load_hold_no_sell_price()
    load_blocked_pairs()
    sync_max_deals_globals()
    load_scan_blockers()
    load_daily_loss_limit()
    load_trail_reentry()
    load_quick_reentry_trades()
    sync_base_usd_from_binance()  # auto-fix base_usd dari Binance API saat startup
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
    if TRENDCONFIRM_ENABLED:
        t_tc = threading.Thread(target=run_thread_trendconfirm, daemon=True, name="T-TrendConfirm")
        threads.append(t_tc)
        n_threads += 1
    if STRAT_AKUM_ENABLED:
        t_akum = threading.Thread(target=run_thread_akum, daemon=True, name="T-Akum")
        threads.append(t_akum)
        n_threads += 1
        t_akum_entry = threading.Thread(target=run_thread_akum_entry, daemon=True, name="T-AkumEntry")
        threads.append(t_akum_entry)
        n_threads += 1
    t_s6 = threading.Thread(target=run_thread_strat6, daemon=True, name="T-Strat6")
    threads.append(t_s6)
    n_threads += 1
    
    for t in threads: t.start()
    # Delay kecil agar banner startup selesai sebelum thread mulai print
    time.sleep(0.5)
    t_web = threading.Thread(target=run_web_dashboard, daemon=True, name="T-Web")
    t_web.start()

    # ── Google Drive: init + diagnostik saat startup ──────────────────────────
    if not _GDRIVE_AVAILABLE:
        log("[DRIVE] Library google-auth/google-api-python-client TIDAK terinstall — Drive sync OFF")
    else:
        sa_json = os.environ.get("GOOGLE_SA_JSON") or os.environ.get("GDRIVE_SERVICE_ACCOUNT", "")
        if not sa_json:
            log("[DRIVE] Env var GOOGLE_SA_JSON / GDRIVE_SERVICE_ACCOUNT tidak ditemukan — Drive sync OFF")
        else:
            log(f"[DRIVE] Env var ditemukan ({len(sa_json)} chars), mencoba koneksi...")
            svc = _get_drive_service()
            if svc:
                log(f"[DRIVE] OK — folder target: {_DRIVE_FOLDER_ID}")
            else:
                log("[DRIVE] GAGAL init — cek isi JSON / permission service account")
    log(f"{n_threads} thread aktif (T1=screener, T2=monitor, T3=intrabar 12h, T3-REV=reversal intrabar"
        + (", T1d=intrabar 4h" if STRAT4H_ENABLED else "")
        + (", T-CrossEMA=strategi#4" if STRAT_CROSSEMA_ENABLED else "")
        + (", T-Akum=strategi#5 Akumulasi" if STRAT_AKUM_ENABLED else "")
        + (", T-AkumEntry=strategi#5 Entry A/B" if STRAT_AKUM_ENABLED else "")
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

        

