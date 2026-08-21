# CONTEXT TRADING BOT — CHAT SUMMARY
*Update terakhir: 17 Agustus 2026 — untuk dilanjutkan di chat baru*

---

## INFRASTRUKTUR

- **Script live**: `binance_screener.py` di repo GitHub `widodobudi/binance-bot`
- **Railway**: project `pacific-grace`, service `worker`, URL `worker-production-e111.up.railway.app`
  - Project ID: `338d54f0-6ad1-4413-a8ec-fe930256cfc7`
  - Service ID: `a7957b0c-807d-4efa-968e-a4fe6d7fd762`
  - Env ID: `f0df9587-224f-4c0f-87c7-009afb65c3e6`
  - Plan: **Hobby** (tidak ada static IP)
- **Volume Railway**: `/data/` — active_deals.json, trades_forwardtest.csv, near_miss_log.txt, open-arm-close.txt, deal_base_usd.json, last_closed.json
- **Google Drive folder tradingview**: ID `1DwtfVtDc1DhoW80AgNUmUO6zYqFi-ZBC`
- **binance_screener.py di Drive**: ID `1mvW80uHP_e9Czl-3FNKLWcIJgFku3ygH`
- **near_miss_log.txt di Drive**: ID `1gQ3BEfidmrWoRN-ehIEnSr9Lhx_yW3H3`
- **Python**: 3.12, pandas_ta (fork MerlinR), Flask port 8080
- **Deploy**: upload ke GitHub → Railway auto-deploy
- **BUILD terkini**: `20260817-G`

---

## RAILWAY VARIABLES

```
HUNTING_CAPITAL_USD = 86
ANTHROPIC_API_KEY   = sk-ant-... (sudah di-set)
BINANCE_API_KEY     = Hx1G8bET... (direct_from_Railway, read-only)
BINANCE_API_SECRET  = 17fKcwd1... (read-only)
```

**Catatan**: `BINANCE_API_KEY` / `SECRET` hanya read-only — tidak bisa trading. Untuk trading langsung Binance butuh API key baru dengan Spot Trading permission + static IP (Railway Pro atau QuotaGuard).

---

## BINANCE API KEYS

| Label | Key (prefix) | Permission | IP |
|---|---|---|---|
| `ai_decision_anthropic` | `8U7CQUVj...` | Read only | Unrestricted |
| `3Commas_v2` | `p80CSTH...` | Read + Spot Trading | IP 3Commas v2 |
| `direct_from_Railway` | `Hx1G8bET...` | Read only | Unrestricted |

---

## 3COMMAS MIGRATION STATUS

- **3Commas v1 shutdown**: 11 September 2026
- **v2 akun**: sudah dibuat di `trade.3commas.io`, plan **Pro** (free 7 hari s/d 24 Agustus 2026)
- **v2 limit**: maksimal **10 pairs per bot** — tidak ada unlimited pairs seperti v1
- **Bot v1 → v2**: TIDAK migrate otomatis, harus dibuat ulang manual
- **Deal aktif**: TIDAK terbawa, harus close manual di v1 sebelum switch
- **Bot IDs v1** (masih dipakai bot Python saat ini):
  - `#16380123` — brkX2-12h
  - `#16921019` — Reversal-8h
  - `#16935970` — brkX2-4h + CrossEMA-4h
  - `#16945621` — Akumulasi Entry A/B
  - ~~`#16951566`~~ — Hunting-4h (tidak dipakai lagi, hunting pakai inject langsung)
- **Alternatif jangka panjang**: trading langsung ke Binance API tanpa 3Commas (butuh static IP)

---

## 7 STRATEGI AKTIF

### Strategi #1 — brkX2-12h (bot #16380123)
- Entry T1: ST+1, close>EMA20, close>EMA50, breakout HH3, vol 0.6x–5.0xMA, RSI<60, Stoch<70, ATR<9%, HTF3D vol>0.7xMA
- Timeout: 5 candle 12h (2.5 hari) | Max slot: 2 | Cooldown: 8 jam
- **Forward-test**: #7/15 (6W/1L, total +17.2%)

### Strategi #2 — Reversal-8h (bot #16921019)
- Entry: 3 merah+turun>=5%, doji<20% body, HA bull, cross-up EMA20
- Max slot: 2 | Hold: maks 30 candle 8h
- **Forward-test**: #7/8 (7W/0L, total +32.6%)

### Strategi #3 — brkX2-4h (bot #16935970)
- Entry: ST+1, MACD>0, ATR>=2% dan <7%, vol>=0.25xMA, RSI 40-60, Stoch<80, HTF 12h 3 candle bullish berturutan, chg_from_open<=3%
- Max slot: 5 | Window intrabar: menit 5-60 | Cooldown: 8 jam
- **Forward-test**: #15/30 (14W/1L, total +13.7%)

### Strategi #4 — CrossEMA-4h (bot #16935970)
- Entry: ST=-1, cross-up EMA20 intrabar menit 5-60
- Max slot: 2 | **Forward-test**: #0/7

### Strategi #5 — Akumulasi-4h (bot #16945621)
- Detektor: scan tiap 30 menit, 4 primary (Range≤18%, EMAGap≤6%, OBV↑, ATR↓≥25%)
- **Entry A (Spring)**: low < support*(1+0.4% buffer) dalam 15 candle terakhir (~60 jam), vol spike>2.5xMA, RSI sempat <20 lalu naik <50, OBV slope positif — **(BARU: buffer 0.4% + window 15 candle)**
- **Entry B (Breakout+Retest)**: breakout resistance + retest
- Max slot: 2 | **Forward-test**: #0/7

### Strategi #6 — Hunting-4h (hunting bot)
- **Entry**: price>EMA20 jarak **0–0.75%**, EMA20<EMA50 gap **0–0.75%**, price_change **0–1.5%**, uptrend (close>close[-5]) — **(BARU: parameter diperketat dari backtest_hunting_sweep 288 kombinasi)**
- Max slot: 3 | Timeout: 15 candle 4h (~60 jam)
- **Trailing factor variatif** (BARU):
  - NORMAL: 0.8 (kondisi biasa)
  - FOMO: 1.3 (Uptrend + Stoch%K > 80)
  - TIGHTENED: 0.5 (Stoch%K baru turun dari >80)
- **Forward-test**: #1/7 (1W/0L, total +0.1%) — direset 16/08 setelah ganti parameter
- **Balance check**: `HUNTING_CAPITAL_USD=86`
- **Active deals saat ini (17/08)**: MANTA (-1.37%), KAVA (-0.52%), HOT (+0.30%)

---

## AI DECISION ENGINE (BARU — 15-17 Agustus 2026)

Bot menggunakan **Anthropic Claude Haiku API** untuk 4 titik keputusan:

| Titik | Trigger | Default kalau API gagal |
|---|---|---|
| OPEN | Setelah semua filter lolos, sebelum kirim webhook | fail-open (buka deal) |
| ARMED | Saat profit >= arm threshold | fail-open (arm) |
| NEAR TIMEOUT | Saat sisa ≤2 candle menuju timeout | fail-safe (EXTEND) |
| CLOSE | Saat trailing stop terpicu | fail-open (close) |

**Fitur AI Decision:**
- Format jawaban kaya: keputusan + Target + Momentum + Warning (bukan 1 kata)
- HTF 3D + 1W context di OPEN dan ARMED (tidak di CLOSE karena time-critical)
- Indicators lengkap 4h: RSI, Stoch%K/D, MACD, BB%b, Williams%R, CCI, OBV, RVOL, EMA200, ADX
- Candlestick pattern: Doji, Engulfing, Upper/Lower wick
- Fee 0.3% dikurangi dari profit sebelum dikirim ke AI (near-timeout dan close)
- Guard double-decision: flag `nt_ai_called_{candle}` cegah 2 keputusan bersamaan
- Notif Telegram + Email (widodobudi@gmail.com) saat quota Anthropic habis dan kembali normal
- `max_tokens = 200`
- Toggle via checkbox **AI CALL** di dashboard per deal

**Env var**: `ANTHROPIC_API_KEY` sudah di-set di Railway

---

## ENDPOINTS FLASK (BARU)

```
POST /fix_deal_usd       form: symbol=XXXUSDT&base_usd=20   # update base_usd + persist ke deal_base_usd.json
POST /inject_deal        form: symbol=XXXUSDT&entry_price=0.00033&base_usd=20&strategy=hunting_4h
POST /admin/remove_deal  json: {"symbol": "XXXUSDT"}        # hapus dari active_deals + deal_base_usd.json
POST /inject_deal        # inject deal dari 3Commas yang tidak masuk active_deals (ghost deal)
POST /toggle             # toggle checkbox AI Call, Auto Close, Auto Add Fund
```

---

## DEAL BASE USD PERSIST (BARU)

- File `/data/deal_base_usd.json` menyimpan `base_usd` per symbol
- Saat startup: `sync_base_usd_from_binance()` restore dari file ini ke `active_deals`
- Saat `/fix_deal_usd` dipanggil: tulis ke file ini
- Saat `/admin/remove_deal`: hapus dari file ini
- **Tidak perlu PowerShell manual setelah redeploy** (selama `/fix_deal_usd` sudah pernah dipanggil)

---

## PROFIL PROFIT (BARU)

- Web dashboard dan notif CLOSE: **profit net -0.2% fee** (0.1% buy + 0.1% sell Binance)
- Header kolom web: "PROFIT / NET -0.2% FEE"
- Notif CLOSE: "Profit: X% (net -0.2% fee)"
- AI decision: menggunakan fee 0.3% (konservatif)

---

## PARAMETER TRAILING

| ATR% | Trailing dist |
|------|--------------|
| 0–4% | 0.4% |
| 4–7% | 1.5% |
| >=7% | 2.5% |

- **ARM_PCT_LOW**: 2.0% (ATR<7%)
- **ARM_PCT_HIGH**: 3.5% (ATR>=7%)
- **Progressive trailing**: ON
- **Hunting-4h**: trailing factor variatif (NORMAL=0.8, FOMO=1.3, TIGHTENED=0.5)

---

## KONSTANTA PENTING

```python
COMMAS_BOT_ID          = 16380123   # brkX2-12h
COMMAS_BOT_ID_REVERSAL = 16921019   # Reversal-8h
COMMAS_BOT_ID_4H       = 16935970   # brkX2-4h + CrossEMA
AKUM_ENTRY_BOT_ID      = 16945621   # Akumulasi Entry A/B

BASE_ORDER_VOLUME      = 50         # $50 semua strategi kecuali hunting
HUNTING_ORDER_VOLUME   = 20         # $20 khusus hunting
HUNTING_CAPITAL_USD    = 86         # estimasi total kapital bot (env var Railway)

# Hunting parameter (hasil backtest_hunting_sweep 288 kombinasi):
DIST_EMA20_MAX         = 0.75       # price > EMA20 jarak max 0.75%
EMA_GAP_MAX            = 0.75       # EMA20 < EMA50 gap max 0.75%
PRICE_CHANGE_MAX       = 1.5        # price change max 1.5%

# Hunting trailing factor:
TRAIL_FACTOR_NORMAL    = 0.8
TRAIL_FACTOR_FOMO      = 1.3        # Uptrend + Stoch%K > 80
TRAIL_FACTOR_TIGHTENED = 0.5        # Stoch baru turun dari >80

# Akumulasi Entry A (BARU):
AKUM_A_REENTRY_CANDLES    = 15      # window 15 candle (~60 jam)
AKUM_A_SUPPORT_TOUCH_BUFFER = 0.004 # toleransi 0.4% di atas support

COOLDOWN_SECONDS       = 28800      # 8 jam cooldown per simbol
FEE_ROUND_TRIP_PCT     = 0.2        # fee 0.1% buy + 0.1% sell

# Hunting forward-test offset (reset 16/08 setelah ganti parameter):
HUNTING_FWDTEST_PHASE_OFFSET = 6
```

---

## FORWARD-TEST STATUS (per 17 Agustus 2026)

| Strategi | Progress | W/L | Total% |
|---|---|---|---|
| brkX2-12h | #7/15 | 6W/1L | +17.2% |
| Reversal-8h | #7/8 | 7W/0L | +32.6% |
| brkX2-4h | #15/30 | 14W/1L | +13.7% |
| CrossEMA-4h | #0/7 | — | — |
| Akumulasi-4h | #0/7 | — | — |
| Hunting-4h | #1/7 | 1W/0L | +0.1% (reset 16/08) |

---

## BACKTEST YANG SUDAH DILAKUKAN

| File | Kesimpulan |
|---|---|
| backtest_hunting_sweep.py | 288 kombinasi → optimal: dist_ema20=0.75%, ema_gap=0.75%, chg=1.5% |
| backtest_trailing_factor_sweep.py | 36 kombinasi → optimal: fn=0.8, ff=1.3, ft=0.5 |
| backtest_hunting_filter_sweep.py | ST+1+RSI<60 terbaik |
| backtest_elapsed_sweep_brkx2_4h.py | chg_from_open≤3% terbaik |
| backtest_brkx2_4h_comprehensive_sweep.py | RSI min 40 sweet spot |

---

## POWERSHELL COMMANDS YANG SERING DIPAKAI

```powershell
# Fix base_usd (run setelah redeploy jika deal_base_usd.json belum ada):
Invoke-WebRequest -Uri "https://worker-production-e111.up.railway.app/fix_deal_usd" -Method POST -Body "symbol=XXXUSDT&base_usd=20" -UseBasicParsing

# Hapus deal ghost:
Invoke-WebRequest -Uri "https://worker-production-e111.up.railway.app/admin/remove_deal" -Method POST -Body '{"symbol":"XXXUSDT"}' -ContentType "application/json" -UseBasicParsing

# Inject deal yang ada di 3Commas tapi tidak ada di bot:
Invoke-WebRequest -Uri "https://worker-production-e111.up.railway.app/inject_deal" -Method POST -Body "symbol=XXXUSDT&entry_price=0.00033&base_usd=20&strategy=hunting_4h" -UseBasicParsing
```

---

## BUG YANG SUDAH DIPATCH (15-17 Agustus 2026)

1. **Ghost deal hunting** — `add_to_active_deals` tidak dipanggil karena stoch_d fetch error → dibungkus try/except terpisah
2. **Double AI decision near-timeout** — flag `nt_ai_called_{candle}` di active_deals
3. **`@app.route` hilang untuk `/admin/remove_deal`** — terputus saat patch inject_deal
4. **`locked=100`** — `get_estimated_locked_usd()` fallback ke $50 jika base_usd tidak ada → fixed dengan deal_base_usd.json
5. **Modal $50 di notif hunting** — `open_deal_with_sizing` tidak bedakan hunting → pakai `HUNTING_ORDER_VOLUME` flat
6. **HOT dikirim 6x** — 3Commas HTTP 200 meski cooldown (tidak eksekusi) → ghost deal warning ditambahkan
7. **BUILD string tidak diupdate** — menyebabkan dispute soal versi yang terdeploy → wajib update setiap patch

---

## HAL YANG PERLU DIPANTAU

- **3Commas v1 shutdown 11 September** — perlu keputusan: buat bot v2 (limit 10 pairs) atau switch ke Binance direct API
- **Reversal-8h #7/8** — hampir tercapai, deal ke-8 = evaluasi
- **Hunting-4h** parameter baru — pantau 7 deal ke depan apakah win rate membaik
- **Akumulasi Entry A** — parameter baru (buffer 0.4% + window 15 candle) — pantau apakah lebih banyak deal terpicu
- **AI Decision** — pantau kualitas reasoning dan keputusan dari Haiku, apakah akurat

---

## CATATAN TEKNIS

- `send_3commas()` return True saat HTTP 200, meski 3Commas tidak eksekusi (cooldown). Ini menyebabkan ghost deal.
- Railway Hobby tidak punya static outbound IP → tidak bisa whitelist Binance API untuk trading
- Railway Pro ($20/bulan) atau QuotaGuard ($19/bulan) solusi untuk static IP
- `deal_base_usd.json` survive redeploy karena di `/data/` (Railway volume)
- `active_deals.json` survive redeploy karena di `/data/`
- Build string format: `BUILD: YYYYMMDD-X` — wajib diupdate setiap patch
