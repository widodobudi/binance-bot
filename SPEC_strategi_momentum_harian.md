# SPESIFIKASI STRATEGI — Momentum Breakout Harian (brkX2)
**Untuk rombakan `binance_screener.py` — forward-test modal kecil**
Tanggal dokumen: 17 Juni 2026

---

## 0. Status & tujuan

- Ini **forward-test**, bukan deploy profit penuh. Modal kecil (base $6).
- Tujuan: mengukur apakah edge yang muncul di backtest juga muncul di eksekusi
  nyata, dan mengukur slippage sesungguhnya.
- Strategi v3 lama (mean-reversion / pool 7 syarat) **DIHENTIKAN**.

## 1. Asal-usul (kenapa strategi ini dipilih)

Setelah pengujian menyeluruh (~5.000+ entry, banyak konsep), hanya strategi ini
yang lolos seluruh uji kejujuran:
- Mean-reversion (beli yang turun), banyak varian → RUGI. Ditolak.
- Momentum 4H → edge tipis, mati oleh slippage.
- **Momentum HARIAN brkX2** → lolos semua:
  - IN-sample & OUT-sample KEDUANYA positif (+4.23% / +4.68%)
  - Walk-forward: 8/8 segmen kronologis positif, 10/11 kuartal positif
  - Edge merata (lolos uji outlier: tanpa 5% teratas tetap +2.66%)
  - Konsisten 4 tahun (2023, 2024, 2025, 2026 semua positif)
  - Bertahan setelah slippage 0.5% (+4.18%)
- Add fund diuji (Filosofi 1 & 2) → TIDAK menambah edge (trade tanpa-add +6.93%
  vs trade add-fund +6.19%). Add fund otomatis ditolak data.

## 2. KEPUTUSAN FINAL (dikonfirmasi Budi)

| Aspek | Keputusan |
|---|---|
| Timeframe | Harian (1D) |
| Konsep "pool" | DIHAPUS — tidak ada lagi pool/window/seleksi-AI antar-kandidat |
| Filter BTC Lapis 1 & 2 | DIMATIKAN (kode disimpan sebagai toggle, default OFF) |
| Maksimal deal aktif | 1 (sementara dana terbatas; bisa dinaikkan nanti) |
| Universe | Semua pair USDT, volume 24h ≥ $2.000.000 |
| 3Commas | Bot yang sama, base order $6 |
| Safety order otomatis (3Commas) | TIDAK ADA (add fund otomatis dimatikan) |
| Add fund | Tidak ada OTOMATIS. Budi boleh add fund MANUAL kapan saja. |
| Eksekusi close | Via 3Commas (panel close deal), konsisten dgn bot lama |
| Evaluasi entry | MODE (a) sekarang — lihat bagian 4 |

## 3. KONDISI-KONDISI STRATEGI

### 3a. OPEN LONG (entry, base $6)
Semua 6 syarat harus terpenuhi pada candle HARIAN:
1. Supertrend uptrend (dir = +1). Parameter: length=10, multiplier=3.0
2. close > EMA20
3. EMA20 > EMA50
4. Breakout: close > tertinggi 10 candle harian SEBELUMNYA
5. Volume candle ≥ 2× MA20 volume  ("brkX2" — kualitas breakout)
6. RSI(14) < 75 (hindari overbought ekstrem)
→ Jika semua terpenuhi: open long $6 via 3Commas. Tanpa pool, tanpa window.

### 3b. ADD FUND
- TIDAK ADA add fund otomatis (data: tidak menambah edge, hanya menaikkan risiko).
- Budi BOLEH add fund manual via 3Commas/Binance kapan saja jika lihat peluang.
- PENTING (keterbatasan disadari): bot TIDAK melacak harga rata-rata setelah add
  fund manual. Trailing arm (+2%), perhitungan profit, dan exit SEMUA dihitung dari
  entry_price AWAL, bukan harga rata-rata gabungan. Jadi jika add fund manual:
  - arming +2% bisa terlalu cepat/lambat relatif posisi gabungan (tergantung harga add);
  - angka profit di notif CLOSE TIDAK mencerminkan P&L riil posisi gabungan.
  KEPUTUSAN: bot tidak dibuat melacak average (teknis buntu: API 3Commas butuh Trusted
  IP yg bentrok IP dinamis Railway; baca Binance API rawan salah-hitung koin di luar bot).
  ATURAN: jika add fund manual, Budi PANTAU & CLOSE MANUAL sendiri posisi gabungan itu —
  jangan andalkan trailing otomatis (yg mengacu entry awal). Tgl keputusan: 18/06/2026.

### 3c. EXIT — TRAILING ADAPTIF OTOMATIS (jaring pengaman)
- Trailing aktif setelah profit dari harga entry ≥ +2% (arm).
- Jarak trailing ADAPTIF per ATR% (ATR14 / harga × 100):
  - ATR% < 1%   → trailing 0.5%
  - 1% – 2%     → 1.0%
  - 2% – 4%     → 1.5%
  - 4% – 7%     → 2.0%
  - > 7%        → 2.5%
- Close saat harga turun dari PUNCAK (sejak entry) sejauh jarak trailing.
- Batas waktu: jika dalam 5 hari (5 candle harian) trailing tidak ter-trigger,
  tutup di harga saat itu.
- Ini OTOMATIS sebagai jaring pengaman. Budi tetap bebas close MANUAL lebih awal
  kapan saja jika lihat sesuatu di chart.

## 4. EVALUASI ENTRY — MODE (a) SEKARANG, MUNGKIN (b) NANTI

**MODE (a) — DIPAKAI SEKARANG (default):**
- Bot mengevaluasi syarat entry pada candle harian yang sudah TUTUP.
- Candle 1D Binance tutup pada 00:00 UTC = 07:00 WIB.
- Bot cek sekali setelah candle harian tutup (~07:00 WIB); jika 6 syarat
  terpenuhi pada candle yang baru tutup → open long.
- ALASAN: backtest dihitung pakai harga CLOSE candle. Mode (a) menjaga
  forward-test SETIA pada backtest, sehingga perbandingan live-vs-backtest valid.

**MODE (b) — KEMUNGKINAN MASA DEPAN (belum dipakai):**
- Evaluasi candle harian BERJALAN (belum tutup), dicek tiap beberapa menit.
- Lebih responsif tapi: (1) sinyal bisa berubah sebelum candle tutup
  (breakout batal sore hari), (2) BERBEDA dari cara backtest menghitung —
  hasil live bisa menyimpang dari backtest bukan karena strategi, tapi metode.
- CATATAN: jika nanti pindah ke (b), perlu backtest ulang dengan metode intrabar
  agar perbandingan tetap adil. JANGAN pindah ke (b) tanpa validasi ulang.

## 5. PARAMETER TEKNIS (ringkas, untuk implementasi)

```
TIMEFRAME           = 1d
SUPERTREND          = length 10, multiplier 3.0
EMA cepat / lambat  = EMA20, EMA50
BREAKOUT_LOOKBACK   = 10 candle
VOLUME_MULT (brkX2) = 2.0 × MA20(volume)
RSI                 = 14, batas atas < 75
TRAIL_ARM           = +2%
TRAIL_DIST          = adaptif per ATR%(14): 0.5/1.0/1.5/2.0/2.5
MAX_HOLD            = 5 candle harian (5 hari)
BASE_ORDER          = $6
ADD_FUND_AUTO       = OFF
BTC_FILTER (L1&L2)  = OFF (toggle tersimpan)
MAX_ACTIVE_DEALS    = 3      (naik dari 1 per 20/06 — percepat kumpul data forward-test)
MIN_VOL_24H         = $1.000.000  (turun dari $2jt per 20/06 — perbanyak pair di universe)
FEE asumsi backtest = 0.2% / posisi
```

## 6. RIWAYAT KEPUTSAN YANG PERNAH DITANYAKAN (arsip)

- Exit pakai trailing adaptif otomatis (bukan TP tetap; TP tetap kalah di semua uji).
- Filter BTC dimatikan karena backtest menunjukkan ia MENURUNKAN hasil
  (bukan menaikkan) — hipotesis awal Budi tidak terbukti, tapi kode disimpan.
- Add fund otomatis dimatikan karena tidak menambah edge (diuji 4 variasi).
- Pool & window & seleksi-AI antar-kandidat dihapus total (artefak strategi lama).
- Max deal 1 sementara, bisa dinaikkan saat dana bertambah.
- Add fund manual oleh Budi tetap diizinkan kapan saja.
- Close via 3Commas (bukan API Binance langsung).
- Forward-test = mengukur realita & slippage, BUKAN cari profit besar.
  Pantau beberapa minggu, bandingkan live vs backtest, baru putuskan naikkan modal.

## 7. CATATAN RISIKO (harus disadari Budi)

- Rugi terdalam per trade di backtest: −35% (entry tunggal). Win rate ~80%,
  tapi 20% yang kalah bisa kalah dalam. Modal & mental harus siap.
- Backtest ≠ live. Slippage nyata (terutama saat breakout, harga bergerak cepat)
  belum tentu sama dengan asumsi. Itu justru yang diukur forward-test.
- Data uji 2023–2026 cenderung pasar naik. Bear market berkepanjangan yang belum
  terwakili bisa mengubah perilaku.
- Ini BUKAN jaminan profit. Ini kandidat terkuat dari pengujian, yang masih
  harus dibuktikan di realita dengan modal kecil dulu.

## 8. CHANGELOG KODE (binance_screener.py) — sesi 17-18/06/2026

Perubahan yang masuk ke LIVE (urut):
1. EXCLUDE EMAS/PERAK: ditambah ke EXCLUDED_BASE_ASSETS (PAXG, XAUT, dll).
   Stablecoin & fiat sudah ada sebelumnya. Universe live ~113 pair.
2. CSV LOG (baru): trades_forwardtest.csv di /data (volume persisten Railway).
   1 baris per trade; dicatat saat OPEN (signal_price, entry_price, slip_pct, atr_pct),
   dilengkapi saat CLOSE (exit_price, profit_pct, exit_reason). Bahan banding live-vs-backtest.
3. HEARTBEAT 6-JAM (baru): notif Telegram tiap 6 jam (4x/hari) = tanda bot hidup +
   status (tidak ada lolos / deal aktif). Plus notif START saat bot menyala.
4. POLLING ADAPTIF EXIT (baru, 18/06): monitor exit normal 15 detik; saat trailing
   ARMED (+2%) DAN harga bergerak >0.5%/cek → percepat ke 2 detik; tenang → balik 15 detik.
   CATATAN DISIPLIN: ini TIDAK bisa di-backtest (polling = perilaku real-time). Deploy ke
   live tanpa validasi backtest, atas keputusan sadar Budi (utamakan tangkap puncak).
   KONSEKUENSI: exit live lebih responsif drpd asumsi backtest (yg pakai high/low candle
   harian) → perbandingan live-vs-backtest utk exit jadi kurang murni. Ingat ini saat analisis.

Yang TIDAK diubah: 7 syarat entry, Stoch<70, semua toggle (BTC OFF, addfund OFF, max 1 deal).
entry_price opsi a = sudah ada sebelum sesi ini (cuma diverifikasi).

Eksperimen yang DIUJI & DITOLAK sesi ini:
- Filter bullish (volume hanya saat candle bullish): backtest tunjukkan REDUNDAN —
  bull=True identik bull=False (0 entry bearish di 103 pair). Tidak dipasang.
- Stoch<60: tanda overfit (win% IN 86.5 → OUT 73.3, entry terlalu sedikit). Dicoret.

## 9. CHANGELOG SESI 20/06/2026 — percepat pengumpulan data + uji Mode (b)

KONTEKS: entry sangat jarang (Stoch<70 + max 1 deal) -> forward-test diperkirakan
butuh BULANAN, bukan mingguan, utk kumpulkan cukup trade. Biaya jalan (Railway +
3Commas ~$26/bln) jauh > modal uji ($6). Maka diputuskan percepat pengumpulan data
TANPA mengubah 7 syarat inti (yg akan jadi overfitting).

Perubahan parameter OPERASIONAL (bukan strategi inti):
1. MAX_ACTIVE_DEALS: 1 -> 3 (modal $6 x 3 = $18). Logika thread1 diubah: dari "buka 1
   deal per scan" -> "buka beberapa kandidat sampai slot penuh" (urut ATR% terkecil dulu).
   PENTING: setting 3Commas "Max active trades" HARUS = 3 & disimpan, atau deal ke-2/3 ditolak.
2. MIN_VOLUME_USD: $2jt -> $1jt. Universe naik dari ~106 ke ~160 pair.
   KONSEKUENSI: koin volume $1-2jt TIDAK ADA di backtest -> live agak menyimpang dari backtest.
   Tidak turun lebih rendah (mis. $500rb) utk hindari koin mikro (slippage besar, manipulatif).

EKSPERIMEN MODE (b) — DIUJI & DITOLAK (backtest intraday 1h, 95 pair, 400 hari):
  MODE a (candle tutup): OUT n=11, win 81.8%, avg 10.77%, tot 118.4
  MODE b (intrabar)    : OUT n=11, win 72.7%, avg  6.67%, tot  73.4
  KESIMPULAN: Mode (a) MENANG telak di IN & OUT. Masuk lebih awal (intraday) ikut ke
  breakout yg belum terkonfirmasi -> sering batal. Menjawab kekhawatiran "kehilangan
  momentum HEI": ya kadang lewatkan yg naik, TAPI lebih sering terhindar dari yg batal.
  Net lebih untung menunggu candle tutup. LIVE TETAP MODE (a). Jangan ulang eksperimen ini.
  (Engine: backtest_modeB.py, import dari backtest_pool.py)

CATATAN BIAYA/TENGGAT:
- 3Commas subscription EXPIRED, bot berhenti 3 JULI 2026. Budi minta waktu 5 hari (s/d
  ~25/06) utk putuskan perpanjang. JANGAN lewat 3 Juli atau forward-test terputus.
- Railway kredit ~$4.58 / ~23 hari (per 18/06). Pantau jangan habis di tengah test.

KEPUTUSAN BELUM DIAMBIL (masih placeholder):
- Patokan minimal forward-test "berhasil/gagal": berapa trade & berapa minggu. BELUM ditetapkan.
  Penting ditetapkan SEBELUM banyak trade masuk (anti-overfit versi live).

## 10. VALIDASI 8-TAHUN (20/06/2026) — 1D vs 12h, walk-forward + per-tahun

KONTEKS: setelah backtest multi-timeframe, 12h sempat terlihat menjanjikan (OUT bagus).
TAPI uji awal cacat: data 12h cuma 2025-2026 (limit candle), sedang 1D 4 tahun -> tidak adil.
DIPERBAIKI: backtest_validate.py dgn PAGINATE (mundur sejauh mungkin), 1D & 12h rentang SAMA
(2018/2019-2026, ~102 pair). Walk-forward berbasis WAKTU NYATA (bukan posisi data).

HASIL (mode asis, 7 syarat identik):
  1D : walk-forward 6/6 segmen POSITIF. Per-tahun: 2022 -43.6% (bear), sisanya positif.
       Tahun terburuk 2022 -43.6% lalu pulih kuat 2023 +241%.
  12h: walk-forward 5/6 (Seg3 = bear 2022 NEGATIF -117.6%). Per-tahun: 2022 -128.9%.
       Total cuan lebih besar di tahun baik (2021 +684%, 2025 +401%) TAPI...

TEMUAN KUNCI: di BEAR MARKET 2022, KEDUANYA rugi, tapi 12h rugi ~3x LEBIH DALAM
(-128.9% vs -43.6%). Timeframe pendek = lebih banyak sinyal = lebih banyak breakout
palsu yg dibalik di pasar turun. Periode uji pendek sebelumnya MENYEMBUNYIKAN kerapuhan ini.

KEPUTUSAN: 1D TETAP DIPAKAI (live). 12h DITOLAK sbg pengganti — lebih agresif tapi lebih
rapuh di kondisi terburuk (persis kekhawatiran "bear market belum terwakili" di bagian 7,
yg kini TERWAKILI dan mengonfirmasi 1D lebih tahan banting). 1D terkonfirmasi 6/6 walk-forward
+ positif 7 dari 9 tahun (2018-2026) dgn kerugian bear terkendali. Jangan ulang eksperimen 12h.

CATATAN: 2022 rugi di KEDUA TF itu WAJAR (momentum long-only memang utk pasar naik).
Yg penting: 1D ruginya terkendali & pulih. Bukan kegagalan strategi.

## 11. UJI SYARAT 1 (Supertrend) — tiadakan/longgarkan — DITOLAK (20/06/2026)

KONTEKS: analisis first-fail menunjukkan Supertrend = biang kelangkaan utama (57% candle
gagal di sini). Diuji apakah meniadakan/melonggarkannya menambah entry tanpa rusak.

HASIL (backtest_supertrend.py, 92 pair, data dipaginate):
  varian   | avg% |  total | 2022(bear)
  base     | 9.25 |  1656  | -44%   <-- LIVE, TERBAIK
  no_st    | 7.11 |  1500  | -75%
  mult2.5  | 8.53 |  1578  | -75%
  mult2.0  | 7.42 |  1469  | -75%
  len7     | 8.15 |  1574  | -75%

KESIMPULAN (paling tidak ambigu dari semua eksperimen):
- base (Supertrend len10/mult3.0) punya avg per-trade TERTINGGI & total TERTINGGI.
- SEMUA varian memperdalam kerugian bear 2022 dari -44% ke -75% (hampir 2x lebih buruk).
- Per-tahun: base menang/imbang di SEMUA tahun; no_st kalah jauh di 2022 (-75 vs -44) & 2025 (+15 vs +60).
- Tidak ada varian yg menambah entry berkualitas — semua tukar kualitas demi kuantitas merugi.
- Supertrend WAJIB: ia penjaga prinsip "jangan beli saat turun" (anti mean-reversion/falling-knife).
  57% kegagalan = FUNGSI bekerja benar (menyaring 57% waktu pasar tak layak masuk), bukan cacat.
DECISION: Supertrend len10/mult3.0 DIPERTAHANKAN. Jangan tiadakan/longgarkan. Jangan ulang.

CATATAN trio eksperimen syarat (20/06):
- Volume 2x: melonggarkan jelas BURUK (avg turun, 2022 makin dalam). WAJIB.
- Supertrend: tiadakan/longgar jelas BURUK (lihat atas). WAJIB.
- Breakout: meniadakan (no_brk) MENGEJUTKAN — 2022 jadi +43% & entry 3x lipat, tapi avg turun
  9.25->5.98 & ubah karakter jadi trend-following. BELUM divalidasi penuh (walk-forward/per-tahun).
  Status: kandidat penasaran, perlu validasi lanjut sebelum disimpulkan. Live tetap pakai breakout.

## 12. KANDIDAT TERVERIFIKASI LOLOS (21/06/2026) — rebound_add & no_brk

Beda dari eksperimen lain yg DITOLAK, dua ini LOLOS validasi penuh (walk-forward 6 segmen
waktu + per-tahun, data ~8 th, 90-99 pair). STATUS: terverifikasi lolos, MENUNGGU fase
implementasi — BUKAN ditolak, BUKAN langsung dipakai. Live TETAP base sampai forward-test
base selesai (20-30 trade) & ada keputusan sadar.

REBOUND_ADD (add fund 1:1 saat rebound: turun >=2% lalu pulih dlm 3 candle):
  - Walk-forward 6/6 positif (= base). avg 11.14% > base 10.28%. total 1871 > 1726.
  - 2022: -17.3% vs base -21.9% (sedikit LEBIH BAIK di bear).
  - Per-tahun: menang/imbang di SEMUA tahun. Jumlah deal SAMA (168) — tdk nambah trade,
    hanya perbaiki hasil deal yg kena rebound (~33% deal).
  - KARAKTER STRATEGI TIDAK BERUBAH (momentum+breakout tetap inti). Peningkatan bersih, risiko rendah.
  - GANJALAN: implementasi live butuh bot melacak HARGA RATA-RATA setelah add fund — saat ini
    bot TIDAK bisa (masalah teknis 3Commas Trusted IP, lihat bagian 3b). Strategi terbukti,
    IMPLEMENTASI belum siap. Selesaikan pelacakan avg-price dulu sebelum terapkan.
  - RENCANA: kandidat kuat utk fase setelah forward-test base sukses.

NO_BRK (syarat breakout DITIADAKAN):
  - Walk-forward 6/6 positif. total 3245 (jauh > base) TAPI avg 6.12% << base 10.28% (edge per
    trade turun ~40%). Jumlah deal 3x lipat (530 vs 168).
  - 2022: +33.7% POSITIF (vs base -21.9%) — tahan bear market.
  - PERINGATAN: karakter BERUBAH dari "momentum breakout (brkX2)" jadi "trend-following tanpa
    breakout" = STRATEGI BERBEDA. Keunggulan total sebagian dari VOLUME trade (3x), bukan kualitas.
    avg tipis + 3x trade = 3x fee/slippage nyata (backtest cuma asумsi 0.2%).
  - STATUS: menarik sbg STRATEGI TERPISAH (mis. bot kedua paralel), BUKAN pengganti brkX2.
    Perlu validasi sendiri (slippage nyata di banyak trade) sebelum dipertimbangkan.

PRINSIP: "lolos backtest" != "langsung ganti live". Forward-test base belum hasilkan 1 trade
nyata; ubah strategi sekarang = buang forward-test, balik ke titik nol validasi live.

## 13. PATOKAN EVALUASI FORWARD-TEST (21/06/2026) — DIKUNCI

Keputusan yg sejak awal menggantung, kini ditetapkan. Dua tahap (gerbang bertingkat),
terpasang di penghitung Telegram binance_screener.py (FWDTEST_CHECK_TRADES=12, FWDTEST_TARGET_TRADES=25):

CEK AWAL — 12 trade CLOSED (sanity check, BUKAN keputusan final):
  Tujuan: deteksi masalah BESAR lebih dini. Saat tercapai, cek: win rate anjlok (<50%)?
  slippage menghancurkan return? Kalau ya -> STOP, cari sebab (bug? strategi tak cocok
  pasar kini?). Kalau sehat -> lanjut. Ini TIDAK memberi lampu hijau naik modal.

EVALUASI FINAL — 25 trade CLOSED (keputusan boleh naik modal):
  Kriteria LULUS (ketiganya):
  1. Win rate live mendekati backtest (~76%).
  2. Rata-rata return/trade live mendekati backtest (~10%). Kalau win rate ok tapi avg jauh
     lebih kecil -> SLIPPAGE memakan edge (cek kolom slip_pct di CSV).
  3. Tidak ada anomali (trade rugi jauh > perkiraan backtest -> indikasi exit/trailing tdk jalan).
  LULUS -> boleh naikkan base order BERTAHAP ($6->$20->$50), tetap tanpa add fund (rebound_add
  baru diterapkan setelah pelacakan harga rata-rata diselesaikan, lihat bagian 12).
  GAGAL -> investigasi sebab sebelum lanjut. JANGAN naik modal.

CATATAN: jumlah dipilih 12/25 (bukan lebih kecil) krn <20 trade mengukur KEBERUNTUNGAN
lebih dari edge. Bot lapor progres ke dua milestone otomatis tiap heartbeat 6 jam.

## 14. PERUBAHAN STRATEGI (21/06/2026) — TIMEFRAME 12h + VOLUME 1.2x

KEPUTUSAN USER (Budi pegang kendali): pindah dari 1D ke 12h, dan turunkan pengali volume
dari 2.0x ke 1.2x. Tujuan: entry lebih sering (1D + vol2.0 terlalu jarang). Didasari
backtest, bukan firasat. Live di-deploy 21/06 19:11 WIB.

DASAR BACKTEST (98 pair, data dipaginate):

Uji pengali volume di 1D (vol 2.0 vs 1.2):
  vol2.0: n=170 win=74.7% avg=9.47% tot=1610 | 2022=-59%
  vol1.2: n=352 win=71.9% avg=5.48% tot=1928 | 2022=-108% | walk-fwd 6/6
  -> vol1.2: ENTRY 2x lipat, total lebih tinggi, tapi avg per-trade & 2022 lebih lemah.

Uji timeframe (semua vol 1.2x):
  TF   |  n  | win%  | avg%  | total | 2022   | walk-fwd
  1d   | 348 | 71.6  | 5.52  | 1920  | -108%  | 6/6
  12h  | 564 | 68.4  | 2.56  | 1443  | -2%    | 5/6   <-- DIPILIH
  6h   | 607 | 64.1  | 0.73  |  445  | (n/a)  | 6/6   (edge tipis)
  4h   | 698 | 56.3  | -0.06 |  -40  | (n/a)  | 3/6   (RUGI)
  -> 12h: entry +62% dari 1D, edge per-trade MASIH sehat (2.56%, jauh di atas 6h/4h),
     walk-fwd 5/6. Satu-satunya TF pendek yg tidak kehilangan edge. 6h tipis, 4h rugi.

TRADE-OFF YG DISADARI (dicatat jujur):
- avg/trade 12h (2.56%) < 1D (5.52%) -> edge lebih tipis, lebih rentan slippage nyata.
- Data 12h cuma mundur ~2022 (1D sampai 2018) -> 12h belum teruji di sebanyak kondisi.
- 2022 antar-TF TIDAK apple-to-apple (kedalaman data beda). Angka 12h 2022=-2% vs 1D=-108%
  perlu dibaca hati-hati, bukan bukti 12h pasti lebih tahan bear.

PERUBAHAN KODE (binance_screener.py):
- TIMEFRAME = "12h" (dari "1d").
- VOLUME_MULT = 1.2 (dari 2.0).
- SECONDS_PER_CANDLE timeframe-aware: batas hold = 5 candle = 2.5 hari di 12h (BUKAN 5 hari).
  Ini penting—kalau dibiarkan 5*86400 dtk, posisi ditahan 2x lebih lama dari backtest.
- Label log/notif "harian" -> "candle 12h".

STATUS: forward-test sekarang menguji 12h+vol1.2 (bukan 1D+vol2.0 lagi). Entry akan jauh
lebih sering (~2x sehari dievaluasi). Patokan evaluasi (cek 12, final 25 trade) tetap berlaku.
CATATAN: konfigurasi awal (1D+vol2.0) tetap tervalidasi terbaik secara edge/ketahanan—
tersimpan di bagian 1-13. Perubahan ini pilihan sadar utk frekuensi > edge maksimal.

## 15. STRATEGI KEDUA (KANDIDAT) — REVERSAL DOJI + HEIKIN ASHI (22/06/2026)

Strategi BARU & TERPISAH dari brkX2. Karakter: REVERSAL (tangkap pembalikan dari bawah),
komplementer dgn brkX2 yg momentum-following. Diusulkan & dirancang oleh Budi.

DEFINISI (timeframe 8h):
  Kondisi awal: close c0 di BAWAH EMA20 DAN EMA50 (fase bearish/awal pemulihan)
  c0          : DOJI (badan |close-open| < 20% dari range high-low)
  c+1, c+2    : 2 candle HEIKIN ASHI bullish (HA_close > HA_open)
  ENTRY (open long awal c+3): jika c+2 ATAU c+3 crossing-up EMA20 (close transisi <EMA20 -> >=EMA20)
  ADD FUND    : saat candle crossing-up EMA50 (close <EMA50 -> >=EMA50); 1:1, sekali
  EXIT        : trailing adaptif (arm +2%, jarak ATR%) — sama spt brkX2

HASIL BACKTEST (91 pair, ~2021-12 s/d 2026-06, fee 0.2%):
  doji20: n=2059 win=81.4% avg=0.87% tot=1784% | walk-fwd 6/6 | 2022=+401%
  doji15: n=1606 win=81.3% avg=0.70% tot=1130% | walk-fwd 6/6 | 2022=+219%
  doji10: n=1106 win=80.7% avg=0.66% tot=726%  | walk-fwd 6/6 | 2022=+96%

VALIDASI (LULUS SEMUA):
- Sensitivitas doji: KETIGA ambang (10/15/20%) ber-edge + walk-fwd 6/6 + 2022 positif.
  -> edge NYATA & robust thd pilihan ambang, bukan artefak setting.
- IN/OUT split (70/30): OUT (data tak terlihat) tetap positif di ketiga ambang
  (doji20: IN avg 0.85% -> OUT avg 0.91%). -> BUKAN overfit; edge bertahan di data baru.
- Per-tahun (doji20): 2022..2026 SEMUA positif (2021 cuma 5 trade, diabaikan).
  Khususnya 2022 (bear) = +401%, justru di tahun brkX2 RUGI (-59% s/d -108%).

KENAPA PENTING: edge KOMPLEMENTER. brkX2 menang di pasar naik/momentum, rugi di bear.
Strategi reversal ini menang JUSTRU di bear 2022 + konsisten di semua kondisi. Dua
strategi karakter beda = potensi diversifikasi.

CATATAN JUJUR (belum siap live):
- avg 0.66-0.87% LEBIH TIPIS dari brkX2 (2.56% di 12h). Edge per-trade kecil -> SENSITIF slippage.
- WAJIB uji ketahanan fee/slippage dulu (sedang dikerjakan: backtest_doji_robust.py).
  Edge tipis bisa runtuh di fee realistis. Tentukan di fee berapa avg jadi nol.
- Ini BACKTEST, belum forward-test. Sama spt brkX2 dulu: bagus di masa lalu != jaminan live.
- Add fund (cross EMA50) perlu avg-price tracking utk live -> blok teknis 3Commas yg sama
  spt rebound_add brkX2 (lihat bagian 12). Implementasi live perlu solusi ini dulu.

STATUS: KANDIDAT STRATEGI KEDUA tervalidasi backtest. Langkah: (a) uji ketahanan fee
[proses], (b) jika lolos, rancang implementasi live terpisah dari brkX2.
File: backtest_doji_ha.py, backtest_doji_validate.py, backtest_doji_robust.py.
