# Bot Trading pacific-grace (sesi 07–08/11/2026)

## Tujuan
- Bot spot Binance langsung (Railway)
- Kurangi rugi besar per deal
- Keputusan berbasis backtest, bukan tebakan

## Strategi & indikator
### LIVE
- TrenKonfirmasi-4h (v2 sejak 10/11)
  - Tembus Keltner 4h, RSI 4h < 65, RVol < 3, Supertrend 1D koin & BTC naik, tolak Ekstrem
  - $8 per deal, tanpa add fund, tanpa AI; trailing 6x, tahan maks 20 candle 4h
  - Versi lama (s/d 09/11): 203 deal, 155W/48L, +130%; uji 1 menit -0,14%/deal
- KeltnerBreak-12h
  - Tembus Keltner (EMA20 + 2×ATR10), intrabar 5–75%
  - Gerbang BTC 1D hijau + volume 3D
  - #85 (62W/23L, -11,6%)
- brkX2-4h
### Paper-test
- RVolTrend-1d (target 15)
- Decouple-4h, Jeda-konfirmasi
### Fitur baru
- Add Fund bersyarat (MACD 4h + Stoch, 30m×5)
- Heartbeat 3 blok + #n (W/L, %)
- Rem Binance: 429-brake, weight-guard, cache

## Sumber data
- Binance data-api.binance.vision (1m, 1h, 4h, 1D, 3D)
- Cache 1h 244 coin, 2022–11/2026
- trades_forwardtest.csv (Drive)
- Log Railway

## Hasil utama
### R2/R3 (kasus SUPER)
- Backtest 5 thn, 58.118 deal
- R3 tolak entry: untung -$208 → ditolak
- R2 CLOSE: -$482, CANCEL tidak lebih baik → ditolak
- Jadi: uji bayangan AI
### KeltnerBreak-12h, 14 calon aturan
- Bagus di 45 deal asli (contoh +10,4%)
- Semua gagal backtest 5 thn → tidak dipasang
### Rumus lama vs Keltner (simulasi, exit sama)
- 5 thn: Keltner +1,29%/deal vs lama +1,00%
- Keltner unggul di 4/5 tahun, TRAIN & TEST
- Era live: BTC +30,4% (lama) vs -10,2% (Keltner)
- Keputusan: tidak kembali ke rumus lama
### Audit 45 deal Keltner
- Asli -51,7% | simulasi 1 menit -16,7%
- Simulasi 1 jam terlalu optimis (+95%)
- DUSK 17/10: Edit Deal reset trailing → -12,4% (bisa +2,9%)
- BAT 16/10: dijual di luar bot → -10,6% (bisa +3,0%)
- Untung kecil (~+3%) vs rugi besar (-10 s/d -12%)

### Add fund saat saldo kurang (uji 08/11)
- Pakai sisa USDT: untung naik di semua tahun (kecuali 1/4 modal di 2025)
- Makin besar tambahan, makin baik: +0,14% / +0,30% / +0,41% per $
- Coin orphan setelah hard stop: 30 hari median -8,1%, 47% turun >10%
- Jual orphan utk add fund: layak
- Catatan: modal baru utk deal baru +1,86% per $ (lebih besar)

### Uji ulang 1 menit KeltnerBreak-12h (4.079 deal, 2022-2026)
- Candle 1 jam: +1,16%/deal → candle 1 menit: +0,01%/deal (impas)
- Untung rata2 +2,56% vs rugi -5,81% → butuh menang 69,5%, nyatanya 69,6%
- TRAIN +0,008 / TEST +0,009; per tahun -0,24 s/d +0,14
- 14 calon aturan: tetap tidak ada yg konsisten
- Akar masalah: exit (trailing makin rapat) memotong untung
### Tahap 1: pantau sesaat setelah open (KB12h, 1 menit)
- Terbaik: 5-10 menit, cek tiap 1-5 menit, keluar bila turun ≥2%
- Efek kecil +0,05%/deal, positif TRAIN & TEST, 4/5 tahun
- Turun 1% → merugikan (terlalu sensitif)
- Setelah add fund: diuji bersama TrenKonfirmasi-4h
- Keputusan: (c) tunggu TrenKonfirmasi-4h; diuji ulang dgn exit baru
### Riset exit KeltnerBreak-12h (1 menit, 4.079 deal, 08/11)
- Bentuk trailing hampir tidak berpengaruh
- Kuncinya: trailing mulai aktif (arm) lebih tinggi
- Arm 4x (Volatil 2,75% → 11%): +0,01% → +0,36%/deal
  - Lebih baik di SEMUA 5 tahun, TRAIN +0,50 / TEST +0,17
  - Menang 70% → 41%; untung rata2 +2,6% → +8,3%
  - Tanpa 1% deal terbaik tetap +0,20%
- Arm 5–8x: angka lebih tinggi tapi 2022 lebih jelek
- Kunci impas setelah sentuh arm lama: merusak hasil → ditolak
- Tanpa trailing sama sekali: lebih jelek
- Status: usulan, belum di-patch (menyangkut uang)
### Cara lain perbesar PnL KB12h (di atas arm 4x, 1 menit)
- Tier Aktif (ATR 2-4%) bintangnya: +0,92%/deal
- Tier Ekstrem (ATR ≥7%) rugi di hampir semua setelan → skip: +0,36 → +0,51
  - Catatan: gerbang ATR dulu ditolak (sim 1 jam, exit lama); data 1 menit beda
- + pantau 10 menit (cek 5 mnt, turun ≥2% keluar): → +0,54, TRAIN +0,59 / TEST +0,47
- Arm beda per tier: 2022 jadi jelek (-0,94) → tidak dipakai
- 14 aturan entry dgn exit baru: hanya "Ekstrem + Supertrend 1D turun" konsisten
- Ide a-d di atas patch live (+0,47 per $):
  - a hold 120 jam: +0,49 | b BTC-dump -4%: +0,60 (naik terutama TRAIN) | c add fund saat untung: DITOLAK (per $ turun)
  - d modal 2x tier Aktif: +0,56, TRAIN = TEST, 5/5 tahun positif (paling kokoh)
  - Gabungan b+a+d: +0,78 per $, TRAIN +0,84 / TEST +0,68; 2022 -0,06, 2025 0,00
  - Status: usulan, tunggu Mas Budi
  - Koreksi: modal 2x Aktif naik hampir hanya di TEST (TRAIN +0,58→+0,56); per tahun 4/5 lebih baik
  - A+ (Aktif 3x $24 + tahan 120 jam): +0,69 per $, 5/5 tahun, TRAIN +0,59 / TEST +0,83
  - Kecilkan modal Ekstrem tidak bisa: $8×½ = $4 < minimal Binance $6
- Pilihan Mas Budi (arm 4x + tolak Ekstrem&ST1D turun + pantau 10m): +0,47%/deal, TRAIN +0,58 / TEST +0,32, 4/5 thn, total +1.841 (> skip semua Ekstrem +1.751)

### Uji ulang 1 menit TrenKonfirmasi-4h (6.000 deal, 09/11)
- Sim 4 jam +1,28 per $ → sim 1 menit -0,15 per $ (menang 67%, 2/5 tahun positif)
- Live: semua +0,38 per $; sejak smallstart 25/10 -0,41; November -1,09
- 120 varian: tidak ada yg konsisten TRAIN & TEST (arm 4x terbaik ~0, TEST tetap negatif)
- Hard stop lebih ketat: lebih jelek; pantau 10 menit (open/add fund): tidak berpengaruh; tolak Ekstrem: efek kecil
- Add fund: total rugi $ -111 → -67 kalau dimatikan (per $ sama); live: rugi besar = deal yg di-add (SUPER, ONE, MARSCOIN, ZAMA)
- Add fund reclaim TC4h DIMATIKAN (70f5b57, 09/11)
- Riset (c): 28 saringan entry (termasuk gerbang BTC live): tidak ada yg positif; gerbang BTC malah -0,19
- Exit (target untung, potong cepat, timeout pendek, arm tinggi): terbaik +0,03 tapi TEST negatif
- Add fund saat untung (pyramid): tidak membantu, rugi $ bertambah
- Kesimpulan: sinyal entry TC4h tidak punya keunggulan di simulasi realistis

### Riset strategi pengganti TC4h (09/11)
- Saringan 14 calon (exit model KB12h baru, sim 1 jam; kontrol Keltner 12h +0,36 = sama dgn uji 1 menit)
- Juara: Keltner 1D. Tanpa Ekstrem + BTC Supertrend 1D naik: arm 4x/10 hari +1,30, arm 6x/10 hari +1,90 (5/5 thn)
- Calon 4h/8h: sekitar 0 atau rugi
- Konfirmasi 1 menit 1.694 deal: arm4/10 hari 1 jam +1,30 → 1 menit +1,20 (hampir sama)
- Terbaik & paling rata: arm 6x / tahan 5 hari: +1,36%/deal, TRAIN +1,50 / TEST +1,21, 5/5 tahun (≥ +0,77)
- Pantau 10 menit: tidak membantu
- Status: usulan strategi baru "KeltnerBreak-1D", tunggu izin Mas Budi

## Riwayat Patch
### rule-watch-log-only (d8cea61)
- 07/11: catat R2/R3, tidak mengubah keputusan
- Status: berjalan
### rule-watch-email (90a0985)
- 07/11: kabar review juga lewat email
- Status: berjalan
### rule-watch-ai-shadow (2d912c6)
- 08/11: AI ditanya saat R2/R3, jawaban hanya dicatat
- Alasan: backtest tolak R2/R3 sbg aturan
- Status: berjalan, tunggu 20 kejadian
### rule-watch-ai-display (37e52fe)
- 08/11: uji bayangan tampil di heartbeat + kartu web
- Status: berjalan (deploy 09:03 WIB)
### edit-deal-keep-trailing (37e52fe)
- 08/11: koreksi entry tidak matikan trailing aktif bila masih untung
- Alasan: kasus DUSK -12,37% (bisa +3,00%)
- Status: berjalan (deploy 09:03 WIB)
### addfund-shortfall-combine-and-partial (5e8384a)
- 08/11: saldo kurang → gabung coin orphan/nganggur → pakai sisa USDT (min $6), tanpa ditunda AI
- Alasan: uji add fund sebagian > TUNDA; orphan cenderung turun
- Status: berjalan (deploy 09:33 WIB)
### deal-chart-strategy-tf (09/11)
- Chart Deal (deal aktif & closed) pakai timeframe strategi: TC4h 4h, KB12h 12h, KB1D 1d; 60 candle sebelum open
- Alasan: dulu interval otomatis (deal baru → candle 1 menit), beda jauh dgn TradingView
- Status: deploy 09/11 malam
### auto-sync-manual-fallback (16fcf27, tag patch-auto-sync-manual-fallback)
- 09/11: add manual via Convert ikut tersinkron (harga dari riwayat Convert; cadangan harga pasar saat terdeteksi)
- Sisa koin lama saat deal dibuka tidak digabung (wallet_base_diff)
- Alasan: kasus MET (wallet 47,24 vs bot 23,98) -- dulu ditolak karena Convert tidak ada di myTrades
- Status: deploy 09/11 malam
### kb12h-quality-filters (0c7442b, tag patch-kb12h-quality-filters)
- 09/11: KB12h tolak SEMUA Ekstrem, BTC ST1D naik, RSI 12h < 65, harga < 3% di atas Keltner 12h, trailing 6x
- Alasan: uji 1 menit +0,47 → +1,29%/deal, 5/5 tahun (arm 4x/5x/8x juga +1,03..+1,20)
- Status: deploy 17:57 WIB
### TC4h: resep KB12h digabung (09/11)
- Hasil: TRAIN positif tapi TEST negatif di hampir semua gabungan; 2022 & 2025 rugi besar → TIDAK dipasang
- TC4h tetap live $8 (keputusan Mas Budi: jangan dihapus)
### Rombak TC4h (09/11)
- 8 desain 4h; terbaik: Keltner 4h + tren 1D naik (ST1D & >EMA50 1D) + RSI<65 + tembus<3% + tolak Ekstrem + BTC ST1D naik
- Uji 1 menit 783 deal, trailing 4x / tahan 15 candle: +0,46%/deal (vs -0,14 sekarang), TRAIN/TEST per coin +0,37 / +0,57, 4/5 tahun (2025 -1,37)
- Status: usulan ganti isi TC4h, tunggu izin → Mas Budi minta cari lebih besar
- Ronde 2 (16.698 breakout, 27 saringan, kombinasi 1-3, TRAIN/TEST per coin, sim 1 jam): RSI<65 + RVol<3 + coin ST1D naik, trailing 6x/15 candle: +1,14%, +1,08/+1,21, 5/5 tahun
- Konfirmasi 1 menit (10/11): RSI<65 + RVol<3 + coin ST1D naik, trailing 6x, tahan 20 candle: +1,35%/deal (1 jam +1,32), TRAIN/TEST per coin +1,28 / +1,42, 5/5 tahun
- Semua varian inti (RSI<65 + coin ST1D naik) positif 5/5 tahun di uji 1 menit
- Status: DISETUJUI 10/11 ("boleh") → dipasang sbg patch tc4h-v2
### Simulasi satu dompet / ukuran modal (13/11, riset)
- 3 strategi live (KB12h, KB1D, TC4h v2), uji 1 menit 2022-2026, modal per deal = % saldo
- Kunci: MAX DEALS, bukan besar modal; slot 3 cuma dapat ~+0,4%/deal (sinyal bergerombol)
- Usulan: 2% saldo per deal (KB12h Aktif 3x) + max 12-20 per strategi: ~11,5-12,3%/thn, turun terdalam ~27%, TRAIN=TEST
- Status: belum dipasang, tunggu dana masuk + izin
### hold-floor-stop-when-empty (12/11)
- Pagar darurat hold_no_sell berhenti memantau koin yang saldonya sudah 0 (DASH: WARN gagal jual tiap menit)
- Status: deploy 12/11
### Backtest "hold dulu" sesudah hard stop (12/11)
- 531 kejadian hard stop TC4h 2022-2026 (uji 1 menit + 1 jam sesudahnya): jual langsung -8,58% vs hold+pagar -5%: 7 hari -9,33%, 30 hari -9,23%
- Hold menang cuma 24% kejadian; pagar -5% kena di 90% kejadian; hold sampai balik modal -8,52% (setara jual langsung)
- Kesimpulan: hold dulu TIDAK menolong, rata-rata sedikit merugikan
### hard-stop-sell-now (12/11, Mas Budi: "BOLEH")
- Kena hard stop / timeout rugi = LANGSUNG dijual, semua strategi (checkbox "Hold, jgn jual" default mati; centang per deal di Monitor tetap bisa)
- Balik: HOLD_NO_SELL_DEFAULT = True
- Status: deploy 12/11
### telegram-no-raw-bold-tags (12/11)
- 4 notif Telegram (Pagar Darurat Hold-No-Sell, Convert Aset, Jual ke USDT, Add Manual Tersinkron) tampil "<b>...</b>" mentah; tag dibuang
- Status: deploy 12/11
### tc4h-v2 (5db2720, tag patch-tc4h-v2, 10/11)
- Isi TrenKonfirmasi-4h diganti desain ronde 2: entry sekali per tutup candle 4h (+2 menit)
- Syarat: tembus Keltner 4h segar, RSI14 4h < 65, RVol < 3, ATR14% < 7, Supertrend 1D koin naik, BTC Supertrend 1D naik
- Exit: hard stop tier ×1,1 (dulu ×1,5), trailing aktif 6x ambang, tanpa arm-dini BB Upper, tanpa grace HTF, tanpa AI, tahan maks 20 candle 4h (dulu 15)
- Modal $8 flat, tanpa add fund; maks 3 deal; email review di deal ke-20
- Uji 1 menit: +1,35%/deal, TRAIN/TEST per coin +1,28 / +1,42, 5/5 tahun
- Balik ke versi lama: TC4H_V2_ENABLED = False
- Status: deploy 10/11 ~14:15 WIB; email review hitung deal sejak 14:15
### kb1d-keltnerbreak-1d (4363bd7, tag patch-kb1d-keltnerbreak-1d)
- 09/11: strategi BARU KeltnerBreak-1D live $8, maks 3 deal, tanpa AI
- Entry tiap tutup candle 1D (07:00 WIB): tembus Keltner, ATR<7%, BTC Supertrend 1D naik
- Exit: hard stop tier ×1,1, trailing aktif 6x ambang, tahan maks 5 hari
- Uji 1 menit: +1,36%/deal, 5/5 tahun
- Status: live; scan pertama 10/11 07:00 WIB
### binance-throttle-relax (30202b0, 09/11)
- Jarak antar-request Binance 0,3 → 0,1 detik; jeda unduh awal 0,6 → 0,2 detik
- Alasan: IP sendiri, pemakaian hanya 3-5% jatah; jaring 429/418/80% tetap
- Status: deploy sore 09/11, pantau [Binance-WEIGHT]
### tc4h-reclaim-addfund-off (70f5b57, tag patch-tc4h-reclaim-addfund-off)
- 09/11: TRENDCONFIRM_RECLAIM_ADDFUND_MAX_ADDS 2 → 0 (deal aktif + orphan)
- Alasan: uji 1 menit 6.000 deal: per $ sama, rugi $ -40% tanpa add fund
- Status: berjalan
### binance-via-own-ip (8e2ab39, tag patch-binance-via-own-ip)
- 08/11: REST Binance lewat droplet DigitalOcean SGP1 146.190.93.255 (tinyproxy) bila BINANCE_PROXY_URL diisi
- Alasan: IP Railway shared di-ban 4x (bobot IP ~9.700 dari tenant lain); Railway: tidak ada IP dedicated
- Tes droplet: ping 200, used-weight 2
- Status: LIVE 23:26 (a2cad10 guard format); IP ditambah di key Master + Sub Account 23:39 → normal
### proxy-health-email + zama-floor-stop (0b3cc97)
- 08/11: email sekali bila 24 jam lewat proxy tanpa ProxyError/-2015 → IP Railway boleh dihapus dari API key Binance
- ZAMA (sudah tidak di wallet) berhenti dicoba jual oleh pagar darurat hold-no-sell
- Status: deploy 23:45 WIB
### kb12h-aplus (0324482, tag patch-kb12h-aplus)
- 08/11: KB12h tier Aktif (ATR 2-4%) modal awal $24 (3x $8); tahan maks 120 jam (10 candle 12h)
- Email review (BUKAN Telegram) di 20 & 30 deal sejak 08/11 20:00 WIB: sesuai = ≥ +0,30% per $ & total > 0 → boleh naik ke $32
- Alasan: backtest 1 menit +0,47 → +0,69 per $, 5/5 tahun
- Status: deploy 22:09 WIB
### kb12h-exit-fix (6806348, tag patch-kb12h-exit-fix)
- 08/11: KB12h saja -- arm trailing 4x; BB-Upper arm-dini OFF; deal terbuka yg sudah armed di-un-arm sekali
- Tolak entry ATR ≥7% + Supertrend 1D turun
- Pantau dini menit 5 & 10: turun ≥2% dari entry → close
- Alasan: uji 1 menit, exit lama impas (+0,01%) → +0,47%/deal, TRAIN +0,58 / TEST +0,32
- Status: deploy 08/11 sore WIB
### KeltnerBreak-12h ganti rumus (3c5c19d)
- 03–04/10: 7-syarat lama → Keltner
- Status: berjalan; simulasi 08/11 mendukung tetap

## Masalah terbuka
- Jual manual di luar bot (BAT, DYM, TIA)
- Simulasi lama pakai candle 1 jam → terlalu optimis (terbukti: KB12h impas di 1 menit)
- Antrean ACT→SUPER gagal (NOTIONAL)
- Shared IP Railway (risiko ban) -- 08/11 terbukti: ban saat restart 17:48, ban ulang 18:43 (bobot IP 9.893, bot sendiri ~1.100) s/d ~20:27 WIB

## Langkah berikutnya
- KB12h ide a-d: hold >60 jam, setel ulang BTC-dump/early-cut, add fund saat untung, modal lebih besar tier Aktif
- Lalu TrenKonfirmasi-4h 1 menit + review-nya + "sesaat setelah add fund"
- Uji ulang "sesaat setelah open" dgn exit baru
- Tunggu 20 kejadian uji bayangan R2/R3
- Pantau [Binance-WEIGHT]
