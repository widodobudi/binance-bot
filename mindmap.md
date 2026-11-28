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
### idle-usdt-earn (14/11, Mas Budi: "MAU", centang Permits Universal Transfer di railway-binance)
- USDT Sub di atas cadangan (maks($30, 25% saldo)) -> pindah ke Master -> Earn Flexible USDT; cadangan < separuh / open kurang dana -> redeem + transfer Master->Sub
- Hanya uang milik bot yang ditarik (catatan idle_earn.json); Earn pribadi Mas Budi di Master tidak disentuh
- Jalur lama ensure_spot_usdt (redeem Earn Master yg tidak pernah sampai ke Sub) dicabut
- Tahap A: cek izin dulu (IDLE_EARN_ENABLED False)
- 14/11 pagi: 2 Sub Account di Master -> BINANCE_SUB_EMAIL diisi (Sub bot), cek saldo tujuan harus cocok; DINYALAKAN ("SAYA PERBOLEHKAN"); APR USDT 3,06%
### pb4h-pantulan-panik (14/11, Mas Budi: "ya" live $6)
- Strategi BARU PantulanPanik-4h (key panic_bounce_4h): jalan HANYA saat BTC Supertrend 1D turun
- Entry tutup candle 4h: BTC turun >=10% 7 hari, koin turun >=10% 24 jam, RSI 4h sebelumnya <25, candle hijau tutup kembali di atas BB bawah, ATR<7
- Exit: hard stop x1,1, trailing 4x, tahan maks 15 candle 4h, tanpa AI; $6, maks 3 deal
- Uji 1 menit 385 deal: +4,69%/deal, WR 74%, TRAIN +4,28 / TEST +5,14, 5/5 tahun; breakout saat BTC turun semua rugi
- Sim dompet (+3 strategi, 1,5%/max20): +10,5%/thn turun 10,7% -> +13,5%/thn turun 7,8%
- Status: deploy 14/11
### pb4h-coin24h-8 (d443a98, 14/11, Mas Budi: "≤ −8%")
- Syarat koin turun 24 jam PantulanPanik-4h: −10% -> −8% (deal lebih sering)
- Uji 1 menit: 553 deal +3,95%/deal WR 72%, TRAIN +3,59 / TEST +4,35, 5/5 tahun (−10%: 385 deal +4,69%)
- Koin turun hanya −5..−8% rata-rata −0,14% -> jangan dilonggarkan lagi tanpa data baru
- Status: deploy 14/11
### scan-dryrun-on-start (912aa6d, 14/11, Mas Budi: "pastikan script bekerja")
- Tiap bot restart: scan ASLI PantulanPanik-4h (candle 4h tutup terakhir) & KeltnerBreak-1D (candle 1D 07:00) dijalankan sekali dlm mode UJI
- Mengisi Kandidat Terdekat/Scan Blockers + log "[T_PB4H] UJI ..." / "[T_KB1D] UJI ..."; berhenti SEBELUM order, candle tidak ditandai selesai
- Status: deploy 14/11
### ai-skip-outcome-log (8842971, 28/11, Mas Budi: "MAU, SETUJU") -- PENCATATAN MURNI, tidak mengubah keputusan buka/jual
- Tiap keputusan AI untuk pembukaan deal (SKIP dan OPEN, semua strategi yg memakai ai_decision_open) dicatat sekali per (koin, strategi, keputusan) per 12 jam bersama harga saat itu -> file ai_skip_outcomes.json di volume data. 48 jam kemudian harga diukur dari candle 5 menit: harga +6/12/24/48 jam, tertinggi/terendah, dan untuk KeltnerBreak-12h simulasi exit bot (fungsi hard_stop_pct/get_arm_pct/trailing_dist_progressive milik bot sendiri, fee 0,2%)
- Log: "[AI-SKIP-LOG] dicatat ..." saat dicatat, "[AI-SKIP-OUTCOME] ..." saat hasil selesai; endpoint baca-saja /api/ai_skip_outcomes (ringkasan SKIP vs OPEN + 200 catatan terbaru). Pelacak jalan dari thread2_monitor tiap 10 menit, maks 6 koin per putaran (1 panggilan klines per koin); catatan disimpan 90 hari
- Alasan: tinjauan 28/11 atas 8 penolakan AI (5 tepat, 2 salah INIT/HAEDAL, 1 ambigu PORTAL; simulasi rata2 -4,8%/koin kalau semua dibuka) terlalu kecil utk menilai AI; uji ciri "koin vs EMA50 1D" di 14.804 entri sejarah TIDAK menunjukkan keunggulan, jadi tidak ada patch pelonggaran AI
- Belum dicatat: penolakan babak-2 (ai_decision_batch_rank) dan keputusan AI selain pembukaan
- Uji: harness dgn kasus nyata MOVE (26/11 07:36) cocok dgn hitungan manual (stop -12,3%), dedupe, tick, file rusak
### nearmiss-tab-focus (7390e43, 27/11, Mas Budi: "poin 1 dan 2") -- hanya TAMPILAN dashboard
- Kandidat Terdekat per Strategi: tab strategi yang dipilih diingat (localStorage nm_active_strategy, juga setelah reload halaman) dan tidak lagi selalu kembali ke KeltnerBreak-12h; tombol Refresh (refreshNearMiss) hanya memperbarui KARTU strategi yang sedang fokus (dicocokkan lewat NAMA tab, bukan nomor urut), kartu lain tidak disentuh; kalau strategi hilang dari daftar, blok diganti penuh sekali lalu kembali ke tab tersimpan/pertama
- Server TIDAK berubah: endpoint /fragment/near_miss tetap merender dashboard utuh (index()) lalu dipotong; beban server per Refresh sama seperti sebelumnya. Versi yang benar2 lebih ringan butuh refactor index()
- Uji: JS dicek node --check + simulasi DOM jsdom (9 skenario: default, pilih tab, Refresh mempertahankan fokus, hanya kartu fokus yg berubah, reload memulihkan tab, strategi hilang, pergeseran urutan tab, gagal fetch)
### nearmiss-panel-gaps (1346754, 27/11, Mas Budi: "Mau") -- hanya TAMPILAN panel Kandidat Terdekat, tidak menyentuh keputusan beli/jual
- KeltnerBreak-12h: panel dulu hanya memuat koin yg GAGAL (>= 5/7); koin yg lolos 7/7 tapi tertahan (candle sudah diproses, AI skip, freshness guard) tidak tampil ("1 kandidat lolos" di log, panel kosong), dan jalur intrabar T1c/T1c-E tidak pernah menyentuh panel. Sekarang: kb12h_panel_seen() mencatat catatan dari T1c/T1c-E (TTL 2 jam) dan T1 menggabungkannya lewat kb12h_panel_items() + kandidat lolos T1 tampil dgn keterangan
- TrenKonfirmasi-4h: saat BTC Supertrend 1D turun (T_TC4H_V2) fungsi keluar tanpa memperbarui panel; sekarang panel dikosongkan + status "BTC Supertrend 1D turun, strategi diam" seperti KB1D/PB4H/WK
- Belum terbukti: brkX2-4h "Scan: -" (tidak ada log "[T1d] Scan 4h intrabar" 16 jam) -- diduga strategi dimatikan di Strategy Control, perlu dicek Mas Budi
### wk-breakeven-lock (8c2625f, 25/11, Mas Budi: "MAU" patch kunci impas, "boleh" deploy)
- Wyckoff-Spring-4h saja: setelah puncak harga >= +5% di atas entry (rata-rata), batas jual naik ke entry +0,5% menggantikan hard stop 7,7%; trailing (arm 5,5x) tetap jalan di atasnya. Memakai mekanisme impas yg sudah ada (BE_TRIGGER_BY_STRATEGY / BE_EXIT_BUFFER_PCT_BY_STRATEGY), AI close dilewati
- Dasar: uji 1 menit 325 deal 2022-2026: rata-rata +0,818% -> +0,932%/deal, deal rugi 48% -> 43%, Train +0,657 -> +0,739, Test +0,996 -> +1,146; hanya 9% deal keluar lewat impas (rata2 +0,25%), 1 dari 29 berakhir rugi bersih -0,86% (di live: dialihkan jadi cancel, koin tetap di wallet, sesuai aturan lama "impas rugi = cancel")
- TIDAK dipasang di PantulanPanik-4h: uji yang sama menurunkan hasilnya (+3,947% -> +3,774% pada +5%; netral hanya di >= +7%)
- Pemicu +4% juga positif (+0,916%), +3% negatif (-0,135), +6% kecil (+0,872)
- Pemicu SAHARA: puncak +9,7% jam 11:55 WIB tanpa trailing arm (ATR live melonjak ke tier Volatil -> ambang arm 15,1%, bukan 11%); dampak ATR live pada ambang arm diukur kecil rata2 (-0,064 poin Wyckoff, -0,038 PantulanPanik), tidak dipatch
- Status: deploy 25/11 10:30 WIB (SUCCESS, 23 thread aktif, tanpa galat)
### ind-rules (f699fb2, 25/11, Mas Budi: "juga untuk Active Deals", "pengaman yang sama dipakai SETUJU", "deploy")
- Aturan indikator ditulis sbg teks, mis. "HARGA > EMA(20,4h)", "HARGA > BBU(20,2,4h)", "RSI(14,1h) > 70" (file baru ind_rules.py: parser, 19 indikator, state persilangan). Candle yg SEDANG berjalan (harga live disuntik ke candle terakhir); hanya jual saat MENYILANG BARU (false -> true); kondisi sudah terpenuhi saat disimpan / setelah restart / aturan diganti = hanya mencatat status awal, tidak jual; setelah jual, state direset
- Auto Sell Asset: kolom aturan di sel Target Jual (tombol Cek, status live); config key rule_text; "price >= threshold" diganti hasil aturan, hold menit/trailing/sell %/BNB tetap
- Active Deals: mode TP baru "Indikator" (tp1_target_mode=rule, tp1_rule_text) di TP tunggal; pengaman tp-net-profit-gate TETAP berlaku (tidak jual kalau profit bersih <= $0; aturan tetap "armed" dan jual begitu profit > 0 selama kondisi masih terpenuhi); log gerbang itu dibatasi 1x/10 menit. TP Tier BELUM mendukung mode Indikator
- Data: klines per (koin, timeframe) di-cache 45 detik (hemat beban API); endpoint baca-saja /api/ind_rule_preview
- Uji: 56 pemeriksaan data buatan (parser, tiap indikator vs hitungan independen, mesin persilangan) + validasi dgn nilai TradingView asli (screenshot PENDLE 4h 23/11 22:47: BB 1.284/1.353/1.214, EMA20 1.313, EMA50 1.366, RSI 57.74, CCI 174.22, Williams %R -23.81, BB %b 1.04, Stoch(14,3,1) 76.19/89.95 semua cocok). ATR% tidak sama dgn panel "ATR%" di chart Mas (2.394 vs 2.82 Wilder-14)
- Status: deploy 25/11 10:48 WIB (SUCCESS). File bot utuh belum dikompilasi (plugin menolak py_compile); TP Tier belum mendukung mode Indikator
### ws-gap-repair (4fb8948, 23/11, Mas Budi: "Mau, cek juga pada jam-jam lain")
- Bug: restart yg backfill REST-nya melintasi penutupan candle (23/11: backfill 4h 22:55:42-23:00:10, WS tersambung 23:00:13) melewatkan pesan penutup candle 19:00 -> cache 737 koin kehilangan candle tertutup terakhir -> scan 4h 23:00 dilewati (log "data 4h BTC tidak lengkap sampai 20 menit"), Kandidat Terdekat kosong; penutupan berikutnya menambah candle dgn lubang di tengah (indikator meleset sampai restart). Cache 8h ikut terkena (selesai 23:01:09, batas 8h 23:00)
- Perbaikan: tiap WS tersambung/tersambung ulang (on_open) jalan _ws_repair_gap utk cache 4h (BX4H) & 8h (WS8H): koin yg candle tertutup terakhirnya lebih tua dari seharusnya diisi ulang via REST + digabung; 0 panggilan REST bila tidak ada yg tertinggal
- Cek riwayat 14-23/11 (semua "Backfill selesai"): hanya restart 23/11 22:54 yg melintasi batas candle 4h/8h; 14/11 15:00 (PB4H dilewati, dulu dikira HTTP 500) kemungkinan serupa (restart ~15:00), belum terbukti
- Belum dilindungi: cache 12h (KB12H, batas 07/19 WIB) dan 1h (MB1H, tiap jam) -- pola sama, backfill-nya lebih singkat; risiko lebih kecil
- Status: deploy 23/11
### wk-arm-5.5x (7a024c4, 23/11, Mas Budi: "addjust jadi 5,50x")
- Wyckoff-Spring-4h: ambang aktif trailing 6x -> 5,5x ATR tier (Aktif: 12% -> 11%); berlaku juga untuk deal PENDLE yg sedang terbuka (dihitung ulang tiap siklus)
- Uji 1 menit 325 deal, kelipatan 0,25 dari 2x-8x: 6x $0,164/deal, 5,5x $0,150, puncak 6,75-7x $0,182; ambang <4x jelas lebih buruk. Dipilih 5,5x agar wick PENDLE 1,398 (+11,3%) sempat mengaktifkan trailing; harga: ~-$0,014/deal vs 6x
- Exit EMA20 untuk Wyckoff TIDAK dipasang (uji dompet: 23,0% -> 21,2%/thn)
- Status: deploy 23/11
### pb4h-ema20-exit (fdb87c0, 23/11, Mas Budi: "Mau, bot dipasang aturan ini juga")
- PantulanPanik-4h: deal ikut di-close begitu candle 4h yg sudah tutup (setelah deal dibuka) tutup di atas EMA20 4h DAN di atas harga entry; gerbang AI close dilewati. Hard stop, trailing, batas 15 candle 4h tidak berubah
- Alasan: kalau harga masih naik, kenaikan lanjutan diserahkan ke strategi lain yg membuka deal baru. Dicek sekali per candle 4h per deal (jeda ulang 45 detik kalau data candle belum siap); log "[PB4H-EMA20]" hanya saat error, alasan close tampil di riwayat deal
- Dasar: uji 1 menit 2022-2026, 553 deal: exit lama +3,95%/deal menang 72% -> +4,06% menang 74%; 24% deal keluar lewat aturan ini; lama tahan 37,4 -> 34,7 jam; terburuk tetap -10,1%; Train +3,59 -> +3,62, Test +4,35 -> +4,56. Tambahan kecil (~+0,11 poin), manfaat utama modal kembali ~3 jam lebih cepat
- Indikator TradingView Panic Bounce 4h Signals: v1.6 sempat memakai aturan yg sama (opsi emaExit), lalu 23/11 DIKEMBALIKAN ke perilaku v1.5 atas permintaan Mas Budi (TradingView tidak bisa menerbitkan ulang nomor lama; versi baru diberi label "v1.5 restored"). Hanya bot yg memakai aturan EMA20
- Status: deploy 23/11
### crowd-bounce-close (56cfb2d, 21/11, Mas Budi: "(b1)")
- Semua strategi: tiap jam (siklus T2 pertama di jam baru) hitung deal aktif yg harganya >=3% di bawah entry; kalau >=4 deal -> deal2 itu ditandai (crowd_flag)
- Deal yg ditandai di-close begitu harga kembali >= +1% dari entry (gerbang AI close dilewati); tanda tetap sampai deal close. Telegram saat ada penandaan baru, log [CROWD] tiap jam
- Dasar: simulasi dompet tahap 2 (1,5%, maks 20/strategi) 2022-2026: +23,0%/thn & turun terdalam -11,5% -> +20,9%/thn & -8,2%; terburuk -19,3% -> -17,2%; Train +87 -> +81%, Test +122 -> +102%; 5/5 tahun positif. Ide 1 (stop deal tren bila 2-3 deal minus) tidak berpengaruh -> tidak dipasang
- Status: deploy 21/11
### wk-htf-size (0dc7792, 21/11, Mas Budi: "YA")
- Wyckoff-Spring-4h: sebelum open long cek candle 1D KOIN yg sudah tutup: close >=10% di bawah EMA20 1D -> modal x2 ($6 -> $12, add fund ikut $12); tidak lolos -> modal biasa
- Tag CSV entry_mode htf_lolos / htf_tidak; Telegram OPEN tampilkan hasil cek; Performance +2 baris (lolos / tidak lolos)
- Dasar: backtest 1m 2022-2026, 325 deal: $ +53 -> +99, per $ modal 1,81% -> 1,99%, Train/Test & 5/5 tahun lebih baik. Syarat PantulanPanik (OR) tidak membantu Wyckoff
- Status: deploy 21/11
### pb4h-htf-size (df5c9a0, 21/11, Mas Budi: "YA")
- PantulanPanik-4h: sebelum open long cek kondisi 1D KOIN (candle 1D tutup): turun >=40% dlm 30 hari ATAU close >=20% di bawah EMA50 1D
- Lolos -> modal x2 ($6 -> $12); tidak lolos -> tetap open, modal biasa ($6). Tag CSV entry_mode = htf_lolos / htf_tidak; Telegram OPEN tampilkan hasil cek
- Performance: 2 baris tambahan "PantulanPanik-4h (lolos cek HTF 1D, modal x2)" & "(tidak lolos HTF, modal biasa)"
- Dasar: backtest 1m 2022-2026, 691 deal: lolos 479 +3,93% WR 71%, tidak lolos 212 +1,76% WR 64%; $ +135 -> +248, Train/Test & 5/5 tahun lebih baik
- Status: deploy 21/11
### earn-on-sub (c604505, 20/11, Mas Budi: "ya")
- Earn USDT nganggur bot PINDAH ke SUB akun: sweep = Spot sub di atas cadangan -> subscribe Earn sub; refill = redeem Earn sub -> Spot sub (tanpa transfer master)
- Migrasi sekali: USDT bot yg tercatat di Earn master (idle_earn.json bot_in_earn) ditarik lewat jalur lama (redeem + master->sub), lalu di-Earn di sub; Telegram 1x
- Sesudahnya Earn master TIDAK disentuh bot lagi; master hanya menerima transfer profit. Subscribe sub ditolak -> USDT tetap di Spot sub + Telegram 1x
- Dasar: uji baca 20/11 15:09 [SUB-EARN-TEST] key sub lihat Flexible USDT canPurchase/canRedeem True. IDLE_EARN_ON_SUB=False = kembali ke alur master
- Status: deploy 20/11
### sub-earn-readonly-test (803b9fb, 20/11, Mas Budi: "MAU" uji baca saja)
- Sekali tiap restart (2 menit stlh start): pakai API key SUB, hanya GET -> daftar produk Simple Earn Flexible USDT, posisi Earn di sub, izin API key sub; hasil di log [SUB-EARN-TEST]
- Tujuan: memastikan Earn idle-USDT bisa dipindah dari master ke sub (usulan Mas Budi), TANPA subscribe/redeem/transfer
- Status: deploy 20/11
### wyckoff-stage2 (d83158f, 18/11, Mas Budi: "MAU")
- Wyckoff-Spring-4h masuk PCT_SIZING_STRATS: mulai tahap 2 (15/12) base 1,5% saldo (min $6), maks 20 deal; batas gabungan jadi 20 x 5 + 10 = 110
- Sekarang (PCT_SIZING_ENABLED=False) tidak ada perubahan: tetap $6 flat, maks 3
- Status: deploy 18/11
### wyckoff-spring-4h (be1f695, 18/11, Mas Budi: "YA" + "tampilkan di menu-menu di mana mesti ditampilkan")
- Strategi BARU Wyckoff-Spring-4h (key wyckoff_spring_4h), TF 4h, HANYA saat BTC Supertrend 1D turun; Akumulasi-4h lama tetap mati
- Kotak sideways 60 candle 4h (10 hari ke BELAKANG, bukan ramalan) sesudah turun >=20%, tinggi <=30%; spring = low jebol <=3% di bawah support lalu hijau tutup di atasnya; entry = uji ulang <=6 candle (hijau, di atas support, low > low spring); ATR<7
- $6 flat, maks 3 deal, tanpa AI; add fund 1x ($6) sekali kalau candle 4h hijau tutup >=2% di atas entry dlm 24 jam (wk_addfund_check); exit hard stop tier x1.1, trailing arm 6x, timeout 15x4h (60 jam); tanpa early exit
- Uji 1 menit 325 deal: +1,61%/deal base, TRAIN +1,53 / TEST +1,69, 5/5 tahun; dgn add fund +$0,164/deal ($6), 1,81% per $ modal; acak -0,27%
- Menu: Strategy Control, Performance, Closed Trades (filter), Kandidat Terdekat + deskripsi, Scan Blockers, Manual Scan (+detail syarat), panel gerbang BTC, heartbeat Telegram, size-cut card, label semua tabel; uji tanpa order tiap restart
- Ikut: hitungan sisa waktu/candle PantulanPanik-4h & KeltnerBreak-1D yg salah (jatuh ke default 12h) diperbaiki
- Status: deploy 18/11
### rvolbreak8h-purge (8fccc3f, 18/11, Mas Budi: "buang sama sekali tak ada bekas apapun" + riwayat "(b) dihapus permanen")
- Kode RVolBreak-8h DIBUANG total (443 baris): scan live, paper test, gerbang BTC, add-fund reclaim, sizing, hard-stop cap, timeout, near-miss, manual scan, Strategy Control, panel gerbang BTC, Performance, filter Closed Trades, Shadow/Paper panel, thread T-RVolBreak8h
- Migrasi sekali jalan purge_rvolbreak8h_20261118: baris trade strategy=rvolbreak_8h DIHAPUS PERMANEN dari trades_forwardtest.csv (ikut tab PAJAK & salinan Drive), key dihapus dari strategy_config.json & paper_test.json
- Yang tersisa hanya catatan riwayat di komentar kode (tidak tampil di dashboard)
- Status: deploy 18/11
### dashboard-light-theme (bb210a1, 18/11, Mas Budi: "adakan juga dong selain dark theme")
- Tombol "Terang/Gelap" di header dashboard; pilihan tersimpan di browser (localStorage), default tetap gelap
- 18/11 revisi: palet terang terlalu silau -> abu-abu redup (bg #c9ccd1, kartu #d8dadd, teks #262b33), tanpa putih murni
- Tema terang = token warna :root diganti + override warna gelap yg ditulis langsung di beberapa input/baris
- Ikut: baris KeltnerBreak-12h (aturan sekarang) tidak lagi menampilkan rincian fase lama (85 deal) saat di-expand
- Status: deploy 18/11
### kb12h-perf-rules-now (4139456, 17/11, Mas Budi: "B")
- Panel Performance: baris KeltnerBreak-12h kini hanya menghitung deal yg MASIH akan dibuka aturan sekarang (untung & rugi sama2)
- 85 deal LIVE dicek ulang (kline Binance saat open): 10 lolos, 70 terblokir, 5 tanpa jam open (dipertahankan kecuali DASH = bug add fund); 15 deal pra-LIVE dibuang
- Deal yg dibuka sejak patch kb12h-quality-filters (09/11 17:57) otomatis ikut. Lama 69W/31L -30,76% -> baru 12W/2L
- Status: deploy 17/11
### earn-pending-retry (52279a0, 15/11, Mas Budi: "MAU DAN HARUS")
- 15/11 11:05/11:10 redeem Earn $18.32+$8.46 ok, transfer Master->Sub ditolak Binance (-1002) -> $26.78 tertahan di Spot Master
- Bug: coba-ulang hanya jalan saat kas Sub tinggi; sekarang tiap 5 menit + sebelum refill, maks jumlah pending & saldo Spot Master
- Selfcheck mencatat izin key Master (enableInternalTransfer, ipRestrict) utk cari sebab -1002
- Status: deploy 15/11
### pb4h-swing-addfund (05ab90c, 15/11, Mas Budi: "Khusus PantulanPanik-4h, Add fund, strength 1, garis muncul + candle hijau")
- PantulanPanik-4h: add fund 1x base, SEKALI per deal, saat garis support Swing (pivot low kiri 4 / kanan 1) muncul sejak entry + candle 4h tutup HIJAU di atas support, maks 56 jam
- Dicek tiap candle 4h tutup (thread T_PB4H); add lewat execute_add_fund (avg baru, trailing reset, hard stop dari avg)
- Backtest 553 deal 1m: $/deal +0.237 -> +0.378, TRAIN +0.215 -> +0.331, TEST +0.261 -> +0.431, terburuk -$0.61 -> -$1.22
- Deal lama (UNI/WIF/HOME 15/11) ikut: garis muncul 11:00 -> add di candle hijau berikut
- Status: deploy 15/11
### stage2-cap-display (3dc3440, 15/11, Mas Budi: "mau")
- Strategy Control: catatan angka batas gabungan yg BERLAKU (tahap 2: 20 x 4 strategi + 10 = 90 deal, eksposur = saldo wallet)
- Gerbang open pakai helper yg sama (effective_global_deal_cap / effective_global_exposure_cap); perilaku tidak berubah
- Status: deploy 15/11
### scan-retry-btc-data (fc4bc0b, 14/11, Mas Budi: "Boleh")
- Kasus 14/11 15:00: Binance HTTP 500 -> data 4h BTC belum lengkap -> scan PantulanPanik-4h melewati SELURUH candle
- Sekarang: data BTC gagal/belum lengkap -> coba lagi tiap 1 menit, maks 20 menit setelah candle tutup (PB4H & KB1D)
- KB1D loop 300s -> 60s; hasil Supertrend 1D yang gagal tidak di-cache. Aturan entry tidak berubah
- Status: deploy 14/11
### nearmiss-kb1d-pb4h (c65e244, 14/11, Mas Budi: "kenapa gak ditaruh di sini")
- KeltnerBreak-1D & PantulanPanik-4h kini ada di: tab Kandidat Terdekat, Scan Blockers, dropdown Manual Scan (+ detail syarat per koin), panel status gerbang BTC (Strategy Control), heartbeat Telegram
- Status gerbang TrenKonfirmasi-4h diperbarui ke v2 (BTC Supertrend 1D naik)
- Analisis > Size Cut: kartu keterangan KB1D & PantulanPanik-4h "0 deal size-cut" (modal flat, Ekstrem ditolak)
- Pelajaran: patch menus-kb1d-pb4h (14/11) cuma 3 tempat -- strategi baru WAJIB dicek di SEMUA daftar strategi
- Status: deploy 14/11
### kb1d-ext3 (20f7ce3, 14/11, Mas Budi: "YA")
- KB1D: lewati breakout yang close-nya sudah >=3% di atas Keltner atas (aturan sama dgn KB12h)
- Uji 1 menit per deal: +1,36% WR 50% -> +1,83% WR 54%, TRAIN +1,79 / TEST +1,87, 5/5 tahun
- Sim dompet 1,5%/max20 + PantulanPanik-4h: +13,5%/thn turun -7,7% (terburuk -15,8%) -> +14,3%/thn -7,6% (terburuk -14,9%)
- Diuji tapi TIDAK layak: rem modal saat dompet turun, stop deal baru saat turun 6-10%, batas deal baru per 4 jam/hari
- Tahap 2 (15/12): Mas Budi pilih 1,5% + maks 20 deal (bukan 2%, bukan maks 15)
- Status: deploy 14/11
### base6-and-pct-sizing (14/11, Mas Budi: "diturunkan ke $6" + "Boleh")
- Tahap 1 (LIVE): modal KB12h / KB1D / TC4h v2 $8 -> $6 (KB12h Aktif $18); sim dompet $60: turun terdalam -31,7% -> -26,1%
- Tahap 2 (MATI, nyalakan stlh dana masuk 15/12): modal = 1,5% saldo (min $6), max 20 deal per strategi, batas gabungan dilonggarkan
- Nyalakan: PCT_SIZING_ENABLED = True
### Simulasi satu dompet / ukuran modal (13/11, riset)
- 3 strategi live (KB12h, KB1D, TC4h v2), uji 1 menit 2022-2026, modal per deal = % saldo
- Kunci: MAX DEALS, bukan besar modal; slot 3 cuma dapat ~+0,4%/deal (sinyal bergerombol)
- Ralat (bug hitung turun terdalam): 2%/max12-20 turun ~12-14% (terburuk 19-23%); 1,5%/max20 +10,5%/thn, turun 10,7% (terburuk 19,5%), 5/5 tahun positif
- Batas total dana di deal (cap 40-60%) tidak sepadan
- Usulan: 1,5% saldo per deal (KB12h Aktif 3x) + max 20 per strategi (butuh saldo >= $333)
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
