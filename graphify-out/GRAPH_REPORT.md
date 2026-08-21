# Graph Report - .  (2026-08-21)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 881 nodes · 2156 edges · 58 communities (52 shown, 6 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 28 edges (avg confidence: 0.58)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Community 0
- Community 1
- Community 2
- Community 3
- Community 4
- Community 5
- Community 6
- Community 7
- Community 8
- Community 9
- Community 10
- Community 11
- Community 12
- Community 13
- Community 14
- Community 15
- Community 16
- Community 17
- Community 18
- Community 19
- Community 20
- Community 21
- Community 22
- Community 23
- Community 24
- Community 25
- Community 26
- Community 27
- Community 28
- Community 29
- Community 30
- Community 31
- Community 32
- Community 33
- Community 34
- Community 35
- Community 36
- Community 37
- Community 38
- Community 39
- Community 40
- Community 41
- Community 42
- Community 43
- Community 44
- Community 45
- Community 46
- Community 47
- Community 48
- Community 49
- Community 50
- Community 51
- Community 52
- Community 53
- Community 54
- Community 55
- Community 56
- Community 57

## God Nodes (most connected - your core abstractions)
1. `log()` - 86 edges
2. `log()` - 65 edges
3. `thread1_scan()` - 39 edges
4. `thread1_scan()` - 37 edges
5. `now_wib()` - 35 edges
6. `thread1d_scan_4h()` - 35 edges
7. `thread1d_scan_4h()` - 34 edges
8. `thread2_monitor()` - 30 edges
9. `now_wib()` - 30 edges
10. `thread1c_scan_intrabar()` - 28 edges

## Surprising Connections (you probably didn't know these)
- `main()` --calls--> `fmt_wf()`  [INFERRED]
  backtest_arm_sweep.py → investigasi_skor4.py
- `main()` --calls--> `fmt_wf()`  [INFERRED]
  backtest_entry_combo_final.py → investigasi_skor4.py
- `main()` --calls--> `fmt_wf()`  [INFERRED]
  backtest_entry_filter2.py → investigasi_skor4.py
- `main()` --calls--> `fmt_wf()`  [INFERRED]
  backtest_reversal_params.py → investigasi_skor4.py
- `main()` --calls--> `fmt_wf()`  [INFERRED]
  backtest_volume_filter.py → investigasi_skor4.py

## Import Cycles
- None detected.

## Communities (58 total, 6 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.10
Nodes (58): active_deal_count_hunting(), add_to_active_deals(), ai_decision_near_timeout(), _binance_get(), check_hunting_strategy(), check_strat6_4h(), compute_indicators_4h(), _csv_ensure_header() (+50 more)

### Community 1 - "Community 1"
Cohesion: 0.06
Nodes (51): active_deal_count_akum(), check_entry_4h(), commas_creds(), compute_indicators_htf(), _convert(), _csv_ensure_header(), csv_log_close(), _deal_log_ensure_header() (+43 more)

### Community 2 - "Community 2"
Cohesion: 0.18
Nodes (12): active_deal_count_akum(), compute_indicators_htf(), get_deal_override(), get_open_indicators(), load_deal_overrides(), ============================================================= BINANCE SCREENER…, Hitung EMA50 dan MACD hist untuk HTF dataframe., Jumlah deal aktif strategi akumulasi entry A/B. (+4 more)

### Community 3 - "Community 3"
Cohesion: 0.11
Nodes (44): active_deal_count_hunting(), add_to_active_deals(), check_hunting_strategy(), check_strat6_4h(), compute_indicators_4h(), csv_log_open(), _fmt_price(), format_near_miss() (+36 more)

### Community 4 - "Community 4"
Cohesion: 0.06
Nodes (42): _ai_call(), ai_decision_armed(), ai_decision_close(), ai_decision_open(), detect_entry_a_spring(), detect_entry_b_breakout(), fetch_htf_context_for_ai(), get_binance_open_orders_value() (+34 more)

### Community 5 - "Community 5"
Cohesion: 0.10
Nodes (38): active_deal_count(), btc_filter_ok(), compute_indicators(), cooldown_remaining(), deal_count_by_strategy(), deal_log_write(), _get_htf_values(), get_ohlcv() (+30 more)

### Community 6 - "Community 6"
Cohesion: 0.11
Nodes (37): active_deal_count(), btc_filter_ok(), compute_indicators(), cooldown_remaining(), deal_count_by_strategy(), deal_log_write(), _get_htf_values(), get_ohlcv() (+29 more)

### Community 7 - "Community 7"
Cohesion: 0.10
Nodes (28): backtest_symbol(), compute_indicators(), entry_ok_base(), fetch_klines(), fetch_universe(), is_choppy(), load_data(), main() (+20 more)

### Community 8 - "Community 8"
Cohesion: 0.07
Nodes (31): compute_indicators_akum(), detect_entry_a_spring(), detect_entry_b_breakout(), drive_append(), _drive_get_or_create_file_id(), _drive_read(), _drive_write(), _get_drive_service() (+23 more)

### Community 9 - "Community 9"
Cohesion: 0.13
Nodes (25): active_deal_count_4h(), csv_last_close(), csv_progress(), heartbeat_4h_tick(), heartbeat_crossema_tick(), heartbeat_general_tick(), heartbeat_rev_tick(), heartbeat_tick() (+17 more)

### Community 10 - "Community 10"
Cohesion: 0.13
Nodes (23): binance_buy_market(), _binance_format_qty(), binance_get_asset_qty(), binance_sell_market(), _binance_trading_request(), commas_creds(), Kirim webhook ke 3Commas dgn retry utk koneksi/timeout (maks 3x). HTTP 4xx dari…, Helper HMAC-signed request ke Binance trading API menggunakan… (+15 more)

### Community 11 - "Community 11"
Cohesion: 0.21
Nodes (20): backtest_symbol(), build_perf_data(), calc_perf_score(), check_entry(), compute_indicators(), fetch_klines(), fetch_universe(), get_close_at_or_before() (+12 more)

### Community 12 - "Community 12"
Cohesion: 0.15
Nodes (21): active_deal_count_4h(), csv_progress(), heartbeat_crossema_tick(), heartbeat_general_tick(), heartbeat_rev_tick(), heartbeat_tick(), next_scheduled_heartbeat_wib(), Jumlah deal aktif strategi brkX2_4h. (+13 more)

### Community 13 - "Community 13"
Cohesion: 0.21
Nodes (19): backtest_symbol(), build_perf_data(), calc_perf_score_custom(), check_entry(), compute_indicators(), fetch_klines(), fetch_universe(), get_script_dir() (+11 more)

### Community 14 - "Community 14"
Cohesion: 0.21
Nodes (18): backtest_symbol(), base_trailing_dist(), compute_indicators(), entry_ok(), fetch_klines(), fetch_universe(), get_script_dir(), is_choppy() (+10 more)

### Community 15 - "Community 15"
Cohesion: 0.21
Nodes (16): backtest_symbol(), base_filters_ok(), compute_indicators(), get_arm_pct(), get_trail_dist(), get_trail_progressive(), load_pkl(), main() (+8 more)

### Community 16 - "Community 16"
Cohesion: 0.26
Nodes (15): backtest_symbol(), compute_indicators(), entry_ok(), fetch_klines(), fetch_universe(), get_arm_pct(), get_script_dir(), get_trail_dist() (+7 more)

### Community 17 - "Community 17"
Cohesion: 0.26
Nodes (15): backtest_symbol(), base_trailing_dist(), compute_indicators(), entry_ok(), fetch_klines(), fetch_universe(), get_arm_pct(), get_script_dir() (+7 more)

### Community 18 - "Community 18"
Cohesion: 0.26
Nodes (15): backtest_symbol(), compute_indicators(), entry_ok(), fetch_klines(), fetch_universe(), get_script_dir(), is_choppy(), load_symbol() (+7 more)

### Community 19 - "Community 19"
Cohesion: 0.28
Nodes (14): backtest_symbol(), base_trailing_dist(), compute_indicators(), entry_ok(), fetch_klines(), fetch_universe(), get_script_dir(), is_choppy() (+6 more)

### Community 20 - "Community 20"
Cohesion: 0.28
Nodes (14): backtest_symbol(), base_trailing_dist(), compute_indicators(), entry_ok(), fetch_klines(), fetch_universe(), get_script_dir(), is_choppy() (+6 more)

### Community 21 - "Community 21"
Cohesion: 0.30
Nodes (13): backtest_symbol(), base_trailing_dist(), check_reversal_setup(), compute_indicators_8h(), fetch_klines_8h(), fetch_universe(), get_script_dir(), load_data_8h() (+5 more)

### Community 22 - "Community 22"
Cohesion: 0.24
Nodes (10): check_entry(), compute_ind(), get_arm_pct(), get_base_trail(), get_trail(), load_pkl(), load_sym_for_strat(), backtest_arm_sweep_all.py Sweep ARM_PCT_LOW x ARM_PCT_HIGH untuk semua 4… (+2 more)

### Community 23 - "Community 23"
Cohesion: 0.27
Nodes (12): add_indicators_12h(), add_indicators_4h(), build_htf_map(), check_entry_4h(), fetch_klines(), fetch_universe(), main(), backtest_elapsed_sweep_brkx2_4h.py ==================================== Sweep… (+4 more)

### Community 24 - "Community 24"
Cohesion: 0.24
Nodes (10): check_entry(), compute_ind(), get_arm_pct(), get_base_trail(), get_trail(), load_pkl(), load_sym_for_strat(), backtest_prog_trail_sweep_all.py Sweep PROG_TRAIL_THRESHOLD x PROG_TRAIL_REDUCE… (+2 more)

### Community 25 - "Community 25"
Cohesion: 0.29
Nodes (12): add_indicators(), check_entry(), detect_retest(), fetch_klines(), fetch_universe(), main(), DataFrame, backtest_retest_arm_sweep.py ============================ Tujuan: Bandingkan… (+4 more)

### Community 26 - "Community 26"
Cohesion: 0.33
Nodes (12): add_indicators(), check_hunting_entry(), detect_retest(), fetch_klines(), fetch_universe(), main(), DataFrame, backtest_retest_hunting_sweep.py ================================= Tujuan:… (+4 more)

### Community 27 - "Community 27"
Cohesion: 0.24
Nodes (12): add_indicators(), base_trail_dist(), check_entry(), get_trail_factor(), load_symbols(), main(), backtest_trailing_factor_sweep.py ================================== Sweep…, Simulasi backtest dengan trailing factor variatif. (+4 more)

### Community 28 - "Community 28"
Cohesion: 0.30
Nodes (11): add_indicators_12h(), add_indicators_4h(), build_htf_map(), check_crossema_entry(), fetch_klines(), fetch_universe(), main(), backtest_elapsed_sweep_crossema_4h.py =======================================… (+3 more)

### Community 29 - "Community 29"
Cohesion: 0.24
Nodes (11): add_indicators(), check_entry(), load_symbols(), main(), backtest_hunting_sweep.py ========================= Sweep 3 parameter…, Hitung metrik dari list trades., Tambah EMA20, EMA50, ATR%, price_change ke dataframe., Return True kalau candle r memenuhi syarat entry hunting. (+3 more)

### Community 30 - "Community 30"
Cohesion: 0.30
Nodes (11): backtest_symbol(), compute_indicators(), fixed_filters_ok(), get_arm_pct(), get_trail_dist(), get_trail_progressive(), load_pkl(), main() (+3 more)

### Community 31 - "Community 31"
Cohesion: 0.26
Nodes (9): compute_indicators(), get_arm_pct(), get_base_trail(), get_trail(), load_pkl(), load_sym(), backtest_prog_trail_sweep.py Sweep kombinasi PROG_TRAIL_THRESHOLD dan…, resample_to_12h() (+1 more)

### Community 32 - "Community 32"
Cohesion: 0.18
Nodes (12): check_entry(), check_entry_reversal(), _cross_up(), entry_detail(), entry_detail_reversal(), is_choppy(), True kalau pair choppy (rata-rata body/range < CHOPPY_BODY_RANGE_MIN selama N…, Evaluasi pada candle TERTUTUP terakhir (mode a). Update 30/07/2026: hapus… (+4 more)

### Community 33 - "Community 33"
Cohesion: 0.17
Nodes (12): _binance_get(), calc_perf_score(), get_ohlcv_htf(), _get_perf_data_1d(), htf_filter_4h_ok(), htf_vol_ratio(), Load data 1D untuk symbol. Cari dari cache lokal dulu, kalau tidak ada fetch…, Hitung performance score untuk symbol pada timestamp query_ts_ms. Return float… (+4 more)

### Community 34 - "Community 34"
Cohesion: 0.18
Nodes (12): check_entry(), check_entry_reversal(), _cross_up(), entry_detail(), entry_detail_reversal(), is_choppy(), True kalau pair choppy (rata-rata body/range < CHOPPY_BODY_RANGE_MIN selama N…, Evaluasi pada candle TERTUTUP terakhir (mode a). Update 30/07/2026: hapus… (+4 more)

### Community 35 - "Community 35"
Cohesion: 0.35
Nodes (10): add_indicators(), check_hunting_entry(), fetch_klines(), fetch_universe(), main(), DataFrame, backtest_hunting_filter_sweep.py ================================= Sweep filter…, Baseline hunting selalu dicek dulu. filter_mask: tuple bool len=7, True =… (+2 more)

### Community 36 - "Community 36"
Cohesion: 0.20
Nodes (11): get_strategy_base_usd(), is_bstock_symbol(), is_nyse_open(), is_strategy_enabled(), load_strategy_config(), open_deal_with_sizing(), Return True jika symbol adalah tokenized stock (bStock Binance)., Return True jika NYSE sedang buka (Senin–Jumat, 21:30–04:00 WIB). Tidak… (+3 more)

### Community 37 - "Community 37"
Cohesion: 0.29
Nodes (10): build_dashboard(), check_signal(), compute_indicators(), fetch_data(), get_market_status(), main(), Check all signal conditions. Returns dict with details or None if conditions…, Cek apakah NSE sedang buka (09:15 - 15:30 IST, Senin-Jumat). (+2 more)

### Community 38 - "Community 38"
Cohesion: 0.29
Nodes (6): check_akumulasi(), compute_ind(), detect_entry_a(), detect_entry_b(), backtest_akum_trailing_sweep.py Sweep ARM_PCT x TRAILING_DIST khusus untuk…, simulate()

### Community 39 - "Community 39"
Cohesion: 0.42
Nodes (8): add_indicators(), check_entry(), fetch_klines(), fetch_universe(), main(), backtest_brkx2_4h_comprehensive_sweep.py…, run_combo(), simulate()

### Community 40 - "Community 40"
Cohesion: 0.39
Nodes (7): add_indicators(), agg(), load_pkl(), main(), backtest_crossema_sweep2.py ============================ Sweep 2 parameter…, require_st_minus1=True → baseline (kondisi lama, ST=-1 wajib)…, simulate()

### Community 41 - "Community 41"
Cohesion: 0.52
Nodes (6): add_indicators(), load_pkl(), main(), DataFrame, backtest_crossema_window_sweep.py ================================== Sweep…, simulate()

### Community 42 - "Community 42"
Cohesion: 0.48
Nodes (6): add_indicators(), load_pkl(), main(), backtest_hunting_gap_chg_sweep.py =================================== Sweep dua…, simulate(), trailing_dist()

### Community 44 - "Community 44"
Cohesion: 0.40
Nodes (4): fetch_klines(), get_top_symbols(), Ambil symbol USDT spot dari Binance, filter by volume, urutkan terbesar., Download n candle 4h untuk satu symbol.

### Community 45 - "Community 45"
Cohesion: 0.50
Nodes (4): calc_perf_score(), _get_perf_data_1d(), Load data 1D untuk symbol. Cari dari cache lokal dulu, kalau tidak ada fetch…, Hitung performance score untuk symbol pada timestamp query_ts_ms. Return float…

### Community 46 - "Community 46"
Cohesion: 0.50
Nodes (4): get_estimated_locked_usd(), has_enough_balance_for_hunting(), Estimasi USDT yang terkunci di semua active deals (base order saja,…, Return True kalau estimasi saldo masih cukup untuk open deal senilai…

### Community 47 - "Community 47"
Cohesion: 0.29
Nodes (7): drive_append(), _drive_get_or_create_file_id(), _drive_read(), _drive_write(), _get_drive_service(), Cari file ID — hanya search, tidak buat baru (service account tidak punya…, Append new_text ke file Drive. Thread-safe.

### Community 48 - "Community 48"
Cohesion: 0.40
Nodes (6): _convert(), load_active_deals(), Saat startup, baca base_usd dari file persisten /data/deal_base_usd.json dan…, remove_from_active_deals(), save_active_deals(), sync_base_usd_from_binance()

### Community 49 - "Community 49"
Cohesion: 0.50
Nodes (4): compute_indicators_akum(), Hitung indikator khusus Akumulasi Detector pada dataframe 4h., Hitung skor akumulasi untuk 1 symbol. Return dict dengan detail atau None kalau…, score_akumulasi()

### Community 50 - "Community 50"
Cohesion: 0.50
Nodes (4): compute_indicators_reversal(), heikin_ashi_bullish(), Return Series bool: HA_close > HA_open (HA bullish) tiap candle., Indikator utk strategi reversal (EMA20/50, ATR%, doji body ratio, HA bullish).

### Community 51 - "Community 51"
Cohesion: 0.50
Nodes (4): get_estimated_locked_usd(), has_enough_balance_for_hunting(), Estimasi USDT yang terkunci di semua active deals (base order saja,…, Return True kalau estimasi saldo masih cukup untuk open deal senilai…

### Community 52 - "Community 52"
Cohesion: 0.50
Nodes (4): compute_indicators_reversal(), heikin_ashi_bullish(), Return Series bool: HA_close > HA_open (HA bullish) tiap candle., Indikator utk strategi reversal (EMA20/50, ATR%, doji body ratio, HA bullish).

## Knowledge Gaps
- **6 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `fmt_wf()` connect `Community 7` to `Community 16`, `Community 17`, `Community 19`, `Community 20`, `Community 21`?**
  _High betweenness centrality (0.014) - this node is a cross-community bridge._
- **Why does `log()` connect `Community 4` to `Community 0`, `Community 2`, `Community 36`, `Community 5`, `Community 9`, `Community 10`, `Community 47`, `Community 48`, `Community 49`, `Community 51`?**
  _High betweenness centrality (0.012) - this node is a cross-community bridge._
- **Why does `log()` connect `Community 8` to `Community 1`, `Community 33`, `Community 3`, `Community 6`, `Community 12`, `Community 46`?**
  _High betweenness centrality (0.006) - this node is a cross-community bridge._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.09679370840895342 - nodes in this community are weakly interconnected._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.06334841628959276 - nodes in this community are weakly interconnected._
- **Should `Community 3` be split into smaller, more focused modules?**
  _Cohesion score 0.11205073995771671 - nodes in this community are weakly interconnected._
- **Should `Community 4` be split into smaller, more focused modules?**
  _Cohesion score 0.05574912891986063 - nodes in this community are weakly interconnected._