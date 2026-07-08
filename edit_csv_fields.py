#!/usr/bin/env python3
"""
edit_csv_fields.py
Jalankan di Railway console:
  python3 /tmp/edit_csv_fields.py

Edit yang dilakukan:
  1. Tambah 'score' ke CSV_FIELDS
  2. Tambah 'score': score di csv_log_open brkX2
  3. Tambah 'score': 0 di csv_log_open reversal
"""
import ast, shutil, os

src = '/app/binance_screener.py'
bak = '/app/binance_screener_backup_score.py'

shutil.copy2(src, bak)
print(f"[OK] Backup: {bak}")

with open(src, 'r', encoding='utf-8') as f:
    code = f.read()

orig_lines = len(code.splitlines())
errors = []

# 1. CSV_FIELDS: tambah 'score' setelah 'base_usd'
M1_old = "    'trail_dist_pct','base_usd',\n    'close_time_wib'"
M1_new = "    'trail_dist_pct','base_usd','score',\n    'close_time_wib'"
if M1_old in code:
    code = code.replace(M1_old, M1_new, 1)
    print("[OK] CSV_FIELDS updated: tambah 'score'")
else:
    errors.append("Marker CSV_FIELDS tidak ditemukan")
    print("WARN: Marker CSV_FIELDS tidak ditemukan, coba alternatif...")
    M1_old2 = "    'trail_dist_pct','base_usd',"
    M1_new2 = "    'trail_dist_pct','base_usd','score',"
    if M1_old2 in code:
        code = code.replace(M1_old2, M1_new2, 1)
        print("[OK] CSV_FIELDS updated (alternatif)")
        errors.clear()

# 2. csv_log_open brkX2: tambah 'score' sebelum 'strategy': 'brkX2'
# Marker: baris terakhir csv_log_open brkX2 sebelum })
M2_old = "                'base_usd': BASE_ORDER_VOLUME,\n                'strategy': 'brkX2',\n            })"
M2_new = "                'base_usd': BASE_ORDER_VOLUME,\n                'score': score,\n                'strategy': 'brkX2',\n            })"
if M2_old in code:
    code = code.replace(M2_old, M2_new, 1)
    print("[OK] csv_log_open brkX2 updated: tambah 'score': score")
else:
    errors.append("Marker csv_log_open brkX2 tidak ditemukan")
    print("WARN: Marker csv_log_open brkX2 tidak ditemukan")

# 3. csv_log_open reversal: tambah 'score' sebelum 'strategy': 'reversal'
M3_old = "                'base_usd': BASE_ORDER_VOLUME,\n                'strategy': 'reversal',\n            })"
M3_new = "                'base_usd': BASE_ORDER_VOLUME,\n                'score': 0,\n                'strategy': 'reversal',\n            })"
if M3_old in code:
    code = code.replace(M3_old, M3_new, 1)
    print("[OK] csv_log_open reversal updated: tambah 'score': 0")
else:
    errors.append("Marker csv_log_open reversal tidak ditemukan")
    print("WARN: Marker csv_log_open reversal tidak ditemukan")

# Verifikasi syntax
print("\nVerifikasi syntax Python...")
try:
    ast.parse(code)
    print("[OK] Syntax valid")
except SyntaxError as e:
    print(f"ERROR SYNTAX baris {e.lineno}: {e.msg}")
    print("ROLLBACK ke backup...")
    shutil.copy2(bak, src)
    exit(1)

if errors:
    print(f"\nWARN: {len(errors)} marker tidak ditemukan:")
    for e in errors: print(f"  - {e}")
    print("File TIDAK diubah (rollback).")
    shutil.copy2(bak, src)
    exit(1)

with open(src, 'w', encoding='utf-8') as f:
    f.write(code)

new_lines = len(code.splitlines())
print(f"\nBaris: {orig_lines} -> {new_lines} (+{new_lines-orig_lines})")
print("\n=== SELESAI ===")
print("Restart Railway agar perubahan aktif.")
print(f"Backup: {bak}")
