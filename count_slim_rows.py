"""
count_slim_rows.py
------------------
Compares the row counts of:
  1. The current npidata_slim.csv  (Ingest NPI data — Psychologists only)
  2. The original npidata_slim.csv (sprint-2 — all specialties)

Run:
    python count_slim_rows.py
"""

import pandas as pd

FILES = {
    "Current  (Ingest NPI data — Psychologists only)": r"C:\Users\omarb\Desktop\medvertex-files\Ingest NPI data\npidata_slim.csv",
    "Original (sprint-2       — all specialties)    ": r"C:\Users\omarb\Desktop\medvertex-files\sprint-2\npidata_slim.csv",
}

print("── Counting rows in npidata_slim.csv files ─────────────────\n")

results = {}
for label, path in FILES.items():
    try:
        count = sum(1 for _ in open(path, encoding="utf-8")) - 1  # subtract header row
        results[label] = count
        print(f"  {label}")
        print(f"  Path  : {path}")
        print(f"  Rows  : {count:>10,}\n")
    except FileNotFoundError:
        print(f"  {label}")
        print(f"  Path  : {path}")
        print(f"  ⚠  File not found — has this stage been run yet?\n")
        results[label] = None

print("── Summary ──────────────────────────────────────────────────")
vals = [v for v in results.values() if v is not None]
if len(vals) == 2:
    current, original = vals
    print(f"  Current slim rows  : {current:>10,}")
    print(f"  Original slim rows : {original:>10,}")
    print(f"  Difference         : {original - current:>10,}  rows exist in sprint-2 but not here")
print("─────────────────────────────────────────────────────────────")
