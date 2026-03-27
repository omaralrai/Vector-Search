"""
count_psychologists.py
----------------------
Scans the full NPPES source file and counts every row that contains
taxonomy code 103T00000X in ANY of the 15 taxonomy code slots.

This is used to verify whether the extract.py output (which only checks
slots 1 and 2) captured all Psychologist records or missed some.

Run:
    python count_psychologists.py
"""

import pandas as pd

INPUT           = r"C:\Users\omarb\Desktop\medvertex-files\sprint-2\npidata_pfile_20050523-20260208.csv"
TAXONOMY_FILTER = "103T00000X"   # Psychologist
CHUNKSIZE       = 50_000

# All 15 taxonomy code columns in the NPPES file
TAXONOMY_COLS = [f"Healthcare Provider Taxonomy Code_{i}" for i in range(1, 16)]

# ---------------------------------------------------------------------------
# Read the header to find which taxonomy columns actually exist in the file
# ---------------------------------------------------------------------------
all_cols      = list(pd.read_csv(INPUT, nrows=0).columns)
present_cols  = [c for c in TAXONOMY_COLS if c in all_cols]
missing_cols  = [c for c in TAXONOMY_COLS if c not in all_cols]

print(f"Taxonomy columns found in file : {len(present_cols)} / 15")
if missing_cols:
    print(f"  Missing slots               : {missing_cols}")
print(f"Scanning for taxonomy code     : {TAXONOMY_FILTER}\n")

# ---------------------------------------------------------------------------
# Stream through the full file and count matches
# ---------------------------------------------------------------------------
total_rows      = 0
matched_rows    = 0
slot_counts     = {col: 0 for col in present_cols}   # count per slot

for chunk in pd.read_csv(
    INPUT,
    chunksize=CHUNKSIZE,
    dtype=str,
    low_memory=False,
    usecols=present_cols,
    keep_default_na=False,
):
    total_rows += len(chunk)

    # Normalise each taxonomy column: strip whitespace + uppercase
    for col in present_cols:
        chunk[col] = chunk[col].str.strip().str.upper()

    # Build a mask: True if ANY slot matches the taxonomy code
    mask = chunk[present_cols].eq(TAXONOMY_FILTER).any(axis=1)
    matched_rows += mask.sum()

    # Count per-slot hits (for the breakdown report)
    for col in present_cols:
        slot_counts[col] += chunk[col].eq(TAXONOMY_FILTER).sum()

    # Progress every 1M rows
    if total_rows % 1_000_000 == 0:
        print(f"  Scanned {total_rows:>10,} rows so far... matched {matched_rows:,}")

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
print("\n── Count complete ──────────────────────────────────────────")
print(f"  Total rows in file           : {total_rows:>10,}")
print(f"  Rows with {TAXONOMY_FILTER}  : {matched_rows:>10,}")
print(f"  Rows without                 : {total_rows - matched_rows:>10,}")
print("\n  Breakdown by taxonomy slot:")
for col, count in slot_counts.items():
    if count > 0:
        slot_num = col.split("_")[-1]
        print(f"    Slot {slot_num:>2} : {count:>8,}")
print("────────────────────────────────────────────────────────────")
print()
print("NOTE: extract.py only checks slots 1 and 2.")
if slot_counts:
    slots_1_2 = slot_counts.get("Healthcare Provider Taxonomy Code_1", 0) + \
                slot_counts.get("Healthcare Provider Taxonomy Code_2", 0)
    print(f"  Rows captured by extract.py  : {slots_1_2:>10,}  (slots 1 + 2, may overlap)")
    print(f"  True total (all 15 slots)    : {matched_rows:>10,}")
    if matched_rows > slots_1_2:
        print(f"\n  ⚠  extract.py may have missed some rows — consider expanding to all slots.")
    else:
        print(f"\n  ✓  All Psychologist rows appear in slots 1 or 2 — extract.py captured them all.")
