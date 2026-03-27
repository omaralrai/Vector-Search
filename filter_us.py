"""
filter_us.py
------------
Second filter applied after extract.py.
Keeps only providers located in the United States.

Filter:
    Field : Provider Business Practice Location Address Country Code (If outside U.S.)
    Value : "US"

IMPORTANT NOTE:
    In the NPPES dataset, this field is named "If outside U.S." — meaning:
      • US-based providers often leave this field blank/null (implies US by default)
      • Non-US providers fill it with their country code (e.g. "CA", "MX", "GB")
      • Some US providers explicitly write "US" in the field

    This script keeps records where the field is "US" OR null/empty,
    since both represent US-based providers.
    If you want ONLY explicit "US" entries, set INCLUDE_NULL = False below.

Input  : npidata_slim.csv       (output of extract.py)
Output : npidata_slim.csv       (overwritten — US-only rows)

Run:
    python filter_us.py
"""

import pandas as pd

INPUT           = "npidata_slim.csv"
OUTPUT          = "npidata_slim.csv"   # overwrites in place — pipeline continues unchanged
COUNTRY_FIELD   = "Provider Business Practice Location Address Country Code (If outside U.S.)"
TARGET_CODE     = "US"
INCLUDE_NULL    = False   # True = keep blank/null rows too (they imply US)
                          # False = keep ONLY rows with explicit "US" value
CHUNKSIZE       = 50_000

# ---------------------------------------------------------------------------
# Guard: verify the country field exists in the file
# ---------------------------------------------------------------------------
header = list(pd.read_csv(INPUT, nrows=0, dtype=str).columns)
if COUNTRY_FIELD not in header:
    raise ValueError(f"Column not found in file: '{COUNTRY_FIELD}'")

# ---------------------------------------------------------------------------
# Stream, filter, write
# ---------------------------------------------------------------------------
first           = True
total_read      = 0
total_kept      = 0
total_null      = 0   # rows kept because field was null/empty (implies US)
total_explicit  = 0   # rows kept because field was explicitly "US"
total_foreign   = 0   # rows dropped (non-US country code)

print(f"Reading  : {INPUT}")
print(f"Filter   : {COUNTRY_FIELD!r} = {TARGET_CODE!r}")
print(f"Include null/empty (implies US): {INCLUDE_NULL}\n")

for chunk in pd.read_csv(
    INPUT,
    chunksize=CHUNKSIZE,
    dtype=str,
    keep_default_na=False,   # keep empty strings as "" not NaN
):
    total_read += len(chunk)

    # Normalize: strip whitespace + uppercase
    country = chunk[COUNTRY_FIELD].str.strip().str.upper()

    # Classify each row
    is_explicit_us = country.eq(TARGET_CODE)
    is_null        = country.eq("")   # blank = not filled in = implies US

    if INCLUDE_NULL:
        mask = is_explicit_us | is_null
    else:
        mask = is_explicit_us

    total_explicit += is_explicit_us[mask].sum()
    total_null     += is_null[mask].sum()
    total_foreign  += (~mask).sum()

    chunk = chunk[mask]

    if chunk.empty:
        continue

    total_kept += len(chunk)
    chunk.to_csv(OUTPUT, index=False, mode="w" if first else "a", header=first)
    first = False

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
print("── Filter complete ──────────────────────────────────────────")
print(f"  Rows read              : {total_read:>10,}")
print(f"  Rows kept              : {total_kept:>10,}")
print(f"    ↳ Explicit 'US'      : {total_explicit:>10,}")
if INCLUDE_NULL:
    print(f"    ↳ Blank/null (→ US) : {total_null:>10,}")
print(f"  Rows dropped (foreign) : {total_foreign:>10,}")
print(f"  Output written to      : {OUTPUT}")
print("─────────────────────────────────────────────────────────────")
