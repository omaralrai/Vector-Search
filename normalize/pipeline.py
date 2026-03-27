"""
normalize/pipeline.py
---------------------
Streaming pipeline over npidata_slim.csv.

Reads the file in chunks, normalises each chunk, and writes:
    • npidata_normalized.csv   – good rows (with validation_warnings column)
    • npidata_quarantine.csv   – hard-fail rows (with validation_errors column)

Usage (called by normalize_run.py):
    from normalize.pipeline import run
    run("npidata_slim.csv", "npidata_normalized.csv", "npidata_quarantine.csv")
"""

from __future__ import annotations

import time
import pandas as pd

from normalize.normalizer import normalize_chunk, COL_REJECTED

# 55 columns kept from npidata_slim.csv (order preserved)
# Removed : Employer Identification Number (EIN)
#           Parent Organization TIN
#           Healthcare Provider Taxonomy Code_2
# Added   : 6 Business Mailing Address fields
DATA_KEEP = [
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "Provider Middle Name",
    "Provider Name Prefix Text",
    "Provider Name Suffix Text",
    "Provider Credential Text",
    "Provider Other Organization Name",
    "Provider Other Organization Name Type Code",
    "Provider Other Last Name",
    "Provider Other First Name",
    "Provider Other Middle Name",
    "Provider Other Name Prefix Text",
    "Provider Other Name Suffix Text",
    # ── Mailing Address ───────────────────────────────────────────────────
    "Provider First Line Business Mailing Address",
    "Provider Second Line Business Mailing Address",
    "Provider Business Mailing Address City Name",
    "Provider Business Mailing Address State Name",
    "Provider Business Mailing Address Postal Code",
    "Provider Business Mailing Address Country Code (If outside U.S.)",
    "Provider Business Mailing Address Telephone Number",
    "Provider Business Mailing Address Fax Number",
    # ── Practice Location Address ─────────────────────────────────────────
    "Provider First Line Business Practice Location Address",
    "Provider Second Line Business Practice Location Address",
    "Provider Business Practice Location Address City Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Provider Business Practice Location Address Country Code (If outside U.S.)",
    "Provider Business Practice Location Address Telephone Number",
    "Provider Business Practice Location Address Fax Number",
    # ── Dates & Status ────────────────────────────────────────────────────
    "Provider Enumeration Date",
    "Last Update Date",
    "NPI Deactivation Reason Code",
    "NPI Deactivation Date",
    "NPI Reactivation Date",
    "Provider Sex Code",
    # ── Authorized Official ───────────────────────────────────────────────
    "Authorized Official Last Name",
    "Authorized Official First Name",
    "Authorized Official Middle Name",
    "Authorized Official Title or Position",
    "Authorized Official Telephone Number",
    "Authorized Official Name Prefix Text",
    "Authorized Official Name Suffix Text",
    "Authorized Official Credential Text",
    # ── Taxonomy & Licenses ───────────────────────────────────────────────
    "Healthcare Provider Taxonomy Code_1",
    "Provider License Number_1",
    "Provider License Number State Code_1",
    "Healthcare Provider Primary Taxonomy Switch_1",
    "Healthcare Provider Taxonomy Code_2",
    "Provider License Number_2",
    # ── Organization Flags ────────────────────────────────────────────────
    "Is Sole Proprietor",
    "Is Organization Subpart",
    "Parent Organization LBN",
    # ── Certification ─────────────────────────────────────────────────────
    "Certification Date",
]


def _write_chunk(df: pd.DataFrame, path: str, first: bool) -> None:
    """Append (or create) a CSV file with one chunk."""
    if df.empty:
        return
    out = df.drop(columns=[COL_REJECTED], errors="ignore")
    out.to_csv(path, index=False, mode="w" if first else "a", header=first)


def run(
    input_path: str = "npidata_slim.csv",
    output_path: str = "npidata_normalized.csv",
    quarantine_path: str = "npidata_quarantine.csv",
    chunksize: int = 50_000,
) -> None:
    """
    Stream npidata_slim.csv through the normalization pipeline.

    Parameters
    ----------
    input_path      : path to the slim CSV produced by extract.py
    output_path     : destination for cleaned rows
    quarantine_path : destination for hard-fail rows
    chunksize       : number of rows per chunk (tune for available RAM)
    """
    # ---------------------------------------------------------------
    # Guard: verify expected columns are present (reads header only)
    # ---------------------------------------------------------------
    header_cols = list(pd.read_csv(input_path, nrows=0, dtype=str).columns)
    missing = [c for c in DATA_KEEP if c not in header_cols]
    if missing:
        raise ValueError(
            "Input file is missing expected columns:\n  - " + "\n  - ".join(missing)
        )

    # ---------------------------------------------------------------
    # Streaming loop
    # ---------------------------------------------------------------
    good_first       = True
    quarantine_first = True

    total_good       = 0
    total_quarantine = 0
    chunk_num        = 0

    start = time.time()

    reader = pd.read_csv(
        input_path,
        chunksize=chunksize,
        dtype=str,
        low_memory=False,
        usecols=DATA_KEEP,
        keep_default_na=False,
    )

    for chunk in reader:
        chunk_num += 1

        chunk = chunk[DATA_KEEP]   # enforce column order

        good_df, quarantine_df = normalize_chunk(chunk)

        _write_chunk(good_df, output_path, good_first)
        _write_chunk(quarantine_df, quarantine_path, quarantine_first)

        good_first       = good_first and good_df.empty
        quarantine_first = quarantine_first and quarantine_df.empty

        total_good       += len(good_df)
        total_quarantine += len(quarantine_df)

        # Progress report every 10 chunks
        if chunk_num % 10 == 0:
            elapsed = time.time() - start
            rows_done = total_good + total_quarantine
            rate = rows_done / elapsed if elapsed > 0 else 0
            print(
                f"  chunk {chunk_num:>5} | "
                f"good={total_good:>9,} | "
                f"quarantine={total_quarantine:>7,} | "
                f"{rate:,.0f} rows/s"
            )

    # ---------------------------------------------------------------
    # Final summary
    # ---------------------------------------------------------------
    elapsed = time.time() - start
    total   = total_good + total_quarantine
    print("\n── Normalization complete ──────────────────────────────")
    print(f"  Total rows processed : {total:>10,}")
    print(f"  Good rows written    : {total_good:>10,}  → {output_path}")
    print(f"  Quarantined rows     : {total_quarantine:>10,}  → {quarantine_path}")
    print(f"  Elapsed              : {elapsed:>10.1f}s")
    print(f"  Throughput           : {total / elapsed:>9,.0f} rows/s" if elapsed else "")
    print("────────────────────────────────────────────────────────")
