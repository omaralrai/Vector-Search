import pandas as pd

INPUT  = r"C:\Users\omarb\Desktop\medvertex-files\sprint-2\npidata_pfile_20050523-20260208.csv"
OUTPUT = "npidata_slim.csv"

# ---------------------------------------------------------------------------
# Taxonomy filter — only rows where Taxonomy Code_1 = 103T00000X are kept.
# Slot 2 and above are ignored entirely.
# ---------------------------------------------------------------------------
TAXONOMY_FILTER = "103T00000X"   # Psychologist

# ---------------------------------------------------------------------------
# Columns to keep in the output slim CSV (55 columns)
# Removed : Employer Identification Number (EIN)
#           Parent Organization TIN
#           Healthcare Provider Taxonomy Code_2  (ignored completely)
# Added   : 6 Business Mailing Address fields
# ---------------------------------------------------------------------------
data_keep = [
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
    # ── Mailing Address ────────────────────────────────────────────────────
    "Provider First Line Business Mailing Address",
    "Provider Second Line Business Mailing Address",
    "Provider Business Mailing Address City Name",
    "Provider Business Mailing Address State Name",
    "Provider Business Mailing Address Postal Code",
    "Provider Business Mailing Address Country Code (If outside U.S.)",
    "Provider Business Mailing Address Telephone Number",
    "Provider Business Mailing Address Fax Number",
    # ── Practice Location Address ──────────────────────────────────────────
    "Provider First Line Business Practice Location Address",
    "Provider Second Line Business Practice Location Address",
    "Provider Business Practice Location Address City Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Provider Business Practice Location Address Country Code (If outside U.S.)",
    "Provider Business Practice Location Address Telephone Number",
    "Provider Business Practice Location Address Fax Number",
    # ── Dates & Status ─────────────────────────────────────────────────────
    "Provider Enumeration Date",
    "Last Update Date",
    "NPI Deactivation Reason Code",
    "NPI Deactivation Date",
    "NPI Reactivation Date",
    "Provider Sex Code",
    # ── Authorized Official ────────────────────────────────────────────────
    "Authorized Official Last Name",
    "Authorized Official First Name",
    "Authorized Official Middle Name",
    "Authorized Official Title or Position",
    "Authorized Official Telephone Number",
    "Authorized Official Name Prefix Text",
    "Authorized Official Name Suffix Text",
    "Authorized Official Credential Text",
    # ── Taxonomy & Licenses ────────────────────────────────────────────────
    "Healthcare Provider Taxonomy Code_1",
    "Provider License Number_1",
    "Provider License Number State Code_1",
    "Healthcare Provider Primary Taxonomy Switch_1",
    "Healthcare Provider Taxonomy Code_2",
    "Provider License Number_2",
    # ── Organization Flags ─────────────────────────────────────────────────
    "Is Sole Proprietor",
    "Is Organization Subpart",
    "Parent Organization LBN",
    # ── Certification ──────────────────────────────────────────────────────
    "Certification Date",
]

# --- verify columns exist (reads only header) ---
cols    = list(pd.read_csv(INPUT, nrows=0).columns)
missing = [c for c in data_keep if c not in cols]
if missing:
    raise ValueError("Missing columns:\n- " + "\n- ".join(missing))

# --- stream + filter + write slim file ---
first       = True
total_read  = 0
total_kept  = 0

for chunk in pd.read_csv(
    INPUT,
    chunksize=50_000,
    dtype=str,
    low_memory=False,
    usecols=data_keep,
    keep_default_na=False,
):
    chunk = chunk[data_keep]   # enforce column order
    total_read += len(chunk)

    # ------------------------------------------------------------------
    # Taxonomy filter — ONLY Healthcare Provider Taxonomy Code_1 is checked.
    # Records where Code_1 is anything other than 103T00000X are excluded.
    # ------------------------------------------------------------------
    mask = (
        chunk["Healthcare Provider Taxonomy Code_1"]
        .str.strip()
        .str.upper()
        .eq(TAXONOMY_FILTER.upper())
    )
    chunk = chunk[mask]

    if chunk.empty:
        continue

    total_kept += len(chunk)
    chunk.to_csv(OUTPUT, index=False, mode="w" if first else "a", header=first)
    first = False

print(f"Done: {OUTPUT}")
print(f"  Rows read   : {total_read:>10,}")
print(f"  Rows kept   : {total_kept:>10,}  (Healthcare Provider Taxonomy Code_1 = {TAXONOMY_FILTER})")
print(f"  Rows skipped: {total_read - total_kept:>10,}")
