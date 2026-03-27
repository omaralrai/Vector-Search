# How to Run — NPI Psychologist Ingestion Pipeline

extract.py — filter by taxonomy code 103T00000X
filter_us.py — keep US-only providers
normalize_run.py — validate and clean
mongo_insert.py — load into npiDB → Psychologist

## Overview

The pipeline has **4 steps** that must be run **in order**. Each step produces a file that the next step reads.

```
extract.py → filter_us.py → normalize_run.py → mongo_insert.py
```

---

## Step 1 — Extract

**Script:** `extract.py`

**What it does:**
Reads the full 11 GB NPPES source file and extracts only providers where `Healthcare Provider Taxonomy Code_1 = 103T00000X` (Psychologist). Writes the filtered results to `npidata_slim.csv`.

**Run:**
```bash
python extract.py
```

**Expected output:**
```
Done: npidata_slim.csv
  Rows read   :  9,368,082
  Rows kept   :     56,454  (Healthcare Provider Taxonomy Code_1 = 103T00000X)
  Rows skipped:  9,311,628
```

---

## Step 2 — Filter US

**Script:** `filter_us.py`

**What it does:**
Reads `npidata_slim.csv` and keeps only US-based providers using the field:
`Provider Business Practice Location Address Country Code (If outside U.S.)`

A provider is considered US-based if:
- The field is **blank/null** — NPPES leaves it empty for US providers by design
- The field is explicitly **"US"**

Any row with a foreign country code (e.g. `CA`, `MX`, `GB`) is dropped.
The output **overwrites** `npidata_slim.csv` in place so the rest of the pipeline continues unchanged.

**Run:**
```bash
python filter_us.py
```

**Expected output:**
```
── Filter complete ──────────────────────────────────────────
  Rows read              :     56,454
  Rows kept              :     XX,XXX
    ↳ Explicit 'US'      :     XX,XXX
    ↳ Blank/null (→ US)  :     XX,XXX
  Rows dropped (foreign) :        XXX
  Output written to      : npidata_slim.csv
─────────────────────────────────────────────────────────────
```

---

## Step 3 — Normalize & Validate

**Script:** `normalize_run.py`

**What it does:**
Reads `npidata_slim.csv` (US Psychologists only at this point) and runs every row through the validation pipeline. Produces two output files:
- `npidata_normalized.csv` — clean rows ready for MongoDB
- `npidata_quarantine.csv` — hard-failed rows (missing/invalid NPI or Entity Type Code)

**Run:**
```bash
python normalize_run.py
```

**Expected output:**
```
── Normalization complete ──────────────────────────────
  Total rows processed :     XX,XXX
  Good rows written    :     XX,XXX  → npidata_normalized.csv
  Quarantined rows     :          0  → npidata_quarantine.csv
  Elapsed              :        X.Xs
  Throughput           :     X,XXX rows/s
────────────────────────────────────────────────────────
```

---

## Step 4 — Load into MongoDB

**Script:** `mongo_insert.py`

**What it does:**
Reads `npidata_normalized.csv` in batches of 10,000 rows and inserts them into MongoDB.

| Setting | Value |
|---|---|
| Database | `npiDB` |
| Collection | `Psychologist` |
| Batch size | 10,000 rows |
| On re-run | Drops and recreates the collection (clean insert) |

**Run:**
```bash
python mongo_insert.py
```

**Expected output:**
```
Connected to MongoDB | db='npiDB' | collection='Psychologist'
Collection 'Psychologist' dropped for clean re-run.
Unique index on 'NPI' ensured.

── Insertion complete ──────────────────────────────────────
  Total rows inserted  :     XX,XXX  → Psychologist
  Total rows skipped   :          0
  MongoDB database     : npiDB
────────────────────────────────────────────────────────────
Connection closed.
```

---

## Full Run (All Steps)

```bash
python extract.py
python filter_us.py
python normalize_run.py
python mongo_insert.py
```

---

## Files Produced

| File | Produced By | Description |
|---|---|---|
| `npidata_slim.csv` | `extract.py` then `filter_us.py` | Psychologist + US-only rows |
| `npidata_normalized.csv` | `normalize_run.py` | Clean rows ready for MongoDB |
| `npidata_quarantine.csv` | `normalize_run.py` | Hard-failed rows (not inserted) |

---

## Requirements

Activate the virtual environment before running:
```bash
& "C:\Users\omarb\Desktop\medvertex-files\Ingest NPI data\.venv\Scripts\Activate.ps1"
```

Or install libraries if running without a venv:
```bash
pip install pandas pymongo python-dotenv
```
