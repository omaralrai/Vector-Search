# NPI Psychologist Ingestion Pipeline

A focused ETL pipeline that extracts, validates, normalizes, and loads **Psychologist** provider records from the NPPES (National Plan and Provider Enumeration System) public dataset into MongoDB.

---

## What It Does

This pipeline filters the full NPPES dataset (11 GB, 7M+ providers) down to only records with taxonomy code **`103T00000X` (Psychologist)**, validates and cleans the data, then inserts it into MongoDB.

```
Raw NPPES CSV (11 GB)
        ↓  extract.py          → filters to Psychologists only
npidata_slim.csv
        ↓  normalize_run.py    → cleans, validates, splits good vs bad rows
npidata_normalized.csv  +  npidata_quarantine.csv
        ↓  mongo_insert.py     → loads into MongoDB
MongoDB: BaseDB → Psychologist collection
```

---

## Project Structure

```
Ingest NPI data/
├── extract.py              # Stage 1: Filter + extract Psychologist rows
├── normalize_run.py        # Stage 2: Entry point for normalization
├── mongo_insert.py         # Stage 3: Load normalized data into MongoDB
├── .env                    # MongoDB connection string (do not share publicly)
├── README.md               # This file
│
└── normalize/              # Normalization package
    ├── __init__.py         # Package marker
    ├── pipeline.py         # Streaming pipeline orchestrator
    ├── normalizer.py       # Applies transformations to each chunk
    ├── validators.py       # Field-by-field validation rules (52 fields)
    └── fields.py           # Shared helpers: trim, phone, date, zip, etc.
```

### Generated Files (created when you run the pipeline)

| File | Created By | Description |
|---|---|---|
| `npidata_slim.csv` | `extract.py` | Psychologist-only rows, 52 columns |
| `npidata_normalized.csv` | `normalize_run.py` | Cleaned rows ready for MongoDB |
| `npidata_quarantine.csv` | `normalize_run.py` | Hard-failed rows (bad NPI / entity type) |

---

## Taxonomy Filter

| Field | Value |
|---|---|
| Taxonomy Code | `103T00000X` |
| Specialty | Psychologist |
| Filter Logic | Row is kept if the code appears in **Taxonomy Slot 1 OR Slot 2** |

Only records matching this taxonomy code will be extracted, processed, and inserted. All other specialties are ignored at the earliest possible stage (`extract.py`).

---

## Database Configuration

| Setting | Value |
|---|---|
| Database | `BaseDB` |
| Collection | `Psychologist` |
| Unique Index | `NPI` field |
| Connection | Loaded from `.env` → `MONGODB_URI` |

---

## Requirements

### Python Libraries

Install with pip (or activate the existing `sprint-2` virtual environment):

```bash
pip install pandas pymongo python-dotenv
```

| Library | Purpose |
|---|---|
| `pandas` | Reads and processes the CSV in chunks |
| `pymongo` | MongoDB client for inserting records |
| `python-dotenv` | Loads `MONGODB_URI` from the `.env` file |

All other imports (`json`, `re`, `os`, `time`, `datetime`) are Python built-ins.

### Environment File

The `.env` file must contain your MongoDB connection string:

```
MONGODB_URI="mongodb+srv://<user>:<password>@<cluster>.mongodb.net/"
```

---

## How to Run

Open a terminal in this directory and run the three stages **in order**:

### Stage 1 — Extract
```bash
python extract.py
```
Reads the raw 11 GB NPPES file from `sprint-2`, filters for Psychologist rows, and writes `npidata_slim.csv` to this directory.

**Expected output:**
```
Done: npidata_slim.csv
  Rows read   :  7,500,000
  Rows kept   :     45,000  (taxonomy = 103T00000X)
  Rows skipped:  7,455,000
```

---

### Stage 2 — Normalize & Validate
```bash
python normalize_run.py
```
Streams `npidata_slim.csv` through the validation pipeline. Produces two output files:
- `npidata_normalized.csv` — clean rows (kept, with any soft warnings noted)
- `npidata_quarantine.csv` — hard-failed rows (missing/invalid NPI or Entity Type Code)

**Expected output:**
```
  chunk    10 | good=   48,500 | quarantine=    200 | 12,000 rows/s
  ...
── Normalization complete ──────────────────────────────
  Total rows processed :     48,700
  Good rows written    :     48,500  → npidata_normalized.csv
  Quarantined rows     :        200  → npidata_quarantine.csv
  Elapsed              :       12.5s
────────────────────────────────────────────────────────
```

---

### Stage 3 — Load into MongoDB
```bash
python mongo_insert.py
```
Reads `npidata_normalized.csv` in 10,000-row batches and inserts into `BaseDB.Psychologist`.

> ⚠️ This will **drop and recreate** the `Psychologist` collection on each run for a clean re-insert. To disable this behaviour, set `CLEAR_BEFORE_INSERT = False` in `mongo_insert.py`.

**Expected output:**
```
Connected to MongoDB | db='BaseDB' | collection='Psychologist'
Collection 'Psychologist' dropped for clean re-run.
Unique index on 'NPI' ensured.
Reading: npidata_normalized.csv
Batch size: 10,000 rows per insert_many call

── Insertion complete ──────────────────────────────────────
  Total rows inserted  :     48,500  → Psychologist
  Total rows skipped   :          0
  MongoDB database     : BaseDB
────────────────────────────────────────────────────────────
Connection closed.
```

---

## Validation Rules Summary

The pipeline applies **52 field-level rules** during Stage 2.

### Hard Failures → Row Quarantined
Rows with any of these issues are moved to `npidata_quarantine.csv` and never inserted into MongoDB:

| Field | Rule |
|---|---|
| `NPI` | Must be present and exactly 10 digits |
| `Entity Type Code` | Must be `"1"` (individual) or `"2"` (organization) |

### Soft Failures → Row Kept with Warning
All other issues are recorded in `validation_warnings` but the row is still inserted:

- Field length violations (e.g. name > 35 chars)
- Invalid phone/fax format
- Invalid date format (expected `MM/DD/YYYY`)
- Invalid ZIP code, country code, sex code
- Cross-field checks (e.g. deactivation date without a reason code)
- Masked EIN/TIN values (automatically set to `null`)

---

## Data Source

| Field | Value |
|---|---|
| Source | NPPES Public Data Dissemination |
| File | `npidata_pfile_20050523-20260208.csv` |
| Location | `sprint-2/` directory (referenced directly — not copied) |
| Date Range | May 2005 – February 2026 |
| Full Size | ~11 GB / 340+ columns / 7M+ rows |
| After Filter | ~52 columns, Psychologist rows only |

---

## Processing Configuration

| Setting | Value | Configurable In |
|---|---|---|
| Extract chunk size | 50,000 rows | `extract.py` |
| Normalize chunk size | 50,000 rows | `normalize_run.py` |
| MongoDB batch size | 10,000 rows | `mongo_insert.py` |
| Drop collection on re-run | `True` | `mongo_insert.py` → `CLEAR_BEFORE_INSERT` |
