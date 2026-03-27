"""
mongo_insert.py
---------------
Reads the normalized Psychologist NPI CSV (output of normalize_run.py) and
inserts every row into MongoDB in batches.

Configuration
-------------
BATCH_SIZE      : how many rows per insert_many call
FILE_PATH       : path to the normalized CSV
DB_NAME         : MongoDB database name
COLLECTION_NAME : MongoDB collection name

The connection string is loaded from the .env file (key: MONGODB_URI).

Do NOT run this file before running normalize_run.py first.
"""

from __future__ import annotations

import json
import os

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BATCH_SIZE           = 10_000                    # rows per insert_many call (larger = faster)
CLEAR_BEFORE_INSERT  = True                      # drop collection before inserting (safe re-run)
FILE_PATH            = "npidata_normalized.csv"  # output from normalize_run.py
DB_NAME              = "npiDB"
COLLECTION_NAME      = "Psychologist"

# ---------------------------------------------------------------------------
# Step 1 – Load environment variables from .env
# ---------------------------------------------------------------------------
load_dotenv()                                # reads .env into os.environ
MONGO_URI = os.getenv("MONGODB_URI")        # pulls the connection string

if not MONGO_URI:
    raise EnvironmentError(
        "MONGODB_URI is not set. "
        "Make sure your .env file contains: MONGODB_URI=<your connection string>"
    )

# ---------------------------------------------------------------------------
# Step 2 – Connect to MongoDB
# ---------------------------------------------------------------------------
client     = MongoClient(MONGO_URI)
db         = client[DB_NAME]
collection = db[COLLECTION_NAME]

print(f"Connected to MongoDB | db={DB_NAME!r} | collection={COLLECTION_NAME!r}")

# Optionally drop the collection before inserting (clean re-run)
if CLEAR_BEFORE_INSERT:
    collection.drop()
    print(f"Collection {COLLECTION_NAME!r} dropped for clean re-run.")

# Ensure NPI is unique — creates the index if it doesn't exist yet, safe to run repeatedly
collection.create_index("NPI", unique=True)
print("Unique index on 'NPI' ensured.")

# ---------------------------------------------------------------------------
# Helper: clean a single record before inserting into Mongo
# ---------------------------------------------------------------------------

def clean_record(record: dict) -> dict:
    """
    Prepare one row (originally a dict from pandas) for MongoDB insertion.

    1. validation_errors / validation_warnings are stored as JSON strings in
       the CSV; parse them back into Python lists so Mongo stores them as arrays.
    2. Any cell that pandas kept as the string "nan", or slipped through as "",
       is converted to None (Mongo null).

    Parameters
    ----------
    record : dict
        A single row from the CSV as a Python dict.

    Returns
    -------
    dict
        The same dict with lists for the validation columns and None for empties.
    """
    # Parse JSON array columns back into Python lists
    for col in ("validation_errors", "validation_warnings"):
        raw = record.get(col)
        if isinstance(raw, str):
            try:
                record[col] = json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                record[col] = []
        elif raw is None:
            record[col] = []

    # Drop validation columns — they live in the CSVs, not needed in MongoDB
    record.pop("validation_errors", None)
    record.pop("validation_warnings", None)

    # Replace residual empty / nan strings with None (MongoDB null)
    for key, val in record.items():
        if val in ("", "nan", "NaN", "None"):
            record[key] = None

    return record


# ---------------------------------------------------------------------------
# Step 3 – Stream CSV and insert in batches
# ---------------------------------------------------------------------------
total_inserted  = 0
total_skipped   = 0
chunk_num       = 0

print(f"Reading: {FILE_PATH}")
print(f"Batch size: {BATCH_SIZE:,} rows per insert_many call\n")

reader = pd.read_csv(
    FILE_PATH,
    chunksize=BATCH_SIZE,      # read exactly BATCH_SIZE rows at a time
    dtype=str,                 # keep everything as string; we manage types ourselves
    keep_default_na=False,     # don't let pandas silently turn "" into NaN
)

for chunk in reader:
    chunk_num += 1

    # Convert the chunk to a list of plain Python dicts
    records = chunk.to_dict(orient="records")

    # Clean each record
    records = [clean_record(r) for r in records]

    if not records:
        continue

    # Insert the batch into MongoDB
    try:
        result = collection.insert_many(records, ordered=False)
        inserted_count = len(result.inserted_ids)
        total_inserted += inserted_count
    except BulkWriteError as bwe:
        #   ordered=False lets Mongo continue even if some docs fail.
        #   BulkWriteError still gets raised; we extract the write result details.
        inserted_count  = bwe.details.get("nInserted", 0)
        total_inserted += inserted_count
        skipped         = len(records) - inserted_count
        total_skipped  += skipped
        print(
            f"  [WARN] Batch {chunk_num}: {skipped} doc(s) failed "
            f"({bwe.details.get('writeErrors', [])[0].get('errmsg', 'unknown error')})"
        )

    # Progress report every 100 batches (= 1,000,000 rows by default)
    if chunk_num % 100 == 0:
        print(
            f"  Batch {chunk_num:>6,} | "
            f"inserted so far: {total_inserted:>10,} | "
            f"skipped: {total_skipped:>6,}"
        )

# ---------------------------------------------------------------------------
# Step 4 – Final summary and close connection
# ---------------------------------------------------------------------------
print("\n── Insertion complete ──────────────────────────────────────")
print(f"  Total rows inserted  : {total_inserted:>10,}  → {COLLECTION_NAME}")
print(f"  Total rows skipped   : {total_skipped:>10,}")
print(f"  MongoDB database     : {DB_NAME}")
print("────────────────────────────────────────────────────────────")

client.close()   # release the network connection
print("Connection closed.")
