"""
generate_embeddings.py
----------------------
Reads Psychologist documents from MongoDB (npiDB → Psychologist), generates
a 384-dimension vector embedding for each one using the free, locally-run
all-MiniLM-L6-v2 model, and writes the embedding back to the same document.

Run AFTER mongo_insert.py has finished loading data into MongoDB.

No API key required. The model downloads once (~90 MB) and runs locally.

Configuration
-------------
BATCH_SIZE      : how many documents to pull from Mongo and embed at once
DB_NAME         : MongoDB database name
COLLECTION_NAME : MongoDB collection name
EMBEDDING_FIELD : the field name that will store the vector on each document

The connection string is loaded from the .env file (key: MONGODB_URI).

Re-run safety
-------------
Documents that already have an embedding field are skipped automatically.
This means the script is safe to re-run — it picks up where it left off.
"""

from __future__ import annotations

import os
from typing import Optional

from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BATCH_SIZE       = 500            # documents per encode + bulk-write cycle
DB_NAME          = "npiDB"
COLLECTION_NAME  = "Psychologist"
EMBEDDING_FIELD  = "embedding"    # field name written to each Mongo document
MODEL_NAME       = "all-MiniLM-L6-v2"   # free, local — 384 dimensions

# ---------------------------------------------------------------------------
# Step 1 – Load environment and connect to MongoDB
# ---------------------------------------------------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

if not MONGO_URI:
    raise EnvironmentError(
        "MONGODB_URI is not set. "
        "Make sure your .env file contains: MONGODB_URI=<your connection string>"
    )

client     = MongoClient(MONGO_URI)
db         = client[DB_NAME]
collection: Collection = db[COLLECTION_NAME]

print(f"Connected to MongoDB | db={DB_NAME!r} | collection={COLLECTION_NAME!r}")

# ---------------------------------------------------------------------------
# Step 2 – Load the embedding model (downloads once, cached locally after)
# ---------------------------------------------------------------------------
print(f"\nLoading model: {MODEL_NAME}")
print("(First run will download ~90 MB — subsequent runs use local cache)\n")
model = SentenceTransformer(MODEL_NAME)
print(f"Model loaded. Output dimensions: {model.get_sentence_embedding_dimension()}\n")

# ---------------------------------------------------------------------------
# Helper: build the text string we will embed for a single document
# ---------------------------------------------------------------------------

# Maps raw code values to human-readable labels for better semantic quality
_ENTITY_TYPE_MAP  = {"1": "Individual", "2": "Organization"}
_SEX_CODE_MAP     = {"M": "Male", "F": "Female"}

def build_text(doc: dict) -> str:
    """
    Build a single text string from the 12 highlighted fields:

      Entity Type Code, Provider First Name, Provider Middle Name,
      Provider Credential Text, Provider Other Organization Name,
      Practice City, Practice State, Practice Postal Code,
      Provider Sex Code, License State Code 1,
      Healthcare Provider Taxonomy Code 2, Is Sole Proprietor

    Raw codes (Entity Type, Sex Code, Is Sole Proprietor) are mapped to
    readable labels so the model captures their meaning semantically.
    None / empty values are silently skipped.
    """
    def _val(key: str) -> Optional[str]:
        v = doc.get(key)
        if v in (None, "None", "nan", "NaN", "", "N/A"):
            return None
        return str(v).strip()

    entity_type  = _ENTITY_TYPE_MAP.get(_val("Entity Type Code") or "", None)
    sex          = _SEX_CODE_MAP.get(_val("Provider Sex Code") or "", _val("Provider Sex Code"))
    sole_prop    = "Sole Proprietor" if _val("Is Sole Proprietor") == "Y" else None

    parts = [
        entity_type,
        _val("Provider First Name"),
        _val("Provider Middle Name"),
        _val("Provider Credential Text"),
        _val("Provider Other Organization Name"),
        _val("Provider Business Practice Location Address City Name"),
        _val("Provider Business Practice Location Address State Name"),
        _val("Provider Business Practice Location Address Postal Code"),
        sex,
        _val("Provider License Number State Code_1"),
        _val("Healthcare Provider Taxonomy Code_2"),
        sole_prop,
    ]

    return " ".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Step 3 – Count pending work
# ---------------------------------------------------------------------------
total_docs    = collection.count_documents({})
already_done  = collection.count_documents({EMBEDDING_FIELD: {"$exists": True}})
pending       = total_docs - already_done

print(f"Total documents   : {total_docs:>10,}")
print(f"Already embedded  : {already_done:>10,}")
print(f"Pending           : {pending:>10,}\n")

if pending == 0:
    print("All documents already have embeddings. Nothing to do.")
    client.close()
    exit(0)

# ---------------------------------------------------------------------------
# Step 4 – Stream, embed, and write back in batches
# ---------------------------------------------------------------------------
cursor = collection.find(
    {EMBEDDING_FIELD: {"$exists": False}},   # skip docs already embedded
    projection={                             # only the 12 highlighted fields + _id
        "_id": 1,
        "Entity Type Code": 1,
        "Provider First Name": 1,
        "Provider Middle Name": 1,
        "Provider Credential Text": 1,
        "Provider Other Organization Name": 1,
        "Provider Business Practice Location Address City Name": 1,
        "Provider Business Practice Location Address State Name": 1,
        "Provider Business Practice Location Address Postal Code": 1,
        "Provider Sex Code": 1,
        "Provider License Number State Code_1": 1,
        "Healthcare Provider Taxonomy Code_2": 1,
        "Is Sole Proprietor": 1,
    }
)

total_embedded = 0
batch_num      = 0

batch_docs: list[dict] = []

def flush_batch(docs: list[dict]) -> int:
    """Encode a batch of documents and bulk-write embeddings back to MongoDB."""
    texts   = [build_text(d) for d in docs]
    vectors = model.encode(texts, batch_size=len(texts), show_progress_bar=False)

    operations = [
        UpdateOne(
            {"_id": d["_id"]},
            {"$set": {EMBEDDING_FIELD: v.tolist()}}
        )
        for d, v in zip(docs, vectors)
    ]

    result = collection.bulk_write(operations, ordered=False)
    return result.modified_count


for doc in cursor:
    batch_docs.append(doc)

    if len(batch_docs) >= BATCH_SIZE:
        batch_num += 1
        count       = flush_batch(batch_docs)
        total_embedded += count
        batch_docs  = []

        if batch_num % 10 == 0:
            print(
                f"  Batch {batch_num:>6,} | "
                f"embedded so far: {total_embedded:>8,} / {pending:,}"
            )

# Flush any remaining docs that didn't fill a full batch
if batch_docs:
    batch_num      += 1
    count           = flush_batch(batch_docs)
    total_embedded += count

# ---------------------------------------------------------------------------
# Step 5 – Summary
# ---------------------------------------------------------------------------
print("\n── Embedding complete ──────────────────────────────────────")
print(f"  Model used        : {MODEL_NAME}")
print(f"  Dimensions        : {model.get_sentence_embedding_dimension()}")
print(f"  Documents embedded: {total_embedded:>10,}")
print(f"  Field written     : '{EMBEDDING_FIELD}'")
print(f"  MongoDB database  : {DB_NAME}")
print(f"  Collection        : {COLLECTION_NAME}")
print("────────────────────────────────────────────────────────────")
print("\nNext step: create the Atlas Vector Search index.")
print("See the guide for the exact index definition to use in Atlas UI.")

client.close()
print("Connection closed.")
