"""
vector_search_query.py
----------------------
Runs a semantic vector search against npiDB → Psychologist using the
Atlas Vector Search index ("vector_index").

Usage
-----
python vector_search_query.py

Edit the QUERY and optional STATE_FILTER below to test different searches.
The connection string is loaded from the .env file (key: MONGODB_URI).
"""

from __future__ import annotations

import os
from typing import Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Configuration — edit these to test different searches
# ---------------------------------------------------------------------------
QUERY        = "male individual psychologist PH.D. sole proprietor Cuyahoga Falls Ohio"   # plain English query
STATE_FILTER = None          # e.g. "NY" to restrict to one state, or None for all states
TOP_K        = 10            # how many results to return
NUM_CANDIDATES = 150         # how many candidates ANN considers before picking TOP_K
                             # higher = more accurate, slightly slower (100-200 is typical)

DB_NAME          = "npiDB"
COLLECTION_NAME  = "Psychologist"
INDEX_NAME       = "vector_index"
MODEL_NAME       = "all-MiniLM-L6-v2"

# ---------------------------------------------------------------------------
# Connect
# ---------------------------------------------------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")
if not MONGO_URI:
    raise EnvironmentError("MONGODB_URI not set in .env")

client     = MongoClient(MONGO_URI)
collection = client[DB_NAME][COLLECTION_NAME]

# ---------------------------------------------------------------------------
# Load model and embed the query
# ---------------------------------------------------------------------------
print(f"Loading model: {MODEL_NAME}")
model       = SentenceTransformer(MODEL_NAME)
query_vector = model.encode(QUERY).tolist()
print(f"Query   : \"{QUERY}\"")
if STATE_FILTER:
    print(f"Filter  : State = {STATE_FILTER}")
print(f"Top K   : {TOP_K}\n")

# ---------------------------------------------------------------------------
# Build the $vectorSearch pipeline
# ---------------------------------------------------------------------------
vector_search_stage: dict = {
    "$vectorSearch": {
        "index":         INDEX_NAME,
        "path":          "embedding",
        "queryVector":   query_vector,
        "numCandidates": NUM_CANDIDATES,
        "limit":         TOP_K,
    }
}

# Add pre-filter only if a state was specified
if STATE_FILTER:
    vector_search_stage["$vectorSearch"]["filter"] = {
        "Provider Business Practice Location Address State Name": STATE_FILTER
    }

project_stage: dict = {
    "$project": {
        "_id":                    0,
        "NPI":                    1,
        "Provider First Name":    1,
        "Provider Last Name (Legal Name)": 1,
        "Provider Credential Text": 1,
        "Provider Business Practice Location Address City Name":  1,
        "Provider Business Practice Location Address State Name": 1,
        "Provider Sex Code":      1,
        "score": {"$meta": "vectorSearchScore"},
    }
}

pipeline = [vector_search_stage, project_stage]

# ---------------------------------------------------------------------------
# Run and display results
# ---------------------------------------------------------------------------
results = list(collection.aggregate(pipeline))

if not results:
    print("No results returned. Check that the index status is READY.")
else:
    print(f"{'#':<4} {'Score':<8} {'Name':<30} {'Credential':<12} {'City':<20} {'State':<6} {'Sex'}")
    print("-" * 95)
    for i, doc in enumerate(results, 1):
        first      = doc.get("Provider First Name") or ""
        last       = doc.get("Provider Last Name (Legal Name)") or ""
        name       = f"{first} {last}".strip()
        credential = doc.get("Provider Credential Text") or ""
        city       = doc.get("Provider Business Practice Location Address City Name") or ""
        state      = doc.get("Provider Business Practice Location Address State Name") or ""
        sex        = doc.get("Provider Sex Code") or ""
        score      = doc.get("score", 0)
        print(f"{i:<4} {score:<8.4f} {name:<30} {credential:<12} {city:<20} {state:<6} {sex}")

client.close()
