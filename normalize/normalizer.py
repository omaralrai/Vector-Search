"""
normalize/normalizer.py
-----------------------
Applies the full normalization pipeline to a single pandas DataFrame chunk.

Usage (called by pipeline.py):
    from normalize.normalizer import normalize_chunk
    good_df, quarantine_df = normalize_chunk(chunk)
"""

from __future__ import annotations

import json
import pandas as pd

from normalize.fields import trim
from normalize.validators import validate_row

# Column names added by the normalizer
COL_ERRORS   = "validation_errors"
COL_WARNINGS = "validation_warnings"
COL_REJECTED = "_rejected"


def normalize_chunk(chunk: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Normalize and validate one chunk of the slim CSV.

    Steps
    -----
    1. Global pass: trim every string cell (handles invisible chars, leading /
       trailing spaces, and null sentinels).  This mutates the chunk's values
       before per-row validation so that max-length checks measure real content.
    2. Per-row validation via validate_row(), which applies field-specific rules
       and returns (normalised_row, errors, warnings).
    3. Rows with hard errors are marked _rejected=True and collected into a
       separate quarantine DataFrame.
    4. Both good and quarantine DataFrames carry validation_errors and
       validation_warnings columns (JSON arrays as strings for CSV portability).

    Parameters
    ----------
    chunk : pd.DataFrame
        A slice of npidata_slim.csv read with dtype=str.

    Returns
    -------
    good_df       : rows that passed or have only soft warnings
    quarantine_df : rows that failed hard validation (bad NPI / entity type)
    """

    # ------------------------------------------------------------------
    # Step 1 – Global trim pass
    # Every column is already str (enforced at read time by pipeline.py).
    # Replace pandas NA/NaN with None so our helpers see Python None.
    # ------------------------------------------------------------------
    # Convert NaN → None for the whole chunk at once
    chunk = chunk.where(chunk.notna(), other=None)
    # pandas sometimes stores missing values as NaN (a float).
    # This replaces every NaN with Python None so our string helpers don't crash.

    # Apply trim() to every cell via applymap / map (pandas ≥ 2.1 uses map)
    try:
        chunk = chunk.map(trim)             # pandas ≥ 2.1
    except AttributeError:
        chunk = chunk.applymap(trim)        # pandas < 2.1

    # ------------------------------------------------------------------
    # Step 2 – Per-row validation
    # ------------------------------------------------------------------
    good_rows: list[dict]       = []
    quarantine_rows: list[dict] = []

    for _, row in chunk.iterrows(): # yields one row at a time as a pandas Series.
        raw_dict = row.to_dict() # convert the row to a dictionary
        normalised, errors, warnings = validate_row(raw_dict) # validate the row

        # Attach audit columns
        normalised[COL_ERRORS]   = json.dumps(errors)
        normalised[COL_WARNINGS] = json.dumps(warnings)
        normalised[COL_REJECTED] = bool(errors)

        if errors:
            quarantine_rows.append(normalised)
        else:
            good_rows.append(normalised)

    # ------------------------------------------------------------------
    # Step 3 – Reconstruct DataFrames
    # ------------------------------------------------------------------
    good_df       = pd.DataFrame(good_rows)       if good_rows       else pd.DataFrame()
    quarantine_df = pd.DataFrame(quarantine_rows) if quarantine_rows else pd.DataFrame()

    #Converts the Python lists back into DataFrames so
    #pipeline.py
    # can write them to CSV.
    #Returns empty DataFrames (not None) if a category had zero rows — safe to write.

    return good_df, quarantine_df
