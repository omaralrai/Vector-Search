"""
normalize_run.py
----------------
Top-level entry point for the Psychologist NPI normalization pipeline.
Run from the 'Ingest NPI data' directory:

    python normalize_run.py

Outputs
-------
    npidata_normalized.csv   — cleaned rows + validation_warnings column
    npidata_quarantine.csv   — hard-fail rows + validation_errors column

Note
----
This script must be run AFTER extract.py, which produces npidata_slim.csv
containing only Psychologist records (taxonomy code 103T00000X).
"""

from normalize.pipeline import run

if __name__ == "__main__":
    run(
        input_path="npidata_slim.csv",
        output_path="npidata_normalized.csv",
        quarantine_path="npidata_quarantine.csv",
        chunksize=50_000,
    )
