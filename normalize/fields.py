"""
normalize/fields.py
-------------------
Global helpers that are applied to every field BEFORE any field-specific
validation.  Import these wherever you need them; keep this module
side-effect-free (no I/O, no pandas imports).
"""

from __future__ import annotations #This line enables newer Python type-hint syntax in older Python versions.

import re #is Python's built-in Regular Expressions library.
from datetime import datetime #is a module that provides classes for working with dates and times.
from typing import Optional, Tuple #is a module that provides classes for working with types.

# ---------------------------------------------------------------------------
# Sentinel values that should be treated as null
# ---------------------------------------------------------------------------


#set is like a list but without duplicates
_NULL_SENTINELS: set[str] = {
    "NULL",
    "N/A",
    "NA",
    "NONE",
    "NAN",
    "",
    "UNAVAIL",        # NPPES-specific: field was not available at filing time
    "UNAVAILABLE",    # longer variant sometimes seen
    "<UNAVAIL>",      # angle-bracket variant seen in NPPES exports
    "<UNAVAILABLE>",  # angle-bracket long variant
    "SUPPRESSED",     # used for EIN/TIN suppression in some exports
}


# ---------------------------------------------------------------------------
# 1. Null coercion
# ---------------------------------------------------------------------------

def coerce_null(value: Optional[str]) -> Optional[str]: #The function coerce_null takes a value that may be a string or None and returns None if the value represents a fake null like "NULL" or "N/A", otherwise it returns the original value unchanged.
    """
    Return None if the value is a known null sentinel (case-insensitive),
    otherwise return the value unchanged.
    Call this *after* trimming so that " " collapses to "" first.
    """
    if value is None:
        return None
    if value.upper() in _NULL_SENTINELS:
        return None
    return value


# ---------------------------------------------------------------------------
# 2. Trim + null-coerce in one step (apply to every field)
# ---------------------------------------------------------------------------



def trim(value: Optional[str]) -> Optional[str]:
    """
    1. If value is None → return None.
    2. Remove all leading/trailing whitespace.
    3. Collapse any internal whitespace (spaces, tabs, newlines, etc.)
       into a single regular space.
    4. Run coerce_null on the cleaned result.
    """
    if value is None:
        return None

    # Guard: pandas can leak float NaN even after the where() pass
    if not isinstance(value, str):
        return None

    # Split on any whitespace and rejoin with single spaces
    value = " ".join(value.split()) #i have written this function instead of value.replace("\t", " ").replace("\r", " ").replace("\n", " ") because it is more efficient and it does the same thing.

    return coerce_null(value)


# collapse_spaces() was removed — trim() already collapses internal
# whitespace via " ".join(value.split()), so a separate function is redundant.


# ---------------------------------------------------------------------------
# 4. Digit-only extraction
# ---------------------------------------------------------------------------

#r"" → raw string (so backslashes are interpreted correctly)
#\D means "not a digit

def digits_only(value: Optional[str]) -> Optional[str]:
    """
    Remove every character that is not 0-9.
    Returns None if the result is an empty string or value is None.
    """
    if value is None:
        return None
    result = re.sub(r"\D", "", value) #re.sub(pattern, replacement, string) means: Replace everything matching pattern with replacement.
    return result if result else None
    #If result is not empty → return it
    #If result is empty → return None

# ---------------------------------------------------------------------------
# 5. Phone / fax normalisation
# ---------------------------------------------------------------------------

# Keep only digits and a leading '+' sign
_PHONE_KEEP = re.compile(r"[^\d+]")
# Break the pattern:
# [] → character set
# ^ inside brackets means "NOT"
# \d → digits (0–9)
# + → literal plus sign
# [^\d+] means: Match any character that is NOT a digit or a plus sign

def normalize_phone(value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Normalise a phone or fax number.

    Returns
    -------
    (normalised_value, warning_message | None)

    Normalisation:
        • Strip everything except digits and a leading '+'.
        • If the cleaned value is empty → return (None, None).

    Warning conditions (row is never rejected):
        • Fewer than 7 digits  → suspiciously short
        • More than 15 digits  → suspiciously long (E.164 max is 15)
    """
    if value is None:
        return None, None

    cleaned = _PHONE_KEEP.sub("", value) #re.sub(pattern, replacement, string) means: Replace everything matching pattern with replacement.
    if not cleaned:
        return None, None #If cleaned is empty → return (None, None)

    digit_count = len(re.sub(r"\D", "", cleaned))

    warning: Optional[str] = None #Initialize warning as None
    if digit_count < 7: #If digit_count is less than 7 → set warning to "Phone/fax '{value}' has fewer than 7 digits after normalisation"
        warning = f"Phone/fax '{value}' has fewer than 7 digits after normalisation"
    elif digit_count > 15: #If digit_count is greater than 15 → set warning to "Phone/fax '{value}' has more than 15 digits after normalisation"
        warning = f"Phone/fax '{value}' has more than 15 digits after normalisation"

    return cleaned, warning


# ---------------------------------------------------------------------------
# 6. Date parsing (MM/DD/YYYY → YYYY-MM-DD ISO string)
# ---------------------------------------------------------------------------

_DATE_FORMAT = "%m/%d/%Y"


def normalize_date(
    value: Optional[str], #Optional[str] means that the value can be a string or None
    field_name: str = "Date", #default value is "Date"
) -> Tuple[Optional[str], Optional[str]]: #retuns tuple of two values: (iso_date_string | None, error_message | None)
    """
    Parse a date string from MM/DD/YYYY format.

    Returns
    -------
    (iso_date_string | None, error_message | None)

    If the value is None or empty → (None, None)   — not an error.
    If the value cannot be parsed → (None, error_message).
    """
    if value is None:
        return None, None

    try:
        dt = datetime.strptime(value.strip(), _DATE_FORMAT) #datetime.strptime(date_string, format) is used to convert a date string to a datetime object.
        return dt.strftime("%Y-%m-%d"), None #dt.strftime("%Y-%m-%d") is used to convert a datetime object to a date string in the format YYYY-MM-DD.
        #it becomes a string

    except ValueError:
        return None, f"'{field_name}' has invalid date format '{value}' (expected MM/DD/YYYY)"

#It means the function returns two values: the cleaned ISO date (or None if missing/invalid) and an error message (or None if there was no error).

# ---------------------------------------------------------------------------
# 7. EIN / TIN masking detection
# ---------------------------------------------------------------------------


#re.compile() takes a regular expression pattern and turns it into a reusable "pattern object" that Python can use to search, match, or replace text efficiently without rebuilding the pattern every time.

# NPPES sometimes publishes masked/suppressed EIN/TIN values
_MASKED_PATTERNS = re.compile(
    r"^\*+$"           # all asterisks
    r"|^X+$"           # all X's
    r"|\*",            # any asterisk anywhere → treat whole value as masked
    re.IGNORECASE,
)


def is_masked(value: Optional[str]) -> bool:
    """Return True if the value looks like a masked/suppressed identifier."""
    if value is None:
        return False
    return bool(_MASKED_PATTERNS.search(value))


# ---------------------------------------------------------------------------
# 8. ISO-2 country code check
# ---------------------------------------------------------------------------
#ISO-2 refers to the international standard two-letter country codes (like US, CA, GB) defined by ISO 3166-1.
def looks_like_iso2(value: Optional[str]) -> bool:
    """Return True if value is exactly 2 ASCII letters (ISO 3166-1 alpha-2)."""
    if value is None:
        return False
    return bool(re.fullmatch(r"[A-Za-z]{2}", value))


# ---------------------------------------------------------------------------
# 9. US ZIP code pattern check
# ---------------------------------------------------------------------------
#A ZIP code is the U.S. postal code used to identify a specific geographic delivery area (like 12345 or 12345-6789).
_US_ZIP = re.compile(r"^\d{5}(-\d{4}|\d{4})?$")  # accepts: 12345 | 12345-6789 | 123456789 (NPPES ZIP+4)


def looks_like_us_zip(value: Optional[str]) -> bool:
    """Return True if value matches 12345 or 12345-6789 US ZIP format."""
    if value is None:
        return False
    return bool(_US_ZIP.match(value))



# match() checks if the pattern matches at the beginning of the string.
# fullmatch() checks if the pattern matches the entire string from start to end.

# Example:

# re.match(r"\d{3}", "123abc")      → matches (because it starts with 123)
# re.fullmatch(r"\d{3}", "123abc")  → no match (because extra characters exist)



# re.compile() transforms a regular expression string into a regex object
# This is useful for performance because the pattern is compiled only once, even if used multiple times.
