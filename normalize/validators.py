"""
normalize/validators.py
-----------------------
Field-by-field validation and normalisation.

Public API
----------
    validate_row(row: dict) -> tuple[dict, list[str], list[str]]

Returns a 3-tuple:
    • normalised_row  – dict with cleaned / converted values
    • errors          – hard-fail issues (NPI, Entity Type Code)
    • warnings        – soft-fail issues (everything else)

Hard-fail rows should be quarantined; soft-fail rows are kept but annotated.

Changes from sprint-2 version:
    Removed : Employer Identification Number (EIN)
              Parent Organization TIN
              Healthcare Provider Taxonomy Code_2
    Added   : 6 Business Mailing Address fields
"""

from __future__ import annotations

from typing import Optional
from datetime import date

from normalize.fields import (
    trim,
    coerce_null,
    normalize_phone,
    normalize_date,
    digits_only,
    is_masked,
    looks_like_iso2,
    looks_like_us_zip,
)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _str_len(v: Optional[str]) -> int:
    return len(v) if v is not None else 0


def _flag_length(value: Optional[str], max_len: int, field: str, warnings: list) -> None:
    if value is not None and len(value) > max_len:
        warnings.append(
            f"'{field}' exceeds max length {max_len} (actual {len(value)})"
        )


def _apply_phone(raw: Optional[str], field: str, row: dict, warnings: list) -> None:
    """Normalize phone/fax in-place on row and collect warnings."""
    cleaned, warn = normalize_phone(raw)
    row[field] = cleaned
    if warn:
        warnings.append(warn)


def _apply_date(
    raw: Optional[str],
    field: str,
    row: dict,
    errors: list,
    warnings: list,
    hard_fail: bool = False,
) -> Optional[str]:
    """
    Parse a date field, store ISO string in row, collect error/warning.
    Returns the iso string (or None) for cross-date comparisons.
    """
    iso, err = normalize_date(raw, field)
    row[field] = iso
    if err:
        if hard_fail:
            errors.append(err)
        else:
            warnings.append(err)
    return iso


def _iso_to_date(iso: Optional[str]) -> Optional[date]:
    """Convert YYYY-MM-DD string to date object for comparisons."""
    if iso is None:
        return None
    try:
        return date.fromisoformat(iso)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def validate_row(row: dict) -> tuple[dict, list[str], list[str]]:
    """
    Apply global trim (already done by normalizer) then all field-specific
    rules.  Returns (normalised_row, errors, warnings).
    """
    errors: list[str] = []
    warnings: list[str] = []

    r = dict(row)

    def t(f: str) -> Optional[str]:
        """Trim + null-coerce a field from the working row."""
        return trim(r.get(f))

    # ------------------------------------------------------------------
    # 1. NPI  — hard fail if invalid
    # ------------------------------------------------------------------
    F = "NPI"
    npi = t(F)
    if not npi:
        errors.append("NPI is missing")
    else:
        npi_digits = digits_only(npi)
        if npi_digits is None or len(npi_digits) != 10:
            errors.append(f"NPI '{npi}' is not exactly 10 digits")
    r[F] = npi

    # ------------------------------------------------------------------
    # 2. Entity Type Code  — hard fail if not "1" or "2"
    # ------------------------------------------------------------------
    F = "Entity Type Code"
    etc = t(F)
    if etc not in {"1", "2"}:
        errors.append(
            f"Entity Type Code '{etc}' is invalid (must be '1' or '2')"
        )
    r[F] = etc
    entity_type = etc

    # ------------------------------------------------------------------
    # 3. Provider Organization Name (Legal Business Name)
    # ------------------------------------------------------------------
    F = "Provider Organization Name (Legal Business Name)"
    v = t(F)
    r[F] = v
    _flag_length(v, 70, F, warnings)

    # ------------------------------------------------------------------
    # 4. Provider Last Name (Legal Name)
    # ------------------------------------------------------------------
    F = "Provider Last Name (Legal Name)"
    v = t(F)
    r[F] = v
    _flag_length(v, 35, F, warnings)
    if entity_type == "1" and v is None:
        warnings.append(f"Entity Type 1: '{F}' is missing")

    # ------------------------------------------------------------------
    # 5. Provider First Name
    # ------------------------------------------------------------------
    F = "Provider First Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 20, F, warnings)
    if entity_type == "1" and v is None:
        warnings.append(f"Entity Type 1: '{F}' is missing")

    # ------------------------------------------------------------------
    # 6. Provider Middle Name
    # ------------------------------------------------------------------
    F = "Provider Middle Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 20, F, warnings)

    # ------------------------------------------------------------------
    # 7. Provider Name Prefix Text
    # ------------------------------------------------------------------
    F = "Provider Name Prefix Text"
    v = t(F)
    r[F] = v
    _flag_length(v, 5, F, warnings)

    # ------------------------------------------------------------------
    # 8. Provider Name Suffix Text
    # ------------------------------------------------------------------
    F = "Provider Name Suffix Text"
    v = t(F)
    r[F] = v
    _flag_length(v, 5, F, warnings)

    # ------------------------------------------------------------------
    # 9. Provider Credential Text
    # ------------------------------------------------------------------
    F = "Provider Credential Text"
    v = t(F)
    r[F] = v
    _flag_length(v, 20, F, warnings)

    # ------------------------------------------------------------------
    # 10. Provider Other Organization Name
    # ------------------------------------------------------------------
    F = "Provider Other Organization Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 70, F, warnings)

    # ------------------------------------------------------------------
    # 11. Provider Other Organization Name Type Code
    # ------------------------------------------------------------------
    F = "Provider Other Organization Name Type Code"
    v = t(F)
    r[F] = v
    _flag_length(v, 1, F, warnings)

    # ------------------------------------------------------------------
    # 12. Provider Other Last Name
    # ------------------------------------------------------------------
    F = "Provider Other Last Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 35, F, warnings)

    # ------------------------------------------------------------------
    # 13. Provider Other First Name
    # ------------------------------------------------------------------
    F = "Provider Other First Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 20, F, warnings)

    # ------------------------------------------------------------------
    # 14. Provider Other Middle Name
    # ------------------------------------------------------------------
    F = "Provider Other Middle Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 20, F, warnings)

    # ------------------------------------------------------------------
    # 15. Provider Other Name Prefix Text
    # ------------------------------------------------------------------
    F = "Provider Other Name Prefix Text"
    v = t(F)
    r[F] = v
    _flag_length(v, 5, F, warnings)

    # ------------------------------------------------------------------
    # 16. Provider Other Name Suffix Text
    # ------------------------------------------------------------------
    F = "Provider Other Name Suffix Text"
    v = t(F)
    r[F] = v
    _flag_length(v, 5, F, warnings)

    # ==================================================================
    # MAILING ADDRESS FIELDS (new)
    # ==================================================================

    # ------------------------------------------------------------------
    # 17. Provider First Line Business Mailing Address
    # ------------------------------------------------------------------
    F = "Provider First Line Business Mailing Address"
    v = t(F)
    r[F] = v
    _flag_length(v, 55, F, warnings)

    # ------------------------------------------------------------------
    # 18. Provider Second Line Business Mailing Address
    # ------------------------------------------------------------------
    F = "Provider Second Line Business Mailing Address"
    v = t(F)
    r[F] = v
    _flag_length(v, 55, F, warnings)

    # ------------------------------------------------------------------
    # 19. Provider Business Mailing Address City Name
    # ------------------------------------------------------------------
    F = "Provider Business Mailing Address City Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 40, F, warnings)

    # ------------------------------------------------------------------
    # 20. Provider Business Mailing Address State Name
    # ------------------------------------------------------------------
    F = "Provider Business Mailing Address State Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 40, F, warnings)

    # ------------------------------------------------------------------
    # 21. Provider Business Mailing Address Postal Code
    # ------------------------------------------------------------------
    F = "Provider Business Mailing Address Postal Code"
    v = t(F)
    if v is not None:
        v = v.replace(" ", "")
    r[F] = v
    _flag_length(v, 20, F, warnings)
    if v is not None and not looks_like_us_zip(v):
        warnings.append(
            f"'{F}' value '{v}' does not match US ZIP (12345 or 12345-6789)"
        )

    # ------------------------------------------------------------------
    # 22. Provider Business Mailing Address Country Code (If outside U.S.)
    # ------------------------------------------------------------------
    F = "Provider Business Mailing Address Country Code (If outside U.S.)"
    v = t(F)
    if v is not None:
        v = v.upper()
    r[F] = v
    if v is not None:
        _flag_length(v, 2, F, warnings)
        if not looks_like_iso2(v):
            warnings.append(f"'{F}' value '{v}' does not look like ISO-2 country code")

    # ------------------------------------------------------------------
    # 23. Provider Business Mailing Address Telephone Number
    # ------------------------------------------------------------------
    F = "Provider Business Mailing Address Telephone Number"
    _apply_phone(t(F), F, r, warnings)

    # ------------------------------------------------------------------
    # 24. Provider Business Mailing Address Fax Number
    # ------------------------------------------------------------------
    F = "Provider Business Mailing Address Fax Number"
    _apply_phone(t(F), F, r, warnings)

    # ==================================================================
    # PRACTICE LOCATION ADDRESS FIELDS
    # ==================================================================

    # ------------------------------------------------------------------
    # 25. Provider First Line Business Practice Location Address
    # ------------------------------------------------------------------
    F = "Provider First Line Business Practice Location Address"
    v = t(F)
    r[F] = v
    _flag_length(v, 55, F, warnings)

    # ------------------------------------------------------------------
    # 26. Provider Second Line Business Practice Location Address
    # ------------------------------------------------------------------
    F = "Provider Second Line Business Practice Location Address"
    v = t(F)
    r[F] = v
    _flag_length(v, 55, F, warnings)

    # ------------------------------------------------------------------
    # 27. Provider Business Practice Location Address City Name
    # ------------------------------------------------------------------
    F = "Provider Business Practice Location Address City Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 40, F, warnings)

    # ------------------------------------------------------------------
    # 28. Provider Business Practice Location Address State Name
    # ------------------------------------------------------------------
    F = "Provider Business Practice Location Address State Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 40, F, warnings)

    # ------------------------------------------------------------------
    # 29. Provider Business Practice Location Address Postal Code
    # ------------------------------------------------------------------
    F = "Provider Business Practice Location Address Postal Code"
    v = t(F)
    if v is not None:
        v = v.replace(" ", "")
    r[F] = v
    _flag_length(v, 20, F, warnings)
    if v is not None and not looks_like_us_zip(v):
        warnings.append(
            f"'{F}' value '{v}' does not match US ZIP (12345 or 12345-6789)"
        )

    # ------------------------------------------------------------------
    # 30. Provider Business Practice Location Address Country Code
    # ------------------------------------------------------------------
    F = "Provider Business Practice Location Address Country Code (If outside U.S.)"
    v = t(F)
    if v is not None:
        v = v.upper()
    r[F] = v
    if v is not None:
        _flag_length(v, 2, F, warnings)
        if not looks_like_iso2(v):
            warnings.append(f"'{F}' value '{v}' does not look like ISO-2 country code")

    # ------------------------------------------------------------------
    # 31. Provider Business Practice Location Address Telephone Number
    # ------------------------------------------------------------------
    F = "Provider Business Practice Location Address Telephone Number"
    _apply_phone(t(F), F, r, warnings)

    # ------------------------------------------------------------------
    # 32. Provider Business Practice Location Address Fax Number
    # ------------------------------------------------------------------
    F = "Provider Business Practice Location Address Fax Number"
    _apply_phone(t(F), F, r, warnings)

    # ------------------------------------------------------------------
    # 33. Provider Enumeration Date
    # ------------------------------------------------------------------
    F = "Provider Enumeration Date"
    enum_iso = _apply_date(t(F), F, r, errors, warnings)

    # ------------------------------------------------------------------
    # 34. Last Update Date
    # ------------------------------------------------------------------
    F = "Last Update Date"
    update_iso = _apply_date(t(F), F, r, errors, warnings)
    if enum_iso and update_iso:
        if _iso_to_date(update_iso) < _iso_to_date(enum_iso):
            warnings.append(
                f"'Last Update Date' ({update_iso}) is before "
                f"'Provider Enumeration Date' ({enum_iso})"
            )

    # ------------------------------------------------------------------
    # 35. NPI Deactivation Reason Code
    # ------------------------------------------------------------------
    F = "NPI Deactivation Reason Code"
    v = t(F)
    if v is not None:
        v = v.upper()
    r[F] = v
    _flag_length(v, 2, F, warnings)
    deact_reason = v

    # ------------------------------------------------------------------
    # 36. NPI Deactivation Date
    # ------------------------------------------------------------------
    F = "NPI Deactivation Date"
    deact_iso = _apply_date(t(F), F, r, errors, warnings)
    if deact_iso and enum_iso:
        if _iso_to_date(deact_iso) < _iso_to_date(enum_iso):
            warnings.append(
                f"'NPI Deactivation Date' ({deact_iso}) is before "
                f"'Provider Enumeration Date' ({enum_iso})"
            )
    if deact_iso and not deact_reason:
        warnings.append(
            "NPI Deactivation Date is set but NPI Deactivation Reason Code is missing"
        )

    # ------------------------------------------------------------------
    # 37. NPI Reactivation Date
    # ------------------------------------------------------------------
    F = "NPI Reactivation Date"
    react_iso = _apply_date(t(F), F, r, errors, warnings)
    if react_iso and deact_iso:
        if _iso_to_date(react_iso) < _iso_to_date(deact_iso):
            warnings.append(
                f"'NPI Reactivation Date' ({react_iso}) is before "
                f"'NPI Deactivation Date' ({deact_iso})"
            )

    # ------------------------------------------------------------------
    # 38. Provider Sex Code
    # ------------------------------------------------------------------
    F = "Provider Sex Code"
    v = t(F)
    if v is not None:
        v = v.upper()
    r[F] = v
    if v is not None and v not in {"M", "F", "U"}:
        warnings.append(f"'{F}' has unexpected value '{v}' (expected M/F/U)")

    # ------------------------------------------------------------------
    # 39. Authorized Official Last Name
    # ------------------------------------------------------------------
    F = "Authorized Official Last Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 35, F, warnings)

    # ------------------------------------------------------------------
    # 40. Authorized Official First Name
    # ------------------------------------------------------------------
    F = "Authorized Official First Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 20, F, warnings)

    # ------------------------------------------------------------------
    # 41. Authorized Official Middle Name
    # ------------------------------------------------------------------
    F = "Authorized Official Middle Name"
    v = t(F)
    r[F] = v
    _flag_length(v, 20, F, warnings)

    # ------------------------------------------------------------------
    # 42. Authorized Official Title or Position
    # ------------------------------------------------------------------
    F = "Authorized Official Title or Position"
    v = t(F)
    r[F] = v
    _flag_length(v, 35, F, warnings)

    # ------------------------------------------------------------------
    # 43. Authorized Official Telephone Number
    # ------------------------------------------------------------------
    F = "Authorized Official Telephone Number"
    _apply_phone(t(F), F, r, warnings)

    # ------------------------------------------------------------------
    # 44. Authorized Official Name Prefix Text
    # ------------------------------------------------------------------
    F = "Authorized Official Name Prefix Text"
    v = t(F)
    r[F] = v
    _flag_length(v, 5, F, warnings)

    # ------------------------------------------------------------------
    # 45. Authorized Official Name Suffix Text
    # ------------------------------------------------------------------
    F = "Authorized Official Name Suffix Text"
    v = t(F)
    r[F] = v
    _flag_length(v, 5, F, warnings)

    # ------------------------------------------------------------------
    # 46. Authorized Official Credential Text
    # ------------------------------------------------------------------
    F = "Authorized Official Credential Text"
    v = t(F)
    r[F] = v
    _flag_length(v, 20, F, warnings)

    # ------------------------------------------------------------------
    # 47. Healthcare Provider Taxonomy Code_1
    # ------------------------------------------------------------------
    F = "Healthcare Provider Taxonomy Code_1"
    v = t(F)
    if v is not None:
        v = v.upper()
    r[F] = v
    _flag_length(v, 10, F, warnings)

    # ------------------------------------------------------------------
    # 48. Provider License Number_1
    # ------------------------------------------------------------------
    F = "Provider License Number_1"
    v = t(F)
    r[F] = v
    _flag_length(v, 20, F, warnings)
    license_1 = v

    # ------------------------------------------------------------------
    # 49. Provider License Number State Code_1
    # ------------------------------------------------------------------
    F = "Provider License Number State Code_1"
    v = t(F)
    if v is not None:
        v = v.upper()
    r[F] = v
    _flag_length(v, 2, F, warnings)
    if license_1 and not v:
        warnings.append(
            f"'Provider License Number_1' is set but '{F}' is missing"
        )

    # ------------------------------------------------------------------
    # 50. Healthcare Provider Primary Taxonomy Switch_1
    # ------------------------------------------------------------------
    F = "Healthcare Provider Primary Taxonomy Switch_1"
    v = t(F)
    if v is not None:
        v = v.upper()
    r[F] = v
    _flag_length(v, 1, F, warnings)
    if v is not None and v not in {"Y", "N"}:
        warnings.append(f"'{F}' has unexpected value '{v}' (expected Y/N)")

    # ------------------------------------------------------------------
    # 51. Healthcare Provider Taxonomy Code_2
    # ------------------------------------------------------------------
    F = "Healthcare Provider Taxonomy Code_2"
    v = t(F)
    if v is not None:
        v = v.upper()
    r[F] = v
    _flag_length(v, 10, F, warnings)

    # ------------------------------------------------------------------
    # 52. Provider License Number_2
    # ------------------------------------------------------------------
    F = "Provider License Number_2"
    v = t(F)
    r[F] = v
    _flag_length(v, 20, F, warnings)

    # ------------------------------------------------------------------
    # 52. Is Sole Proprietor
    # ------------------------------------------------------------------
    F = "Is Sole Proprietor"
    v = t(F)
    if v is not None:
        v = v.upper()
    r[F] = v
    if v is not None and v not in {"Y", "N"}:
        warnings.append(f"'{F}' has unexpected value '{v}' (expected Y/N)")

    # ------------------------------------------------------------------
    # 53. Is Organization Subpart
    # ------------------------------------------------------------------
    F = "Is Organization Subpart"
    v = t(F)
    if v is not None:
        v = v.upper()
    r[F] = v
    is_subpart = v
    if v is not None and v not in {"Y", "N"}:
        warnings.append(f"'{F}' has unexpected value '{v}' (expected Y/N)")

    # ------------------------------------------------------------------
    # 54. Parent Organization LBN
    # ------------------------------------------------------------------
    F = "Parent Organization LBN"
    v = t(F)
    r[F] = v
    _flag_length(v, 70, F, warnings)
    if is_subpart == "Y" and not v:
        warnings.append(
            f"'Is Organization Subpart' is Y but '{F}' is missing"
        )

    # ------------------------------------------------------------------
    # 55. Certification Date
    # ------------------------------------------------------------------
    F = "Certification Date"
    cert_iso = _apply_date(t(F), F, r, errors, warnings)
    if cert_iso:
        from datetime import date as _date
        try:
            if _date.fromisoformat(cert_iso) > _date.today():
                warnings.append(
                    f"'Certification Date' ({cert_iso}) is in the future"
                )
        except ValueError:
            pass

    return r, errors, warnings
