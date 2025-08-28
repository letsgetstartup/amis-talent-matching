"""Shared header mapping and validation utilities for CSV imports.

Focus:
- Deterministic, Hebrew-friendly column canonicalization.
- Authoritative headers for candidate identity fields that must not be overridden by LLM.

Usage:
    from scripts.header_mapping import canon_header, CandidateHeaderPolicy
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Dict, Iterable, Optional


# Canonical keys used across the app
CANDIDATE_KEYS = {
    "external_candidate_id",
    "external_order_id",
    "full_name",
    "city",
    "phone",
    "email",
    "education",
    "experience",
    "notes",
    "required_profession",
    "field_of_occupation",
}

JOB_KEYS = {
    "order_id",
    "title",
    "description",
    "requirements1",
    "requirements2",
    "salary",
    "client",
    "employment_type",
    "status",
    "open_date",
    "branch",
    "work_location",
    "required_profession",
    "field_of_occupation",
    # Extended keys for Score Agents CSV format
    "job_applications_count",
    "recruiter_name",
    "source_created_at",
}


# Authoritative headers for candidate identity
# These must be present and used as-is to populate identity fields.
AUTHORITATIVE_CANDIDATE_HEADERS = {
    "full_name": ["שם מועמד"],
    "city": ["שם ישוב", "שם יישוב"],
}


# Alias registry (case-sensitive where relevant, Hebrew exact matches preferred)
ALIAS_MAP: Dict[str, str] = {
    # Candidate: IDs
    "מספר מועמד": "external_candidate_id",
    "מספר הזמנה": "external_order_id",

    # Candidate: identity (note: authoritative handled separately but included for completeness)
    "שם מועמד": "full_name",
    "שם מלא": "full_name",
    "מועמד": "full_name",

    "שם ישוב": "city",
    "שם יישוב": "city",
    "עיר": "city",
    "מגורים": "city",

    # Candidate: contact
    "טלפון": "phone",
    "נייד": "phone",
    "מספר נייד": "phone",
    "מייל": "email",
    "אימייל": "email",
    'דוא"ל': "email",

    # Candidate: content
    "השכלה": "education",
    "נסיון": "experience",
    "ניסיון": "experience",

    # Candidate: notes/free text
    "הערות": "notes",
    "Notes": "notes",
    "Notes_candidate": "notes",

    # ESCO-related (can appear in either jobs or candidates context)
    "מקצוע נדרש": "required_profession",
    "תחום עיסוק": "field_of_occupation",

    # Jobs
    "מספר הזמנה (הזמנת שירות)": "order_id",
    "מספר הזמנה (הזמנה)": "order_id",
    "מספר הזמנה": "order_id",
    # Some exports use "מספר משרה" to denote the order/job number
    "מספר משרה": "order_id",
    "שם משרה": "title",
    "תאור תפקיד": "description",
    "דרישות תפקיד": "requirements1",
    "דרישות התפקיד": "requirements2",
    "טווח שכר מוצע": "salary",
    "לקוח": "client",
    "סוג העסקה": "employment_type",
    "מצב": "status",
    "תאריך פתיחה": "open_date",
    "סניף": "branch",
    "מקום עבודה": "work_location",
    
        # --- English job CSV support (for Score Agents / external exports) ---
        # Common columns seen in English CSVs should map to our canonical job keys
        # so downstream import -> text -> ingest flow keeps working without changes.
        "external_job_id": "order_id",          # use as stable file/id key
        "job_id": "order_id",                  # alternate naming
        "job_description": "description",
        "description": "description",          # tolerate generic name
        "requirements": "requirements1",       # single requirements column
        "profession": "required_profession",   # requested: include profession in job schema
        "occupation_field": "field_of_occupation",
        "city": "work_location",               # source city -> Location line
    # Additional columns observed in Score Agents export
    "job_applications": "job_applications_count",
    "recruiter_name": "recruiter_name",
    "creation date": "source_created_at",
}


def _fuzzy_job_header(h: str) -> Optional[str]:
    """Fuzzy logic specific to job columns.
    Mirrors import_jobs_csv.py leniency: any header containing these phrases.
    """
    if "מקצוע" in h:
        return "required_profession"
    if "תחום עיסוק" in h:
        return "field_of_occupation"
    return None


def canon_header(header: str, kind: Optional[str] = None) -> str:
    """Return canonical key for a given header text.

    kind: 'candidate' | 'job' | None (applies general aliases first, then kind-specific fuzziness)
    """
    h = (header or "").strip()
    if not h:
        return h
    # Exact alias first
    if h in ALIAS_MAP:
        return ALIAS_MAP[h]
    # Fuzzy job handling
    if kind == "job":
        fuzzy = _fuzzy_job_header(h)
        if fuzzy:
            return fuzzy
    # Generic fallbacks
    # Email variations
    if re.search(r"מייל|אימייל|דוא\"?ל", h, re.I):
        return "email"
    # Phone variations
    if re.search(r"טלפון|נייד", h, re.I):
        return "phone"
    # Experience
    if re.search(r"נסיון|ניסיון", h):
        return "experience"
    # Education
    if "השכלה" in h:
        return "education"
    # Notes (English/Hebrew)
    if re.search(r"notes?_candidate|^notes$|הערות", h, re.I):
        return "notes"
    return h


@dataclass
class CandidateHeaderPolicy:
    """Validates and resolves candidate CSV headers with an emphasis on authoritative identity fields."""
    field_map: Dict[str, str]  # original header -> canonical key

    @classmethod
    def from_headers(cls, headers: Iterable[str]) -> "CandidateHeaderPolicy":
        fmap = {h: canon_header(h, kind="candidate") for h in headers}
        return cls(field_map=fmap)

    def authoritative_sources(self) -> Dict[str, Optional[str]]:
        """Return the exact original header names to use for authoritative fields (full_name, city).
        If missing, value is None.
        """
        chosen: Dict[str, Optional[str]] = {"full_name": None, "city": None}
        for canon_key, variations in AUTHORITATIVE_CANDIDATE_HEADERS.items():
            for v in variations:
                if v in self.field_map:
                    chosen[canon_key] = v
                    break
        return chosen

    def require_authoritative(self) -> Dict[str, bool]:
        src = self.authoritative_sources()
        return {k: (src.get(k) is not None) for k in ("full_name", "city")}

    def canonical_index(self) -> Dict[str, str]:
        """Invert to canonical->original chosen header (first match)."""
        inv: Dict[str, str] = {}
        for orig, canon in self.field_map.items():
            # keep the first occurrence only
            inv.setdefault(canon, orig)
        return inv
