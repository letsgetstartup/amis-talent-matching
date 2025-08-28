import io, csv
from talentdb.scripts.header_mapping import canon_header, CandidateHeaderPolicy

def test_basic_hebrew_aliases():
    headers = ["שם מועמד","שם ישוב","טלפון","מייל","השכלה","ניסיון"]
    pol = CandidateHeaderPolicy.from_headers(headers)
    inv = pol.canonical_index()
    assert inv["full_name"] == "שם מועמד"
    assert inv["city"] in {"שם ישוב","שם יישוב"} or inv["city"] == "שם ישוב"
    # Canon basics
    assert canon_header("מייל", kind="candidate") == "email"
    assert canon_header("טלפון", kind="candidate") == "phone"


def test_notes_header_canonicalization_en_he():
    # English variants
    assert canon_header("Notes", kind="candidate") == "notes"
    assert canon_header("Notes_candidate", kind="candidate") == "notes"
    # Hebrew
    assert canon_header("הערות", kind="candidate") == "notes"
